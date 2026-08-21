/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { assert, contains } from '@firebase/util';

import { ChildrenNode } from './snap/ChildrenNode';
import { buildChildSet } from './snap/childSet';
import { NAME_COMPARATOR, NAME_ONLY_COMPARATOR } from './snap/comparators';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { IndexMap } from './snap/IndexMap';
import { LeafNode } from './snap/LeafNode';
import { NamedNode, Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { Indexable } from './util/misc';
import { SortedMap } from './util/SortedMap';
import { yieldMacrotask } from './util/yieldMacrotask';

/**
 * Per-key work units charged per main-thread slice of a sliced server-push
 * decode (one charge per JSON key visited, at every depth, and one per node
 * compared by the graft's budgeted equality). Sized like the peek walk's
 * budget (_PEEK_MATERIALIZE_SLICE_VISITS): one slice stays well inside a
 * frame budget on mobile hardware while keeping total slice count (and its
 * scheduling overhead) low on large payloads.
 * @internal
 */
export const _INGEST_DECODE_SLICE_VISITS = 4000;

/**
 * Thrown out of a sliced decode when the caller's continuation check fails
 * after a yield — the listen this decode serves was torn down (stop, account
 * switch, dispose, or a successor ingest) and nothing may be applied from it.
 * @internal
 */
export class IngestCancelledError extends Error {
  constructor() {
    super('sliced ingest cancelled');
  }
}

/** Shared slice state for one decode: budget counter + liveness check. */
export interface DecodeSliceState {
  visits: number;
  isCurrent: () => boolean;
}

/**
 * Charges one work unit; returns a promise EXACTLY when the budget exhausts
 * (yield + liveness re-check), null otherwise. The null fast path allocates
 * nothing — with one charge per JSON key, an unconditional await here would
 * put a microtask on every key of a multi-MB payload, recreating a large
 * fraction of the overhead this decoder exists to remove.
 */
function charge(state: DecodeSliceState): Promise<void> | null {
  if (++state.visits < _INGEST_DECODE_SLICE_VISITS) {
    return null;
  }
  state.visits = 0;
  return yieldMacrotask().then(() => {
    if (!state.isCurrent()) {
      throw new IngestCancelledError();
    }
  });
}

/**
 * Public charge for ingest bodies that do per-unit work OUTSIDE the decoder
 * (e.g. folding one decoded range merge over a base tree): shares the same
 * slice budget and yield/liveness contract as the decode itself. @internal
 */
export function chargeSlice(state: DecodeSliceState): Promise<void> | null {
  return charge(state);
}

/**
 * Budgeted replica of {@link nodeFromJSON}: the same Node for the same JSON —
 * identical priority handling, '.value' unwrapping, '.sv' leaf semantics,
 * metadata-key skipping, empty-child pruning, and childSet construction —
 * but every JSON key visited charges one unit of the shared slice budget,
 * and the walk yields a macrotask when the budget exhausts so a large
 * server push can never decode as one monolithic main-thread task.
 *
 * Key enumeration is prototype-safe ({@link contains}) exactly like
 * nodeFromJSON's each(): "hasOwnProperty" (or any Object.prototype name) is
 * a legal child key, and JSON.parse makes it an own string property — a
 * direct method call through the object would invoke user data and throw.
 *
 * Primitive children decode synchronously through nodeFromJSON itself (a
 * single bounded leaf) — a promise per leaf would dominate allocation on
 * exactly the wide flat collections this bounds (the peek walk's inline-leaf
 * precedent). Fidelity is enforced by test corpus equality (node.equals +
 * hash) against nodeFromJSON; when editing either function, keep them in
 * lockstep.
 * @internal
 */
export async function decodeNodeSliced(
  json: unknown | null,
  state: DecodeSliceState,
  priority: unknown = null
): Promise<Node> {
  if (json === null) {
    return ChildrenNode.EMPTY_NODE;
  }

  if (typeof json === 'object' && '.priority' in json) {
    priority = (json as Record<string, unknown>)['.priority'];
  }

  assert(
    priority === null ||
      typeof priority === 'string' ||
      typeof priority === 'number' ||
      (typeof priority === 'object' && '.sv' in (priority as object)),
    'Invalid priority type found: ' + typeof priority
  );

  if (
    typeof json === 'object' &&
    '.value' in json &&
    (json as Record<string, unknown>)['.value'] !== null
  ) {
    json = (json as Record<string, unknown>)['.value'];
  }

  // Valid leaf nodes include non-objects or server-value wrapper objects
  if (typeof json !== 'object' || '.sv' in (json as object)) {
    const y = charge(state);
    if (y !== null) {
      await y;
    }
    const jsonLeaf = json as string | number | boolean | Indexable;
    return new LeafNode(jsonLeaf, nodeFromJSON(priority));
  }

  if (!(json instanceof Array)) {
    const children: NamedNode[] = [];
    let childrenHavePriority = false;
    const obj = json as Record<string, unknown>;
    for (const key in obj) {
      if (contains(obj, key) && key.substring(0, 1) !== '.') {
        // Ignore metadata nodes
        const y = charge(state);
        if (y !== null) {
          await y;
        }
        const raw = obj[key];
        const childNode =
          typeof raw !== 'object' || raw === null
            ? nodeFromJSON(raw) // primitive leaf / null — bounded, synchronous
            : await decodeNodeSliced(raw, state);
        if (!childNode.isEmpty()) {
          childrenHavePriority =
            childrenHavePriority || !childNode.getPriority().isEmpty();
          children.push(new NamedNode(key, childNode));
        }
      }
    }
    return assembleChildrenNode(children, childrenHavePriority, priority);
  } else {
    let node: Node = ChildrenNode.EMPTY_NODE;
    const arr = json as unknown as Record<string, unknown>;
    for (const key in arr) {
      if (contains(arr, key) && key.substring(0, 1) !== '.') {
        // ignore metadata nodes.
        const y = charge(state);
        if (y !== null) {
          await y;
        }
        const raw = arr[key];
        const childNode =
          typeof raw !== 'object' || raw === null
            ? nodeFromJSON(raw)
            : await decodeNodeSliced(raw, state);
        if (childNode.isLeafNode() || !childNode.isEmpty()) {
          node = node.updateImmediateChild(key, childNode);
        }
      }
    }
    return node.updatePriority(nodeFromJSON(priority));
  }
}

/**
 * The childSet-assembly tail of nodeFromJSON's object branch, shared by the
 * sliced decoder's interior nodes and by the full-root assembly in
 * decodeFullRootSliced.
 * @internal
 */
export function assembleChildrenNode(
  children: NamedNode[],
  childrenHavePriority: boolean,
  priority: unknown
): Node {
  if (children.length === 0) {
    return ChildrenNode.EMPTY_NODE;
  }
  const childSet = buildChildSet(
    children,
    NAME_ONLY_COMPARATOR,
    namedNode => namedNode.name,
    NAME_COMPARATOR
  ) as SortedMap<string, Node>;
  if (childrenHavePriority) {
    const sortedChildSet = buildChildSet(children, PRIORITY_INDEX.getCompare());
    return new ChildrenNode(
      childSet,
      nodeFromJSON(priority),
      new IndexMap(
        { '.priority': sortedChildSet },
        { '.priority': PRIORITY_INDEX }
      )
    );
  } else {
    return new ChildrenNode(childSet, nodeFromJSON(priority), IndexMap.Default);
  }
}

/**
 * Budgeted structural equality: Node.equals with every compared node
 * charging the shared slice budget, so grafting a large unchanged subtree
 * cannot itself become the monolithic walk the decoder exists to remove.
 * Same comparison semantics as ChildrenNode/LeafNode.equals (priority,
 * child count, PRIORITY_INDEX-iterated pairwise children).
 */
async function nodesEqualSliced(
  a: Node,
  b: Node,
  state: DecodeSliceState
): Promise<boolean> {
  if (a === b) {
    return true;
  }
  const y = charge(state);
  if (y !== null) {
    await y;
  }
  if (a.isLeafNode() || b.isLeafNode()) {
    // Leaf equality is bounded — delegate to the node's own equals.
    return a.equals(b);
  }
  const aChildren = a as ChildrenNode;
  const bChildren = b as ChildrenNode;
  if (!aChildren.getPriority().equals(bChildren.getPriority())) {
    return false;
  }
  if (aChildren.numChildren() !== bChildren.numChildren()) {
    return false;
  }
  const aIter = aChildren.getIterator(PRIORITY_INDEX);
  const bIter = bChildren.getIterator(PRIORITY_INDEX);
  let aCurrent = aIter.getNext();
  let bCurrent = bIter.getNext();
  while (aCurrent !== null && bCurrent !== null) {
    if (aCurrent.name !== bCurrent.name) {
      return false;
    }
    if (!(await nodesEqualSliced(aCurrent.node, bCurrent.node, state))) {
      return false;
    }
    aCurrent = aIter.getNext();
    bCurrent = bIter.getNext();
  }
  return aCurrent === null && bCurrent === null;
}

/**
 * Decodes one full-root plain-children push into a single Node, sliced, with
 * IDENTITY GRAFTING against the live base: each decoded top-level child that
 * is structurally equal to the base's same-named child is replaced by the
 * base's OBJECT (graft by identity), so the eventual single SyncTree
 * overwrite diffs the two roots with === short-circuits on every unchanged
 * child — one atomic apply whose cost tracks the CHANGED portion, never the
 * whole tree. The equality probe itself is budgeted (nodesEqualSliced), and
 * unequal children cost one comparison walk only where they diverge.
 *
 * `base` null (uninitialized/leaf/prioritized cache) skips grafting — the
 * apply then diffs against empty/being-replaced state, which is trivial or
 * bounded by the view processor itself.
 *
 * Only called for payloads {@link sliceableAsChildren} accepted, so
 * priority/leaf/array roots never reach it: the assembled root's priority is
 * null by construction.
 * @internal
 */
export async function decodeFullRootSliced(
  json: Record<string, unknown>,
  base: Node | null,
  isCurrent: () => boolean
): Promise<Node> {
  const state: DecodeSliceState = { visits: 0, isCurrent };
  const children: NamedNode[] = [];
  let childrenHavePriority = false;
  for (const key in json) {
    if (contains(json, key) && key.substring(0, 1) !== '.') {
      const y = charge(state);
      if (y !== null) {
        await y;
      }
      const raw = json[key];
      let childNode =
        typeof raw !== 'object' || raw === null
          ? nodeFromJSON(raw)
          : await decodeNodeSliced(raw, state);
      if (childNode.isEmpty()) {
        continue;
      }
      if (base !== null) {
        const baseChild = base.getImmediateChild(key);
        if (
          !baseChild.isEmpty() &&
          (await nodesEqualSliced(baseChild, childNode, state))
        ) {
          // Equal content: graft the live child by identity so the apply's
          // diff (and every downstream memoized consumer) sees ===.
          childNode = baseChild;
        }
      }
      childrenHavePriority =
        childrenHavePriority || !childNode.getPriority().isEmpty();
      children.push(new NamedNode(key, childNode));
    }
  }
  return assembleChildrenNode(children, childrenHavePriority, null);
}

/**
 * Whether a server push body is shaped for the sliced children ingest: a
 * plain JSON object of children — no leaf value, no '.value'/'.sv' wrapper,
 * no root '.priority', not an array. Everything else takes the ordinary
 * synchronous path; those shapes are either bounded (leaves) or vanishingly
 * rare at a persistent root (arrays, prioritized roots).
 * @internal
 */
export function sliceableAsChildren(
  data: unknown
): data is Record<string, unknown> {
  return (
    typeof data === 'object' &&
    data !== null &&
    !(data instanceof Array) &&
    !('.value' in data) &&
    !('.priority' in data) &&
    !('.sv' in data)
  );
}

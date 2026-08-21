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
 * Budgeted replica of {@link nodeFromJSON}: the same Node for the same JSON —
 * identical priority handling, '.value' unwrapping, '.sv' leaf semantics,
 * metadata-key skipping, empty-child pruning, and childSet construction —
 * driven by a SYNCHRONOUS explicit-stack walk that awaits only when the
 * shared slice budget trips (one charge per JSON key at every depth). The
 * earlier async-recursive form allocated a promise chain per interior node;
 * on a multi-MB payload that is hundreds of thousands of microtasks —
 * observed in Safari field traces as a 70k-microtask storm saturating the
 * main thread. The explicit stack keeps the hot path 100% synchronous
 * between budget boundaries.
 *
 * Key enumeration is prototype-safe ({@link contains}) exactly like
 * nodeFromJSON's each(): "hasOwnProperty" (or any Object.prototype name) is
 * a legal child key, and JSON.parse makes it an own string property — a
 * direct method call through the object would invoke user data and throw.
 *
 * Fidelity is enforced by test corpus equality (node.equals + hash)
 * against nodeFromJSON; when editing either function, keep them in
 * lockstep.
 * @internal
 */
export async function decodeNodeSliced(
  json: unknown | null,
  state: DecodeSliceState,
  priority: unknown = null
): Promise<Node> {
  interface DecodeFrame {
    /** Own-key list of the object being decoded, iterated by index. */
    keys: string[];
    index: number;
    obj: Record<string, unknown>;
    isArray: boolean;
    priority: unknown;
    children: NamedNode[];
    childrenHavePriority: boolean;
    arrayNode: Node;
    /** Where the finished node lands: parent frame + key, or the result. */
    parent: DecodeFrame | null;
    parentKey: string | null;
  }

  let result: Node | null = null;

  const finishFrame = (frame: DecodeFrame): void => {
    let node: Node;
    if (frame.isArray) {
      node = frame.arrayNode.updatePriority(nodeFromJSON(frame.priority));
    } else {
      node = assembleChildrenNode(
        frame.children,
        frame.childrenHavePriority,
        frame.priority
      );
    }
    if (frame.parent === null) {
      result = node;
      return;
    }
    attachChild(frame.parent, frame.parentKey!, node);
  };

  const attachChild = (
    parent: DecodeFrame,
    key: string,
    childNode: Node
  ): void => {
    if (parent.isArray) {
      if (childNode.isLeafNode() || !childNode.isEmpty()) {
        parent.arrayNode = parent.arrayNode.updateImmediateChild(
          key,
          childNode
        );
      }
    } else if (!childNode.isEmpty()) {
      parent.childrenHavePriority =
        parent.childrenHavePriority || !childNode.getPriority().isEmpty();
      parent.children.push(new NamedNode(key, childNode));
    }
  };

  /**
   * Opens a frame for `raw` (or resolves it immediately when it is a
   * bounded leaf). Returns the frame to descend into, or null.
   */
  const openValue = (
    raw: unknown,
    parent: DecodeFrame | null,
    parentKey: string | null
  ): DecodeFrame | null => {
    let value = raw;
    let valuePriority: unknown = null;
    if (value !== null && typeof value === 'object') {
      const record = value as Record<string, unknown>;
      if ('.priority' in record) {
        valuePriority = record['.priority'];
      }
      assert(
        valuePriority === null ||
          typeof valuePriority === 'string' ||
          typeof valuePriority === 'number' ||
          (typeof valuePriority === 'object' &&
            '.sv' in (valuePriority as object)),
        'Invalid priority type found: ' + typeof valuePriority
      );
      if ('.value' in record && record['.value'] !== null) {
        value = record['.value'];
      }
    }
    if (
      value === null ||
      typeof value !== 'object' ||
      '.sv' in (value as object)
    ) {
      // Bounded leaf (or explicit null): decode synchronously.
      const leaf =
        value === null
          ? ChildrenNode.EMPTY_NODE
          : new LeafNode(
              value as string | number | boolean | Indexable,
              nodeFromJSON(valuePriority)
            );
      if (parent === null) {
        result = leaf;
      } else {
        attachChild(parent, parentKey!, leaf);
      }
      return null;
    }
    const record = value as Record<string, unknown>;
    const isArray = value instanceof Array;
    const keys: string[] = [];
    for (const key in record) {
      if (contains(record, key) && key.substring(0, 1) !== '.') {
        keys.push(key);
      }
    }
    return {
      keys,
      index: 0,
      obj: record,
      isArray,
      priority: valuePriority,
      children: [],
      childrenHavePriority: false,
      arrayNode: ChildrenNode.EMPTY_NODE,
      parent,
      parentKey
    };
  };

  // Root: honor the explicitly passed priority exactly like the recursive
  // form (the root's own '.priority' key, when present, overrides it).
  const rootFrame = openValue(json, null, null);
  if (rootFrame === null) {
    // Root was a leaf/null; apply the caller's priority when the JSON did
    // not carry its own.
    if (result !== null && priority !== null && result.isLeafNode()) {
      const leaf = result as LeafNode;
      if (leaf.getPriority().isEmpty()) {
        result = new LeafNode(leaf.getValue(), nodeFromJSON(priority));
      }
    }
    return result ?? ChildrenNode.EMPTY_NODE;
  }
  if (rootFrame.priority === null) {
    rootFrame.priority = priority;
  }

  const stack: DecodeFrame[] = [rootFrame];
  while (stack.length > 0) {
    const frame = stack[stack.length - 1];
    if (frame.index >= frame.keys.length) {
      stack.pop();
      finishFrame(frame);
      continue;
    }
    const key = frame.keys[frame.index++];
    const y = charge(state);
    if (y !== null) {
      await y;
    }
    const child = openValue(frame.obj[key], frame, key);
    if (child !== null) {
      stack.push(child);
    }
  }
  return result ?? ChildrenNode.EMPTY_NODE;
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
 * Budgeted structural equality: Node.equals semantics driven by a
 * SYNCHRONOUS explicit-stack walk that awaits only when the shared slice
 * budget trips. The naive async recursion allocated a promise (plus its
 * continuation microtasks) for EVERY compared node pair — on a large
 * mostly-equal graft probe that is millions of microtasks, which saturates
 * the scheduler and spikes GC on exactly the mobile boots the slicing
 * exists to protect (observed as a Safari microtask storm in field traces).
 * Same comparison semantics as ChildrenNode/LeafNode.equals (priority,
 * child count, PRIORITY_INDEX-iterated pairwise children).
 */
async function nodesEqualSliced(
  a: Node,
  b: Node,
  state: DecodeSliceState
): Promise<boolean> {
  interface EqFrame {
    aIter: ReturnType<ChildrenNode['getIterator']>;
    bIter: ReturnType<ChildrenNode['getIterator']>;
  }
  const stack: EqFrame[] = [];

  // Compares one pair without descending; pushes a frame for children.
  // Returns false on definite inequality, true to continue.
  const compare = (x: Node, y: Node): boolean => {
    if (x === y) {
      return true;
    }
    if (x.isLeafNode() || y.isLeafNode()) {
      // Leaf equality is bounded — delegate to the node's own equals.
      return x.equals(y);
    }
    const xc = x as ChildrenNode;
    const yc = y as ChildrenNode;
    if (!xc.getPriority().equals(yc.getPriority())) {
      return false;
    }
    if (xc.numChildren() !== yc.numChildren()) {
      return false;
    }
    stack.push({
      aIter: xc.getIterator(PRIORITY_INDEX),
      bIter: yc.getIterator(PRIORITY_INDEX)
    });
    return true;
  };

  if (!compare(a, b)) {
    return false;
  }
  while (stack.length > 0) {
    const frame = stack[stack.length - 1];
    const aCurrent = frame.aIter.getNext();
    const bCurrent = frame.bIter.getNext();
    if (aCurrent === null || bCurrent === null) {
      if (aCurrent !== bCurrent) {
        return false;
      }
      stack.pop();
      continue;
    }
    if (aCurrent.name !== bCurrent.name) {
      return false;
    }
    const y = charge(state);
    if (y !== null) {
      await y;
    }
    if (!compare(aCurrent.node, bCurrent.node)) {
      return false;
    }
  }
  return true;
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

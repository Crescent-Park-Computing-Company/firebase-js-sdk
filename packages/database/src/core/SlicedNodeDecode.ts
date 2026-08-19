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

import { assert } from '@firebase/util';

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
 * decode (one charge per JSON key visited, at every depth). Sized like the
 * peek walk's budget (_PEEK_MATERIALIZE_SLICE_VISITS): one slice stays well
 * inside a frame budget on mobile hardware while keeping total slice count
 * (and its scheduling overhead) low on large payloads.
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
 * but every JSON key visited charges one unit of the shared slice budget,
 * and the walk yields a macrotask when the budget exhausts so a large
 * server push can never decode as one monolithic main-thread task.
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
      if (obj.hasOwnProperty(key) && key.substring(0, 1) !== '.') {
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
      if (arr.hasOwnProperty(key) && key.substring(0, 1) !== '.') {
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
 * sliced decoder's interior nodes and by the ingest pump's cold-path root
 * assembly (per-top-level-child decode, then one node for a single
 * overwrite).
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
 * Cost surcharge per emitted top-level child, in budget units. The decode
 * budget charges JSON keys, but the pump's per-child APPLY (SyncTree
 * overwrite + event raise + write-through accounting) is uncharged work
 * riding the same slice — many tiny children would otherwise pack hundreds
 * of applies into one task. The surcharge caps a slice at roughly
 * budget/surcharge applies (~60) so slices stay frame-sized either way.
 */
const CHILD_APPLY_SURCHARGE = 64;

/**
 * Streaming top level of a sliced full-root decode: hands each top-level
 * child of a plain-children push to `onChild` as (key, Node) without ever
 * assembling the root — the ingest pump applies changed children as
 * per-child server overwrites against the live base or collects them for a
 * cold single overwrite. Only called for payloads
 * {@link sliceableAsChildren} accepted, so priority/leaf/array roots never
 * reach it.
 * @internal
 */
export async function decodeChildrenSliced(
  json: Record<string, unknown>,
  isCurrent: () => boolean,
  onChild: (key: string, node: Node) => void
): Promise<void> {
  const state: DecodeSliceState = { visits: 0, isCurrent };
  for (const key in json) {
    if (json.hasOwnProperty(key) && key.substring(0, 1) !== '.') {
      const y = charge(state);
      if (y !== null) {
        await y;
      }
      const raw = json[key];
      const childNode =
        typeof raw !== 'object' || raw === null
          ? nodeFromJSON(raw)
          : await decodeNodeSliced(raw, state);
      if (!childNode.isEmpty()) {
        onChild(key, childNode);
        state.visits += CHILD_APPLY_SURCHARGE;
      }
    }
  }
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

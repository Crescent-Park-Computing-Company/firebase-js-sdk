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
import { NamedNode, Node } from './snap/Node';
/**
 * Per-key work units charged per main-thread slice of a sliced server-push
 * decode (one charge per JSON key visited, at every depth). Sized like the
 * peek walk's budget (_PEEK_MATERIALIZE_SLICE_VISITS): one slice stays well
 * inside a frame budget on mobile hardware while keeping total slice count
 * (and its scheduling overhead) low on large payloads.
 * @internal
 */
export declare const _INGEST_DECODE_SLICE_VISITS = 4000;
/**
 * Thrown out of a sliced decode when the caller's continuation check fails
 * after a yield — the listen this decode serves was torn down (stop, account
 * switch, dispose, or a successor ingest) and nothing may be applied from it.
 * @internal
 */
export declare class IngestCancelledError extends Error {
    constructor();
}
/** Shared slice state for one decode: budget counter + liveness check. */
export interface DecodeSliceState {
    visits: number;
    isCurrent: () => boolean;
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
export declare function decodeNodeSliced(json: unknown | null, state: DecodeSliceState, priority?: unknown): Promise<Node>;
/**
 * The childSet-assembly tail of nodeFromJSON's object branch, shared by the
 * sliced decoder's interior nodes and by the ingest pump's cold-path root
 * assembly (per-top-level-child decode, then one node for a single
 * overwrite).
 * @internal
 */
export declare function assembleChildrenNode(children: NamedNode[], childrenHavePriority: boolean, priority: unknown): Node;
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
export declare function decodeChildrenSliced(json: Record<string, unknown>, isCurrent: () => boolean, onChild: (key: string, node: Node) => void): Promise<void>;
/**
 * Whether a server push body is shaped for the sliced children ingest: a
 * plain JSON object of children — no leaf value, no '.value'/'.sv' wrapper,
 * no root '.priority', not an array. Everything else takes the ordinary
 * synchronous path; those shapes are either bounded (leaves) or vanishingly
 * rare at a persistent root (arrays, prioritized roots).
 * @internal
 */
export declare function sliceableAsChildren(data: unknown): data is Record<string, unknown>;

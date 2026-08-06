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
import { Node } from './snap/Node';
/**
 * A compound hash of a node: the node's tree serialized in key order, cut
 * into ranges at the given split posts, with each range hashed separately.
 *
 * Sent on a listen as `ch: { hs: hashes, ps: posts }`, next to the simple
 * hash `h`. When the simple hash misses, the server diffs per range and
 * pushes only the ranges whose hash differs (as range-merge messages)
 * instead of the whole tree.
 *
 * This is a port of the compound hash support in the Android and iOS SDKs
 * (Android: com.google.firebase.database.snapshot.CompoundHash); the wire
 * semantics match them exactly.
 */
export declare class CompoundHash {
    readonly posts: string[];
    readonly hashes: string[];
    /**
     * @param posts - The split positions between ranges: the path of the last
     * leaf each range serialized, slash-joined ('/' for the root). Always one
     * shorter than `hashes`.
     * @param hashes - base64(sha1(...)) of each range's serialization, ending
     * with one trailing '' for the open tail range.
     */
    constructor(posts: string[], hashes: string[]);
}
/**
 * The state a split strategy sees after each leaf is serialized.
 */
export interface CompoundHashSplitState {
    /** Length of the current range's serialization so far. */
    hashLength(): number;
    /** Path of the node currently being processed, as key segments. */
    currentPath(): string[];
}
/**
 * Decides after each serialized leaf whether to end the current range.
 */
export type CompoundHashSplitStrategy = (state: CompoundHashSplitState) => boolean;
/**
 * The default strategy: cut a range once its serialized text exceeds
 * max(512, sqrt(estimatedSize * 100)) bytes, so a tree splits into roughly
 * sqrt(size)-sized ranges. Never splits right after a `.priority` leaf: the
 * server treats a priority and the node it belongs to as one unit.
 */
export declare function simpleSizeSplitStrategy(node: Node): CompoundHashSplitStrategy;
/**
 * Computes the compound hash of a node.
 */
export declare function compoundHashFromNode(node: Node, splitStrategy?: CompoundHashSplitStrategy): CompoundHash;
/**
 * Iterates children in key order with the node's priority interleaved as a
 * `.priority` pseudo-child, matching the serialization the server hashes
 * (Android ChildrenNode.forEachChild(visitor, includePriority = true)): the
 * priority is emitted immediately before the first child key that sorts
 * after '.priority'. A priority that sorts after every child is dropped, as
 * it is on Android and iOS — both ends of the protocol must agree.
 */
export declare function forEachChildWithPriority(node: Node, action: (key: string, child: Node, includedInHash: boolean) => void, includeTrailingPriority?: boolean): void;
/**
 * Builds the protocol compound hash while serializing disjoint persistence
 * entries in traversal order. The first full cache write therefore walks each
 * Node once: the returned JSON is stored in the chunk and the same visit feeds
 * the wire hash builder.
 */
export declare class CompoundHashAccumulator {
    private readonly builder_;
    private openPath_;
    constructor(root: Node);
    serializeEntry(path: string[], node: Node, includedInHash?: boolean): unknown;
    hashEntry(path: string[], node: Node, includedInHash?: boolean): void;
    finish(): CompoundHash;
    private moveToPath_;
    private hashNode_;
    private serializeNode_;
}
/**
 * Estimates the serialized size of a node in bytes — a cheap approximation
 * that only drives the default split threshold and the persistence chunk
 * planner, never a wire value (port of Android NodeSizeEstimator).
 */
export declare function estimateSerializedNodeSize(node: Node): number;
/**
 * Computes a compound hash in bounded slices of main-thread time, yielding
 * to the event loop between slices, so hashing a large tree for persistence
 * never blocks the UI the way a monolithic walk would. Same traversal as
 * compoundHashFromNode (see CompoundHashWalker), so the result is identical.
 */
export declare function compoundHashFromNodeAsync(node: Node, splitStrategy?: CompoundHashSplitStrategy, sliceMs?: number, onProgress?: () => void): Promise<CompoundHash>;
/**
 * Computes the canonical Node hash without populating every subtree's
 * lazyHash_. Only frames on the current depth-first path are retained; each
 * child hash is folded into its parent and released. Persistence uses this at
 * write time, stores the resulting root hash in the manifest, and stamps only
 * the restored root on the next boot.
 */
export declare function canonicalHashFromNodeAsync(node: Node, sliceMs?: number, onProgress?: () => void): Promise<string>;

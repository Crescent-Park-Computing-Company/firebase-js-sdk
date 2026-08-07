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
import { LeafNode } from './snap/LeafNode';
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
export declare class CompoundHashBuilder {
    private splitStrategy_;
    posts: string[];
    hashes: string[];
    /** Serialized text length of each completed range (same order as posts). */
    sizes: number[];
    /**
     * When set, completed range texts are handed to the sink instead of being
     * hashed synchronously; `hashes` receives a placeholder the caller fills in
     * (the sink receives the index to fill). Lets the persistence flush hash
     * ranges with WebCrypto off the main thread's synchronous path.
     */
    hashSink: ((text: string, index: number) => void) | null;
    /** null when not currently inside a range. */
    private currentHash_;
    /**
     * Key stack of the node being processed. Kept beyond currentDepth_ so the
     * path of the last processed leaf survives popping back out of its parent.
     */
    private currentPath_;
    private currentDepth_;
    private lastLeafDepth_;
    private needsComma_;
    private readonly splitState_;
    constructor(splitStrategy_: CompoundHashSplitStrategy);
    processLeaf(node: LeafNode): void;
    startChild(key: string): void;
    endChild(): void;
    finishHashing(): void;
    /**
     * Seeds the builder into the exact state the natural full-tree walk has
     * immediately after ending a range at the leaf `path`: no open range, the
     * walker positioned at that leaf's depth. A subsequent walk of the leaves
     * AFTER `path` then serializes ranges byte-identically to the corresponding
     * portion of a full walk — the next range's opening parenthesis prefix is
     * reconstructed from the common path with this boundary, which is exactly
     * what ensureRange_ derives from currentPath_/currentDepth_.
     */
    seedBoundary(path: string[]): void;
    /**
     * Ends the open range at the last processed leaf regardless of the split
     * strategy — used by the stable-range rewalk to close a dirty run exactly
     * at a preserved boundary post so the following clean range's interval is
     * untouched. No-op when no range is open.
     */
    forceEndRange(): void;
    private ensureRange_;
    private endRange_;
}
/**
 * Builds the protocol compound hash while serializing disjoint persistence
 * entries in traversal order. The first full cache write therefore walks each
 * Node once: the returned JSON is stored in the chunk and the same visit feeds
 * the wire hash builder.
 */
/**
 * ============================ STABLE RANGES ============================
 *
 * A committed persistence generation stores its compound hash as a list of
 * ranges [{ post, hash, size }] whose BOUNDARIES ARE PRESERVED across
 * generations. A flush re-hashes only ranges whose leaf interval intersects
 * a changed subtree; clean ranges keep their stored hash without their bytes
 * ever being read. The wire protocol permits this: posts are arbitrary
 * client-chosen markers, and the server recomputes each interval's hash from
 * the posts alone — boundaries never expire, only balance matters.
 *
 * Balance is kept with a half/double hysteresis around the ideal size `s`
 * from simpleSizeSplitStrategy: a rewalked run re-splits naturally at ~s (so
 * a range that grew past ~2s divides), and a clean range smaller than s/2
 * adjacent to a dirty run is absorbed into the run and re-emitted merged.
 * Occasional under-sized survivors are harmless — a small range is valid,
 * merely suboptimal — so rebalancing is amortized, never a correctness step.
 */
/** One stored range: interval end marker, its hash, serialized text length. */
export interface StableRange {
    post: string;
    hash: string;
    size: number;
}
/**
 * Compares two range markers (slash-joined leaf paths) in compound-hash leaf
 * order: segment-wise nameCompare, a strict prefix sorting first. Posts are
 * ordering markers only — they need not exist as leaves in the current tree,
 * so the comparison must be total over arbitrary paths.
 */
export declare function compareRangeMarkers(a: string[], b: string[]): number;
/**
 * Walks the leaves of `node` whose paths lie in the half-open marker interval
 * (fromPost, toPost], feeding the builder exactly the startChild / endChild /
 * processLeaf sequence the natural full-tree walk produces for those leaves.
 * The builder must have been seeded at `fromPost` (seedBoundary) so the first
 * emitted range opens with the same common-ancestor prefix the full walk
 * would write. `toPost === null` walks to the end of the tree.
 *
 * Subtrees entirely outside the interval are pruned without reading them —
 * the cost is O(interval bytes + pruned fanout), not O(tree).
 */
export declare function walkLeafInterval(node: Node, fromPost: string[] | null, toPost: string[] | null, builder: CompoundHashBuilder): void;
/**
 * The identity-diff: collects the paths of maximal subtrees that differ
 * between two versions of an immutable, structurally shared tree. Unchanged
 * subtrees are recognized by object identity and never descended. A child
 * present in only one version reports that child's path. Descends at most
 * `maxDepth` levels before treating a differing subtree as wholly changed —
 * dirty mapping only needs interval bounds, not precise leaves.
 */
export declare function collectChangedSubtreePaths(before: Node, after: Node, maxDepth?: number, maxPaths?: number): string[][] | null;
/**
 * Marks the ranges whose leaf interval intersects any changed subtree. Range
 * i covers the half-open marker interval (posts[i-1], posts[i]]; the virtual
 * tail after the last post is reported via the returned `tailDirty` (leaves
 * appended after the previously last leaf fall there).
 */
export declare function markDirtyRanges(ranges: StableRange[], changedPaths: string[][]): {
    dirty: boolean[];
    tailDirty: boolean;
};
/**
 * Produces the next generation's stable ranges: clean ranges carry over
 * verbatim; each maximal dirty run (pre-extended over undersized clean
 * neighbors) is re-serialized over the current tree between its preserved
 * outer boundaries, re-splitting naturally at the current ideal size. Ranges
 * are emitted through `builder`, whose hashSink/hashes the caller owns —
 * pass a sink to hash the dirty texts with WebCrypto afterwards.
 *
 * Returns the new range list with hashes for SINK-DEFERRED entries empty
 * (the caller fills them from the sink's completions, matching indexes in
 * builder.hashes/sizes/posts order for the dirty emissions).
 */
export declare function rebuildStableRanges(node: Node, previous: StableRange[], dirty: boolean[], tailDirty: boolean, builder: CompoundHashBuilder): StableRange[];
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
 * Schedules the next slice of a background computation: idle time where the
 * platform offers it, a macrotask otherwise.
 */
export declare function scheduleSlice(fn: () => void): void;
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

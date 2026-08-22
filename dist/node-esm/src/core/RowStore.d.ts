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
 * Subtrees estimated above this split into per-child rows (Android's
 * CHILDREN_NODE_SPLIT_SIZE_THRESHOLD). 16 KiB keeps single rows small enough
 * that an incremental rewrite of one row is cheap, while a 60 MB workspace
 * stays around a few thousand rows — one getAll per restore.
 */
export declare const ROW_SPLIT_THRESHOLD_BYTES: number;
/**
 * Separator for encoded row-key path segments. RTDB rejects control
 * characters in keys, so \x01 can never appear inside a segment. Every
 * component (scope, root, each path segment) is terminated by a separator,
 * so a subtree prefix never matches a sibling that merely shares a string
 * prefix ('docs' vs 'docs2'). The subtree range is [prefix, prefix+\uffff):
 * every key continuing the prefix starts with a code unit below \uffff.
 */
export declare const ROW_KEY_SEPARATOR = "\u0001";
/**
 * Encodes a row key: authScope · encodedRoot · encodedRelativePath. The
 * scope and root are one prefix segment each (roots are full path strings
 * like '/users/alice', never containing \x01); the relative path contributes
 * one segment per level. Every component ends with a separator so prefix
 * ranges never match sibling keys that merely share a string prefix.
 */
/**
 * Total, reversible per-segment encoding over raw UTF-16 code units. Only
 * three code units are escaped — '%' (the escape lead), the separator
 * \x01, and the range sentinel \uffff — as '%xxxx' hex. Everything else
 * passes through verbatim, INCLUDING lone surrogates: Firebase key
 * validation accepts them, and encodeURIComponent (the previous encoding)
 * throws URIError on them, which wedged every flush of such a key into a
 * retry loop. IndexedDB compares string keys by code unit, so pass-through
 * segments keep prefix-range semantics exactly.
 */
export declare function encodeRowSegment(segment: string): string;
export declare function decodeRowSegment(encoded: string): string;
export declare function encodeRowKey(scope: string, rootString: string, relativePath: string[]): string;
/** Decodes the relative-path segments out of a row key. */
export declare function decodeRowKeyRelativePath(key: string, scope: string, rootString: string): string[];
/** The IDBKeyRange covering every row of (scope, root) at or under relPath. */
export declare function rowKeyRange(scope: string, rootString: string, relativePath: string[]): IDBKeyRange;
/**
 * Splits `node` (the subtree at `relativePath`) into disjoint rows following
 * Android's saveNested: children nodes above the threshold recurse into
 * per-child rows; everything else is one row of export-format JSON text.
 * Returns [relativePathSegments, jsonText] pairs.
 *
 * Interior priorities of split nodes are stored as a '.priority' pseudo-row
 * (Android stores a priority row the same way); assembly reapplies them.
 * Empty children nodes produce no row — absence of rows under a prefix after
 * a range delete IS the deletion.
 */
export declare function splitNodeIntoRows(relativePath: string[], node: Node, splitThreshold?: number): Array<[string[], string]>;
/**
 * Assembles a subtree from its rows (arbitrary order). Returns EMPTY_NODE
 * when there are no rows.
 */
export declare function assembleRows(rows: Array<[string[], string]>): Node;
/**
 * assembleRows in bounded slices: awaits `yieldFn` after every
 * `sliceBudgetBytes` of parsed row text so a large restore never blocks the
 * main thread in one task. Identical result to assembleRows by
 * construction (same partition, same application order).
 */
export declare function assembleRowsSliced(rows: Array<[string[], string]>, yieldFn: () => Promise<void>, sliceBudgetBytes: number): Promise<Node>;
/**
 * In-memory index of one root's row paths, kept by the writer so dirty
 * paths can be normalized to existing row boundaries before a rewrite
 * (upholding the disjoint-rows invariant without reading IDB on the write
 * path). Rebuilt from getAllKeys on restore and on lease acquisition.
 */
export declare class RowIndex {
    /** Encoded relative paths ('a\x01b\x01') of every row, lexicographic. */
    private keys_;
    static fromRelativePaths(paths: string[][]): RowIndex;
    /**
     * The relative path of the EXISTING row that contains `relativePath`
     * (i.e. an ancestor-or-self row), or null when no row contains it — a
     * write below the deepest existing row, or into empty space; the caller
     * then writes at the dirty path itself, which cannot conflict.
     */
    rowBoundaryFor(relativePath: string[]): string[] | null;
    /** Replaces every row at or under `relativePath` with `newRows`. */
    replaceSubtree(relativePath: string[], newRows: string[][]): void;
    rowCount(): number;
    private has_;
}
/**
 * Sorts sibling keys in Firebase child order (nameCompare) — the hash walk
 * and any row-ordered traversal must NOT trust IDB's lexicographic key
 * order, which disagrees with nameCompare on integer-like keys ('10' < '9'
 * numerically but '10' < '9' lexicographically happens to agree; '2' vs
 * '10' does not).
 */
export declare function sortRelativePathsByName(paths: string[][]): string[][];

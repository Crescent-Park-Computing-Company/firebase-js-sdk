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
 * Subtrees estimated above this split into per-child rows. 16 KiB keeps a
 * row cheap to re-serialize while a 60 MB workspace stays at a few
 * thousand rows.
 */
export declare const ROW_SPLIT_THRESHOLD_BYTES: number;
/**
 * Separator for encoded chunk/meta key components. RTDB rejects control
 * characters in keys, so \x01 never appears in a raw segment; the segment
 * escape below guarantees it never appears in an encoded one either. The
 * prefix range [prefix, prefix+\uffff) is exact because \uffff is likewise
 * escaped out of encoded segments.
 */
export declare const ROW_KEY_SEPARATOR = "\u0001";
/**
 * Total, reversible per-segment encoding over raw UTF-16 code units. Only
 * three code units are escaped — '%' (the escape lead), the separator
 * \x01, and the range sentinel \uffff — as '%xxxx' hex. Everything else
 * passes through verbatim, INCLUDING lone surrogates: Firebase key
 * validation accepts them, and encodeURIComponent throws URIError on
 * them. IndexedDB compares string keys by code unit, so pass-through
 * segments keep prefix-range semantics exactly.
 */
export declare function encodeRowSegment(segment: string): string;
export declare function decodeRowSegment(encoded: string): string;
/**
 * Splits `node` (the subtree at `relativePath`) into disjoint rows:
 * [relativePathSegments, exportJsonText] pairs. Empty children produce no
 * row. Interior priorities of split nodes become '.priority' pseudo-rows.
 */
export declare function splitNodeIntoRows(relativePath: string[], node: Node, splitThreshold?: number): Array<[string[], string]>;
/**
 * Assembles a subtree from its rows (arbitrary order). Returns EMPTY_NODE
 * when there are no rows.
 */
export declare function assembleRows(rows: Array<[string[], string]>): Node;
/**
 * assembleRows in bounded slices: awaits `yieldFn` after every
 * `sliceBudgetBytes` of parsed row text so a large restore never blocks
 * the main thread in one task. Identical result by construction.
 */
export declare function assembleRowsSliced(rows: Array<[string[], string]>, yieldFn: () => Promise<void>, sliceBudgetBytes: number): Promise<Node>;

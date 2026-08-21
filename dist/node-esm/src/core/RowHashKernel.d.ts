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
/**
 * Streaming compound-hash kernel over persisted rows.
 *
 * Computes the wire compound hash ({posts, hashes}) directly from RowStore
 * rows — export-format JSON text keyed by relative path — without building
 * Node trees or materializing the workspace. Peak memory is one parsed row
 * plus the open range's canonical text.
 *
 * The kernel is a single self-contained factory with zero imports so the
 * SAME code runs in two places:
 *   - main thread / tests: `createRowHashKernel(sha1)` called directly;
 *   - the hash worker: `createRowHashKernel.toString()` is embedded in the
 *     worker script text and instantiated there (see HashWorker.ts).
 * Nothing inside the factory may reference module-scope identifiers.
 *
 * Grammar parity: emits byte-identical canonical range text to
 * CompoundHashBuilder walking the assembled Node (verified by tests):
 *   - children in nameCompare order, priority interleaved as a '.priority'
 *     pseudo-child before the first key sorting after it, and dropped when
 *     it would sort after every child (Android parity);
 *   - leaves as `type:value` with IEEE754-hex numbers and quoted strings,
 *     `priority:P:` prefix for leaf priorities;
 *   - ranges split when the open range's text exceeds the threshold, never
 *     directly after a '.priority' leaf; posts are last-leaf paths.
 *
 * Rows must be DISJOINT (the RowStore invariant). A violated invariant is
 * detected during the walk (a row nested inside the previous row's subtree)
 * and surfaces as an `overlap` error — the caller falls back to
 * assemble-then-hash, so the reported hash is always truthful.
 */
/** One row: relative path segments plus export-format JSON text. */
export interface KernelRow {
    path: string[];
    json: string;
}
export interface KernelCompoundHash {
    posts: string[];
    hashes: string[];
}
export interface RowHashKernel {
    /**
     * Hashes rows of ONE root. `splitThreshold` overrides the size-derived
     * default (tests pin it to compare against fixedSizeSplitStrategy).
     * When the factory received a `yieldFn`, the walk awaits it after every
     * `sliceBudgetBytes` of processed row text (default 256 KiB) so a large
     * root hashed on the main thread stays in bounded slices; without a
     * yieldFn the walk is one synchronous pass (worker / tests).
     * Rejects with an Error whose `message` is 'overlap' when the rows are
     * not disjoint.
     */
    hashRows(rows: KernelRow[], splitThreshold?: number, sliceBudgetBytes?: number): Promise<KernelCompoundHash>;
}
export declare function createRowHashKernel(sha1Base64: (text: string) => Promise<string>, yieldFn?: () => Promise<void>): RowHashKernel;

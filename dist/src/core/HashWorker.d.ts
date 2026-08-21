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
 * Web Worker host for the row hash kernel.
 *
 * The worker is built from an inline script string (Blob URL) embedding
 * `createRowHashKernel.toString()`, so the exact parity-tested kernel runs
 * off the main thread with no bundler/asset dependency. Nothing large ever
 * crosses the boundary: the request names the IndexedDB row range, the
 * worker opens its OWN readonly connection, streams the rows through the
 * kernel, and posts back only `{posts, hashes}` (tens of KB at most).
 *
 * SHA-1 uses WebCrypto inside the worker. Any failure — no Worker, no
 * crypto.subtle, IDB open error, kernel overlap rejection — rejects, and
 * the caller (RowPersistenceManager.computeListenHashes) falls back to the
 * main-thread sliced kernel run.
 */
import { KernelCompoundHash } from './RowHashKernel';
export interface WorkerHashRequest {
    dbName: string;
    storeName: string;
    metaStoreName: string;
    /** The meta key of the root (= the row-key prefix with no segments). */
    metaKey: string;
    /**
     * Generation nonce the caller last restored/committed. Read from meta in
     * the SAME readonly transaction as the rows; a mismatch (foreign tab's
     * newer commit, staged rows without meta) rejects with 'gen-mismatch' and
     * the listen goes uncertified — never a hash of rows the live cache does
     * not hold.
     */
    expectedGen: string;
    /** Row-key range bounds for the root (lower inclusive, upper exclusive). */
    lowerKey: string;
    upperKey: string;
    /** Length of the scope·root prefix to strip from row keys. */
    prefixLength: number;
    /** \x01 — passed in so the worker script stays literal-free. */
    separator: string;
}
export declare function workerHashAvailable(): boolean;
/**
 * One-shot worker hash over a stored row range. The worker is spawned per
 * request and terminated on settle — hashing happens at boot and reconnect,
 * not in a loop, and a fresh worker cannot hold a stale IDB snapshot.
 * Rejects on any failure or after `timeoutMs`; the generation keeps running
 * server-side of nothing — a rejected promise simply routes the caller to
 * the main-thread fallback.
 */
export declare function hashRowsInWorker(request: WorkerHashRequest, timeoutMs: number): Promise<KernelCompoundHash>;

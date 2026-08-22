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
    /** The root's meta key. */
    metaKey: string;
    /**
     * The generation the caller restored/committed. Chunks are immutable and
     * keyed under their gen, so reading [lowerKey, upperKey) either returns
     * exactly that generation's chunks or misses some (a foreign swap GC'd
     * it) — verified against meta.chunkCount read in the SAME transaction.
     * Any mismatch rejects and the listen goes uncertified.
     */
    expectedGen: string;
    /** Chunk-key range bounds for the generation (lower incl., upper excl.). */
    lowerKey: string;
    upperKey: string;
    /**
     * True: values are chunk texts — JSON arrays of [pathSegments, exportJson]
     * row pairs, concatenated in key order to form the snapshot's rows.
     */
    chunked: boolean;
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

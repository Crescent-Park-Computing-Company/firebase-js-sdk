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

/* istanbul ignore file — createRowHashKernel/workerMain are serialized
 * with Function.toString() into the Blob worker; instrumented bodies would
 * reference module-scope coverage counters that do not exist in the worker. */

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

import { createRowHashKernel, KernelCompoundHash } from './RowHashKernel';

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

export function workerHashAvailable(): boolean {
  return (
    typeof Worker !== 'undefined' &&
    typeof Blob !== 'undefined' &&
    typeof URL !== 'undefined' &&
    typeof URL.createObjectURL === 'function'
  );
}

/**
 * The worker body. Runs `createRowHashKernel` (embedded by toString) over
 * the rows it reads from IndexedDB and posts one result per request.
 * Kept as a function so it is syntax-checked by the compiler; never called
 * on this thread.
 */
function workerMain(): void {
  // `KERNEL_FACTORY` is textually substituted with createRowHashKernel's
  // source when the Blob script is assembled.
  type Kernel = ReturnType<typeof createRowHashKernel>;
  const factory = (0, eval)('(KERNEL_FACTORY)') as typeof createRowHashKernel;
  const sha1 = async (text: string): Promise<string> => {
    // VERBATIM port of @firebase/util stringToByteArray (utf8.ts) — the
    // byte encoder the canonical SDK sha1() uses. NOT TextEncoder: the
    // canonical encoder consumes the char after any lead surrogate
    // unconditionally (deterministic garbage for Firebase-legal lone
    // surrogates, and a throw when one ends the string), while TextEncoder
    // substitutes U+FFFD — same range text, different bytes, different
    // hash, failed certification. Wire compatibility pins these bytes.
    const str = text;
    const out: number[] = [];
    let p = 0;
    for (let i = 0; i < str.length; i++) {
      let c = str.charCodeAt(i);
      if (c >= 0xd800 && c <= 0xdbff) {
        const high = c - 0xd800;
        i++;
        if (i >= str.length) {
          // Canonical encoder asserts here; failing the hash (caught by the
          // caller → uncertified listen) matches the main-thread outcome.
          throw new Error('Surrogate pair missing trail surrogate.');
        }
        const low = str.charCodeAt(i) - 0xdc00;
        c = 0x10000 + (high << 10) + low;
      }
      if (c < 128) {
        out[p++] = c;
      } else if (c < 2048) {
        out[p++] = (c >> 6) | 192;
        out[p++] = (c & 63) | 128;
      } else if (c < 65536) {
        out[p++] = (c >> 12) | 224;
        out[p++] = ((c >> 6) & 63) | 128;
        out[p++] = (c & 63) | 128;
      } else {
        out[p++] = (c >> 18) | 240;
        out[p++] = ((c >> 12) & 63) | 128;
        out[p++] = ((c >> 6) & 63) | 128;
        out[p++] = (c & 63) | 128;
      }
    }
    const bytes = new Uint8Array(out);
    const digest = await crypto.subtle.digest('SHA-1', bytes);
    const arr = new Uint8Array(digest);
    let bin = '';
    for (let i = 0; i < arr.length; i++) {
      bin += String.fromCharCode(arr[i]);
    }
    return btoa(bin);
  };
  const kernel: Kernel = factory(sha1);
  self.onmessage = (event: MessageEvent) => {
    const req = event.data as WorkerHashRequest;
    const fail = (message: string): void => {
      (self as unknown as Worker).postMessage({ error: message });
    };
    try {
      const open = indexedDB.open(req.dbName);
      open.onerror = () => fail('idb-open');
      open.onsuccess = () => {
        const db = open.result;
        let txn: IDBTransaction;
        try {
          txn = db.transaction([req.storeName, req.metaStoreName], 'readonly');
        } catch (e) {
          db.close();
          fail('idb-txn');
          return;
        }
        const store = txn.objectStore(req.storeName);
        const metaReq = txn.objectStore(req.metaStoreName).get(req.metaKey);
        const range = IDBKeyRange.bound(
          req.lowerKey,
          req.upperKey,
          false,
          true
        );
        const valuesReq = store.getAll(range);
        let values: string[] | null = null;
        let metaDone = false;
        let metaGen: string | null = null;
        let metaChunkCount = -1;
        const maybeRun = (): void => {
          if (values === null || !metaDone) {
            return;
          }
          db.close();
          // Chunks are immutable under their gen key: the range read
          // either returned the complete generation (count matches the
          // meta read in this same transaction) or the generation was
          // GC'd/replaced — decline, never hash a partial snapshot.
          if (metaGen !== req.expectedGen || values.length !== metaChunkCount) {
            fail('gen-mismatch');
            return;
          }
          const rows = [];
          for (let i = 0; i < values.length; i++) {
            const parsed = JSON.parse(values[i]) as Array<[string[], string]>;
            for (let j = 0; j < parsed.length; j++) {
              rows.push({ path: parsed[j][0], json: parsed[j][1] });
            }
          }
          kernel.hashRows(rows).then(
            result => (self as unknown as Worker).postMessage(result),
            err => fail(err instanceof Error ? err.message : 'kernel-failure')
          );
        };
        metaReq.onerror = () => {
          db.close();
          fail('idb-meta');
        };
        metaReq.onsuccess = () => {
          const meta = metaReq.result as
            | { gen?: string; chunkCount?: number }
            | undefined;
          metaGen = meta && typeof meta.gen === 'string' ? meta.gen : null;
          metaChunkCount =
            meta && typeof meta.chunkCount === 'number' ? meta.chunkCount : -1;
          metaDone = true;
          maybeRun();
        };
        valuesReq.onerror = () => {
          db.close();
          fail('idb-values');
        };
        valuesReq.onsuccess = () => {
          values = valuesReq.result as string[];
          maybeRun();
        };
      };
    } catch (e) {
      fail(e instanceof Error ? e.message : 'worker-failure');
    }
  };
}

let workerUrl: string | null = null;

function getWorkerUrl(): string {
  if (workerUrl === null) {
    const script =
      'const KERNEL_FACTORY = ' +
      createRowHashKernel.toString() +
      ';\n(' +
      workerMain
        .toString()
        // The eval indirection exists only to satisfy the module compiler;
        // in the worker the factory source is in scope directly.
        .replace("(0, eval)('(KERNEL_FACTORY)')", 'KERNEL_FACTORY') +
      ')();';
    workerUrl = URL.createObjectURL(
      new Blob([script], { type: 'application/javascript' })
    );
  }
  return workerUrl;
}

/**
 * One-shot worker hash over a stored row range. The worker is spawned per
 * request and terminated on settle — hashing happens at boot and reconnect,
 * not in a loop, and a fresh worker cannot hold a stale IDB snapshot.
 * Rejects on any failure or after `timeoutMs`; the generation keeps running
 * server-side of nothing — a rejected promise simply routes the caller to
 * the main-thread fallback.
 */
export function hashRowsInWorker(
  request: WorkerHashRequest,
  timeoutMs: number
): Promise<KernelCompoundHash> {
  return new Promise((resolve, reject) => {
    let worker: Worker;
    try {
      worker = new Worker(getWorkerUrl());
    } catch (e) {
      reject(e instanceof Error ? e : new Error('worker-spawn'));
      return;
    }
    let settled = false;
    const timer = setTimeout(() => {
      finish(() => reject(new Error('worker-timeout')));
    }, timeoutMs);
    const finish = (complete: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      worker.terminate();
      complete();
    };
    worker.onmessage = event => {
      const data = event.data as KernelCompoundHash | { error: string };
      if ((data as { error: string }).error !== undefined) {
        finish(() => reject(new Error((data as { error: string }).error)));
      } else {
        finish(() => resolve(data as KernelCompoundHash));
      }
    };
    worker.onerror = event => {
      finish(() => reject(new Error(event.message || 'worker-error')));
    };
    worker.postMessage(request);
  });
}

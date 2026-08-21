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

import { createRowHashKernel, KernelCompoundHash } from './RowHashKernel';

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
    const bytes = new TextEncoder().encode(text);
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
        const keysReq = store.getAllKeys(range);
        const valuesReq = store.getAll(range);
        let keys: string[] | null = null;
        let values: string[] | null = null;
        let metaDone = false;
        let metaGen: string | null = null;
        const maybeRun = (): void => {
          if (keys === null || values === null || !metaDone) {
            return;
          }
          db.close();
          if (metaGen !== req.expectedGen) {
            fail('gen-mismatch');
            return;
          }
          const rows = [];
          for (let i = 0; i < keys.length; i++) {
            const rest = keys[i].slice(req.prefixLength);
            let path: string[];
            if (rest === '') {
              path = [];
            } else {
              path = rest.split(req.separator);
              path.pop();
              // Row keys hold URI-encoded segments (RowStore encodeRowKey);
              // the kernel must hash the REAL child names or its posts and
              // range text diverge from the server's tree.
              for (let j = 0; j < path.length; j++) {
                path[j] = decodeURIComponent(path[j]);
              }
            }
            rows.push({ path, json: values[i] });
          }
          kernel.hashRows(rows).then(
            result => (self as unknown as Worker).postMessage(result),
            err =>
              fail(err instanceof Error ? err.message : 'kernel-failure')
          );
        };
        metaReq.onerror = () => {
          db.close();
          fail('idb-meta');
        };
        metaReq.onsuccess = () => {
          const meta = metaReq.result as { gen?: string } | undefined;
          metaGen = meta && typeof meta.gen === 'string' ? meta.gen : null;
          metaDone = true;
          maybeRun();
        };
        keysReq.onerror = () => {
          db.close();
          fail('idb-keys');
        };
        valuesReq.onerror = () => {
          db.close();
          fail('idb-values');
        };
        keysReq.onsuccess = () => {
          keys = keysReq.result as string[];
          maybeRun();
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
      const data = event.data as
        | KernelCompoundHash
        | { error: string };
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

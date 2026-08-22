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
 * Fake IndexedDB for the row manager: string keys, getAll/getAllKeys with
 * ranges, range deletes, multi-store transactions, versioned open with
 * upgrade. Shared `stores` gives multi-manager (multi-tab) tests one
 * storage substrate.
 *
 * Transaction semantics mirror the real API where the manager depends on
 * them: WRITES BUFFER per transaction and land on the shared Maps only at
 * commit (oncomplete); abort() discards the buffer and fires onabort, and
 * no further requests in that transaction run. Reads see the transaction's
 * own uncommitted writes layered over the committed state (IndexedDB
 * read-your-own-writes), while other transactions never observe them —
 * so tests CAN detect torn/partially-visible generations.
 */
const DB_VERSION_KEY = '__version__';

export function makeFakeIdb(
  shared?: Map<string, Map<string, unknown>>,
  log?: { puts: string[]; deletes: string[] }
): IDBFactory {
  const stores = shared ?? new Map<string, Map<string, unknown>>();
  // Version bookkeeping lives IN the shared map so managers sharing a
  // substrate see one database: like real IndexedDB, onupgradeneeded fires
  // only when the requested version exceeds the stored one — not on every
  // open (which would rerun upgrade handlers that drop/recreate stores and
  // silently wipe the shared data between managers).
  if (!stores.has(DB_VERSION_KEY)) {
    stores.set(DB_VERSION_KEY, new Map([['v', 0]]));
  }
  const versionBox = stores.get(DB_VERSION_KEY)! as Map<string, number>;
  const async = (fn: () => void): void => {
    void Promise.resolve().then(fn);
  };
  const inRange = (key: string, range: unknown): boolean => {
    if (range === undefined || range === null) {
      return true;
    }
    if (typeof range === 'string') {
      return key === range;
    }
    const r = range as { lower: string; upper: string };
    return key >= r.lower && key < r.upper;
  };

  interface TxnState {
    aborted: boolean;
    /** Per-store overlay: value = the new value, or DELETED. */
    overlays: Map<string, Map<string, unknown>>;
    onabortFire: () => void;
  }
  const DELETED = Symbol('deleted');

  const makeTxn = (log_?: { puts: string[]; deletes: string[] }) => {
    const state: TxnState = {
      aborted: false,
      overlays: new Map(),
      onabortFire: () => txn.onabort?.()
    };
    const overlayFor = (name: string): Map<string, unknown> => {
      if (!state.overlays.has(name)) {
        state.overlays.set(name, new Map());
      }
      return state.overlays.get(name)!;
    };
    const committedFor = (name: string): Map<string, unknown> => {
      if (!stores.has(name)) {
        stores.set(name, new Map());
      }
      return stores.get(name)!;
    };
    /** The transaction's view: committed state + its own overlay. */
    const view = (name: string): Map<string, unknown> => {
      const merged = new Map(committedFor(name));
      for (const [k, v] of overlayFor(name)) {
        if (v === DELETED) {
          merged.delete(k);
        } else {
          merged.set(k, v);
        }
      }
      return merged;
    };
    // Real IndexedDB auto-commits when the REQUEST QUEUE DRAINS: after a
    // request callback (and its microtask continuations) issues no further
    // request, the transaction commits. A fixed creation-time timer would
    // commit under a consumer that awaits one request before issuing the
    // next (the staging ownership check) — the exact lifetime bug class
    // this fake exists to surface, so model the drain, not a timer.
    let pendingRequests = 0;
    let committed = false;
    const commit = (): void => {
      if (committed || state.aborted) {
        return;
      }
      committed = true;
      for (const [name, overlay] of state.overlays) {
        const committedStore = committedFor(name);
        for (const [key, value] of overlay) {
          if (value === DELETED) {
            if (committedStore.delete(key)) {
              log_?.deletes.push(name + ':' + key);
            }
          } else {
            committedStore.set(key, value);
            log_?.puts.push(name + ':' + key);
          }
        }
      }
      txn.oncomplete?.();
    };
    const maybeCommitAfterDrain = (): void => {
      // Real IndexedDB keeps a transaction alive across `await`s of its OWN
      // requests: the continuation runs in the request callback's task, and
      // auto-commit happens only when control returns to the event loop
      // with no pending requests. Model that with a run of EMPTY microtask
      // hops: a consumer's await-chain issues its next request within a
      // couple of hops, so requiring several consecutive quiet hops lets
      // arbitrary same-chain continuations (awaited get, then puts) run
      // first — while staying timer-free so tests that only pump
      // microtasks still observe commits.
      let quiet = 0;
      const tick = (): void => {
        if (committed || state.aborted) {
          return;
        }
        if (pendingRequests > 0) {
          return; // the active request's resolution reschedules the check
        }
        quiet++;
        if (quiet >= 8) {
          commit();
          return;
        }
        async(tick);
      };
      async(tick);
    };
    const request = (
      result?: unknown
    ): {
      result: unknown;
      onsuccess: null | (() => void);
      onerror: null | (() => void);
    } => {
      const req = {
        result,
        onsuccess: null as null | (() => void),
        onerror: null as null | (() => void)
      };
      pendingRequests++;
      async(() => {
        pendingRequests--;
        if (!state.aborted && !committed) {
          req.onsuccess?.();
        }
        if (pendingRequests === 0) {
          maybeCommitAfterDrain();
        }
      });
      return req;
    };
    const txn = {
      objectStore: (name: string) => ({
        get: (key: string) => request(view(name).get(key)),
        put: (value: unknown, key: string) => {
          if (!state.aborted && !committed) {
            overlayFor(name).set(key, value);
          }
          return request();
        },
        delete: (keyOrRange: unknown) => {
          if (!state.aborted && !committed) {
            for (const key of [...view(name).keys()]) {
              if (inRange(key, keyOrRange)) {
                overlayFor(name).set(key, DELETED);
              }
            }
          }
          return request();
        },
        getAllKeys: (range?: unknown) =>
          request([...view(name).keys()].filter(k => inRange(k, range)).sort()),
        getAll: (range?: unknown) =>
          request(
            [...view(name).entries()]
              .filter(([k]) => inRange(k, range))
              .sort(([a], [b]) => (a < b ? -1 : 1))
              .map(([, v]) => v)
          )
      }),
      abort: () => {
        if (state.aborted || committed) {
          return;
        }
        state.aborted = true;
        state.overlays.clear();
        async(() => state.onabortFire());
      },
      oncomplete: null as null | (() => void),
      onerror: null as null | (() => void),
      onabort: null as null | (() => void)
    };
    // A transaction with no requests at all still commits (empty commit).
    maybeCommitAfterDrain();
    return txn;
  };

  return {
    open: (_name: string, version?: number) => {
      const req: {
        result: unknown;
        onupgradeneeded: null | (() => void);
        onsuccess: null | (() => void);
        onerror: null | (() => void);
        onblocked: null | (() => void);
      } = {
        result: undefined,
        onupgradeneeded: null,
        onsuccess: null,
        onerror: null,
        onblocked: null
      };
      const db = {
        objectStoreNames: {
          contains: (name: string) => stores.has(name)
        },
        createObjectStore: (name: string) => {
          stores.set(name, new Map());
        },
        deleteObjectStore: (name: string) => {
          stores.delete(name);
        },
        transaction: () => makeTxn(log),
        close: () => {},
        onversionchange: null
      };
      req.result = db;
      async(() => {
        const current = versionBox.get('v') ?? 0;
        if (version !== undefined && version > current) {
          versionBox.set('v', version);
          req.onupgradeneeded?.();
        }
        req.onsuccess?.();
      });
      return req as unknown as IDBOpenDBRequest;
    }
  } as unknown as IDBFactory;
}

export const flushMicrotasks = async (rounds = 20): Promise<void> => {
  for (let i = 0; i < rounds; i++) {
    await Promise.resolve();
  }
};

export const wait = (ms: number): Promise<void> =>
  new Promise(resolve => setTimeout(resolve, ms));

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
export function makeFakeIdb(
  shared?: Map<string, Map<string, unknown>>,
  log?: { puts: string[]; deletes: string[] }
): IDBFactory {
  const stores = shared ?? new Map<string, Map<string, unknown>>();
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
      async(() => {
        if (!state.aborted) {
          req.onsuccess?.();
        }
      });
      return req;
    };
    const txn = {
      objectStore: (name: string) => ({
        get: (key: string) => request(view(name).get(key)),
        put: (value: unknown, key: string) => {
          if (!state.aborted) {
            overlayFor(name).set(key, value);
          }
          return request();
        },
        delete: (keyOrRange: unknown) => {
          if (!state.aborted) {
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
        if (state.aborted) {
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
    // Commit after every queued request has resolved: two microtask hops
    // order it after request callbacks. Aborted transactions never commit.
    async(() =>
      async(() => {
        if (state.aborted) {
          return;
        }
        for (const [name, overlay] of state.overlays) {
          const committed = committedFor(name);
          for (const [key, value] of overlay) {
            if (value === DELETED) {
              if (committed.delete(key)) {
                log_?.deletes.push(name + ':' + key);
              }
            } else {
              committed.set(key, value);
              log_?.puts.push(name + ':' + key);
            }
          }
        }
        txn.oncomplete?.();
      })
    );
    return txn;
  };

  return {
    open: () => {
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
        req.onupgradeneeded?.();
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

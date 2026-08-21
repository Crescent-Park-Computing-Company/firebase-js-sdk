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
 * Minimal fake IndexedDB for the row manager: string keys, getAll/getAllKeys
 * with ranges, range deletes, multi-store transactions, versioned open with
 * upgrade. Shared `stores` gives multi-manager (multi-tab) tests one
 * storage substrate.
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
  const request = (result?: unknown): { result: unknown; onsuccess: null | (() => void); onerror: null | (() => void) } => {
    const req = { result, onsuccess: null as null | (() => void), onerror: null as null | (() => void) };
    async(() => req.onsuccess?.());
    return req;
  };
  const makeStore = (name: string) => {
    if (!stores.has(name)) {
      stores.set(name, new Map());
    }
    const data = stores.get(name)!;
    return {
      get: (key: string) => request(data.get(key)),
      put: (value: unknown, key: string) => {
        data.set(key, value);
        log?.puts.push(name + ':' + key);
        return request();
      },
      delete: (keyOrRange: unknown) => {
        for (const key of [...data.keys()]) {
          if (inRange(key, keyOrRange)) {
            data.delete(key);
            log?.deletes.push(name + ':' + key);
          }
        }
        return request();
      },
      getAllKeys: (range?: unknown) =>
        request([...data.keys()].filter(k => inRange(k, range)).sort()),
      getAll: (range?: unknown) =>
        request(
          [...data.entries()]
            .filter(([k]) => inRange(k, range))
            .sort(([a], [b]) => (a < b ? -1 : 1))
            .map(([, v]) => v)
        )
    };
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
        transaction: (names: string | string[]) => {
          const txn = {
            objectStore: (name: string) => makeStore(name),
            oncomplete: null as null | (() => void),
            onerror: null as null | (() => void),
            onabort: null as null | (() => void)
          };
          // Complete after every queued request has resolved: two
          // microtask hops order it after request callbacks.
          async(() => async(() => txn.oncomplete?.()));
          return txn;
        },
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

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
import { expect } from 'chai';

import { PersistenceManager } from '../src/core/Persistence';
import { Node } from '../src/core/snap/Node';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';

/* eslint-disable @typescript-eslint/no-explicit-any */

// Deterministic PRNG (mulberry32) so failures reproduce.
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randomJson(rand: () => number, depth: number): unknown {
  const roll = rand();
  if (depth <= 0 || roll < 0.35) {
    const leafRoll = rand();
    if (leafRoll < 0.4) {
      return Math.floor(rand() * 1000);
    }
    if (leafRoll < 0.7) {
      return 'v'.repeat(1 + Math.floor(rand() * 40)) + Math.floor(rand() * 10);
    }
    if (leafRoll < 0.8) {
      return rand() < 0.5;
    }
    if (leafRoll < 0.9) {
      // Leaf with priority (export-format object form).
      return { '.value': Math.floor(rand() * 100), '.priority': rand() };
    }
    return 'x'.repeat(2000 + Math.floor(rand() * 4000));
  }
  const out: Record<string, unknown> = {};
  const children = 1 + Math.floor(rand() * 6);
  for (let i = 0; i < children; i++) {
    const key =
      rand() < 0.2
        ? String(Math.floor(rand() * 50))
        : 'k' + Math.floor(rand() * 1e6).toString(36);
    out[key] = randomJson(rand, depth - 1);
  }
  if (rand() < 0.15) {
    out['.priority'] = 1 + Math.floor(rand() * 5);
  }
  return out;
}

function mutate(rand: () => number, node: Node, mutations: number): Node {
  let current = node;
  for (let i = 0; i < mutations; i++) {
    // Collect the top-level keys to pick a mutation site.
    const keys: string[] = [];
    current.forEachChild(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      require('../src/core/snap/indexes/KeyIndex').KEY_INDEX,
      (key: string) => {
        keys.push(key);
      }
    );
    const roll = rand();
    if (keys.length === 0 || roll < 0.3) {
      // Add a child (possibly a whole subtree).
      const key = 'n' + Math.floor(rand() * 1e6).toString(36);
      current = current.updateImmediateChild(
        key,
        nodeFromJSON(randomJson(rand, 2))
      );
    } else if (roll < 0.55) {
      // Replace a child's subtree.
      const key = keys[Math.floor(rand() * keys.length)];
      current = current.updateImmediateChild(
        key,
        nodeFromJSON(randomJson(rand, 2))
      );
    } else if (roll < 0.75) {
      // Delete a child.
      const key = keys[Math.floor(rand() * keys.length)];
      current = current.updateImmediateChild(
        key,
        nodeFromJSON(null)
      );
    } else {
      // Deep update below a child.
      const key = keys[Math.floor(rand() * keys.length)];
      current = current.updateChild(
        new Path(key + '/deep' + Math.floor(rand() * 10)),
        nodeFromJSON(randomJson(rand, 1))
      );
    }
  }
  return current;
}

// A copy of the persistence test's fake IndexedDB (kept local so this file
// stays self-contained).
function makeFakeIndexedDB(): { factory: IDBFactory; data: Map<string, unknown> } {
  const data = new Map<string, unknown>();
  const state = { version: 9, hasStore: true };
  const async = (fn: () => void) => {
    void Promise.resolve().then(fn);
  };
  const makeRequest = (result: unknown) => {
    let doneFlag = false;
    const req: any = {
      get result() {
        if (!doneFlag) {
          throw new Error('InvalidStateError');
        }
        return result;
      },
      onsuccess: null,
      onerror: null
    };
    async(() => {
      doneFlag = true;
      if (req.onsuccess) {
        req.onsuccess();
      }
    });
    return req;
  };
  const store: any = {
    get: (key: string) => makeRequest(data.get(key)),
    put: (value: unknown, key: string) => {
      data.set(key, structuredClone(value));
      return makeRequest(undefined);
    },
    clear: () => {
      data.clear();
      return makeRequest(undefined);
    },
    delete: (key: any) => {
      if (typeof key === 'string') {
        data.delete(key);
      } else if (key && typeof key.includes === 'function') {
        for (const storedKey of [...data.keys()]) {
          if (key.includes(storedKey)) {
            data.delete(storedKey);
          }
        }
      }
      return makeRequest(undefined);
    },
    openCursor: () => {
      const entries = [...data.entries()];
      const req: any = { result: null, onsuccess: null, onerror: null };
      let index = 0;
      const step = () => {
        if (index < entries.length) {
          const [key, value] = entries[index++];
          req.result = {
            key,
            value,
            delete: () => {
              data.delete(key);
              return makeRequest(undefined);
            },
            continue: () => async(step)
          };
        } else {
          req.result = null;
        }
        if (req.onsuccess) {
          req.onsuccess();
        }
      };
      async(step);
      return req;
    }
  };
  const tx: any = {
    objectStore: () => store,
    oncomplete: null,
    onabort: null,
    onerror: null
  };
  const makeDb = () => ({
    version: state.version,
    close: () => {},
    objectStoreNames: { contains: () => state.hasStore },
    createObjectStore: () => {
      state.hasStore = true;
      return store;
    },
    transaction: () => {
      const t = { ...tx };
      setTimeout(() => t.oncomplete && t.oncomplete(), 0);
      return t;
    }
  });
  const factory: any = {
    open: () => {
      const req: any = {
        result: null,
        onupgradeneeded: null,
        onsuccess: null,
        onerror: null,
        onblocked: null,
        transaction: null
      };
      async(() => {
        req.result = makeDb();
        if (req.onsuccess) {
          req.onsuccess();
        }
      });
      return req;
    }
  };
  return { factory, data };
}

function flushAsync(): Promise<void> {
  let chain = Promise.resolve();
  for (let i = 0; i < 10; i++) {
    chain = chain.then(
      () => new Promise<void>(resolve => setTimeout(resolve, 0))
    );
  }
  return chain;
}

describe('Persistence round-trip property', function () {
  this.timeout(120000);

  for (const seed of [1, 2, 3, 4, 5, 6, 7, 8]) {
    it(`random tree + incremental mutations round-trip exactly (seed ${seed})`, async () => {
      const rand = rng(seed * 7919);
      const { factory } = makeFakeIndexedDB();
      // A tiny segment target so even small random trees exercise
      // multi-segment splits, deep-child promotion, and demotion.
      const manager = new PersistenceManager(
        'prop',
        factory,
        true,
        8000,
        100 * 1024 * 1024,
        0,
        2 * 1024
      );
      manager.setAuthScope(null);
      const path = new Path('prop/root');
      manager.track(path.toString());

      let node = nodeFromJSON(randomJson(rand, 3));
      if (node.isEmpty()) {
        node = nodeFromJSON({ seeded: true });
      }
      manager.serverCacheUpdated(path, node);
      await manager.flushNow(path.toString());
      await flushAsync();

      for (let generation = 0; generation < 6; generation++) {
        // Reload cycle: a fresh manager (fresh tab) restores, mutates, and
        // flushes incrementally against the restored generation.
        const reloaded = new PersistenceManager(
          'prop',
          factory,
          true,
          8000,
          100 * 1024 * 1024,
          0,
          2 * 1024
        );
        reloaded.setAuthScope(null);
        reloaded.track(path.toString());
        const restored = await reloaded.restoreForListen(path.toString());
        expect(restored.record, `restore gen ${generation}`).to.not.equal(
          null
        );
        // Exact round-trip: values AND hash (priorities included).
        expect(restored.record!.node.val(true)).to.deep.equal(node.val(true));
        expect(restored.record!.node.hash()).to.equal(node.hash());

        let next = mutate(rand, restored.record!.node, 3);
        if (next.isEmpty()) {
          next = nodeFromJSON({ reseeded: generation });
        }
        node = next;
        reloaded.serverCacheUpdated(path, node);
        await reloaded.flushNow(path.toString());
        await flushAsync();
      }

      // Final check with one more fresh manager.
      const last = new PersistenceManager(
        'prop',
        factory,
        true,
        8000,
        100 * 1024 * 1024,
        0,
        2 * 1024
      );
      last.setAuthScope(null);
      last.track(path.toString());
      const final = await last.restoreForListen(path.toString());
      expect(final.record).to.not.equal(null);
      expect(final.record!.node.val(true)).to.deep.equal(node.val(true));
      expect(final.record!.node.hash()).to.equal(node.hash());
    });
  }
});

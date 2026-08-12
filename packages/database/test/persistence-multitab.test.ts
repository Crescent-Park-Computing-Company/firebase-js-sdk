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
 * Multi-tab flush economics: cross-tab write leases (Web Locks) and the
 * manifest-only stale-baseline adoption.
 *
 * The regression these lock in: two live tabs flushing the same churning
 * root used to leapfrog each other's manifest revisions, and every CAS
 * loser re-read + decoded the ENTIRE stored root, adopted it as an
 * identity-diff baseline that shares no identity with the live tree (so
 * every range marked dirty), re-staged the full root, and retried
 * IMMEDIATELY — unbounded full-tree work in every tab for as long as both
 * lived, which crashed large workspaces and monopolized IndexedDB against
 * boot restores.
 */
import { expect } from 'chai';

import {
  PersistenceManager,
  persistenceStats
} from '../src/core/Persistence';
import { Node } from '../src/core/snap/Node';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';

// ── minimal fake Web Locks manager (exclusive mode, FIFO grants) ────────────
function makeFakeWebLocks(): {
  locks: {
    request: (
      name: string,
      options: { mode: 'exclusive' },
      callback: (lock: unknown) => Promise<void>
    ) => Promise<void>;
  };
  holders: Map<string, number>;
} {
  const queues = new Map<
    string,
    Array<{ callback: (lock: unknown) => Promise<void>; settle: () => void }>
  >();
  const busy = new Set<string>();
  const holders = new Map<string, number>();
  const pump = (name: string) => {
    if (busy.has(name)) {
      return;
    }
    const next = queues.get(name)?.shift();
    if (next === undefined) {
      return;
    }
    busy.add(name);
    holders.set(name, (holders.get(name) ?? 0) + 1);
    void Promise.resolve()
      .then(() => next.callback({ name, mode: 'exclusive' }))
      .catch(() => {})
      .then(() => {
        busy.delete(name);
        next.settle();
        pump(name);
      });
  };
  return {
    locks: {
      request: (name, _options, callback) =>
        new Promise<void>(settle => {
          if (!queues.has(name)) {
            queues.set(name, []);
          }
          queues.get(name)!.push({ callback, settle });
          pump(name);
        })
    },
    holders
  };
}

// ── minimal in-memory IDBFactory (same shape as persistence.test.ts) ────────
function makeFakeIndexedDB(
  options: {
    onGet?: (key: string) => void;
    onPut?: (key: string, value: unknown) => void;
  } = {}
): { factory: IDBFactory; data: Map<string, unknown> } {
  const data = new Map<string, unknown>();
  const state = { version: 9, hasStore: true };
  const async = (fn: () => void) => {
    void Promise.resolve().then(fn);
  };
  const makeRequest = (result: unknown) => {
    let doneFlag = false;
    const req: {
      readonly result: unknown;
      onsuccess: null | (() => void);
      onerror: null | (() => void);
    } = {
      get result() {
        if (!doneFlag) {
          throw new Error('InvalidStateError: request not done');
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
  const store = {
    get: (key: string) => {
      options.onGet?.(key);
      return makeRequest(data.get(key));
    },
    getKey: (key: string) => makeRequest(data.has(key) ? key : undefined),
    put: (value: unknown, key: string) => {
      const stored = structuredClone(value);
      data.set(key, stored);
      options.onPut?.(key, stored);
      return makeRequest(undefined);
    },
    clear: () => {
      data.clear();
      return makeRequest(undefined);
    },
    delete: (key: string | IDBKeyRange) => {
      if (typeof key === 'string') {
        data.delete(key);
      } else if (key && typeof (key as IDBKeyRange).includes === 'function') {
        for (const storedKey of [...data.keys()]) {
          if ((key as IDBKeyRange).includes(storedKey)) {
            data.delete(storedKey);
          }
        }
      }
      return makeRequest(undefined);
    },
    openCursor: () => {
      const entries = [...data.entries()];
      const req: {
        result: unknown;
        onsuccess: null | (() => void);
        onerror: null | (() => void);
      } = { result: null, onsuccess: null, onerror: null };
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
  const tx = {
    objectStore: () => store,
    oncomplete: null as null | (() => void),
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
  const factory = {
    open: (_name: string, version?: number) => {
      const req: {
        result: unknown;
        onupgradeneeded: null | (() => void);
        onsuccess: null | (() => void);
        onerror: null | (() => void);
        onblocked: null | (() => void);
        transaction: null | { objectStore: () => typeof store };
      } = {
        result: null,
        onupgradeneeded: null,
        onsuccess: null,
        onerror: null,
        onblocked: null,
        transaction: null
      };
      async(() => {
        const upgrading = version !== undefined && version > state.version;
        if (upgrading) {
          state.version = version!;
        }
        req.result = makeDb();
        req.transaction = upgrading ? { objectStore: () => store } : null;
        if (upgrading && req.onupgradeneeded) {
          req.onupgradeneeded();
        }
        req.result = makeDb();
        if (req.onsuccess) {
          req.onsuccess();
        }
      });
      return req;
    }
  } as unknown as IDBFactory;
  return { factory, data };
}

function flushAsync(turns = 12): Promise<void> {
  let chain = Promise.resolve();
  for (let i = 0; i < turns; i++) {
    chain = chain.then(() => new Promise<void>(r => setTimeout(r, 0)));
  }
  return chain;
}

function makeWorkspace(): Record<string, unknown> {
  const root: Record<string, unknown> = {};
  for (let a = 0; a < 8; a++) {
    const entries: Record<string, unknown> = {};
    for (let e = 0; e < 8; e++) {
      entries['entry-' + e] = { body: ('v' + a + '-' + e + '|').repeat(200) };
    }
    root['app-' + a] = { data: { entries } };
  }
  return root;
}

const ROOT_PATH = new Path('tabs/file-system');
const ROOT = ROOT_PATH.toString();
const CHURN_REL = 'app-0/data/entries/entry-0/body';

function mkManager(
  factory: IDBFactory,
  writeDelay = 0,
  rangeTarget = 4 * 1024
): PersistenceManager {
  const manager = new PersistenceManager(
    'test-repo',
    factory,
    true,
    8000,
    100 * 1024 * 1024,
    writeDelay,
    rangeTarget
  );
  manager.setAuthScope('u1');
  return manager;
}

/** Manifest + every referenced range record present, all content readable. */
async function expectSelfContainedAndEqual(
  factory: IDBFactory,
  data: Map<string, unknown>,
  expected: Node
): Promise<void> {
  const manifest = data.get('test-repo|' + ROOT) as {
    ranges: Array<{ recordId: string }>;
  };
  expect(manifest).to.not.equal(undefined);
  for (const range of manifest.ranges) {
    expect(data.has('test-repo|' + ROOT + '#range:' + range.recordId)).to.equal(
      true
    );
  }
  const reader = mkManager(factory);
  reader.track(ROOT);
  const restored = (await reader.restoreForListen(ROOT)).record;
  expect(restored).to.not.equal(null);
  expect(restored!.node.val(true)).to.deep.equal(expected.val(true));
  reader.dispose();
}

describe('PersistenceManager multi-tab write economics', () => {
  // Node 21+ defines globalThis.navigator as a getter; stub/restore via
  // property descriptors rather than assignment.
  let savedNavigator: PropertyDescriptor | undefined;

  const stubNavigator = (value: unknown) => {
    Object.defineProperty(globalThis, 'navigator', {
      value,
      configurable: true,
      writable: true
    });
  };

  beforeEach(() => {
    savedNavigator = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
  });

  afterEach(() => {
    if (savedNavigator) {
      Object.defineProperty(globalThis, 'navigator', savedNavigator);
    } else {
      delete (globalThis as { navigator?: unknown }).navigator;
    }
  });

  it('with Web Locks, only the lease holder writes under cross-tab churn', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    const gen1 = shared.data.get('test-repo|' + ROOT) as { revision: string };
    expect(gen1).to.not.equal(undefined);

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    const restored = (await tabB.restoreForListen(ROOT)).record;
    expect(restored).to.not.equal(null);
    let treeB: Node = restored!.node;

    // Steady churn observed by both tabs.
    const writeThroughs0 = persistenceStats.writeThroughs;
    const hashed0 = persistenceStats.rangesHashed;
    for (let i = 0; i < 5; i++) {
      const value = nodeFromJSON('churn-' + i);
      treeA = treeA.updateChild(new Path(CHURN_REL), value);
      treeB = treeB.updateChild(new Path(CHURN_REL), value);
      tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
      tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
      await tabA.flushNow(ROOT);
      await tabB.flushNow(ROOT);
      await flushAsync();
    }

    // The holder's generations stayed incremental: one dirty range per churn
    // event, never a full re-stage; the non-holder wrote nothing.
    const manifest = shared.data.get('test-repo|' + ROOT) as {
      revision: string;
    };
    expect(manifest.revision.startsWith(gen1.revision.split('-')[0])).to.equal(
      true,
      'every committed generation must come from the lease holder (tab A)'
    );
    expect(persistenceStats.writeThroughs - writeThroughs0).to.equal(5);
    expect(persistenceStats.rangesHashed - hashed0).to.equal(5);
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeA);
    tabA.dispose();
    tabB.dispose();
  });

  it('lease takeover: survivor adopts the manifest without reading ranges and re-stages once', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const gets: string[] = [];
    const shared = makeFakeIndexedDB({ onGet: key => gets.push(key) });

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;

    // A commits one more generation; B (non-holder) accumulates in memory.
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT); // skipped: not the holder
    await flushAsync();

    // Holder goes away; the lease transfers to B, whose baseline (its boot
    // restore) is now stale — the takeover resolves through the CAS +
    // manifest-only adoption, never a range read.
    tabA.dispose();
    await flushAsync();
    gets.length = 0;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-owns'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT); // CAS conflict -> adopt -> deferred retry
    await flushAsync(30);

    const rangeGets = gets.filter(key => key.includes('#range:'));
    expect(rangeGets).to.deep.equal(
      [],
      'stale-baseline adoption must never read (or decode) range payloads'
    );
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);
    tabB.dispose();
  });

  it('without Web Locks, a CAS conflict adopts manifest-only and defers the retry to the write window', async () => {
    // Node default: no navigator -> leases fail open, CAS is the backstop.
    const gets: string[] = [];
    const puts: string[] = [];
    const shared = makeFakeIndexedDB({
      onGet: key => gets.push(key),
      onPut: key => puts.push(key)
    });
    const WRITE_DELAY = 250;

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const tabB = mkManager(shared.factory, WRITE_DELAY);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;

    // A commits a newer generation; B's baseline is now stale.
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('newer-a'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const writeThroughs0 = persistenceStats.writeThroughs;
    gets.length = 0;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-wins'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT); // conflict -> manifest-only adopt
    puts.length = 0;
    // Plenty of macrotask turns for an immediate (undeferred) retry to have
    // begun STAGING — while staying far under the write window.
    await flushAsync(30);

    // No range payload was read during adoption, and the retry has not even
    // STARTED staging — it waits out the ordinary write window instead of
    // re-flushing back-to-back.
    expect(gets.filter(key => key.includes('#range:'))).to.deep.equal([]);
    expect(puts.filter(key => key.includes('#range:'))).to.deep.equal(
      [],
      'conflict retry must not begin re-staging before the write window elapses'
    );
    expect(persistenceStats.writeThroughs).to.equal(writeThroughs0);

    await new Promise(resolve => setTimeout(resolve, WRITE_DELAY + 20));
    await flushAsync(20);
    expect(persistenceStats.writeThroughs).to.equal(writeThroughs0 + 1);
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);
    tabA.dispose();
    tabB.dispose();
  });

  it('an adopted (undecoded) baseline never serves covering peeks', async () => {
    const shared = makeFakeIndexedDB();
    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;

    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('newer'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();

    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('newer'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT); // conflict -> adopts manifest-only baseline
    await flushAsync(4);

    // A covering peek off the adopted baseline would need rootNode, which is
    // deliberately absent; the peek must fall through to a real read (which
    // resolves the exact subtree) instead of crashing or resolving nothing.
    const record = await tabB.peek(ROOT + '/app-1', 'u1');
    expect(record === null || record.node.isEmpty() === false).to.equal(true);
    tabA.dispose();
    tabB.dispose();
  });
});

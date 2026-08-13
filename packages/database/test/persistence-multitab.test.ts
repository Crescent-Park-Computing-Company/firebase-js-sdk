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
 * Multi-tab write economics: the manager-wide writer lease (Web Locks
 * leader election), heartbeat liveness + steal-on-stale takeover, and the
 * manifest-only stale-baseline adoption.
 *
 * The regression these lock in: two live tabs flushing the same churning
 * root used to leapfrog each other's manifest revisions, and every CAS
 * loser re-read + decoded the ENTIRE stored root, re-staged it in full, and
 * retried immediately — unbounded full-tree work in every tab, which
 * crashed large workspaces and monopolized IndexedDB against boot restores.
 *
 * These tests stub ONLY `navigator` (configurable in every browser). They
 * never redefine `window`/`document` — those are non-configurable own
 * properties in Chrome and Firefox, and the design needs no lifecycle
 * listeners: holder liveness is proven by heartbeat, not lifecycle events.
 */
import { expect } from 'chai';

import { PersistenceManager, persistenceStats } from '../src/core/Persistence';
import { Node } from '../src/core/snap/Node';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';

// ── fake Web Locks manager (exclusive, FIFO, abortable, steal-capable) ──────
function makeFakeWebLocks(events: string[] = []): {
  locks: {
    request: (
      name: string,
      options: { mode: 'exclusive'; signal?: AbortSignal; steal?: boolean },
      callback: (lock: unknown) => Promise<void>
    ) => Promise<void>;
  };
  holders: Map<string, number>;
  queuedCount: (name: string) => number;
  isHeld: (name: string) => boolean;
} {
  interface PendingRequest {
    callback: (lock: unknown) => Promise<void>;
    settle: () => void;
    reject: (error: Error) => void;
  }
  const queues = new Map<string, PendingRequest[]>();
  const busy = new Map<string, PendingRequest>();
  const holders = new Map<string, number>();
  const abortError = () => {
    const error = new Error('The request was aborted.');
    error.name = 'AbortError';
    return error;
  };
  const pump = (name: string) => {
    if (busy.has(name)) {
      return;
    }
    const next = queues.get(name)?.shift();
    if (next === undefined) {
      return;
    }
    busy.set(name, next);
    holders.set(name, (holders.get(name) ?? 0) + 1);
    events.push('grant:' + holders.get(name));
    void Promise.resolve()
      .then(() => next.callback({ name, mode: 'exclusive' }))
      .catch(() => {})
      .then(() => {
        // Settle only the holder that still owns the lock (a stolen
        // holder was already rejected and replaced).
        if (busy.get(name) === next) {
          busy.delete(name);
          next.settle();
          pump(name);
        }
      });
  };
  return {
    locks: {
      request: (name, options, callback) =>
        new Promise<void>((settle, reject) => {
          if (options.steal && options.signal) {
            // Platform contract (verified in Chrome): signal+steal is
            // rejected outright — a steal that carried a signal would
            // silently never steal.
            const error = new Error(
              "The 'signal' and 'steal' options cannot be used together."
            );
            error.name = 'NotSupportedError';
            reject(error);
            return;
          }
          if (!queues.has(name)) {
            queues.set(name, []);
          }
          const pending: PendingRequest = { callback, settle, reject };
          if (options.steal) {
            // Web Locks steal: the held lock is released immediately, the
            // old holder's request promise rejects with AbortError, and the
            // stealing request is granted first.
            const holder = busy.get(name);
            if (holder !== undefined) {
              busy.delete(name);
              holder.reject(abortError());
            }
            queues.get(name)!.unshift(pending);
          } else {
            queues.get(name)!.push(pending);
          }
          options.signal?.addEventListener('abort', () => {
            const queue = queues.get(name);
            const index = queue ? queue.indexOf(pending) : -1;
            if (queue && index !== -1) {
              queue.splice(index, 1);
              reject(abortError());
            }
          });
          pump(name);
        })
    },
    holders,
    queuedCount: name => queues.get(name)?.length ?? 0,
    isHeld: name => busy.has(name)
  };
}

// ── shared heartbeat store (the localStorage seam) ──────────────────────────
function makeFakeHeartbeatStore(): {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  data: Map<string, string>;
} {
  const data = new Map<string, string>();
  return {
    data,
    getItem: key => data.get(key) ?? null,
    setItem: (key, value) => {
      data.set(key, value);
    }
  };
}

// ── minimal in-memory IDBFactory (same shape as persistence.test.ts) ────────
function makeFakeIndexedDB(
  options: {
    onGet?: (key: string) => void;
    onPut?: (key: string, value: unknown) => void;
    onDelete?: (key: string) => void;
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
        options.onDelete?.(key);
      } else if (key && typeof (key as IDBKeyRange).includes === 'function') {
        for (const storedKey of [...data.keys()]) {
          if ((key as IDBKeyRange).includes(storedKey)) {
            data.delete(storedKey);
            options.onDelete?.(storedKey);
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
              options.onDelete?.(key);
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
const MANIFEST_KEY = 'test-repo|' + ROOT;
const LOCK = 'firebase-database-persistence-write|test-repo';
const CHURN_REL = 'app-0/data/entries/entry-0/body';

function mkManager(
  factory: IDBFactory,
  options: {
    writeDelay?: number;
    heartbeatMs?: number;
    staleMs?: number;
    store?: {
      getItem(k: string): string | null;
      setItem(k: string, v: string): void;
    } | null;
  } = {}
): PersistenceManager {
  const manager = new PersistenceManager(
    'test-repo',
    factory,
    true,
    8000,
    100 * 1024 * 1024,
    options.writeDelay ?? 0,
    4 * 1024,
    undefined,
    undefined,
    options.heartbeatMs ?? 60_000,
    options.staleMs ?? 600_000,
    options.store ?? null
  );
  manager.setAuthScope('u1');
  return manager;
}

/** Manifest + every referenced range record present, content readable. */
async function expectSelfContainedAndEqual(
  factory: IDBFactory,
  data: Map<string, unknown>,
  expected: Node
): Promise<void> {
  const manifest = data.get(MANIFEST_KEY) as {
    ranges: Array<{ recordId: string }>;
  };
  expect(manifest).to.not.equal(undefined);
  for (const range of manifest.ranges) {
    expect(data.has(MANIFEST_KEY + '#range:' + range.recordId)).to.equal(true);
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
  // property descriptors. ONLY navigator is ever stubbed — it is a
  // configurable/replaceable property in every engine, unlike
  // window/document.
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
    const gen1 = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(gen1).to.not.equal(undefined);

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    const restored = (await tabB.restoreForListen(ROOT)).record;
    expect(restored).to.not.equal(null);
    let treeB: Node = restored!.node;

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

    const manifest = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(manifest.revision.split('-')[0]).to.equal(
      gen1.revision.split('-')[0],
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

    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT); // skipped: not the holder
    await flushAsync();

    // The read log is cleared BEFORE the dispose: the grant callback arms
    // B's (zero-delay) write window immediately, so the takeover flush
    // itself runs inside the assertion's observation window.
    gets.length = 0;
    tabA.dispose();
    await flushAsync();
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-owns'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
    await flushAsync(30);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);

    const rangeGets = gets.filter(key => key.includes('#range:'));
    expect(rangeGets).to.deep.equal(
      [],
      'stale-baseline adoption must never read (or decode) range payloads'
    );
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);
    tabB.dispose();
  });

  it('a silent holder is stolen from; the stolen holder stops writing and re-queues politely', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();
    const store = makeFakeHeartbeatStore();

    // A heartbeats only at grant (huge interval) — the "suspended holder":
    // its JavaScript may be paused at any time with no lifecycle event at
    // all (Safari/Firefox background suspension, frozen tabs, wedged pages).
    const tabA = mkManager(shared.factory, {
      heartbeatMs: 100_000,
      staleMs: 100_000,
      store
    });
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    const instanceA = (
      shared.data.get(MANIFEST_KEY) as { revision: string }
    ).revision.split('-')[0];
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);

    // B ticks fast and treats a 60ms-stale heartbeat as a dead holder.
    const tabB = mkManager(shared.factory, {
      heartbeatMs: 20,
      staleMs: 60,
      store
    });
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-steals'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);

    // Wait past staleness: B's tick steals, the grant arms B's window, and
    // B commits through the ordinary CAS + adoption path.
    await new Promise(resolve => setTimeout(resolve, 150));
    await flushAsync(30);
    await new Promise(resolve => setTimeout(resolve, 30));
    await flushAsync(30);
    const manifest = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(manifest.revision.split('-')[0]).to.not.equal(
      instanceA,
      'a live tab must steal the lease from a silent holder'
    );
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);

    // The stolen holder's write gate closed and it re-queued POLITELY (one
    // queued request, no counter-steal).
    const writeThroughs0 = persistenceStats.writeThroughs;
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-stale'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(persistenceStats.writeThroughs).to.equal(
      writeThroughs0,
      'a stolen holder must stop writing until the lease is granted again'
    );
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);
    expect(fakeLocks.isHeld(LOCK)).to.equal(true); // B still holds
    tabA.dispose();
    tabB.dispose();
  });

  it("a follower's eviction purge waits for the lease; the holder's generation survives until then", async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    expect((await tabB.restoreForListen(ROOT)).record).to.not.equal(null);

    // B (a follower) is evicted. Its purge must NOT run while A holds the
    // lease and has a live generation — A's lastFlush_ still describes
    // storage and identical rewrites would silently short-circuit.
    tabB.evict(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(
      undefined,
      "a follower's eviction must not delete the holder's live generation"
    );
    // A (the holder) keeps committing normally meanwhile.
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-still-on'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string }).revision
    ).to.be.a('string');

    // The lease transfers to B: the recorded purge executes on grant.
    tabA.dispose();
    await flushAsync(20);
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'the pending eviction purge must execute once the lease is granted'
    );
    tabB.dispose();
  });

  it('re-tracking a root cancels its pending eviction purge', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    const treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    expect((await tabB.restoreForListen(ROOT)).record).to.not.equal(null);
    tabB.evict(ROOT_PATH); // pending purge (B is a follower)
    tabB.track(ROOT); // access restored: the stale intent must be DROPPED,
    tabB.untrack(ROOT); // even if the root is later untracked again
    await flushAsync(8);
    tabA.dispose(); // lease transfers to B
    await flushAsync(20);
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(
      undefined,
      'a re-tracked root must not be purged by a stale eviction intent'
    );
    tabB.dispose();
  });

  it('corrupt-record cleanup deletes only the generation the verdict was reached on', async () => {
    // No Web Locks: the revision-NAMED delete alone must protect a
    // successor generation — no ownership involved. The successor is
    // committed via the onGet hook, which runs synchronously BEFORE the
    // store returns a value: the restore's read (1st manifest get) sees the
    // corrupt R1, and by the time ANY later read or delete touches the
    // manifest, the healthy successor is already committed — exactly the
    // real interleaving (verdict on R1, writer commits R2, cleanup runs).
    let manifestReads = 0;
    let manifestDeleted = false;
    let commitSuccessor: (() => void) | null = null;
    const shared = makeFakeIndexedDB({
      onGet: key => {
        if (key === MANIFEST_KEY && ++manifestReads >= 2) {
          commitSuccessor?.();
        }
      },
      onDelete: key => {
        if (key === MANIFEST_KEY) {
          manifestDeleted = true;
        }
      }
    });

    const writer = mkManager(shared.factory);
    writer.track(ROOT);
    const treeW: Node = nodeFromJSON(makeWorkspace());
    writer.serverCacheUpdated(ROOT_PATH, treeW, undefined);
    await writer.flushNow(ROOT);
    await flushAsync();
    const good = shared.data.get(MANIFEST_KEY) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };

    // Corrupt the stored generation R1: drop one range record.
    const droppedKey = MANIFEST_KEY + '#range:' + good.ranges[0].recordId;
    const droppedRecord = shared.data.get(droppedKey);
    shared.data.delete(droppedKey);
    commitSuccessor = () => {
      commitSuccessor = null;
      shared.data.set(droppedKey, droppedRecord);
      shared.data.set(MANIFEST_KEY, {
        ...(shared.data.get(MANIFEST_KEY) as object),
        revision: 'successor-1'
      });
    };

    const reader = mkManager(shared.factory);
    reader.track(ROOT);
    manifestReads = 0; // count only the reader's reads from here on:
    // read #1 = the restore (sees corrupt R1); read #2 = the cleanup's
    // revision guard — the successor commits synchronously before it.
    const result = await reader.restoreForListen(ROOT);
    expect(result.record).to.equal(null);
    expect(result.reason).to.equal('corrupt');
    await flushAsync(20);
    expect(manifestDeleted).to.equal(
      false,
      'the revision-named cleanup must leave the successor manifest in place'
    );
    const survivorManifest = shared.data.get(MANIFEST_KEY) as {
      revision: string;
    };
    expect(survivorManifest).to.not.equal(
      undefined,
      'a successor generation must survive corrupt-record cleanup'
    );
    expect(survivorManifest.revision).to.equal('successor-1');
    writer.dispose();
    reader.dispose();
  });

  it('invalidate deletes only the restored generation; unnamable baselines delete nothing', async () => {
    const shared = makeFakeIndexedDB();
    const writer = mkManager(shared.factory);
    writer.track(ROOT);
    let treeW: Node = nodeFromJSON(makeWorkspace());
    writer.serverCacheUpdated(ROOT_PATH, treeW, undefined);
    await writer.flushNow(ROOT);
    await flushAsync();
    const r1 = (shared.data.get(MANIFEST_KEY) as { revision: string }).revision;

    // Reader restores R1, then the writer commits R2; the reader's
    // invalidate (named to R1) must not remove R2.
    const reader = mkManager(shared.factory);
    reader.track(ROOT);
    expect((await reader.restoreForListen(ROOT)).record).to.not.equal(null);
    treeW = treeW.updateChild(new Path(CHURN_REL), nodeFromJSON('w2'));
    writer.serverCacheUpdated(ROOT_PATH, treeW, [CHURN_REL.split('/')]);
    await writer.flushNow(ROOT);
    await flushAsync();
    const r2 = (shared.data.get(MANIFEST_KEY) as { revision: string }).revision;
    expect(r2).to.not.equal(r1);
    reader.invalidate(ROOT_PATH);
    await flushAsync(10);
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string }).revision
    ).to.equal(r2, 'invalidate must not remove a successor generation');

    // Named to the CURRENT generation, invalidate does clean up.
    const reader2 = mkManager(shared.factory);
    reader2.track(ROOT);
    expect((await reader2.restoreForListen(ROOT)).record).to.not.equal(null);
    reader2.invalidate(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'invalidate still removes the generation it restored'
    );
    writer.dispose();
    reader.dispose();
    reader2.dispose();
  });

  it('dispose aborts a still-queued lease request', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    await flushAsync();
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);
    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);
    tabB.dispose();
    expect(fakeLocks.queuedCount(LOCK)).to.equal(
      0,
      'a disposed manager must leave the lock queue'
    );
    tabA.dispose();
  });

  it('without Web Locks, a CAS conflict adopts manifest-only and defers the retry to the write window', async () => {
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

    const tabB = mkManager(shared.factory, { writeDelay: WRITE_DELAY });
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;

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
    await flushAsync(30);

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

  it('a window that elapsed mid-conflict defers to the adoption write window', async () => {
    const puts: string[] = [];
    const shared = makeFakeIndexedDB({ onPut: key => puts.push(key) });
    const WRITE_DELAY = 250;

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    const tabB = mkManager(shared.factory, { writeDelay: WRITE_DELAY });
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();

    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b1'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    const inFlight = tabB.flushNow(ROOT); // CAS conflict -> adopt
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b2'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    (tabB as unknown as { scheduleFlush_: (p: string) => void }).scheduleFlush_(
      ROOT
    );
    await inFlight;
    puts.length = 0;
    await flushAsync(10);
    expect(puts.filter(key => key.includes('#range:'))).to.deep.equal(
      [],
      'the queue-drain retry must not begin staging before the write window'
    );
    await new Promise(resolve => setTimeout(resolve, WRITE_DELAY + 20));
    await flushAsync(30);
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

    const record = await tabB.peek(ROOT + '/app-1', 'u1');
    expect(record).to.equal(null);
    const control = await tabA.peek(ROOT + '/app-1', 'u1');
    expect(control).to.not.equal(null);
    expect(control!.node.val(true)).to.deep.equal(
      treeA.getChild(new Path('app-1')).val(true)
    );
    tabA.dispose();
    tabB.dispose();
  });

  it('the ancestor-covered untrack delete only removes generations this manager verified or wrote', async () => {
    const shared = makeFakeIndexedDB();
    const CHILD_PATH = new Path('tabs/file-system/sub-app');
    const CHILD = CHILD_PATH.toString();
    const CHILD_KEY = 'test-repo|' + CHILD;
    const childRel = ['data', 'entries', 'entry-0', 'body'];

    const writer = mkManager(shared.factory);
    writer.track(CHILD);
    let treeW: Node = nodeFromJSON(makeWorkspace());
    writer.serverCacheUpdated(CHILD_PATH, treeW, undefined);
    await writer.flushNow(CHILD);
    await flushAsync();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    tabA.track(CHILD);
    expect((await tabA.restoreForListen(CHILD)).record).to.not.equal(null);

    treeW = treeW.updateChild(new Path(childRel.join('/')), nodeFromJSON('w2'));
    writer.serverCacheUpdated(CHILD_PATH, treeW, [childRel]);
    await writer.flushNow(CHILD);
    await flushAsync();
    const w2 = shared.data.get(CHILD_KEY) as { revision: string };

    tabA.untrack(CHILD);
    await flushAsync(8);
    const survivor = shared.data.get(CHILD_KEY) as { revision: string };
    expect(survivor).to.not.equal(
      undefined,
      "another writer's fresh generation must survive the housekeeping delete"
    );
    expect(survivor.revision).to.equal(w2.revision);

    // An ADOPTED (undecoded) baseline authorizes nothing.
    tabA.track(CHILD);
    let treeB: Node = (await tabA.restoreForListen(CHILD)).record!.node;
    treeW = treeW.updateChild(new Path(childRel.join('/')), nodeFromJSON('w3'));
    writer.serverCacheUpdated(CHILD_PATH, treeW, [childRel]);
    await writer.flushNow(CHILD);
    await flushAsync();
    treeB = treeB.updateChild(new Path(childRel.join('/')), nodeFromJSON('w3'));
    tabA.serverCacheUpdated(CHILD_PATH, treeB, [childRel]);
    await tabA.flushNow(CHILD); // CAS conflict -> adopts (rootNode null)
    tabA.untrack(CHILD); // before the deferred retry's timer fires
    await flushAsync(8);
    expect(shared.data.get(CHILD_KEY)).to.not.equal(
      undefined,
      'an adopted baseline must not authorize the housekeeping delete'
    );

    // The guard must not kill the cleanup: our own generation IS removed.
    writer.untrack(CHILD);
    await flushAsync(8);
    tabA.track(CHILD);
    let treeOwn: Node = (await tabA.restoreForListen(CHILD)).record!.node;
    treeOwn = treeOwn.updateChild(
      new Path(childRel.join('/')),
      nodeFromJSON('a-own')
    );
    tabA.serverCacheUpdated(CHILD_PATH, treeOwn, [childRel]);
    await tabA.flushNow(CHILD);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);
    expect(
      (shared.data.get(CHILD_KEY) as { revision: string }).revision
    ).to.not.equal(w2.revision);
    tabA.untrack(CHILD);
    await flushAsync(8);
    expect(shared.data.get(CHILD_KEY)).to.equal(
      undefined,
      "the manager's own stale shadow is still cleaned up"
    );
    writer.dispose();
    tabA.dispose();
  });

  it('re-arms the write windows when lock acquisition fails open', async () => {
    let rejectRequest: ((error: Error) => void) | null = null;
    stubNavigator({
      locks: {
        request: () =>
          new Promise<void>((_resolve, reject) => {
            rejectRequest = reject;
          })
      }
    });
    const shared = makeFakeIndexedDB();
    const manager = mkManager(shared.factory);
    manager.track(ROOT);
    const tree: Node = nodeFromJSON(makeWorkspace());
    manager.serverCacheUpdated(ROOT_PATH, tree, undefined);
    await manager.flushNow(ROOT); // skipped: request still pending
    await flushAsync();
    expect(shared.data.get(MANIFEST_KEY)).to.equal(undefined);

    rejectRequest!(new Error('lock manager failure'));
    await flushAsync(4);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);
    await expectSelfContainedAndEqual(shared.factory, shared.data, tree);
    manager.dispose();

    // A synchronously-throwing request() must not break track().
    stubNavigator({
      locks: {
        request: () => {
          throw new Error('synchronous lock failure');
        }
      }
    });
    const manager2 = mkManager(shared.factory);
    const ROOT2_PATH = new Path('tabs/file-system-2');
    const ROOT2 = ROOT2_PATH.toString();
    expect(() => manager2.track(ROOT2)).to.not.throw();
    manager2.serverCacheUpdated(ROOT2_PATH, tree, undefined);
    await manager2.flushNow(ROOT2);
    await flushAsync();
    expect(shared.data.get('test-repo|' + ROOT2)).to.not.equal(undefined);
    manager2.dispose();
  });
});

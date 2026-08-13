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

import {
  PersistenceManager,
  persistenceStats,
  _setWebLocksForTesting
} from '../src/core/Persistence';
import { Repo, repoInterrupt, repoResume } from '../src/core/Repo';
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
const LOCK = 'firebase-database-persistence-write|test-repo|' + ROOT;
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
    scope?: string | null;
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
  manager.setAuthScope(options.scope === undefined ? 'u1' : options.scope);
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
  // Every test PINS its lock environment through the injection seam — no
  // global is ever stubbed. The default is lock-less (the CAS-only
  // environment); lease tests opt in with
  // `_setWebLocksForTesting(fakeLocks.locks)`. Pinning matters because the
  // ambient environment differs across runtimes (Node has no navigator,
  // real browsers have REAL Web Locks): the "without Web Locks" family
  // genuinely diverges under real locks (a single writer would be elected,
  // suppressing the CAS conflicts under test), and a REAL lock manager
  // would let one test's manager block a later test's writes. afterEach
  // restores lock-less — never ambient — so the files that run after this
  // suite in the same bundle stay deterministic too.
  beforeEach(() => {
    _setWebLocksForTesting(null);
  });

  afterEach(() => {
    _setWebLocksForTesting(null);
  });

  it('with Web Locks, only the lease holder writes under cross-tab churn', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
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
    _setWebLocksForTesting(fakeLocks.locks);
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
    _setWebLocksForTesting(fakeLocks.locks);
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

  it('eviction purges the revoked record immediately, even while another tab holds the writer lease', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    const treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(undefined);

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    expect((await tabB.restoreForListen(ROOT)).record).to.not.equal(null);

    // B (a follower) is evicted. The purge must NOT wait for the writer
    // lease: A holds it for its tab's lifetime, and if A were not listening
    // to this root it would never receive the revocation — the revoked
    // bytes would outlive the access that produced them for as long as A's
    // tab lived. Same-scope record -> deleted immediately.
    tabB.evict(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'a revoked record must be purged immediately, not behind the lease'
    );
    expect(fakeLocks.isHeld(LOCK)).to.equal(true); // A still the writer
    tabA.dispose();
    tabB.dispose();
  });

  it("eviction leaves another identity's record alone", async () => {
    // No locks needed: the guard under test is the SCOPE check inside the
    // purge transaction. u2's committed generation does not contain u1's
    // revoked bytes (the record was overwritten wholesale), and u2's access
    // is its own — u1's eviction must not delete it.
    const shared = makeFakeIndexedDB();

    const other = mkManager(shared.factory, { scope: 'u2' });
    other.track(ROOT);
    other.serverCacheUpdated(
      ROOT_PATH,
      nodeFromJSON(makeWorkspace()),
      undefined
    );
    await other.flushNow(ROOT);
    await flushAsync();
    other.dispose();
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(undefined);

    const mine = mkManager(shared.factory, { scope: 'u1' });
    mine.track(ROOT);
    mine.evict(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(
      undefined,
      "u1's eviction must not delete u2's record"
    );
    mine.dispose();
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

  it('an empty-tree flush is a CAS commit: it never deletes a successor generation', async () => {
    // Lock-less (CAS-only), mirroring the stolen-holder resume: the lease
    // gate runs BEFORE async work, so by the time the empty-branch
    // transaction executes, another writer may have committed. The delete
    // must CAS against the baseline revision inside that transaction and
    // treat a mismatch exactly like a losing commit (adopt manifest-only,
    // defer to the write window — where the lease gate runs again).
    const shared = makeFakeIndexedDB();

    // A establishes its baseline rev1.
    const tabA = mkManager(shared.factory, { writeDelay: 60_000 });
    tabA.track(ROOT);
    const treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();

    // B commits the successor rev2.
    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('successor'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
    await flushAsync();
    const successor = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(successor).to.not.equal(undefined);

    // A's server tree goes EMPTY; its baseline is stale rev1.
    tabA.serverCacheUpdated(ROOT_PATH, nodeFromJSON(null), undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string })?.revision
    ).to.equal(
      successor.revision,
      'an empty flush with a stale baseline must not delete the successor'
    );
    // ...and it adopted the winner exactly like a losing commit.
    const baselines = (
      tabA as unknown as {
        lastFlush_: Map<string, { rootNode: unknown; revision: string }>;
      }
    ).lastFlush_;
    expect(baselines.get(ROOT)?.rootNode).to.equal(null);
    expect(baselines.get(ROOT)?.revision).to.equal(successor.revision);
    tabA.dispose();
    tabB.dispose();
  });

  it('an empty-tree flush without a baseline adopts a same-scope record instead of deleting it', async () => {
    const shared = makeFakeIndexedDB();

    const writer = mkManager(shared.factory);
    writer.track(ROOT);
    writer.serverCacheUpdated(
      ROOT_PATH,
      nodeFromJSON(makeWorkspace()),
      undefined
    );
    await writer.flushNow(ROOT);
    await flushAsync();
    const committed = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(committed).to.not.equal(undefined);

    // A fresh manager (no baseline: nothing restored, nothing flushed)
    // whose server tree is empty. Mirroring the commit arms, only an
    // absent record or a replaceable-foreign manifest may be removed; a
    // same-scope current-format record is a CAS conflict.
    const fresh = mkManager(shared.factory, { writeDelay: 60_000 });
    fresh.track(ROOT);
    fresh.serverCacheUpdated(ROOT_PATH, nodeFromJSON(null), undefined);
    await fresh.flushNow(ROOT);
    await flushAsync();
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string })?.revision
    ).to.equal(
      committed.revision,
      'a baseline-less empty flush must not delete a same-scope record'
    );
    fresh.dispose();
    writer.dispose();
  });

  it('an empty-tree flush deletes exactly its own baseline generation', async () => {
    const shared = makeFakeIndexedDB();
    const manager = mkManager(shared.factory);
    manager.track(ROOT);
    manager.serverCacheUpdated(
      ROOT_PATH,
      nodeFromJSON(makeWorkspace()),
      undefined
    );
    await manager.flushNow(ROOT);
    await flushAsync();
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(undefined);

    manager.serverCacheUpdated(ROOT_PATH, nodeFromJSON(null), undefined);
    await manager.flushNow(ROOT);
    await flushAsync();
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'the empty generation commits: the owned record is removed'
    );
    for (const key of shared.data.keys()) {
      expect(key.startsWith(MANIFEST_KEY + '#')).to.equal(
        false,
        'range sidecars are removed with the manifest'
      );
    }
    manager.dispose();
  });

  it('disjoint roots in different tabs each get their own writer — no starvation', async () => {
    // THE round-7 shape: tab A tracks only /foo, tab B tracks only /bar.
    // A lock that covers more than the resource it gates (a manager-wide
    // lock) would elect A the writer for EVERYTHING and leave /bar
    // unpersisted for A's whole lifetime — there is no cross-tab relay of
    // B's tree to A, and A never tracks /bar. Per-root locks make the two
    // tabs independent by construction.
    const FOO_PATH = new Path('tabs/foo');
    const BAR_PATH = new Path('tabs/bar');
    const FOO = FOO_PATH.toString();
    const BAR = BAR_PATH.toString();
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(FOO);
    tabA.serverCacheUpdated(FOO_PATH, nodeFromJSON({ a: 1 }), undefined);
    await tabA.flushNow(FOO);
    await flushAsync();

    const tabB = mkManager(shared.factory);
    tabB.track(BAR);
    tabB.serverCacheUpdated(BAR_PATH, nodeFromJSON({ b: 2 }), undefined);
    await tabB.flushNow(BAR);
    await flushAsync();

    expect(shared.data.get('test-repo|' + FOO)).to.not.equal(
      undefined,
      "A's root must persist"
    );
    expect(shared.data.get('test-repo|' + BAR)).to.not.equal(
      undefined,
      "B's root must persist even though A booted first"
    );
    expect(
      fakeLocks.isHeld('firebase-database-persistence-write|test-repo|' + FOO)
    ).to.equal(true);
    expect(
      fakeLocks.isHeld('firebase-database-persistence-write|test-repo|' + BAR)
    ).to.equal(true);
    tabA.dispose();
    tabB.dispose();
  });

  it("untrack flushes the final tree under the lease, then hands the root's lock to the next tab", async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
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

    // A's LAST update, then untrack: the final flush must land (it runs
    // under A's still-held lease), and only then does the lock transfer.
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-final'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    tabA.untrack(ROOT);
    await flushAsync(10);
    const afterFinal = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(afterFinal.revision.split('-')[0]).to.be.a('string');

    // B is granted and becomes the writer without A's tab dying. The
    // release → grant → write-window chain crosses several async hops, so
    // wait for the OUTCOME (a new committed revision), bounded — if the
    // handoff regresses, the loop times out and the assertion fails.
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-next'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    const handedBy = Date.now() + 2000;
    while (Date.now() < handedBy) {
      await tabB.flushNow(ROOT);
      await flushAsync(2);
      const current = shared.data.get(MANIFEST_KEY) as { revision: string };
      if (current.revision !== afterFinal.revision) {
        break;
      }
    }
    const afterB = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(afterB.revision).to.not.equal(
      afterFinal.revision,
      "B must become ROOT's writer once A untracks — no tab-lifetime squat"
    );
    tabA.dispose();
    tabB.dispose();
  });

  it('unnamable-manifest cleanup re-reaches its verdict in the delete transaction', async () => {
    // A manifest too malformed to carry a revision cannot be CAS-named.
    // "No CAS writer produced it" was established by a READONLY read; by
    // the time the cleanup transaction runs, a concurrent writer may have
    // replaced the garbage with a valid generation — the delete must
    // re-read and only remove what is STILL invalid.
    let commitSuccessor: (() => void) | null = null;
    let manifestReads = 0;
    const shared = makeFakeIndexedDB({
      onGet: key => {
        if (key === MANIFEST_KEY && ++manifestReads >= 2) {
          // Fires synchronously before the CLEANUP transaction's re-read
          // returns: the successor is committed between the restore's
          // verdict and the delete — the exact race.
          commitSuccessor?.();
          commitSuccessor = null;
        }
      }
    });
    shared.data.set(MANIFEST_KEY, { formatVersion: 999, garbage: true });

    const writer = mkManager(shared.factory);
    writer.track(ROOT);
    const tree = nodeFromJSON(makeWorkspace());
    commitSuccessor = () => {
      shared.data.set(MANIFEST_KEY, successor);
    };
    // Pre-build a valid successor via a throwaway manager on a private
    // store, then transplant it at the hook.
    const staging = makeFakeIndexedDB();
    const stager = mkManager(staging.factory);
    stager.track(ROOT);
    stager.serverCacheUpdated(ROOT_PATH, tree, undefined);
    await stager.flushNow(ROOT);
    await flushAsync();
    const successor = staging.data.get(MANIFEST_KEY);
    expect(successor).to.not.equal(undefined);
    for (const [key, value] of staging.data) {
      if (key.startsWith(MANIFEST_KEY + '#')) {
        shared.data.set(key, value);
      }
    }
    stager.dispose();

    const reader = mkManager(shared.factory);
    reader.track(ROOT);
    const restored = (await reader.restoreForListen(ROOT)).record;
    expect(restored).to.equal(null); // the garbage was rightly unusable
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      successor,
      'the valid successor committed mid-race must survive the cleanup'
    );
    reader.dispose();
    writer.dispose();

    // And genuinely-still-invalid garbage does get removed.
    const shared2 = makeFakeIndexedDB();
    shared2.data.set(MANIFEST_KEY, { formatVersion: 999, garbage: true });
    const reader2 = mkManager(shared2.factory);
    reader2.track(ROOT);
    expect((await reader2.restoreForListen(ROOT)).record).to.equal(null);
    await flushAsync(10);
    expect(shared2.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'still-invalid garbage is cleaned up'
    );
    reader2.dispose();
  });

  it('eviction preserves a valid anonymous-scope record — null is an identity, not malformation', async () => {
    const shared = makeFakeIndexedDB();

    const anon = mkManager(shared.factory, { scope: null });
    anon.track(ROOT);
    anon.serverCacheUpdated(
      ROOT_PATH,
      nodeFromJSON(makeWorkspace()),
      undefined
    );
    await anon.flushNow(ROOT);
    await flushAsync();
    anon.dispose();
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(undefined);

    const signedIn = mkManager(shared.factory, { scope: 'u1' });
    signedIn.track(ROOT);
    signedIn.evict(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(
      undefined,
      "the anonymous identity's record must survive a signed-in eviction"
    );
    signedIn.dispose();

    // A record whose scope field is ACTUALLY malformed is still purged.
    shared.data.set(MANIFEST_KEY, {
      ...(shared.data.get(MANIFEST_KEY) as object),
      authScope: 42
    });
    const again = mkManager(shared.factory, { scope: 'u1' });
    again.track(ROOT);
    again.evict(ROOT_PATH);
    await flushAsync(10);
    expect(shared.data.get(MANIFEST_KEY)).to.equal(
      undefined,
      'a malformed-scope record is dropped at eviction'
    );
    again.dispose();
  });

  it('an offline holder hands the writer lease to an online follower', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
    const shared = makeFakeIndexedDB();

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    const gen1 = shared.data.get(MANIFEST_KEY) as { revision: string };

    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;

    // A goes deliberately offline: its JS keeps running (it would keep
    // heartbeating), but its server cache is frozen — liveness is not
    // eligibility. The lease must transfer to the online tab.
    tabA.setNetworkSuspended(true);
    await flushAsync(6);
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);
    expect(
      (
        tabB as unknown as { writerLeases_: Map<string, { state: string }> }
      ).writerLeases_.get(ROOT)?.state
    ).to.equal('held', 'the online follower must become the writer');

    // B persists the newer server state A never saw.
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('online-b'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
    await flushAsync();
    const gen2 = shared.data.get(MANIFEST_KEY) as { revision: string };
    expect(gen2.revision).to.not.equal(gen1.revision);

    // The suspended tab is INELIGIBLE, not merely lease-less: its stale
    // tree must not overwrite B's fresh generation.
    const writeThroughs0 = persistenceStats.writeThroughs;
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('stale-a'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync(6);
    expect(persistenceStats.writeThroughs).to.equal(writeThroughs0);
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string }).revision
    ).to.equal(gen2.revision, 'an offline tab must never write');
    tabA.dispose();
    tabB.dispose();
  });

  it('a resumed tab re-queues politely and persists what it saw before suspending', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
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

    // A: one more update lands, then A suspends before its window fires —
    // the pending tree stays in memory and must persist after resume.
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('pre-offline'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    tabA.setNetworkSuspended(true);
    await flushAsync(6); // B is granted

    // Resume: A re-queues BEHIND B — never seizes.
    tabA.setNetworkSuspended(false);
    await flushAsync(4);
    const leasesA = (
      tabA as unknown as { writerLeases_: Map<string, { state: string }> }
    ).writerLeases_;
    expect(leasesA.get(ROOT)?.state).to.equal('requested');
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);

    // B leaves; A is granted and its pre-offline data flushes (bounded).
    const before = (shared.data.get(MANIFEST_KEY) as { revision: string })
      .revision;
    tabB.dispose();
    const flushedBy = Date.now() + 2000;
    while (Date.now() < flushedBy) {
      await flushAsync(2);
      const current = shared.data.get(MANIFEST_KEY) as { revision: string };
      if (current.revision !== before) {
        break;
      }
    }
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeA);
    tabA.dispose();
  });

  it('offline ineligibility holds where Web Locks do not exist; the flag survives a rebind', async () => {
    // CAS-only environment (the suite default): the gate is the ONLY
    // eligibility mechanism, and it must close while suspended — an
    // offline tab's adopt-then-restage would otherwise overwrite an online
    // writer's fresh generation with stale data.
    const shared = makeFakeIndexedDB();
    const manager = mkManager(shared.factory);
    manager.track(ROOT);
    let tree: Node = nodeFromJSON(makeWorkspace());
    manager.serverCacheUpdated(ROOT_PATH, tree, undefined);
    await manager.flushNow(ROOT);
    await flushAsync();
    const gen1 = shared.data.get(MANIFEST_KEY) as { revision: string };

    manager.setNetworkSuspended(true);
    tree = tree.updateChild(new Path(CHURN_REL), nodeFromJSON('while-off'));
    manager.serverCacheUpdated(ROOT_PATH, tree, [CHURN_REL.split('/')]);
    await manager.flushNow(ROOT);
    await flushAsync(4);
    expect(
      (shared.data.get(MANIFEST_KEY) as { revision: string }).revision
    ).to.equal(gen1.revision, 'no writes while suspended, even lock-less');

    // Resume re-arms the window: the data seen while suspended persists.
    manager.setNetworkSuspended(false);
    const flushedBy = Date.now() + 2000;
    while (Date.now() < flushedBy) {
      await flushAsync(2);
      const current = shared.data.get(MANIFEST_KEY) as { revision: string };
      if (current.revision !== gen1.revision) {
        break;
      }
    }
    await expectSelfContainedAndEqual(shared.factory, shared.data, tree);

    // A rebind (auth-scope change) while suspended must stay suspended.
    manager.setNetworkSuspended(true);
    const rebound = manager.rebindTo('test-repo-2');
    expect(
      (rebound as unknown as { networkSuspended_: boolean }).networkSuspended_
    ).to.equal(true, 'rebind must carry the suspension');
    rebound.dispose();
  });

  it('repoInterrupt/repoResume drive persistence eligibility', () => {
    const shared = makeFakeIndexedDB();
    const manager = mkManager(shared.factory);
    manager.track(ROOT);
    const connectionCalls: string[] = [];
    const repo = {
      persistentConnection_: {
        interrupt: (reason: string) => connectionCalls.push('i:' + reason),
        resume: (reason: string) => connectionCalls.push('r:' + reason)
      },
      persistence_: manager
    } as unknown as Repo;

    repoInterrupt(repo);
    expect(
      (manager as unknown as { networkSuspended_: boolean }).networkSuspended_
    ).to.equal(true);
    repoResume(repo);
    expect(
      (manager as unknown as { networkSuspended_: boolean }).networkSuspended_
    ).to.equal(false);
    expect(connectionCalls).to.deep.equal([
      'i:repo_interrupt',
      'r:repo_interrupt'
    ]);
    manager.dispose();
  });

  it('an absent heartbeat never justifies a steal', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
    const shared = makeFakeIndexedDB();
    // One shared underlying map; the HOLDER's store throws on write
    // (storage-disabled document), so no heartbeat is ever stamped. The
    // follower reads the absence and must NOT steal: silence means the
    // liveness protocol is not operating, not that the holder is dead —
    // stealing here would seize from a healthy writer over and over.
    const data = new Map<string, string>();
    const throwingWrites = {
      getItem: (k: string) => data.get(k) ?? null,
      setItem: () => {
        throw new Error('storage denied');
      }
    };
    const readable = {
      getItem: (k: string) => data.get(k) ?? null,
      setItem: (k: string, v: string) => {
        data.set(k, v);
      }
    };

    const holder = mkManager(shared.factory, {
      store: throwingWrites,
      heartbeatMs: 100_000,
      staleMs: 100_000
    });
    holder.track(ROOT);
    holder.serverCacheUpdated(
      ROOT_PATH,
      nodeFromJSON(makeWorkspace()),
      undefined
    );
    await flushAsync();

    const follower = mkManager(shared.factory, {
      store: readable,
      heartbeatMs: 15,
      staleMs: 40
    });
    follower.track(ROOT);
    await new Promise(resolve => setTimeout(resolve, 200));
    await flushAsync();
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);
    expect(
      (
        holder as unknown as {
          writerLeases_: Map<string, { state: string }>;
        }
      ).writerLeases_.get(ROOT)?.state
    ).to.equal('held', 'the healthy holder must keep the lease');
    // The holder's first failed stamp disabled its dead channel.
    expect(
      (holder as unknown as { heartbeatStore_: unknown }).heartbeatStore_
    ).to.equal(null);
    holder.dispose();
    follower.dispose();
  });

  it('a throwing heartbeat read disables stealing for this manager', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
    const shared = makeFakeIndexedDB();

    const holder = mkManager(shared.factory, {
      store: makeFakeHeartbeatStore(),
      heartbeatMs: 100_000,
      staleMs: 100_000
    });
    holder.track(ROOT);
    await flushAsync();

    const denied = {
      getItem: () => {
        throw new Error('storage denied');
      },
      setItem: () => {
        throw new Error('storage denied');
      }
    };
    const follower = mkManager(shared.factory, {
      store: denied,
      heartbeatMs: 15,
      staleMs: 40
    });
    follower.track(ROOT);
    await new Promise(resolve => setTimeout(resolve, 200));
    await flushAsync();
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);
    expect(
      (
        holder as unknown as {
          writerLeases_: Map<string, { state: string }>;
        }
      ).writerLeases_.get(ROOT)?.state
    ).to.equal('held');
    expect(
      (follower as unknown as { heartbeatStore_: unknown }).heartbeatStore_
    ).to.equal(null, 'the first read failure kills the channel for good');
    holder.dispose();
    follower.dispose();
  });

  it('dispose aborts a still-queued lease request', async () => {
    const fakeLocks = makeFakeWebLocks();
    _setWebLocksForTesting(fakeLocks.locks);
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
    // Wait for the ADOPTION itself (bounded), not a fixed number of
    // scheduler turns: the CAS-losing flush settles through IDB callbacks
    // whose macrotask count differs across engines (Firefox needs more
    // rounds than Chrome/Node). If adoption regresses the loop times out
    // and the assertions below fail on the un-adopted state.
    const adoptedBy = Date.now() + 2000;
    const baselines = (
      tabB as unknown as {
        lastFlush_: Map<string, { rootNode: unknown }>;
      }
    ).lastFlush_;
    while (Date.now() < adoptedBy && baselines.get(ROOT)?.rootNode !== null) {
      await flushAsync(1);
    }

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
    _setWebLocksForTesting({
      request: () =>
        new Promise<void>((_resolve, reject) => {
          rejectRequest = reject;
        })
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
    _setWebLocksForTesting({
      request: () => {
        throw new Error('synchronous lock failure');
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

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

// ── minimal fake Web Locks manager (exclusive mode, FIFO grants, abortable
//    pending requests — mirrors the platform contract the manager relies on) ─
function makeFakeWebLocks(events: string[] = []): {
  locks: {
    request: (
      name: string,
      options: { mode: 'exclusive'; signal?: AbortSignal },
      callback: (lock: unknown) => Promise<void>
    ) => Promise<void>;
  };
  holders: Map<string, number>;
  /** Requests waiting in the queue (excludes the current holder). */
  queuedCount: (name: string) => number;
  /** A callback is currently holding the lock. */
  isHeld: (name: string) => boolean;
} {
  interface PendingRequest {
    callback: (lock: unknown) => Promise<void>;
    settle: () => void;
    reject: (error: Error) => void;
  }
  const queues = new Map<string, PendingRequest[]>();
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
    events.push('grant:' + holders.get(name));
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
      request: (name, options, callback) =>
        new Promise<void>((settle, reject) => {
          if (!queues.has(name)) {
            queues.set(name, []);
          }
          const pending: PendingRequest = { callback, settle, reject };
          queues.get(name)!.push(pending);
          // Web Locks contract: aborting the signal drops a request that has
          // not been granted yet and rejects with an AbortError.
          options.signal?.addEventListener('abort', () => {
            const queue = queues.get(name);
            const index = queue ? queue.indexOf(pending) : -1;
            if (queue && index !== -1) {
              queue.splice(index, 1);
              const abortError = new Error('The request was aborted.');
              abortError.name = 'AbortError';
              reject(abortError);
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

// ── minimal document/window event-target stub for page-lifecycle tests ─────
function makeFakeLifecycleTarget(): {
  target: {
    addEventListener: (type: string, handler: (event: unknown) => void) => void;
    removeEventListener: (
      type: string,
      handler: (event: unknown) => void
    ) => void;
  };
  dispatch: (type: string, event?: unknown) => void;
  listenerCount: () => number;
} {
  const listeners = new Map<string, Set<(event: unknown) => void>>();
  return {
    target: {
      addEventListener: (type, handler) => {
        if (!listeners.has(type)) {
          listeners.set(type, new Set());
        }
        listeners.get(type)!.add(handler);
      },
      removeEventListener: (type, handler) => {
        listeners.get(type)?.delete(handler);
      }
    },
    dispatch: (type, event = {}) => {
      for (const handler of [...(listeners.get(type) ?? [])]) {
        handler(event);
      }
    },
    listenerCount: () =>
      [...listeners.values()].reduce((sum, set) => sum + set.size, 0)
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

  let savedDocument: PropertyDescriptor | undefined;
  let savedWindow: PropertyDescriptor | undefined;

  const stubGlobal = (name: 'navigator' | 'document' | 'window', value: unknown) => {
    Object.defineProperty(globalThis, name, {
      value,
      configurable: true,
      writable: true
    });
  };
  const stubNavigator = (value: unknown) => stubGlobal('navigator', value);
  const restoreGlobal = (
    name: 'navigator' | 'document' | 'window',
    descriptor: PropertyDescriptor | undefined
  ) => {
    if (descriptor) {
      Object.defineProperty(globalThis, name, descriptor);
    } else {
      delete (globalThis as Record<string, unknown>)[name];
    }
  };

  beforeEach(() => {
    savedNavigator = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
    savedDocument = Object.getOwnPropertyDescriptor(globalThis, 'document');
    savedWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
  });

  afterEach(() => {
    restoreGlobal('navigator', savedNavigator);
    restoreGlobal('document', savedDocument);
    restoreGlobal('window', savedWindow);
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
    // manifest-only adoption, never a range read. The read log is cleared
    // BEFORE the dispose: the grant callback arms B's (zero-delay) write
    // window immediately, so the takeover flush itself runs inside the
    // observation window of the assertion below.
    gets.length = 0;
    tabA.dispose();
    await flushAsync();
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-owns'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
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

    // A covering peek off the adopted baseline would need rootNode, which
    // is deliberately absent (serving from an undecoded baseline is exactly
    // the decode this path removed). The peek must fall through to the
    // exact-root read — which MISSES, because no record exists at the
    // subtree's own key — without crashing on the null baseline.
    const record = await tabB.peek(ROOT + '/app-1', 'u1');
    expect(record).to.equal(null);
    // Positive control: a DECODABLE baseline (tab A committed its own
    // generation, rootNode real) must keep serving covering peeks.
    const control = await tabA.peek(ROOT + '/app-1', 'u1');
    expect(control).to.not.equal(null);
    expect(control!.node.val(true)).to.deep.equal(
      treeA.getChild(new Path('app-1')).val(true)
    );
    tabA.dispose();
    tabB.dispose();
  });

  it('cancels queued lock requests when a non-holder root untracks', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();
    const LOCK = 'firebase-database-persistence-write|test-repo|' + ROOT;

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    const treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);

    // Listener re-homes untrack and later re-track the same root routinely.
    // Every released-before-grant request must leave the browser's queue —
    // without cancellation each cycle would strand one more queued request
    // (retaining the manager) until the holding tab exits.
    const tabB = mkManager(shared.factory);
    for (let cycle = 0; cycle < 5; cycle++) {
      tabB.track(ROOT);
      expect(fakeLocks.queuedCount(LOCK)).to.equal(1);
      tabB.untrack(ROOT);
      await flushAsync(4); // untrack releases after its final flush settles
      expect(fakeLocks.queuedCount(LOCK)).to.equal(
        0,
        'released-before-grant requests must leave the lock queue'
      );
    }

    // Cancellation must not fail the lease open: a re-tracked root still
    // defers to the current holder.
    tabB.track(ROOT);
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    const writeThroughs0 = persistenceStats.writeThroughs;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-waits'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
    await flushAsync();
    expect(persistenceStats.writeThroughs).to.equal(writeThroughs0);

    // The holder exits: the surviving (re-tracked) request is granted and
    // the survivor becomes the writer.
    tabA.dispose();
    await flushAsync();
    await tabB.flushNow(ROOT);
    await flushAsync(30);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);
    expect(persistenceStats.writeThroughs).to.be.greaterThan(writeThroughs0);
    tabB.dispose();
  });

  it('a frozen holder releases the lease; a live tab takes over; resume re-queues', async () => {
    const fakeLocks = makeFakeWebLocks();
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB();
    const LOCK = 'firebase-database-persistence-write|test-repo|' + ROOT;

    // Each "tab" gets its own document/window lifecycle target; the manager
    // captures the globals when it installs its handlers (first lease).
    const lifecycleA = makeFakeLifecycleTarget();
    stubGlobal('document', lifecycleA.target);
    stubGlobal('window', lifecycleA.target);
    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    let treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    const gen1 = shared.data.get('test-repo|' + ROOT) as { revision: string };
    const instanceA = gen1.revision.split('-')[0];
    expect(fakeLocks.isHeld(LOCK)).to.equal(true);

    const lifecycleB = makeFakeLifecycleTarget();
    stubGlobal('document', lifecycleB.target);
    stubGlobal('window', lifecycleB.target);
    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);

    // Tab A freezes without untrack()/dispose() ever running. Its held lock
    // must be released so live tabs are not starved of writes indefinitely.
    lifecycleA.dispatch('freeze');
    await flushAsync(4);
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-takes-over'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    await tabB.flushNow(ROOT);
    await flushAsync(30);
    const manifest = shared.data.get('test-repo|' + ROOT) as {
      revision: string;
    };
    expect(manifest.revision.split('-')[0]).to.not.equal(
      instanceA,
      'a live tab must take the lease over from a frozen holder'
    );
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);

    // The frozen page resumes: it re-queues behind the current holder
    // (never seizes the lock back), and takes over again when B exits.
    lifecycleA.dispatch('resume');
    await flushAsync(4);
    expect(fakeLocks.queuedCount(LOCK)).to.equal(1);
    tabB.dispose();
    await flushAsync(4);
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-again'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync(30);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);
    const finalManifest = shared.data.get('test-repo|' + ROOT) as {
      revision: string;
    };
    expect(finalManifest.revision.split('-')[0]).to.equal(instanceA);

    // dispose removes the lifecycle handlers it installed.
    tabA.dispose();
    expect(lifecycleA.listenerCount()).to.equal(0);
  });

  it('evict holds the lease through its queued delete; a successor commits only after it', async () => {
    const events: string[] = [];
    const fakeLocks = makeFakeWebLocks(events);
    stubNavigator({ locks: fakeLocks.locks });
    const shared = makeFakeIndexedDB({
      onDelete: key => events.push('del:' + key)
    });
    const MANIFEST_KEY = 'test-repo|' + ROOT;

    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    const treeA: Node = nodeFromJSON(makeWorkspace());
    tabA.serverCacheUpdated(ROOT_PATH, treeA, undefined);
    await tabA.flushNow(ROOT);
    await flushAsync();
    expect(shared.data.get(MANIFEST_KEY)).to.not.equal(undefined);

    // Tab B queues behind A and already has pending data to write.
    const tabB = mkManager(shared.factory);
    tabB.track(ROOT);
    let treeB: Node = (await tabB.restoreForListen(ROOT)).record!.node;
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b-pending'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);

    // A's access is revoked WHILE a slow flush of A's is still in flight —
    // the queued delete lands behind it, holding the hazard window open.
    // (A fresh-identity tree forces a full restage: many macrotasks.)
    tabA.serverCacheUpdated(ROOT_PATH, nodeFromJSON(makeWorkspace()), undefined);
    void tabA.flushNow(ROOT); // deliberately not awaited: in flight at evict
    // The purge is unconditional, but the lease must be held THROUGH the
    // queued delete: B may only be granted (and commit) after the delete
    // has run, so the delete can never erase B's fresh generation.
    events.length = 0;
    tabA.evict(ROOT_PATH);
    await flushAsync(30);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);

    const deleteIndex = events.indexOf('del:' + MANIFEST_KEY);
    const grantIndex = events.findIndex(e => e.startsWith('grant:'));
    expect(deleteIndex).to.not.equal(-1, 'the evict purge must run');
    expect(grantIndex).to.not.equal(-1, 'the successor must be granted');
    expect(deleteIndex).to.be.lessThan(
      grantIndex,
      'the lease must not transfer until the queued delete settled'
    );
    // B then committed a fresh, self-contained generation that survived.
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);
    tabB.dispose();
    tabA.dispose();
  });

  it('the ancestor-covered untrack delete only removes generations this manager verified or wrote', async () => {
    // No Web Locks (Node default): the revision guard alone must protect a
    // follower's housekeeping delete from erasing another writer's fresh
    // generation — no lease ordering involved.
    const shared = makeFakeIndexedDB();
    const CHILD_PATH = new Path('tabs/file-system/sub-app');
    const CHILD = CHILD_PATH.toString();
    const CHILD_KEY = 'test-repo|' + CHILD;
    const childRel = ['data', 'entries', 'entry-0', 'body'];

    // Writer W owns the child root and commits generation W1.
    const writer = mkManager(shared.factory);
    writer.track(CHILD);
    let treeW: Node = nodeFromJSON(makeWorkspace());
    writer.serverCacheUpdated(CHILD_PATH, treeW, undefined);
    await writer.flushNow(CHILD);
    await flushAsync();

    // Tab A tracks the ancestor AND the child, and restored W1.
    const tabA = mkManager(shared.factory);
    tabA.track(ROOT);
    tabA.track(CHILD);
    expect((await tabA.restoreForListen(CHILD)).record).to.not.equal(null);

    // W commits a NEWER generation W2; A's knowledge (W1) is now stale.
    treeW = treeW.updateChild(new Path(childRel.join('/')), nodeFromJSON('w2'));
    writer.serverCacheUpdated(CHILD_PATH, treeW, [childRel]);
    await writer.flushNow(CHILD);
    await flushAsync();
    const w2 = shared.data.get(CHILD_KEY) as { revision: string };

    // A untracks the child (covered by the tracked ancestor). Its
    // housekeeping delete must NOT erase W2 — a generation it never saw.
    tabA.untrack(CHILD);
    await flushAsync(8);
    const survivor = shared.data.get(CHILD_KEY) as { revision: string };
    expect(survivor).to.not.equal(
      undefined,
      'another writer\'s fresh generation must survive the housekeeping delete'
    );
    expect(survivor.revision).to.equal(w2.revision);

    // An ADOPTED (undecoded) baseline carries W's revision for content A
    // never saw — it must not authorize the delete either.
    tabA.track(CHILD);
    let treeB: Node = (await tabA.restoreForListen(CHILD)).record!.node;
    treeW = treeW.updateChild(new Path(childRel.join('/')), nodeFromJSON('w3'));
    writer.serverCacheUpdated(CHILD_PATH, treeW, [childRel]);
    await writer.flushNow(CHILD);
    await flushAsync();
    treeB = treeB.updateChild(new Path(childRel.join('/')), nodeFromJSON('w3'));
    tabA.serverCacheUpdated(CHILD_PATH, treeB, [childRel]);
    await tabA.flushNow(CHILD); // CAS conflict -> adopts W's manifest (rootNode null)
    // Untrack synchronously, BEFORE the deferred retry's timer fires: the
    // baseline is still the adopted (undecoded) one, and the store still
    // carries W's generation — exactly the state that must not be deleted.
    tabA.untrack(CHILD);
    await flushAsync(8);
    expect(shared.data.get(CHILD_KEY)).to.not.equal(
      undefined,
      'an adopted baseline must not authorize the housekeeping delete'
    );

    // The guard must not kill the cleanup either: a generation this manager
    // itself wrote IS removed when it untracks under a covering ancestor.
    writer.untrack(CHILD); // W leaves; A becomes the sole writer
    await flushAsync(8);
    tabA.track(CHILD);
    let treeOwn: Node = (await tabA.restoreForListen(CHILD)).record!.node;
    treeOwn = treeOwn.updateChild(new Path(childRel.join('/')), nodeFromJSON('a-own'));
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
      'the manager\'s own stale shadow is still cleaned up'
    );
    writer.dispose();
    tabA.dispose();
  });

  it('a window that elapsed mid-conflict defers to the adoption write window', async () => {
    // Node default (no Web Locks): the CAS + debounced adoption is the only
    // protection. An update whose write window ELAPSES while the losing
    // flush is still in flight marks the queue pending; the drain must NOT
    // re-flush immediately — that is back-to-back full-tree staging under
    // sustained lease-less churn, the exact thrash the debounce prevents.
    const puts: string[] = [];
    const shared = makeFakeIndexedDB({ onPut: key => puts.push(key) });
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
    treeA = treeA.updateChild(new Path(CHURN_REL), nodeFromJSON('a-newer'));
    tabA.serverCacheUpdated(ROOT_PATH, treeA, [CHURN_REL.split('/')]);
    await tabA.flushNow(ROOT);
    await flushAsync();

    // B's conflicting flush is IN FLIGHT when a second update's window
    // elapses (scheduleFlush_ is exactly what the timer runs; the queue is
    // busy, so it marks flushPending_).
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b1'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    const inFlight = tabB.flushNow(ROOT); // CAS conflict -> adopt
    treeB = treeB.updateChild(new Path(CHURN_REL), nodeFromJSON('b2'));
    tabB.serverCacheUpdated(ROOT_PATH, treeB, [CHURN_REL.split('/')]);
    (
      tabB as unknown as { scheduleFlush_: (p: string) => void }
    ).scheduleFlush_(ROOT);
    await inFlight;
    puts.length = 0;
    await flushAsync(10); // ample for an immediate (undeferred) retry to stage
    expect(puts.filter(key => key.includes('#range:'))).to.deep.equal(
      [],
      'the queue-drain retry must not begin staging before the write window'
    );

    // After the window: exactly one deferred retry, carrying the LATEST
    // tree — the update that marked the queue pending is not lost.
    await new Promise(resolve => setTimeout(resolve, WRITE_DELAY + 20));
    await flushAsync(30);
    await expectSelfContainedAndEqual(shared.factory, shared.data, treeB);
    tabA.dispose();
    tabB.dispose();
  });

  it('re-arms the write window when lock acquisition fails open', async () => {
    // A lock manager that rejects (non-abort) AFTER the write window has
    // already fired and been skipped.
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
    expect(shared.data.get('test-repo|' + ROOT)).to.equal(undefined);

    // The request now fails for a non-cancellation reason. Failing open
    // must restore the consumed write window, not just drop the gate.
    rejectRequest!(new Error('lock manager failure'));
    await flushAsync(4);
    await new Promise(resolve => setTimeout(resolve, 20));
    await flushAsync(30);
    await expectSelfContainedAndEqual(shared.factory, shared.data, tree);
    manager.dispose();

    // A synchronously-throwing request() must not break track(), and the
    // root still persists through the fail-open gate.
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

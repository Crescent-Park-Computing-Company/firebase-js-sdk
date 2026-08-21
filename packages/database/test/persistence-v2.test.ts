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

import {
  QueryImpl,
  ValueEventRegistration,
  onValue,
  off
} from '../src/api/Reference_impl';
import {
  Repo,
  newIngestQueue,
  repoGetValue,
  repoOnConnectStatusForTest,
  repoStartServerListen,
  repoStopServerListen
} from '../src/core/Repo';
import { RowPersistenceManager } from '../src/core/RowPersistence';
import { ListenWireResult } from '../src/core/ServerActions';
import {
  ListenHashFn,
  getNodeCanonicalHash,
  getNodeCompoundHash
} from '../src/core/ServerCacheSeed';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { SnapshotHolder } from '../src/core/SnapshotHolder';
import { newSparseSnapshotTree } from '../src/core/SparseSnapshotTree';
import {
  SyncTree,
  syncTreeAddEventRegistration,
  syncTreeApplyServerOverwrite,
  syncTreeGetCompleteServerCache,
  syncTreeRemoveEventRegistration
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { Tree } from '../src/core/util/Tree';
import { EventQueue } from '../src/core/view/EventQueue';
import {
  QueryParams,
  queryParamsLimitToFirst
} from '../src/core/view/QueryParams';

import { makeFakeIdb, flushMicrotasks, wait } from './helpers/fakeIdb';

function makeManager(idb: IDBFactory): RowPersistenceManager {
  const manager = new RowPersistenceManager(
    'test-repo',
    idb,
    null, // Node: no Web Locks -> always writer
    1,
    1,
    512,
    30000,
    300000,
    1000,
    1 << 20
  );
  manager.setAuthScope('alice');
  return manager;
}

/**
 * The minimal Repo surface the listen flow touches. `listen` records calls;
 * restore resolution is controlled by the manager's fake IndexedDB.
 */
function makeListenHarness(manager: RowPersistenceManager) {
  const calls: string[] = [];
  const hashFns: ListenHashFn[] = [];
  const serverCallbacks: Array<
    (status: string, wire?: Partial<ListenWireResult>) => void
  > = [];
  const getResponders: Array<(payload: unknown) => void> = [];
  const infoSyncTree = new SyncTree({
    startListening: () => [],
    stopListening: () => {}
  });
  const repo = {
    pendingSeedRestores_: new Map<string, { cancelled: boolean }>(),
    ingestQueue_: newIngestQueue(),
    listenOutcomes_: new Map(),
    transactionQueueTree_: new Tree(),
    persistence_: manager,
    persistenceAuthScope_: 'alice',
    persistenceAuthScopeListeners_: new Set<() => void>(),
    eventQueue_: new EventQueue(),
    infoData_: new SnapshotHolder(),
    infoSyncTree_: infoSyncTree,
    onDisconnect_: newSparseSnapshotTree(),
    serverSyncTree_: new SyncTree({
      startListening: () => [],
      stopListening: () => {}
    }),
    server_: {
      listen: (
        _query: unknown,
        hashFn: ListenHashFn,
        _tag: unknown,
        onListen: (
          status: string,
          data: unknown,
          wire: ListenWireResult
        ) => void
      ) => {
        calls.push('listen');
        hashFns.push(hashFn);
        serverCallbacks.push((status, overrides = {}) =>
          onListen(status, null, {
            bytes: 0,
            hadHash: hashFn() !== '',
            hadCompoundHash: hashFn.compoundHash?.() !== undefined,
            dataReceived: false,
            rangeMerged: false,
            ...overrides
          })
        );
      },
      unlisten: () => calls.push('unlisten'),
      get: () => {
        calls.push('get');
        return new Promise(resolve => {
          getResponders.push(resolve);
        });
      }
    }
  } as unknown as Repo;
  const path = new Path('users/alice');
  manager.setPersistentPath(path.toString(), true);
  const query = {
    _path: path,
    _queryParams: { loadsAllData: () => true },
    _queryIdentifier: 'default'
  } as never;
  const hashFn = (() => '') as never;
  const onComplete = (() => []) as never;
  return {
    repo,
    query,
    path,
    hashFn,
    onComplete,
    calls,
    hashFns,
    serverCallbacks,
    getResponders
  };
}

/**
 * An event registration whose events actually raise — the event queue
 * dereferences every event it queues, so a null-returning createEvent stub
 * would abort the raise inside the restore flow's apply.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function stubRegistration(): any {
  return {
    respondsTo: () => true,
    createEvent: (_change: unknown, query: { _path: Path }) => ({
      getPath: () => query._path,
      getEventType: () => 'value',
      getEventRunner: () => () => {},
      toString: () => 'stub-event'
    }),
    getEventRunner: () => () => {},
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    createCancelEvent: () => null as any,
    matches: () => false,
    hasAnyCallback: () => true
  };
}

/**
 * Registers the root's default view WITHOUT starting a wire listen (the
 * harness SyncTree's startListening is a stub; the test drives
 * repoStartServerListen directly, as the real one would have).
 */
function registerRootView(h: { repo: Repo; path: Path }): void {
  syncTreeAddEventRegistration(
    h.repo.serverSyncTree_,
    new QueryImpl(h.repo, h.path, new QueryParams(), false),
    stubRegistration()
  );
}

async function persistRoot(
  manager: RowPersistenceManager,
  path: Path,
  json: unknown
): Promise<void> {
  manager.track(path.toString());
  manager.serverCacheUpdated(path, nodeFromJSON(json));
  await manager.flushNow(path.toString());
  await flushMicrotasks();
}

/** Waits until `calls` contains `expected` occurrences of 'listen'. */
async function waitForListen(calls: string[], expected = 1): Promise<void> {
  for (
    let i = 0;
    i < 200 && calls.filter(c => c === 'listen').length < expected;
    i++
  ) {
    await wait(5);
  }
  expect(calls.filter(c => c === 'listen').length).to.be.at.least(expected);
}

describe('repo listen flow (v2: restore -> apply -> hash -> listen)', () => {
  it('restores the cached base, applies it, then sends the listen with h:"" + ch', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), {
        docs: { a: 'x'.repeat(300), b: 'y'.repeat(300) }
      });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    registerRootView(h);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    // The listen is NOT sent synchronously — restore + hash come first.
    expect(h.calls).to.deep.equal([]);
    await waitForListen(h.calls);

    // The base was applied to the SyncTree BEFORE the listen went out.
    const cache = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    );
    expect(cache).to.not.equal(null);
    expect((cache!.val() as { docs: object }).docs).to.not.equal(undefined);

    // The applied node carries the wire claim (what the real SyncTree
    // hashFn reads at send time): h:'' plus the stored compound hash.
    expect(getNodeCanonicalHash(cache!)).to.equal('');
    const ch = getNodeCompoundHash(cache!);
    expect(ch).to.not.equal(undefined);
    expect(ch!.hashes.length).to.equal(ch!.posts.length + 1);
    manager.dispose();
  });

  it('cold path: no cache sends one plain listen immediately-ish', async () => {
    const manager = makeManager(makeFakeIdb());
    const h = makeListenHarness(manager);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    await waitForListen(h.calls);
    expect(h.hashFns[0]()).to.equal('');
    expect(h.hashFns[0].compoundHash?.()).to.equal(undefined);
    // Nothing applied to the SyncTree.
    expect(
      syncTreeGetCompleteServerCache(h.repo.serverSyncTree_, h.path)
    ).to.equal(null);
    manager.dispose();
  });

  it('grafts a certified descendant over the restored base and flushes before hashing', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), {
        left: { a: 'stale-left' },
        right: { b: 'stale-right' }
      });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    registerRootView(h);
    // A deeper listen's certified answer lands before the root restore.
    syncTreeAddEventRegistration(
      h.repo.serverSyncTree_,
      new QueryImpl(
        h.repo,
        new Path('users/alice/left'),
        new QueryParams(),
        false
      ),
      stubRegistration()
    );
    syncTreeApplyServerOverwrite(
      h.repo.serverSyncTree_,
      new Path('users/alice/left'),
      nodeFromJSON({ a: 'fresh-left' })
    );

    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    await waitForListen(h.calls);

    // The applied base carries the graft, not the stale bytes.
    const cache = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    )!;
    expect(cache.getChild(new Path('left')).val()).to.deep.equal({
      a: 'fresh-left'
    });
    expect(cache.getChild(new Path('right')).val()).to.deep.equal({
      b: 'stale-right'
    });

    // The stored rows were flushed to include the graft BEFORE hashing:
    // the claimed compound hash equals the applied tree's own hash.
    const ch = getNodeCompoundHash(cache);
    expect(ch).to.not.equal(undefined);
    const restored = await manager.restoreForListen(h.path.toString());
    expect(restored.node!.equals(cache)).to.equal(true);
    manager.dispose();
  });

  it('a get() answered during the restore window does not install into the SyncTree', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), { k: 'stale' });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    registerRootView(h);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    // While the restore is pending, a get() at a child path resolves.
    const childQuery = new QueryImpl(
      h.repo,
      new Path('users/alice/k'),
      new QueryParams(),
      false
    );
    const getPromise = repoGetValue(
      h.repo,
      childQuery,
      new ValueEventRegistration({
        onValue: () => {},
        callback: () => {}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any)
    );
    expect(h.getResponders.length).to.equal(1);
    h.getResponders[0]('fresh');
    const value = await getPromise;
    // Caller sees fresh data...
    expect(value.val()).to.equal('fresh');
    await waitForListen(h.calls);
    // ...but the SyncTree holds the consistent restored base.
    const cache = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    )!;
    expect(cache.val()).to.deep.equal({ k: 'stale' });
    manager.dispose();
  });

  it('stop-listen during a pending restore cancels without ever sending', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), { k: 1 });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    // Cancel while the restore is in flight.
    repoStopServerListen(h.repo, h.query, null);
    await wait(60);
    await flushMicrotasks();
    // Neither a listen nor an unlisten went out (nothing was on the wire).
    expect(h.calls).to.deep.equal([]);
    manager.dispose();
  });

  it('a server-certified root skips the restore (cold)', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), { k: 'stored' });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    // The root is already certified in the SyncTree before the listen starts.
    registerRootView(h);
    syncTreeApplyServerOverwrite(
      h.repo.serverSyncTree_,
      h.path,
      nodeFromJSON({ k: 'live' })
    );
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    await waitForListen(h.calls);
    const cache = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    )!;
    expect(cache.val()).to.deep.equal({ k: 'live' });
    manager.dispose();
  });
});

describe('persistent listener options (v2)', () => {
  function makeQueryHarness() {
    const manager = makeManager(makeFakeIdb());
    const syncTree = new SyncTree({
      startListening: () => [],
      stopListening: () => {}
    });
    const repo = {
      persistence_: manager,
      serverSyncTree_: syncTree,
      infoSyncTree_: syncTree,
      eventQueue_: new EventQueue(),
      pendingSeedRestores_: new Map()
    } as unknown as Repo;
    const path = new Path('selected/root');
    const query = new QueryImpl(repo, path, new QueryParams(), false);
    return { manager, path, query, syncTree };
  }

  it('selects before subscribe and releases with the returned unsubscribe', () => {
    const { manager, path, query } = makeQueryHarness();
    const unsubscribe = onValue(query, () => {}, undefined, {
      persistent: true
    });
    expect(manager.isPersistentPath(path.toString())).to.equal(true);
    unsubscribe();
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
    manager.dispose();
  });

  it('releases selection when off() removes the registration', () => {
    const { manager, path, query } = makeQueryHarness();
    const callback = () => {};
    onValue(query, callback, { persistent: true });
    off(query, 'value', callback);
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
    manager.dispose();
  });

  it('rejects persistence on a filtered query', () => {
    const { manager, path, query } = makeQueryHarness();
    const filtered = new QueryImpl(
      query._repo,
      path,
      queryParamsLimitToFirst(new QueryParams(), 1),
      false
    );
    expect(() => onValue(filtered, () => {}, { persistent: true })).to.throw(
      /complete, unfiltered/i
    );
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
    manager.dispose();
  });

  it('reference-counts registrations at the same path', () => {
    const { manager, path, query } = makeQueryHarness();
    const unsubscribeA = onValue(query, () => {}, { persistent: true });
    const unsubscribeB = onValue(query, () => {}, { persistent: true });
    unsubscribeA();
    expect(manager.isPersistentPath(path.toString())).to.equal(true);
    unsubscribeB();
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
    manager.dispose();
  });

  it('activates persistence when joining an existing non-persistent listen', async () => {
    const { manager, path, query, syncTree } = makeQueryHarness();
    onValue(query, () => {});
    syncTreeApplyServerOverwrite(syncTree, path, nodeFromJSON({ a: 1 }));
    expect(manager.trackedRootFor(path.toString())).to.equal(null);

    onValue(query, () => {}, { persistent: true });
    expect(manager.trackedRootFor(path.toString())).to.equal(path.toString());
    await manager.flushNow(path.toString());
    await flushMicrotasks();
    const record = await manager.peek(path.toString(), 'alice');
    expect(record?.node.val()).to.deep.equal({ a: 1 });
    manager.dispose();
  });

  it('releases selection when the SDK cancels every registration', () => {
    const { manager, path, query, syncTree } = makeQueryHarness();
    onValue(query, () => {}, { persistent: true });
    expect(manager.isPersistentPath(path.toString())).to.equal(true);
    syncTreeRemoveEventRegistration(
      syncTree,
      query,
      null,
      new Error('permission denied')
    );
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
    manager.dispose();
  });
});

describe('reconnect hash preparation (v2)', () => {
  it('disconnect stamps h:"" synchronously and the compound hash after the off-thread walk', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), {
        docs: { a: 'x'.repeat(300) }
      });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    registerRootView(h);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    await waitForListen(h.calls);

    // A server update replaces the boot-stamped node: the new cache node
    // carries no stamps, so an immediate reconnect hashFn would fall back
    // to the synchronous full-tree walk.
    syncTreeApplyServerOverwrite(
      h.repo.serverSyncTree_,
      new Path('users/alice/docs/b'),
      nodeFromJSON('fresh')
    );
    manager.serverCacheUpdated(
      h.path,
      syncTreeGetCompleteServerCache(h.repo.serverSyncTree_, h.path)!,
      [['docs', 'b']]
    );
    const cacheBefore = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    )!;
    expect(getNodeCanonicalHash(cacheBefore)).to.equal(undefined);

    // Disconnect: '' is stamped SYNCHRONOUSLY (no walk can happen from
    // here), the pending dirt flushes, and the compound hash lands async.
    repoOnConnectStatusForTest(h.repo, false);
    expect(getNodeCanonicalHash(cacheBefore)).to.equal('');
    await wait(40);
    await flushMicrotasks();
    const ch = getNodeCompoundHash(cacheBefore);
    expect(ch).to.not.equal(undefined);
    // The claimed hash equals the stored rows, which equal the live cache.
    const restored = await manager.restoreForListen(h.path.toString());
    expect(restored.node!.equals(cacheBefore)).to.equal(true);
    manager.dispose();
  });
});

describe('getPersistedValue → listener handoff (v2, public API path)', () => {
  it('the peek and the authenticated restore share one physical decode', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), {
        a: { deep: 'value' },
        b: 42
      });
      writer.dispose();
    }
    const manager = makeManager(idb);
    // Pre-auth peek primes the scope (getPersistedValue's manager calls).
    const peeked = await manager.peek('/users/alice', 'alice');
    expect(peeked).to.not.equal(null);
    expect(manager.hasRetainedPeek('/users/alice', peeked!.node)).to.equal(
      true
    );
    // The authenticated listener's restore consumes the SAME decode.
    const restored = await manager.restoreForListen('/users/alice');
    expect(restored.node).to.equal(peeked!.node);
    manager.dispose();
  });
});

describe('joined persistent rejoin during untrack drain', () => {
  it('a persistent re-subscribe that joins a live plain listener revives the root', async () => {
    const { manager, path, query } = (function makeQueryHarness() {
      const m = makeManager(makeFakeIdb());
      const syncTree = new SyncTree({
        startListening: () => [],
        stopListening: () => {}
      });
      const repo = {
        persistence_: m,
        serverSyncTree_: syncTree,
        infoSyncTree_: syncTree,
        eventQueue_: new EventQueue(),
        pendingSeedRestores_: new Map()
      } as unknown as Repo;
      const p = new Path('selected/root');
      const q = new QueryImpl(repo, p, new QueryParams(), false);
      return { manager: m, path: p, query: q };
    })();
    // A plain listener keeps the view alive the whole time.
    onValue(query, () => {});
    // First persistent registration joins the live listen.
    const unsub = onValue(query, () => {}, { persistent: true });
    expect(manager.trackedPaths()).to.deep.equal([path.toString()]);
    // Remove + re-add in one stack: the release untracks (async drain),
    // the rejoin takes the joined-listen activation path — which must
    // cancel the pending teardown, not assume "already tracked".
    unsub();
    onValue(query, () => {}, { persistent: true });
    await flushMicrotasks();
    await wait(10);
    await flushMicrotasks();
    expect(manager.trackedPaths()).to.deep.equal([path.toString()]);
    manager.dispose();
  });
});

describe('warm boot stays certified under boot-time write-throughs', () => {
  it('a certification write-through during the restore window does not downgrade the claim', async () => {
    const idb = makeFakeIdb();
    {
      const writer = makeManager(idb);
      await persistRoot(writer, new Path('users/alice'), {
        docs: { a: 'x'.repeat(300) }
      });
      writer.dispose();
    }
    const manager = makeManager(idb);
    const h = makeListenHarness(manager);
    registerRootView(h);
    repoStartServerListen(h.repo, h.query, null, h.hashFn, h.onComplete);
    // While the restore/hash runs, a listen certification re-states known
    // state ([] = nothing changed) — the exact write-through every deeper
    // certified listener produces during a busy boot.
    manager.serverCacheUpdated(
      h.path,
      nodeFromJSON({ docs: { a: 'x'.repeat(300) } }),
      []
    );
    await waitForListen(h.calls);
    const cache = syncTreeGetCompleteServerCache(
      h.repo.serverSyncTree_,
      h.path
    )!;
    // The claim survived: '' + compound hash stamped (a 'restored' boot,
    // range merges — NOT an uncertified full resend).
    expect(getNodeCanonicalHash(cache)).to.equal('');
    expect(getNodeCompoundHash(cache)).to.not.equal(undefined);
    manager.dispose();
  });
});

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

import { getPersistedValue } from '../src/api/Database';
import { QueryImpl } from '../src/api/Reference_impl';
import { PersistenceManager, PersistedRecord } from '../src/core/Persistence';
import {
  repoStartServerListen,
  repoStopServerListen,
  repoWhenListenComplete,
  Repo
} from '../src/core/Repo';
import {
  computeCanonicalHash,
  computeCompoundHash
} from '../src/core/ServerCacheSeed';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import {
  SyncTree,
  syncTreeAddEventRegistration,
  syncTreeApplyServerOverwrite,
  syncTreeGetCompleteServerCache
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { EventQueue } from '../src/core/view/EventQueue';
import { QueryParams } from '../src/core/view/QueryParams';

/**
 * A minimal in-memory IDBFactory covering exactly the calls the manager
 * makes (open → objectStore get/put/delete, upgrade path). Callbacks fire on
 * a microtask, mirroring IndexedDB's async contract.
 */
function makeFakeIndexedDB(options: { startWithoutStore?: boolean } = {}): {
  factory: IDBFactory;
  data: Map<string, unknown>;
} {
  const data = new Map<string, unknown>();
  // Mirrors real IndexedDB semantics closely enough for the manager: object
  // stores exist only once created in a version-change transaction, and a
  // versioned open above the current version fires onupgradeneeded.
  const state = { version: 1, hasStore: !options.startWithoutStore };
  const async = (fn: () => void) => {
    void Promise.resolve().then(fn);
  };
  const makeRequest = (result: unknown) => {
    const req: {
      result: unknown;
      onsuccess: null | (() => void);
      onerror: null | (() => void);
    } = { result, onsuccess: null, onerror: null };
    async(() => req.onsuccess && req.onsuccess());
    return req;
  };
  const store = {
    get: (key: string) => makeRequest(data.get(key)),
    put: (value: unknown, key: string) => {
      data.set(key, value);
      return makeRequest(undefined);
    },
    delete: (key: string) => {
      data.delete(key);
      return makeRequest(undefined);
    },
    openCursor: () => {
      // Walks a snapshot of the entries, one onsuccess per row then null,
      // mirroring IndexedDB's cursor contract closely enough for the sweep.
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
    objectStoreNames: {
      contains: () => state.hasStore
    },
    createObjectStore: () => {
      state.hasStore = true;
      return store;
    },
    transaction: () => {
      if (!state.hasStore) {
        throw new Error('NotFoundError: object store not found');
      }
      const t = { ...tx };
      // Requests complete on microtasks; the transaction completes on a
      // macrotask — after every request issued against it, as in real
      // IndexedDB.
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
      } = {
        result: null,
        onupgradeneeded: null,
        onsuccess: null,
        onerror: null,
        onblocked: null
      };
      async(() => {
        const upgrading = version !== undefined && version > state.version;
        if (upgrading) {
          state.version = version!;
        }
        req.result = makeDb();
        if (upgrading && req.onupgradeneeded) {
          req.onupgradeneeded();
        }
        // createObjectStore during upgrade mutates state; rebuild so the
        // handed-out db reflects it.
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

function flushAsync(): Promise<void> {
  // Several macrotask turns: the manager's transactions complete on
  // macrotasks (see the fake above), and one operation may chain multiple
  // transactions (open -> read -> delete; data put -> hash put).
  let chain = Promise.resolve();
  for (let i = 0; i < 8; i++) {
    chain = chain.then(
      () => new Promise<void>(resolve => setTimeout(resolve, 0))
    );
  }
  return chain;
}

describe('PersistenceManager', () => {
  it('restores what a flush persisted, hashes coupled to the tree', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const json = { a: 'x', b: { c: 1 } };
    const node = nodeFromJSON(json);
    const path = new Path('some/root');

    manager.track(path.toString());
    manager.serverCacheUpdated(path, node);
    await manager.flushNow(path.toString());
    await flushAsync();

    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored).to.not.equal(null);
    expect(restored.json).to.deep.equal(json);
    expect(restored.hash).to.equal(computeCanonicalHash(json));
    expect(restored.compoundHash).to.deep.equal(computeCompoundHash(json));
  });

  it('resolves null for a root never persisted', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    expect(await manager.restore('missing/root')).to.equal(null);
  });

  it('expired records are dropped on restore', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    data.set('test-repo|/old/root', {
      json: { a: 1 },
      updatedAt: Date.now() - 15 * 24 * 60 * 60 * 1000,
      revision: 1
    });
    expect(await manager.restore('/old/root')).to.equal(null);
    await flushAsync();
    expect(data.has('test-repo|/old/root')).to.equal(false);
  });

  it('a superseding update never leaves the stored pair uncoupled', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('busy/root');
    manager.track(path.toString());

    manager.serverCacheUpdated(path, nodeFromJSON({ v: 1 }));
    const first = manager.flushNow(path.toString());
    // Supersede while the first flush's hash recompute is in flight; its
    // flush queues behind the first (one writer per root).
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 2 }));
    const second = manager.flushNow(path.toString());
    await first;
    await second;
    await flushAsync();

    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    // The stored record is the SECOND tree with the SECOND tree's hash.
    expect(restored.json).to.deep.equal({ v: 2 });
    expect(restored.hash).to.equal(computeCanonicalHash({ v: 2 }));
  });

  it('a burst within the debounce flushes the newest tree', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('hot/root');
    manager.track(path.toString());
    // Two updates back-to-back — the flush reads latest_ when it runs, so a
    // single flush persists the second tree.
    manager.serverCacheUpdated(path, nodeFromJSON({ n: 1 }));
    manager.serverCacheUpdated(path, nodeFromJSON({ n: 2 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.json).to.deep.equal({ n: 2 });
    expect(restored.hash).to.equal(computeCanonicalHash({ n: 2 }));
  });

  it('evict during an in-flight flush deletes both records', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('gone/racing');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ secret: 1 }));
    const flushing = manager.flushNow(path.toString());
    // Evict before the flush's hash record lands: the delete queues behind
    // the flush, so nothing survives.
    manager.evict(path);
    await flushing;
    await flushAsync();
    expect(data.has('test-repo|/gone/racing')).to.equal(false);
    expect(data.has('test-repo|/gone/racing#hash')).to.equal(false);
  });

  it('evict removes the stored record', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('gone/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ secret: true }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(data.has('test-repo|/gone/root')).to.equal(true);

    manager.evict(path);
    await flushAsync();
    expect(data.has('test-repo|/gone/root')).to.equal(false);
  });

  it('trackedRootFor maps descendants to their root, not prefixes', () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    manager.track('/users/alice');
    expect(manager.trackedRootFor('/users/alice')).to.equal('/users/alice');
    expect(manager.trackedRootFor('/users/alice/settings')).to.equal(
      '/users/alice'
    );
    expect(manager.trackedRootFor('/users/alicelong')).to.equal(null);
    expect(manager.trackedRootFor('/users')).to.equal(null);
  });

  it('repairs a database created without the object store', async () => {
    // A versionless open by unrelated tooling can leave the database existing
    // with no store; the manager must reopen a version up and create it.
    const { factory, data } = makeFakeIndexedDB({ startWithoutStore: true });
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('repaired/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ ok: true }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(data.has('test-repo|/repaired/root')).to.equal(true);
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.json).to.deep.equal({ ok: true });
  });

  it('degrades to cold loads without IndexedDB', async () => {
    const manager = new PersistenceManager('test-repo', null);
    const path = new Path('no/idb');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ a: 1 }));
    await manager.flushNow(path.toString());
    expect(await manager.restore(path.toString())).to.equal(null);
  });

  it('untrack flushes the final tree, keeps the record, drops the memory', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('rotated/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ kept: true }));

    manager.untrack(path.toString());
    await flushAsync();

    // The debounced write-through still landed…
    expect(data.has('test-repo|/rotated/root')).to.equal(true);
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.json).to.deep.equal({ kept: true });
    // …but the root no longer flows through persistence: a later update is
    // ignored, proving both the tracking and the retained tree are gone.
    manager.serverCacheUpdated(path, nodeFromJSON({ kept: false }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(
      ((await manager.restore(path.toString())) as PersistedRecord).json
    ).to.deep.equal({ kept: true });
  });

  it('sweeps expired records for its own prefix on first open', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const expired = Date.now() - 15 * 24 * 60 * 60 * 1000;
    data.set('test-repo|/old/root', {
      json: { stale: true },
      updatedAt: expired,
      revision: 1
    });
    data.set('test-repo|/fresh/root', {
      json: { fresh: true },
      updatedAt: Date.now(),
      revision: 1
    });
    data.set('other-repo|/old/root', {
      json: { foreign: true },
      updatedAt: expired,
      revision: 1
    });

    const manager = new PersistenceManager('test-repo', factory);
    // Any operation triggers the first open, which chains the sweep.
    await manager.restore('/fresh/root');
    await flushAsync();
    await flushAsync();

    expect(data.has('test-repo|/old/root')).to.equal(false);
    expect(data.has('test-repo|/fresh/root')).to.equal(true);
    // Another manager's records are not this manager's to expire.
    expect(data.has('other-repo|/old/root')).to.equal(true);
  });
});

describe('repoStartServerListen / repoStopServerListen', () => {
  /**
   * The minimal Repo surface the two functions touch. `listen` records calls;
   * restore resolution is controlled by the manager's fake IndexedDB.
   */
  function makeListenHarness() {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const calls: string[] = [];
    const serverCallbacks: Array<(status: string) => void> = [];
    const repo = {
      pendingSeedRestores_: new Map<string, { cancelled: boolean }>(),
      listenCompletions_: new Map<
        string,
        { complete: boolean; waiters: Array<() => void> }
      >(),
      persistence_: manager,
      eventQueue_: new EventQueue(),
      serverSyncTree_: new SyncTree({
        startListening: () => [],
        stopListening: () => {}
      }),
      server_: {
        listen: (
          _query: unknown,
          _hashFn: unknown,
          _tag: unknown,
          onListen: (status: string) => void
        ) => {
          calls.push('listen');
          serverCallbacks.push(onListen);
        },
        unlisten: (...args: unknown[]) => calls.push('unlisten')
      }
    } as unknown as Repo;
    const path = new Path('users/alice');
    const query = {
      _path: path,
      _queryParams: { loadsAllData: () => true }
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
      serverCallbacks,
      data
    };
  }

  it('sends the listen after the restore resolves', async () => {
    const { repo, query, hashFn, onComplete, calls } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    expect(calls).to.deep.equal([]);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
  });

  it('a stop during the restore cancels the listen instead of orphaning it', async () => {
    const { repo, query, hashFn, onComplete, calls } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    repoStopServerListen(repo, query, null);
    await flushAsync();
    // Neither sent nor unlistened: the listen never existed on the wire.
    expect(calls).to.deep.equal([]);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
    // And the root left tracking with the live listens gone.
    expect(
      repo.persistence_!.trackedRootFor(new Path('users/alice').toString())
    ).to.equal(null);
  });

  it('a normal stop unlistens and untracks the root', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    repoStopServerListen(repo, query, null);
    expect(calls).to.deep.equal(['listen', 'unlisten']);
    // Untracked: a subsequent server update no longer flows to storage.
    expect(repo.persistence_!.trackedRootFor(path.toString())).to.equal(null);
  });

  it('a hashless record is hash-primed and still sends the listen', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    // A record persisted before its hash landed (no #hash sibling).
    data.set('test-repo|' + path.toString(), {
      json: { a: 1 },
      updatedAt: Date.now(),
      revision: 1
    });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
  });

  it('whenListenComplete resolves on the server response, not the restore', async () => {
    const { repo, query, path, hashFn, onComplete, serverCallbacks } =
      makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    let settled = false;
    void repoWhenListenComplete(repo, path.toString()).then(() => {
      settled = true;
    });
    // The restore resolves and the listen goes out — still not complete.
    await flushAsync();
    expect(settled).to.equal(false);
    serverCallbacks[0]('ok');
    await flushAsync();
    expect(settled).to.equal(true);
    // Already complete: a late waiter resolves immediately.
    let late = false;
    void repoWhenListenComplete(repo, path.toString()).then(() => {
      late = true;
    });
    await flushAsync();
    expect(late).to.equal(true);
  });

  it('whenListenComplete resolves when the listen stops first', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    let settled = false;
    void repoWhenListenComplete(repo, path.toString()).then(() => {
      settled = true;
    });
    repoStopServerListen(repo, query, null);
    await flushAsync();
    expect(settled).to.equal(true);
  });

  it('whenListenComplete resolves immediately with no listen at all', async () => {
    const { repo } = makeListenHarness();
    let settled = false;
    void repoWhenListenComplete(repo, '/nowhere').then(() => {
      settled = true;
    });
    await flushAsync();
    expect(settled).to.equal(true);
  });
});

describe('stale restore vs live server data', () => {
  it('a restore that loses the race does not clobber certified data', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('users/alice');
    // A record from the last session…
    data.set('test-repo|/users/alice', {
      json: { stale: true },
      updatedAt: Date.now(),
      revision: 1
    });

    const calls: string[] = [];
    const syncTree = new SyncTree({
      startListening: () => [],
      stopListening: () => {}
    });
    const repo = {
      pendingSeedRestores_: new Map<string, { cancelled: boolean }>(),
      listenCompletions_: new Map<
        string,
        { complete: boolean; waiters: Array<() => void> }
      >(),
      persistence_: manager,
      eventQueue_: new EventQueue(),
      serverSyncTree_: syncTree,
      server_: {
        listen: () => calls.push('listen'),
        unlisten: () => calls.push('unlisten')
      }
    } as unknown as Repo;
    const query = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    const registration = {
      respondsTo: () => true,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createEvent: () => null as any,
      getEventRunner: () => () => {},
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createCancelEvent: () => null as any,
      matches: () => false,
      hasAnyCallback: () => true
    };
    syncTreeAddEventRegistration(syncTree, query, registration);

    repoStartServerListen(
      repo,
      query,
      null,
      (() => '') as never,
      (() => []) as never
    );
    // …but the server certifies fresher data while the restore is in flight.
    syncTreeApplyServerOverwrite(syncTree, path, nodeFromJSON({ live: true }));
    await flushAsync();

    const cache = syncTreeGetCompleteServerCache(syncTree, path);
    expect(cache).to.not.equal(null);
    expect(cache!.val(true)).to.deep.equal({ live: true });
  });
});

describe('getPersistedValue', () => {
  function makeDatabaseWithPersistence(): {
    db: unknown;
    manager: PersistenceManager;
  } {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const repo = { persistence_: manager };
    const db = {
      _checkNotDeleted: () => {},
      _repoInternal: repo
      // getModularInstance returns objects without _delegate untouched.
    };
    return { db, manager };
  }

  it('drills a subpath out of the nearest persisted root', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('users/alice');
    manager.track(root.toString());
    manager.serverCacheUpdated(
      root,
      nodeFromJSON({ settings: { theme: 'dark' }, name: 'alice' })
    );
    await manager.flushNow(root.toString());
    await flushAsync();

    expect(
      await getPersistedValue(db as never, '/users/alice/settings/theme')
    ).to.equal('dark');
    expect(await getPersistedValue(db as never, '/users/alice')).to.deep.equal({
      settings: { theme: 'dark' },
      name: 'alice'
    });
    expect(
      await getPersistedValue(db as never, '/users/alice/missing/deep')
    ).to.equal(null);
    expect(await getPersistedValue(db as never, '/users/bob')).to.equal(null);
  });

  it('unwraps export-format records to snapshot.val() semantics', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('prio/root');
    manager.track(root.toString());
    // A prioritized leaf persists as {'.value': ..., '.priority': ...}
    // (export format); the peek must return the plain value.
    const node = nodeFromJSON({ a: { '.value': 42, '.priority': 7 }, b: 'x' });
    manager.serverCacheUpdated(root, node);
    await manager.flushNow(root.toString());
    await flushAsync();

    expect(await getPersistedValue(db as never, '/prio/root/a')).to.equal(42);
    expect(await getPersistedValue(db as never, '/prio/root')).to.deep.equal({
      a: 42,
      b: 'x'
    });
  });
});

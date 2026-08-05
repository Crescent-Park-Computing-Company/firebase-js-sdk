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
import {
  PersistenceManager,
  PersistedRecord,
  PERSISTENCE_CHUNK_TARGET_BYTES
} from '../src/core/Persistence';
import {
  repoSettleListenCompletions,
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
import {
  QueryParams,
  queryParamsLimitToFirst
} from '../src/core/view/QueryParams';

/**
 * A minimal in-memory IDBFactory covering exactly the calls the manager
 * makes (open → objectStore get/put/delete, upgrade path). Callbacks fire on
 * a microtask, mirroring IndexedDB's async contract. `onPut` (when given)
 * runs synchronously as each put lands — the seam tests use to interleave
 * work at exact write boundaries.
 */
function makeFakeIndexedDB(
  options: {
    startWithoutStore?: boolean;
    onPut?: (key: string, value: unknown) => void;
  } = {}
): {
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
    // Mirrors real IDBRequest semantics: reading `result` before the request
    // completes throws InvalidStateError.
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
    get: (key: string) => makeRequest(data.get(key)),
    put: (value: unknown, key: string) => {
      data.set(key, value);
      if (options.onPut) {
        options.onPut(key, value);
      }
      return makeRequest(undefined);
    },
    delete: (key: string) => {
      // Key-range deletes (used only as an orphan-cleanup optimization)
      // no-op here: the fake predates IDBKeyRange in Node.
      if (typeof key === 'string') {
        data.delete(key);
      }
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
  // transactions (open -> read -> delete; chunked data put -> hash put).
  let chain = Promise.resolve();
  for (let i = 0; i < 8; i++) {
    chain = chain.then(
      () => new Promise<void>(resolve => setTimeout(resolve, 0))
    );
  }
  return chain;
}

/** The stored keys for a root, in key order — manifest, chunks, hash. */
function keysFor(data: Map<string, unknown>, root: string): string[] {
  return [...data.keys()].filter(k => k === root || k.startsWith(root + '#'));
}

/** A string that forces its subtree over the chunk target on its own. */
function bigLeaf(seed: string): string {
  return seed.repeat(Math.ceil(PERSISTENCE_CHUNK_TARGET_BYTES / seed.length));
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
    expect(restored.node.val(true)).to.deep.equal(json);
    expect(restored.hash).to.equal(computeCanonicalHash(json));
    expect(restored.compoundHash).to.deep.equal(computeCompoundHash(json));
  });

  it('splits a large root into chunks and reassembles it exactly', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    // Three children over the chunk target each, one small, plus priorities
    // on a split node and on a leaf — every planner path in one tree.
    const json = {
      big1: { deep: { '.value': bigLeaf('a'), '.priority': 1 } },
      big2: { x: bigLeaf('b'), y: bigLeaf('c'), '.priority': 'p' },
      small: { s: 1 }
    };
    const node = nodeFromJSON(json);
    const path = new Path('chunked/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, node);
    await manager.flushNow(path.toString());
    await flushAsync();

    const chunkKeys = keysFor(data, 'test-repo|/chunked/root').filter(k =>
      k.includes('#c')
    );
    expect(chunkKeys.length).to.be.greaterThan(1);

    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal(node.val(true));
    expect(restored.hash).to.equal(node.hash());
  });

  it('rewrites only the chunks an update dirtied', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('incremental/root');
    const v1 = nodeFromJSON({
      big1: bigLeaf('a'),
      big2: bigLeaf('b'),
      small: 'v1'
    });
    manager.track(path.toString());
    manager.serverCacheUpdated(path, v1);
    await manager.flushNow(path.toString());
    await flushAsync();

    const before = new Map(
      keysFor(data, 'test-repo|/incremental/root').map(k => [k, data.get(k)])
    );
    // Immutable update: the untouched big subtrees keep their identity.
    const v2 = v1.updateChild(new Path('small'), nodeFromJSON('v2'));
    manager.serverCacheUpdated(path, v2);
    await manager.flushNow(path.toString());
    await flushAsync();

    let rewritten = 0;
    let kept = 0;
    for (const key of keysFor(data, 'test-repo|/incremental/root')) {
      if (key.includes('#c')) {
        if (data.get(key) === before.get(key)) {
          kept++;
        } else {
          rewritten++;
        }
      }
    }
    // The big chunks were skipped by identity; only small's chunk rewrote.
    expect(kept).to.be.greaterThan(0);
    expect(rewritten).to.equal(1);

    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal(v2.val(true));
    expect(restored.hash).to.equal(computeCanonicalHash(v2.val(true)));
  });

  it('a restored-then-certified unchanged tree flushes nothing', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const managerA = new PersistenceManager('test-repo', factory);
    const path = new Path('warm/root');
    managerA.track(path.toString());
    managerA.serverCacheUpdated(path, nodeFromJSON({ steady: true }));
    await managerA.flushNow(path.toString());
    await flushAsync();

    // Next session: restore, then the listen 'ok' write-through hands the
    // SAME node back (the sync tree holds the seeded tree by reference).
    const managerB = new PersistenceManager('test-repo', factory);
    const restored = (await managerB.restore(
      path.toString()
    )) as PersistedRecord;
    const before = new Map(
      keysFor(data, 'test-repo|/warm/root').map(k => [k, data.get(k)])
    );
    managerB.track(path.toString());
    managerB.serverCacheUpdated(path, restored.node);
    await managerB.flushNow(path.toString());
    await flushAsync();

    for (const [key, value] of before) {
      expect(data.get(key)).to.equal(value);
    }
    expect(keysFor(data, 'test-repo|/warm/root').length).to.equal(
      before.size
    );
  });

  it('an unchanged tree with an aging manifest refreshes the manifest alone', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const oldUpdatedAt = Date.now() - 2 * 24 * 60 * 60 * 1000; // 2 days
    const json = { steady: true };
    data.set('test-repo|/aging/root', {
      revision: 'ext-1',
      updatedAt: oldUpdatedAt,
      chunkCount: 1,
      chunkRevisions: ['ext-1']
    });
    data.set('test-repo|/aging/root#c000000', {
      revision: 'ext-1',
      entries: [['', json]]
    });
    data.set('test-repo|/aging/root#hash', {
      hash: computeCanonicalHash(json),
      compoundHash: computeCompoundHash(json),
      updatedAt: oldUpdatedAt,
      revision: 'ext-1'
    });

    const manager = new PersistenceManager('test-repo', factory);
    const restored = (await manager.restore(
      new Path('aging/root').toString()
    )) as PersistedRecord;
    const chunkBefore = data.get('test-repo|/aging/root#c000000');
    const hashBefore = data.get('test-repo|/aging/root#hash');

    manager.track(new Path('aging/root').toString());
    manager.serverCacheUpdated(new Path('aging/root'), restored.node);
    await manager.flushNow(new Path('aging/root').toString());
    await flushAsync();

    // Chunks and hash untouched — and still joined, because the refreshed
    // manifest kept its revision.
    expect(data.get('test-repo|/aging/root#c000000')).to.equal(chunkBefore);
    expect(data.get('test-repo|/aging/root#hash')).to.equal(hashBefore);
    const manifest = data.get('test-repo|/aging/root') as {
      revision: string;
      updatedAt: number;
    };
    expect(manifest.revision).to.equal('ext-1');
    expect(manifest.updatedAt).to.be.greaterThan(oldUpdatedAt);

    const again = (await manager.restore(
      new Path('aging/root').toString()
    )) as PersistedRecord;
    expect(again.node.val(true)).to.deep.equal(json);
    expect(again.hash).to.equal(computeCanonicalHash(json));
  });

  it('restores a legacy monolithic record, hashes joined', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const json = { legacy: { a: 1 } };
    data.set('test-repo|/legacy/root', {
      json,
      updatedAt: Date.now(),
      revision: 'oldtab-1'
    });
    data.set('test-repo|/legacy/root#hash', {
      hash: computeCanonicalHash(json),
      compoundHash: computeCompoundHash(json),
      updatedAt: Date.now(),
      revision: 'oldtab-1'
    });
    const manager = new PersistenceManager('test-repo', factory);
    const restored = (await manager.restore('/legacy/root')) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal(json);
    expect(restored.hash).to.equal(computeCanonicalHash(json));
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
      revision: 'ext-1'
    });
    expect(await manager.restore('/old/root')).to.equal(null);
    await flushAsync();
    expect(data.has('test-repo|/old/root')).to.equal(false);
  });

  it('an interrupted chunk write restores as a miss, never a stitched tree', async () => {
    const { factory, data } = makeFakeIndexedDB();
    // Manifest expects two chunks of revision ext-2, but chunk 1 still
    // carries an older write's token (interrupted mid-write).
    data.set('test-repo|/torn/root', {
      revision: 'ext-2',
      updatedAt: Date.now(),
      chunkCount: 2,
      chunkRevisions: ['ext-2', 'ext-2']
    });
    data.set('test-repo|/torn/root#c000000', {
      revision: 'ext-2',
      entries: [['a', 1]]
    });
    data.set('test-repo|/torn/root#c000001', {
      revision: 'ext-1',
      entries: [['b', 2]]
    });
    const manager = new PersistenceManager('test-repo', factory);
    expect(await manager.restore('/torn/root')).to.equal(null);
    await flushAsync();
    // The torn leftovers were evicted.
    expect(keysFor(data, 'test-repo|/torn/root').length).to.equal(0);
  });

  it('a superseding update never leaves the stored pair uncoupled', async () => {
    // The interleaving under test: v2 arrives AFTER flush #1 snapshotted v1
    // (its manifest just landed) but BEFORE its hash recompute finishes. A
    // flush that re-read latest_ when writing the hash record would pair
    // v2's hash with v1's data.
    const path = new Path('busy/root');
    let interleave: (() => void) | null = null;
    const { factory } = makeFakeIndexedDB({
      onPut: key => {
        if (key === 'test-repo|/busy/root' && interleave !== null) {
          const run = interleave;
          interleave = null;
          run();
        }
      }
    });
    const manager = new PersistenceManager('test-repo', factory);
    interleave = () => manager.serverCacheUpdated(path, nodeFromJSON({ v: 2 }));
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 1 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(interleave).to.equal(null); // the hook fired at the manifest put

    // Flush #1's pair: v1 data with v1's hash — the superseding update never
    // bled into it.
    const mid = (await manager.restore(path.toString())) as PersistedRecord;
    expect(mid.node.val(true)).to.deep.equal({ v: 1 });
    expect(mid.hash).to.equal(computeCanonicalHash({ v: 1 }));

    // And flush #2 (still throttled) then writes the v2 pair.
    await manager.flushNow(path.toString());
    await flushAsync();
    const final = (await manager.restore(path.toString())) as PersistedRecord;
    expect(final.node.val(true)).to.deep.equal({ v: 2 });
    expect(final.hash).to.equal(computeCanonicalHash({ v: 2 }));
  });

  it('a surviving hash sidecar from another session never pairs with new data', async () => {
    const { factory, data } = makeFakeIndexedDB();
    // Session 1 left a data+hash pair. Simulate its token.
    data.set('test-repo|/shared/root', {
      json: { old: true },
      updatedAt: Date.now(),
      revision: 'oldtab-1'
    });
    data.set('test-repo|/shared/root#hash', {
      hash: computeCanonicalHash({ old: true }),
      compoundHash: computeCompoundHash({ old: true }),
      updatedAt: Date.now(),
      revision: 'oldtab-1'
    });
    // Session 2 (this manager) writes a NEW tree; its manifest put atomically
    // deletes the old sidecar, so even before its own hash lands the store
    // can never say "new data, old hash".
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('shared/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ fresh: true }));
    await manager.flushNow(path.toString());
    await flushAsync();
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ fresh: true });
    expect(restored.hash).to.equal(computeCanonicalHash({ fresh: true }));
  });

  it('a burst within the throttle window flushes the newest tree', async () => {
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
    expect(restored.node.val(true)).to.deep.equal({ n: 2 });
    expect(restored.hash).to.equal(computeCanonicalHash({ n: 2 }));
  });

  it('a throttle firing into a busy queue coalesces to one trailing flush', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const path = new Path('slow/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 1 }));
    const first = manager.flushNow(path.toString());
    // While flush #1 runs, the throttle fires repeatedly (simulated by
    // driving the private scheduler the timer calls): every firing beyond
    // the first must fold into ONE pending flush, not queue its own.
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 2 }));
    interface WithScheduler {
      scheduleFlush_(p: string): void;
      queues_: Map<string, Promise<void>>;
      flushPending_: Set<string>;
    }
    const internals = manager as unknown as WithScheduler;
    internals.scheduleFlush_(path.toString());
    internals.scheduleFlush_(path.toString());
    internals.scheduleFlush_(path.toString());
    expect(internals.flushPending_.size).to.equal(1);
    await first;
    await flushAsync();
    // The one pending flush drained and wrote v2.
    expect(internals.flushPending_.size).to.equal(0);
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ v: 2 });
  });

  it('evict during an in-flight flush deletes every record', async () => {
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
    expect(keysFor(data, 'test-repo|/gone/racing').length).to.equal(0);
  });

  it('evict removes the stored records and the tracking', async () => {
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
    expect(keysFor(data, 'test-repo|/gone/root').length).to.equal(0);
    // The root left tracking too: SyncTree never calls stopListening for a
    // server-revoked listen, so evict is the only untrack it will get —
    // otherwise later updates under a live ancestor would keep routing to
    // this dead root.
    expect(manager.trackedRootFor('/gone/root')).to.equal(null);
    manager.serverCacheUpdated(path, nodeFromJSON({ secret: 2 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(keysFor(data, 'test-repo|/gone/root').length).to.equal(0);
  });

  it('trackedRootFor maps descendants to their NEAREST root', () => {
    const { factory } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    manager.track('/users/alice');
    expect(manager.trackedRootFor('/users/alice')).to.equal('/users/alice');
    expect(manager.trackedRootFor('/users/alice/settings')).to.equal(
      '/users/alice'
    );
    expect(manager.trackedRootFor('/users/alicelong')).to.equal(null);
    expect(manager.trackedRootFor('/users')).to.equal(null);
    // Nested roots: the deepest containing root wins, not the first added.
    manager.track('/users');
    expect(manager.trackedRootFor('/users/alice/settings')).to.equal(
      '/users/alice'
    );
    expect(manager.trackedRootFor('/users/bob')).to.equal('/users');
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
    expect(restored.node.val(true)).to.deep.equal({ ok: true });
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

    // The throttled write-through still landed…
    expect(data.has('test-repo|/rotated/root')).to.equal(true);
    const restored = (await manager.restore(
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ kept: true });
    // …but the root no longer flows through persistence: a later update is
    // ignored, proving both the tracking and the retained tree are gone.
    manager.serverCacheUpdated(path, nodeFromJSON({ kept: false }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(
      (
        (await manager.restore(path.toString())) as PersistedRecord
      ).node.val(true)
    ).to.deep.equal({ kept: true });
  });

  it('untrack under a live tracked ancestor deletes the child record', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', factory);
    const child = new Path('users/alice/inbox');
    manager.track(child.toString());
    manager.serverCacheUpdated(child, nodeFromJSON({ msg: 1 }));
    await manager.flushNow(child.toString());
    await flushAsync();
    expect(data.has('test-repo|/users/alice/inbox')).to.equal(true);

    // An ancestor listen takes over (SyncTree stops the shadowed child
    // listen): the ancestor's record contains this subtree and keeps
    // flushing — a frozen child record would only shadow it on the next
    // session's peeks.
    manager.track('/users/alice');
    manager.untrack(child.toString());
    await flushAsync();
    expect(keysFor(data, 'test-repo|/users/alice/inbox').length).to.equal(0);
  });

  it('restoreNearest prefers the freshest containing record', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const hourAgo = Date.now() - 60 * 60 * 1000;
    // A child record frozen an hour ago (its listen stopped)…
    data.set('test-repo|/users/alice/inbox', {
      json: { msg: 'stale' },
      updatedAt: hourAgo,
      revision: 'ext-1'
    });
    // …and the still-flushing ancestor's fresher tree.
    data.set('test-repo|/users/alice', {
      json: { inbox: { msg: 'fresh' }, name: 'alice' },
      updatedAt: Date.now(),
      revision: 'ext-2'
    });
    const manager = new PersistenceManager('test-repo', factory);
    const result = await manager.restoreNearest('/users/alice/inbox');
    expect(result).to.not.equal(null);
    expect(result!.root).to.equal('/users/alice');
    expect(result!.record.node.getChild(new Path('inbox/msg')).val()).to.equal(
      'fresh'
    );
  });

  it('sweeps expired and orphaned records for its own prefix', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const expired = Date.now() - 15 * 24 * 60 * 60 * 1000;
    // An expired chunked root: manifest, chunk, and hash all go.
    data.set('test-repo|/old/root', {
      revision: 'ext-1',
      updatedAt: expired,
      chunkCount: 1,
      chunkRevisions: ['ext-1']
    });
    data.set('test-repo|/old/root#c000000', {
      revision: 'ext-1',
      entries: [['', { stale: true }]]
    });
    data.set('test-repo|/old/root#hash', {
      hash: 'h',
      compoundHash: { hashes: [''], posts: [] },
      updatedAt: expired,
      revision: 'ext-1'
    });
    // A fresh chunked root: everything stays — including the hash record,
    // whose expiry FOLLOWS THE MANIFEST (it carries no authority of its
    // own), and even when its own updatedAt is ancient (a refreshed
    // manifest keeps its couple alive).
    data.set('test-repo|/fresh/root', {
      revision: 'ext-2',
      updatedAt: Date.now(),
      chunkCount: 1,
      chunkRevisions: ['ext-2']
    });
    data.set('test-repo|/fresh/root#c000000', {
      revision: 'ext-2',
      entries: [['', { fresh: true }]]
    });
    data.set('test-repo|/fresh/root#hash', {
      hash: 'h',
      compoundHash: { hashes: [''], posts: [] },
      updatedAt: expired,
      revision: 'ext-2'
    });
    // Orphans under the fresh root: a chunk beyond the manifest's count and
    // a chunk with no manifest at all.
    data.set('test-repo|/fresh/root#c000007', {
      revision: 'ext-0',
      entries: [['', { orphan: true }]]
    });
    data.set('test-repo|/vanished/root#c000000', {
      revision: 'ext-0',
      entries: [['', { orphan: true }]]
    });
    // A fresh legacy record survives by its own updatedAt.
    data.set('test-repo|/legacy/root', {
      json: { legacy: true },
      updatedAt: Date.now(),
      revision: 'ext-3'
    });
    // Another manager's records are not this manager's to touch.
    data.set('other-repo|/old/root', {
      json: { foreign: true },
      updatedAt: expired,
      revision: 'ext-1'
    });

    const manager = new PersistenceManager('test-repo', factory);
    await manager.sweepNow();
    await flushAsync();

    expect(keysFor(data, 'test-repo|/old/root').length).to.equal(0);
    expect(keysFor(data, 'test-repo|/fresh/root')).to.deep.equal([
      'test-repo|/fresh/root',
      'test-repo|/fresh/root#c000000',
      'test-repo|/fresh/root#hash'
    ]);
    expect(data.has('test-repo|/vanished/root#c000000')).to.equal(false);
    expect(data.has('test-repo|/legacy/root')).to.equal(true);
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

  /**
   * An event registration whose events actually raise (running `runner`) —
   * the event queue dereferences every event it queues, so a null-returning
   * createEvent stub would abort the raise instead of exercising the replay
   * path under test.
   */
  function stubRegistration(runner: () => void = () => {}, matches = false) {
    return {
      respondsTo: () => true,
      createEvent: (_change: unknown, query: { _path: Path }) => ({
        getPath: () => query._path,
        getEventType: () => 'value',
        getEventRunner: () => runner,
        toString: () => 'stub-event'
      }),
      getEventRunner: () => runner,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createCancelEvent: () => null as any,
      matches: () => matches,
      hasAnyCallback: () => true
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
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

  it('an unsubscribe from inside the cached replay cancels the listen', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    data.set('test-repo|' + path.toString(), {
      json: { a: 1 },
      updatedAt: Date.now(),
      revision: 'ext-2'
    });
    // A replayed event callback unsubscribes synchronously: registration's
    // event runner calls repoStopServerListen mid-replay.
    const realQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    // One unsubscribe, however many events replay — a real off() detaches
    // the registration itself; this stub bypasses that bookkeeping.
    let unsubscribed = false;
    const registration = stubRegistration(() => {
      if (!unsubscribed) {
        unsubscribed = true;
        repoStopServerListen(repo, query, null);
      }
    }, true);
    syncTreeAddEventRegistration(repo.serverSyncTree_, realQuery, registration);
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    // The wire listen must never have been sent — no orphan to unlisten.
    expect(calls).to.deep.equal([]);
  });

  it('a hashless record is hash-primed and still sends the listen', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    // A record persisted before its hash landed (no #hash sibling).
    data.set('test-repo|' + path.toString(), {
      json: { a: 1 },
      updatedAt: Date.now(),
      revision: 'ext-1'
    });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
  });

  it('a certified descendant is grafted over the restored tree', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    data.set('test-repo|' + path.toString(), {
      json: { sibling: 'stale', inbox: { msg: 'stale' } },
      updatedAt: Date.now(),
      revision: 'ext-1'
    });
    const parentQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      parentQuery,
      stubRegistration()
    );
    const childPath = new Path('users/alice/inbox');
    const childQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      childPath,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      childQuery,
      stubRegistration()
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    // While the restore is in flight, the server certifies the child (a
    // get(), an overlapping deeper listen).
    syncTreeApplyServerOverwrite(
      repo.serverSyncTree_,
      childPath,
      nodeFromJSON({ msg: 'fresh' })
    );
    await flushAsync();

    expect(calls).to.deep.equal(['listen']);
    // The stored siblings painted, but the certified child was grafted —
    // never regressed to the stale bytes.
    const cache = syncTreeGetCompleteServerCache(repo.serverSyncTree_, path);
    expect(cache).to.not.equal(null);
    expect(cache!.val(true)).to.deep.equal({
      sibling: 'stale',
      inbox: { msg: 'fresh' }
    });
  });

  it('partial server data below the root skips the seed instead of clobbering it', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    data.set('test-repo|' + path.toString(), {
      json: { sibling: 'stale', inbox: { a: 'stale', b: 'stale' } },
      updatedAt: Date.now(),
      revision: 'ext-1'
    });
    // A FILTERED query below holds live server data: complete for its own
    // limited window, incomplete as a tree — nothing graftable.
    const childPath = new Path('users/alice/inbox');
    const filteredQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      childPath,
      queryParamsLimitToFirst(new QueryParams(), 1),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      filteredQuery,
      stubRegistration()
    );
    syncTreeApplyServerOverwrite(
      repo.serverSyncTree_,
      childPath,
      nodeFromJSON({ a: 'live', b: 'live' })
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();

    // The listen went out, but unseeded: applying the stale tree would have
    // replaced the filtered view's live server data.
    expect(calls).to.deep.equal(['listen']);
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)
    ).to.equal(null);
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

  it('whenListenComplete resolves when the repo is deleted', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    let settled = false;
    void repoWhenListenComplete(repo, path.toString()).then(() => {
      settled = true;
    });
    await flushAsync();
    expect(settled).to.equal(false);
    // deleteApp: the listen can never respond — the waiter must not hang.
    repoSettleListenCompletions(repo);
    await flushAsync();
    expect(settled).to.equal(true);
    expect(repo.listenCompletions_.size).to.equal(0);
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
      revision: 'ext-1'
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

  it('rejects invalid path input with a descriptive error', () => {
    const { db } = makeDatabaseWithPersistence();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(() => getPersistedValue(db as never, 123 as any)).to.throw(
      /invalid path/i
    );
    expect(() => getPersistedValue(db as never, 'bad#path')).to.throw(
      /invalid path/i
    );
  });
});

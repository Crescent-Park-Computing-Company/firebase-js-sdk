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

import { stringify } from '@firebase/util';
import { expect } from 'chai';

import { getPersistedValue, setPersistenceEnabled } from '../src/api/Database';
import { DataSnapshot, QueryImpl } from '../src/api/Reference_impl';
import {
  CompoundHashBuilder,
  canonicalHashFromNodeAsync,
  compoundHashFromNode,
  fixedSizeSplitStrategy,
  rebuildStableRanges,
  walkLeafInterval
} from '../src/core/CompoundHash';
import {
  PersistenceManager,
  PersistedRecord,
  PersistedSeedHashes,
  PersistenceRestoreResult,
  persistenceStats
} from '../src/core/Persistence';
import {
  repoCancelPendingSeedRestores,
  repoClearListenOutcomes,
  repoDispose,
  repoOnDataUpdateForTest,
  repoOnListenOutcome,
  repoStartServerListen,
  repoStopServerListen,
  ListenOutcome,
  Repo
} from '../src/core/Repo';
import { ListenWireResult } from '../src/core/ServerActions';
import {
  ListenHashFn,
  PendingListenHashStore
} from '../src/core/ServerCacheSeed';
import { PRIORITY_INDEX } from '../src/core/snap/indexes/PriorityIndex';
import { Node } from '../src/core/snap/Node';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import {
  SyncTree,
  syncTreeAddEventRegistration,
  syncTreeApplyServerOverwrite,
  syncTreeGetCompleteServerCache
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { sha1 } from '../src/core/util/util';
import { EventQueue } from '../src/core/view/EventQueue';
import {
  QueryParams,
  queryParamsLimitToFirst
} from '../src/core/view/QueryParams';

function computeCanonicalHash(json: unknown): string {
  return nodeFromJSON(json).hash();
}

function computeCompoundHash(json: unknown) {
  const hash = compoundHashFromNode(nodeFromJSON(json));
  return { hashes: hash.hashes, posts: hash.posts };
}

function expectRangesDescribeNode(
  node: Node,
  ranges: Array<{ post: string; hash: string }>
): void {
  let previous: string[] | null = null;
  for (const range of ranges) {
    const end = range.post === '/' ? [] : range.post.split('/');
    const builder = new CompoundHashBuilder(() => false);
    if (previous !== null) {
      builder.seedBoundary(previous);
    }
    walkLeafInterval(node, previous, end, builder);
    expect(builder.posts).to.deep.equal([range.post]);
    expect(builder.hashes).to.deep.equal([range.hash]);
    previous = end;
  }
}

function makeChunk(revision: string, entries: Array<[string, unknown]>) {
  const payload = stringify(entries);
  return {
    revision,
    contentHash: sha1(payload),
    payload
  };
}

/**
 * Hand-crafts a current-format (v10) stored generation: the manifest with
 * stable ranges computed from the tree, plus the structured-clone tree
 * record — what a real flush of `json` would have committed.
 */
function makeStoredGeneration(
  json: unknown,
  options: {
    revision?: string;
    updatedAt?: number;
    authScope?: string | null;
  } = {}
) {
  const node = nodeFromJSON(json);
  const payloads: unknown[] = [];
  const builder = new CompoundHashBuilder(fixedSizeSplitStrategy(256 * 1024));
  builder.payloadSink = payload => payloads.push(payload);
  const stable = rebuildStableRanges(node, [], [], false, builder, 256 * 1024);
  const revision = options.revision ?? 'ext-1';
  let previousPost: string | null = null;
  const ranges = stable.map((range, i) => ({
    ...range,
    recordId: revision + '-' + i.toString(36)
  }));
  const rangeRecords = ranges.map((range, i) => {
    const record = {
      recordId: range.recordId,
      start: previousPost,
      end: range.post,
      tree: payloads[i]
    };
    previousPost = range.post;
    return record;
  });
  return {
    manifest: {
      formatVersion: 11,
      revision,
      updatedAt: options.updatedAt ?? Date.now(),
      authScope: options.authScope ?? null,
      estimatedBytes: 1024,
      hash: '',
      ranges
    },
    rangeRecords
  };
}

function installStoredGeneration(
  data: Map<string, unknown>,
  root: string,
  generation: ReturnType<typeof makeStoredGeneration>
): void {
  data.set(root, generation.manifest);
  for (const record of generation.rangeRecords) {
    data.set(root + '#range:' + record.recordId, record);
  }
}

async function restoreForTest(
  manager: PersistenceManager,
  pathString: string
): Promise<PersistedRecord | null> {
  manager.track(pathString);
  return (await manager.restoreForListen(pathString)).record;
}

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
    dbVersion?: number;
    onGet?: (key: string) => void;
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
  const state = {
    version: options.dbVersion ?? 9,
    hasStore: !options.startWithoutStore
  };
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
    get: (key: string) => {
      options.onGet?.(key);
      return makeRequest(data.get(key));
    },
    put: (value: unknown, key: string) => {
      data.set(key, value);
      if (options.onPut) {
        options.onPut(key, value);
      }
      return makeRequest(undefined);
    },
    clear: () => {
      data.clear();
      return makeRequest(undefined);
    },
    delete: (key: string | IDBKeyRange) => {
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

function scopedManager(
  ...args: ConstructorParameters<typeof PersistenceManager>
): PersistenceManager {
  const manager = new PersistenceManager(...args);
  manager.setAuthScope(null);
  return manager;
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

/** A string large enough to make a root span multiple compound ranges. */
const BIG_LEAF_BYTES = 1024 * 1024;
function bigLeaf(seed: string): string {
  return seed.repeat(Math.ceil(BIG_LEAF_BYTES / seed.length));
}

describe('PersistenceManager', () => {
  it('restores what a flush persisted, hashes coupled to the tree', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const json = { a: 'x', b: { c: 1 } };
    const node = nodeFromJSON(json);
    const path = new Path('some/root');

    manager.track(path.toString());
    manager.serverCacheUpdated(path, node);
    await manager.flushNow(path.toString());
    await flushAsync();

    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored).to.not.equal(null);
    expect(restored.node.val(true)).to.deep.equal(json);
    expect(restored.hash).to.equal('');
    expect(restored.compoundHash).to.deep.equal(computeCompoundHash(json));
  });

  it('stores one manifest + immutable range records and reassembles exactly', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    // Large multi-range tree, plus priorities on an interior node and on a
    // leaf — the export-format and range-serialization paths in one tree.
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

    const manifest = data.get('test-repo|/chunked/root') as {
      ranges: Array<{ recordId: string; post: string }>;
    };
    const keys = keysFor(data, 'test-repo|/chunked/root').sort();
    expect(keys.length).to.equal(1 + manifest.ranges.length);
    expect(keys[0]).to.equal('test-repo|/chunked/root');
    for (const range of manifest.ranges) {
      const record = data.get(
        'test-repo|/chunked/root#range:' + range.recordId
      ) as { tree: unknown; end: string };
      expect(record).to.not.equal(undefined);
      expect(typeof record.tree).to.equal('object');
      expect(record.end).to.equal(range.post);
    }
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal(node.val(true));
    expect(restored.hash).to.equal('');
    expect(restored.compoundHash).to.deep.equal(
      computeCompoundHash(node.val(true))
    );
  });

  it('re-hashes only the ranges an update dirtied, boundaries preserved', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
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

    const manifestBefore = data.get('test-repo|/incremental/root') as {
      ranges: Array<{
        post: string;
        hash: string;
        size: number;
        recordId: string;
      }>;
    };
    expect(manifestBefore.ranges.length).to.be.greaterThan(1);
    const hashedAfterFirst = persistenceStats.rangesHashed;

    // Immutable update: the untouched big subtrees keep their identity.
    const v2 = v1.updateChild(new Path('small'), nodeFromJSON('v2'));
    manager.serverCacheUpdated(path, v2);
    await manager.flushNow(path.toString());
    await flushAsync();

    const manifestAfter = data.get('test-repo|/incremental/root') as {
      ranges: Array<{
        post: string;
        hash: string;
        size: number;
        recordId: string;
      }>;
    };
    // Clean ranges carried over by identity: same post AND same hash object;
    // only the dirtied tail of the range list was re-hashed.
    const dirtyHashed = persistenceStats.rangesHashed - hashedAfterFirst;
    expect(dirtyHashed).to.be.greaterThan(0);
    expect(dirtyHashed).to.be.lessThan(manifestAfter.ranges.length);
    let reused = 0;
    for (const range of manifestAfter.ranges) {
      if (
        manifestBefore.ranges.some(
          prev => prev.post === range.post && prev.hash === range.hash
        )
      ) {
        reused++;
      }
    }
    expect(reused).to.equal(manifestAfter.ranges.length - dirtyHashed);
    const liveRangeKeys = new Set(
      manifestAfter.ranges.map(
        range => 'test-repo|/incremental/root#range:' + range.recordId
      )
    );
    expect(
      keysFor(data, 'test-repo|/incremental/root').filter(key =>
        key.includes('#range:')
      )
    ).to.have.members([...liveRangeKeys]);

    // The incremental manifest's compound hash must equal a from-scratch
    // computation over the same tree at the same posts — the wire contract.
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal(v2.val(true));
    expect(restored.hash).to.equal('');
    expectRangesDescribeNode(v2, manifestAfter.ranges);
  });

  it('uses a constant configurable target for persisted range sizes', async () => {
    const shared = makeFakeIndexedDB();
    const json: Record<string, string> = {};
    for (let i = 0; i < 192; i++) {
      json['k' + i.toString().padStart(3, '0')] = 'x'.repeat(8192);
    }
    const node = nodeFromJSON(json);
    const write = async (prefix: string, target: number) => {
      const manager = scopedManager(
        prefix,
        shared.factory,
        true,
        8000,
        100 * 1024 * 1024,
        0,
        target
      );
      const path = new Path('fixed/root');
      manager.track(path.toString());
      manager.serverCacheUpdated(path, node);
      await manager.flushNow(path.toString());
      await flushAsync();
      return shared.data.get(prefix + '|/fixed/root') as {
        ranges: Array<{ size: number }>;
      };
    };
    const small = await write('small', 64 * 1024);
    const large = await write('large', 512 * 1024);
    expect(small.ranges.length).to.be.greaterThan(large.ranges.length);
    // A range can exceed the target by at most the leaf that crossed it.
    expect(Math.max(...small.ranges.map(range => range.size))).to.be.lessThan(
      80 * 1024
    );
  });

  it('rebases a stale cross-tab writer instead of mixing range generations', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('tabs/root');
    const initial = scopedManager('test-repo', shared.factory);
    initial.track(path.toString());
    initial.serverCacheUpdated(path, nodeFromJSON({ a: 1, b: 1 }));
    await initial.flushNow(path.toString());
    await flushAsync();

    const tabA = scopedManager('test-repo', shared.factory);
    const tabB = scopedManager('test-repo', shared.factory);
    const baseA = (await restoreForTest(tabA, path.toString()))!;
    const baseB = (await restoreForTest(tabB, path.toString()))!;
    tabA.serverCacheUpdated(
      path,
      baseA.node.updateChild(new Path('a'), nodeFromJSON(2))
    );
    tabB.serverCacheUpdated(
      path,
      baseB.node.updateChild(new Path('b'), nodeFromJSON(3))
    );
    await tabA.flushNow(path.toString());
    await tabB.flushNow(path.toString());
    await flushAsync();
    await flushAsync();

    const final = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(final!.node.val()).to.deep.equal({ a: 1, b: 3 });
    const manifest = shared.data.get('test-repo|/tabs/root') as {
      ranges: Array<{ recordId: string }>;
    };
    for (const range of manifest.ranges) {
      expect(
        shared.data.has('test-repo|/tabs/root#range:' + range.recordId)
      ).to.equal(true);
    }
  });

  it('a restored-then-certified unchanged tree flushes nothing', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const managerA = scopedManager('test-repo', factory);
    const path = new Path('warm/root');
    managerA.track(path.toString());
    managerA.serverCacheUpdated(path, nodeFromJSON({ steady: true }));
    await managerA.flushNow(path.toString());
    await flushAsync();

    // Next session: restore, then the listen 'ok' write-through hands the
    // SAME node back (the sync tree holds the seeded tree by reference).
    const managerB = scopedManager('test-repo', factory);
    const restored = (await restoreForTest(
      managerB,
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
    expect(keysFor(data, 'test-repo|/warm/root').length).to.equal(before.size);
  });

  it('coalesces an optimistic peek and listener restore onto one decode', async () => {
    const seeded = makeFakeIndexedDB();
    const path = new Path('coalesced/root');
    const writer = scopedManager('test-repo', seeded.factory);
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ a: 1, b: { c: 2 } }));
    await writer.flushNow(path.toString());
    await flushAsync();

    let treeGets = 0;
    const readerFactory = makeFakeIndexedDB({
      onGet: key => {
        if (key.startsWith('test-repo|/coalesced/root#range:')) {
          treeGets++;
        }
      }
    });
    for (const [key, value] of seeded.data) {
      readerFactory.data.set(key, value);
    }
    const reader = scopedManager('test-repo', readerFactory.factory);

    // Start the optimistic peek and authenticated listener together. They
    // share one physical range read, and the listener still receives the
    // manifest as soon as it arrives even though the peek started the read.
    let manifests = 0;
    const peekPromise = reader.peek(path.toString());
    reader.track(path.toString());
    const restorePromise = reader.restoreForListen(path.toString(), () => {
      manifests++;
    });
    const [peeked, restored] = await Promise.all([peekPromise, restorePromise]);

    expect(peeked).to.not.equal(null);
    expect(restored).to.not.equal(null);
    expect(manifests).to.equal(1);
    expect(peeked!.node).to.equal(restored.record!.node);
    const manifest = seeded.data.get('test-repo|/coalesced/root') as {
      ranges: unknown[];
    };
    expect(treeGets).to.equal(manifest.ranges.length);
  });

  it('does not repopulate in-memory state after the root is untracked', async () => {
    const seeded = makeFakeIndexedDB();
    const path = new Path('late/root');
    const writer = scopedManager('test-repo', seeded.factory);
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ cached: true }));
    await writer.flushNow(path.toString());
    await flushAsync();

    const readerFactory = makeFakeIndexedDB();
    for (const [key, value] of seeded.data) {
      readerFactory.data.set(key, value);
    }
    const reader = scopedManager('test-repo', readerFactory.factory);
    reader.track(path.toString());
    const restoring = reader.restoreForListen(path.toString());
    reader.untrack(path.toString());

    expect((await restoring).record).to.equal(null);
    expect(reader.trackedRootFor(path.toString())).to.equal(null);
  });

  it('an unchanged tree with an aging manifest refreshes the manifest alone', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const oldUpdatedAt = Date.now() - 2 * 24 * 60 * 60 * 1000; // 2 days
    const json = { steady: true };
    const stored = makeStoredGeneration(json, { updatedAt: oldUpdatedAt });
    installStoredGeneration(data, 'test-repo|/aging/root', stored);

    const manager = scopedManager('test-repo', factory);
    const restored = (await restoreForTest(
      manager,
      new Path('aging/root').toString()
    )) as PersistedRecord;
    const rangesBefore = keysFor(data, 'test-repo|/aging/root')
      .filter(key => key.includes('#range:'))
      .map(key => data.get(key));

    manager.track(new Path('aging/root').toString());
    manager.serverCacheUpdated(new Path('aging/root'), restored.node);
    await manager.flushNow(new Path('aging/root').toString());
    await flushAsync();

    // Tree record and ranges stay joined while only the manifest timestamp
    // refreshes.
    expect(
      keysFor(data, 'test-repo|/aging/root')
        .filter(key => key.includes('#range:'))
        .map(key => data.get(key))
    ).to.deep.equal(rangesBefore);
    const manifest = data.get('test-repo|/aging/root') as {
      revision: string;
      updatedAt: number;
      ranges: unknown;
    };
    expect(manifest.revision).to.equal('ext-1');
    expect(manifest.updatedAt).to.be.greaterThan(oldUpdatedAt);
    expect(manifest.ranges).to.deep.equal(stored.manifest.ranges);

    const again = (await restoreForTest(
      manager,
      new Path('aging/root').toString()
    )) as PersistedRecord;
    expect(again.node.val(true)).to.deep.equal(json);
    expect(again.hash).to.equal('');
  });

  it('a legacy pre-blob record restores as a miss and is reclaimed', async () => {
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
    const manager = scopedManager('test-repo', factory);
    // Format 10 does not read pre-blob layouts: one cold boot, records gone.
    expect(await restoreForTest(manager, '/legacy/root')).to.equal(null);
    await flushAsync();
    expect(keysFor(data, 'test-repo|/legacy/root')).to.deep.equal([]);
  });

  it('skips restore immediately when the browser schema marker is stale', async () => {
    const { factory } = makeFakeIndexedDB({ dbVersion: 7 });
    const manager = scopedManager('test-repo', factory, false);
    expect((await manager.restoreForListen('/cold/root')).record).to.equal(
      null
    );
    // The v8 migration continues in the background for the later live flush.
    await flushAsync();
  });

  it('clears pre-chunk cache formats without reading their values', async () => {
    const { factory, data } = makeFakeIndexedDB({ dbVersion: 1 });
    data.set('test-repo|/huge/legacy', {
      json: { old: true },
      updatedAt: Date.now(),
      revision: 'old-1'
    });
    const manager = scopedManager('test-repo', factory);
    // First open upgrades + clears inside IDB; restore never gets the value.
    expect(await restoreForTest(manager, '/huge/legacy')).to.equal(null);
    expect(data.size).to.equal(0);
  });

  it('evicts manifests from an unsupported record format', async () => {
    const { factory, data } = makeFakeIndexedDB();
    data.set('test-repo|/old-format/root', {
      formatVersion: 0,
      revision: 'old-format',
      updatedAt: Date.now(),
      chunkCount: 1,
      chunkRevisions: ['old-format']
    });
    data.set(
      'test-repo|/old-format/root#c000000@old-format',
      makeChunk('old-format', [['', { old: true }]])
    );
    const manager = scopedManager('test-repo', factory);
    expect(await restoreForTest(manager, '/old-format/root')).to.equal(null);
    await flushAsync();
    expect(keysFor(data, 'test-repo|/old-format/root')).to.deep.equal([]);
  });

  it('reports progress while a large cache hash is sliced', async () => {
    const json: Record<string, string> = {};
    for (let i = 0; i < 50; i++) {
      json[String(i)] = 'x'.repeat(100);
    }
    let pulses = 0;
    const hash = await canonicalHashFromNodeAsync(nodeFromJSON(json), 0, () => {
      pulses++;
    });
    expect(hash).to.equal(computeCanonicalHash(json));
    expect(pulses).to.be.greaterThan(0);
  });

  it('restore timeout resets while chunks keep making progress', async () => {
    interface TimeoutSeam {
      raceRestoreTimeout_<T>(
        start: (progress: () => void) => Promise<T | null>,
        timeoutMs: number
      ): Promise<T | null>;
    }
    const { factory } = makeFakeIndexedDB();
    const seam = scopedManager('test-repo', factory) as unknown as TimeoutSeam;
    const result = await seam.raceRestoreTimeout_(
      progress =>
        new Promise(resolve => {
          setTimeout(() => {
            progress();
            setTimeout(() => resolve('done'), 15);
          }, 15);
        }),
      20
    );
    // Total wall time (~30ms) exceeded the budget, but neither idle gap did.
    expect(result).to.equal('done');
  });

  it('listener restore degrades to a miss when cache progress stalls', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory, true, 10);
    const path = new Path('stalled/root');
    manager.track(path.toString());
    // Model an IndexedDB request that fires neither success nor error. The
    // production read still has its transaction-level bound; this seam pins
    // the listener-level invariant independently.
    (manager as unknown as { readRecord_: () => Promise<never> }).readRecord_ =
      () => new Promise(() => {});

    expect((await manager.restoreForListen(path.toString())).record).to.equal(
      null
    );
  });

  it('a stalled IndexedDB open degrades to a cache miss', async () => {
    const request = {
      onupgradeneeded: null,
      onsuccess: null,
      onerror: null,
      onblocked: null
    };
    const factory = {
      open: () => request
    } as unknown as IDBFactory;
    const manager = scopedManager('test-repo', factory, true, 10);

    manager.track('/stalled/open');
    expect((await manager.restoreForListen('/stalled/open')).record).to.equal(
      null
    );
  });

  it('resolves null for a root never persisted', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    expect(await restoreForTest(manager, 'missing/root')).to.equal(null);
  });

  it('expired records are dropped on restore', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    data.set('test-repo|/old/root', {
      json: { a: 1 },
      updatedAt: Date.now() - 15 * 24 * 60 * 60 * 1000,
      revision: 'ext-1'
    });
    expect(await restoreForTest(manager, '/old/root')).to.equal(null);
    await flushAsync();
    expect(data.has('test-repo|/old/root')).to.equal(false);
  });

  it('detects a structurally broken tree record and falls back cold', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const path = new Path('corrupt/root');
    const writer = scopedManager('test-repo', factory);
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ safe: true }));
    await writer.flushNow(path.toString());
    await flushAsync();

    // Structural corruption (a record restore cannot decode). Semantic
    // corruption is deliberately NOT detected locally: the range handshake
    // self-heals it (the server pushes the differing ranges).
    const corruptManifest = data.get('test-repo|/corrupt/root') as {
      ranges: Array<{ recordId: string; post: string }>;
    };
    const first = corruptManifest.ranges[0];
    data.set('test-repo|/corrupt/root#range:' + first.recordId, {
      recordId: first.recordId,
      start: null,
      end: first.post,
      tree: null
    });

    const reader = scopedManager('test-repo', factory);
    reader.track(path.toString());
    expect((await reader.restoreForListen(path.toString())).record).to.equal(
      null
    );
    await flushAsync();
    expect(keysFor(data, 'test-repo|/corrupt/root')).to.deep.equal([]);
  });

  it('a manifest↔range mismatch restores as a miss, never stitched', async () => {
    // A single-transaction commit makes a torn generation near-impossible,
    // but a foreign or partial write can still leave a mismatched pair. The
    // revision join catches it: a mismatch is a miss, never a wrong tree.
    const { factory, data } = makeFakeIndexedDB();
    const stored = makeStoredGeneration({ a: 1 }, { revision: 'ext-1' });
    installStoredGeneration(data, 'test-repo|/torn/root', stored);
    const tornRange = stored.rangeRecords[0];
    data.set('test-repo|/torn/root#range:' + tornRange.recordId, {
      ...tornRange,
      end: 'wrong/end'
    });

    const manager = scopedManager('test-repo', factory);
    expect(await restoreForTest(manager, '/torn/root')).to.equal(null);
    await flushAsync();
    // The broken pair was reclaimed.
    expect(keysFor(data, 'test-repo|/torn/root')).to.deep.equal([]);
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
    const manager = scopedManager('test-repo', factory);
    interleave = () => manager.serverCacheUpdated(path, nodeFromJSON({ v: 2 }));
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 1 }));
    await Promise.resolve();
    const firstFlush = (
      manager as unknown as { queues_: Map<string, Promise<void>> }
    ).queues_.get(path.toString());
    expect(firstFlush).to.not.equal(undefined);
    await firstFlush;
    await flushAsync();
    expect(interleave).to.equal(null); // the hook fired at the manifest put

    // Flush #1's pair: v1 data with v1's hash — the superseding update never
    // bled into it.
    const mid = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(mid.node.val(true)).to.deep.equal({ v: 1 });
    expect(mid.hash).to.equal('');

    // And flush #2 (still throttled) then writes the v2 pair.
    await manager.flushNow(path.toString());
    await flushAsync();
    const final = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(final.node.val(true)).to.deep.equal({ v: 2 });
    expect(final.hash).to.equal('');
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
    const manager = scopedManager('test-repo', factory);
    const path = new Path('shared/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ fresh: true }));
    await manager.flushNow(path.toString());
    await flushAsync();
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ fresh: true });
    expect(restored.hash).to.equal('');
  });

  it('persists the first authoritative tree without waiting for the throttle', async () => {
    const { factory } = makeFakeIndexedDB();
    const path = new Path('first/root');
    const manager = scopedManager('test-repo', factory);
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ first: true }));
    await flushAsync();
    const restored = await restoreForTest(manager, path.toString());
    expect(restored?.node.val()).to.deep.equal({ first: true });
  });

  it('a burst within the throttle window flushes the newest tree', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('hot/root');
    manager.track(path.toString());
    // Two updates back-to-back — the flush reads latest_ when it runs, so a
    // single flush persists the second tree.
    manager.serverCacheUpdated(path, nodeFromJSON({ n: 1 }));
    manager.serverCacheUpdated(path, nodeFromJSON({ n: 2 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ n: 2 });
    expect(restored.hash).to.equal('');
  });

  it('a throttle firing into a busy queue coalesces to one trailing flush', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
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
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ v: 2 });
  });

  it('evict during an in-flight flush deletes every record', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
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
    const manager = scopedManager('test-repo', factory);
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
    const manager = scopedManager('test-repo', factory);
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
    const manager = scopedManager('test-repo', factory);
    const path = new Path('repaired/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ ok: true }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(data.has('test-repo|/repaired/root')).to.equal(true);
    const restored = (await restoreForTest(
      manager,
      path.toString()
    )) as PersistedRecord;
    expect(restored.node.val(true)).to.deep.equal({ ok: true });
  });

  it('degrades to cold loads without IndexedDB', async () => {
    const manager = scopedManager('test-repo', null);
    const path = new Path('no/idb');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ a: 1 }));
    await manager.flushNow(path.toString());
    expect(await restoreForTest(manager, path.toString())).to.equal(null);
  });

  it('untrack flushes the final tree, keeps the record, drops the memory', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('rotated/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ kept: true }));

    manager.untrack(path.toString());
    await flushAsync();

    // The throttled write-through still landed…
    expect(data.has('test-repo|/rotated/root')).to.equal(true);
    const firstReader = scopedManager('test-repo', factory);
    const restored = (await restoreForTest(
      firstReader,
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
        (await restoreForTest(
          scopedManager('test-repo', factory),
          path.toString()
        )) as PersistedRecord
      ).node.val(true)
    ).to.deep.equal({ kept: true });
  });

  it('untrack under a live tracked ancestor deletes the child record', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
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

  it('prunes least-recently-used inactive roots under the cache budget', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const mb = BIG_LEAF_BYTES;
    const add = (name: string, updatedAt: number) => {
      const stored = makeStoredGeneration(
        { name },
        { revision: name, updatedAt }
      );
      const withSize = {
        ...stored,
        manifest: { ...stored.manifest, estimatedBytes: mb }
      };
      installStoredGeneration(data, `test-repo|/${name}`, withSize);
    };
    const now = Date.now();
    add('active-old', now - 3000);
    add('inactive-middle', now - 2000);
    add('inactive-new', now - 1000);
    const manager = scopedManager('test-repo', factory, true, 8000, 2 * mb);
    manager.track('/active-old');
    await manager.sweepNow();
    await flushAsync();

    expect(keysFor(data, 'test-repo|/active-old').length).to.equal(2);
    expect(keysFor(data, 'test-repo|/inactive-middle')).to.deep.equal([]);
    expect(keysFor(data, 'test-repo|/inactive-new')).to.deep.equal([]);
  });

  it('defers cleanup while a warm restore is active', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    data.set('test-repo|/expired', {
      json: { stale: true },
      updatedAt: Date.now() - 15 * 24 * 60 * 60 * 1000,
      revision: 'old'
    });
    const internals = manager as unknown as { activeRestoreCount_: number };
    internals.activeRestoreCount_ = 1;
    await manager.sweepNow();
    expect(data.has('test-repo|/expired')).to.equal(true);

    internals.activeRestoreCount_ = 0;
    await manager.sweepNow();
    await flushAsync();
    expect(data.has('test-repo|/expired')).to.equal(false);
    manager.dispose();
  });

  it('sweeps expired, legacy, and orphaned records for its own prefix', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const expired = Date.now() - 15 * 24 * 60 * 60 * 1000;
    // An expired current-format root: manifest and tree both go.
    const oldGen = makeStoredGeneration(
      { stale: true },
      { revision: 'ext-1', updatedAt: expired }
    );
    installStoredGeneration(data, 'test-repo|/old/root', oldGen);
    // A fresh current-format root stays, both records intact.
    const freshGen = makeStoredGeneration(
      { fresh: true },
      { revision: 'ext-2' }
    );
    installStoredGeneration(data, 'test-repo|/fresh/root', freshGen);
    // Legacy sidecars under the fresh root (pre-blob chunk/hash records) are
    // orphans of the current format and are swept.
    data.set('test-repo|/fresh/root#hash', {
      hash: 'legacy',
      compoundHash: { hashes: [''], posts: [] },
      updatedAt: Date.now(),
      revision: 'ext-2'
    });
    data.set(
      'test-repo|/fresh/root#c000000@ext-0',
      makeChunk('ext-0', [['', { orphan: true }]])
    );
    // A tree record with no manifest at all.
    data.set('test-repo|/vanished/root#range:orphan', {
      revision: 'ext-0',
      tree: { orphan: true }
    });
    // Legacy pre-blob base records expire regardless of their own updatedAt:
    // the current format cannot read them, so retention buys nothing.
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

    const manager = scopedManager('test-repo', factory);
    await manager.sweepNow();
    await flushAsync();

    expect(keysFor(data, 'test-repo|/old/root').length).to.equal(0);
    expect(keysFor(data, 'test-repo|/fresh/root').sort()).to.deep.equal(
      [
        'test-repo|/fresh/root',
        ...freshGen.rangeRecords.map(
          record => 'test-repo|/fresh/root#range:' + record.recordId
        )
      ].sort()
    );
    expect(data.has('test-repo|/vanished/root#range:orphan')).to.equal(false);
    expect(data.has('test-repo|/legacy/root')).to.equal(false);
    expect(data.has('other-repo|/old/root')).to.equal(true);
  });
});

describe('explicit persistent roots', () => {
  it('does not retain listeners the application did not select', () => {
    const manager = scopedManager('test-repo', makeFakeIndexedDB().factory);
    expect(manager.isPersistentPath('/transient')).to.equal(false);
    manager.setPersistentPath('/kept', true);
    expect(manager.isPersistentPath('/kept')).to.equal(true);
    manager.setPersistentPath('/kept', false);
    expect(manager.isPersistentPath('/kept')).to.equal(false);
  });
});

describe('repoStartServerListen / repoStopServerListen', () => {
  /**
   * The minimal Repo surface the two functions touch. `listen` records calls;
   * restore resolution is controlled by the manager's fake IndexedDB.
   */
  function makeListenHarness() {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const calls: string[] = [];
    const hashFns: ListenHashFn[] = [];
    const serverCallbacks: Array<
      (status: string, wire?: Partial<ListenWireResult>) => void
    > = [];
    const serverProgress: Array<(wire: ListenWireResult) => void> = [];
    const pendingHashes = new PendingListenHashStore();
    const repo = {
      pendingSeedRestores_: new Map<string, { cancelled: boolean }>(),
      pendingListenHashes_: pendingHashes,
      bootBuffers_: new Map(),
      listenOutcomes_: new Map(),
      persistence_: manager,
      eventQueue_: new EventQueue(),
      serverSyncTree_: new SyncTree({
        startListening: () => [],
        stopListening: () => {},
        getPendingListenHashes: pathString => pendingHashes.get(pathString)
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
          ) => void,
          onProgress?: (wire: ListenWireResult) => void
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
          serverProgress.push(onProgress ?? (() => {}));
        },
        unlisten: (...args: unknown[]) => calls.push('unlisten')
      }
    } as unknown as Repo;
    const path = new Path('users/alice');
    manager.setPersistentPath(path.toString(), true);
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
      hashFns,
      serverCallbacks,
      serverProgress,
      data
    };
  }

  async function persistHarnessRoot(repo: Repo, path: Path, json: unknown) {
    const manager = repo.persistence_!;
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(json));
    await manager.flushNow(path.toString());
    await flushAsync();
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

  it('an unsubscribe from inside the cached replay unlistens the sent listen', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { a: 1 });
    // A replayed event callback unsubscribes synchronously: registration's
    // event runner calls repoStopServerListen mid-replay. Manifest-first
    // boot sends the listen BEFORE the cached replay (that is the point),
    // so the stop must unlisten the already-sent listen — balanced wire
    // traffic, no orphaned server listen, no leaked boot state.
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
    expect(calls).to.deep.equal(['listen', 'unlisten']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
    expect(repo.bootBuffers_.size).to.equal(0);
  });

  it('a hashless record falls back cold without blocking on rehash', async () => {
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

  it('a certified descendant makes the parent restore go cold', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, {
      sibling: 'stale',
      inbox: { msg: 'stale' }
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
    // The persisted parent was not applied with hashes for a different tree.
    // The independently certified child remains current.
    const childCache = syncTreeGetCompleteServerCache(
      repo.serverSyncTree_,
      childPath
    );
    expect(childCache?.val(true)).to.deep.equal({ msg: 'fresh' });
  });

  it('an empty filtered descendant still blocks a stale parent restore', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { inbox: { stale: true } });
    const childPath = new Path('users/alice/inbox');
    const filteredQuery = new QueryImpl(
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
      nodeFromJSON(null)
    );
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );
  });

  it('partial server data below the root skips the seed instead of clobbering it', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, {
      sibling: 'stale',
      inbox: { a: 'stale', b: 'stale' }
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
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );
  });

  it('without a manifest callback the listen waits for the restore', async () => {
    // A restore that resolves without ever surfacing a manifest (stub
    // managers, legacy paths) keeps the pre-manifest-first sequencing:
    // apply the base, then listen.
    const { repo, query, path, hashFn, onComplete, calls, serverCallbacks } =
      makeListenHarness();
    let resolveRecord: (record: PersistedRecord | null) => void = () => {};
    const recordPromise = new Promise<PersistenceRestoreResult>(resolve => {
      resolveRecord = record =>
        resolve({
          record,
          reason: record ? undefined : 'missing'
        });
    });
    const node = nodeFromJSON({ cached: true });
    const compoundHash = computeCompoundHash(node.val(true));
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      restoreForListen: () => recordPromise,
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal([]);

    resolveRecord({
      node,
      hash: computeCanonicalHash(node.val(true)),
      compoundHash,
      updatedAt: Date.now(),
      revision: 'r1'
    });
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);

    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    serverCallbacks[0]('ok');
    await flushAsync();
    expect(outcomes.at(-1)?.certified).to.equal(true);
  });

  it('manifest-first: sends the listen before range restore resolves', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    const node = nodeFromJSON({ a: 1, b: 2 });
    const compoundHash = computeCompoundHash(node.val(true));
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      restoreForListen: async (
        _path: string,
        onManifest: (h: PersistedSeedHashes) => void
      ) => {
        onManifest({ hash: '', compoundHash });
        await gate;
        return {
          record: {
            node,
            hash: '',
            compoundHash,
            updatedAt: Date.now(),
            revision: 'r1'
          }
        };
      },
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    const realQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      realQuery,
      stubRegistration()
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await Promise.resolve();
    expect(calls).to.deep.equal(['listen']);
    expect(
      repo.pendingListenHashes_.get(path.toString())?.compoundHash.posts
    ).to.deep.equal(compoundHash.posts);

    release();
    await flushAsync();
    expect(repo.pendingListenHashes_.get(path.toString())).to.equal(undefined);
    expect(repo.bootBuffers_.size).to.equal(0);
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)?.val()
    ).to.deep.equal({ a: 1, b: 2 });
  });

  it('buffers an early listen ok until the cached base is installed', async () => {
    const { repo, query, path, hashFn, onComplete, calls, serverCallbacks } =
      makeListenHarness();
    const node = nodeFromJSON({ cached: true });
    const compoundHash = computeCompoundHash(node.val(true));
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      restoreForListen: async (
        _path: string,
        onManifest: (h: PersistedSeedHashes) => void
      ) => {
        onManifest({ hash: '', compoundHash });
        await gate;
        return {
          record: {
            node,
            hash: '',
            compoundHash,
            updatedAt: Date.now(),
            revision: 'r1'
          }
        };
      },
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    const realQuery = new QueryImpl(
      null as any,
      path,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      realQuery,
      stubRegistration()
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await Promise.resolve();
    expect(calls).to.deep.equal(['listen']);
    serverCallbacks[0]('ok');
    expect(repo.bootBuffers_.get(path.toString())?.length).to.equal(1);
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );

    release();
    await flushAsync();
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)?.val()
    ).to.deep.equal({ cached: true });
    expect(
      repo.listenOutcomes_.get(path.toString())?.outcome?.certified
    ).to.equal(true);
  });

  it('buffers tagged descendant pushes covered by a booting root', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    const node = nodeFromJSON({ cached: true });
    const compoundHash = computeCompoundHash(node.val(true));
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      restoreForListen: async (
        _path: string,
        onManifest: (h: PersistedSeedHashes) => void
      ) => {
        onManifest({ hash: '', compoundHash });
        await gate;
        return {
          record: {
            node,
            hash: '',
            compoundHash,
            updatedAt: Date.now(),
            revision: 'r1'
          }
        };
      },
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await Promise.resolve();
    repoOnDataUpdateForTest(
      repo,
      path.toString() + '/child',
      { x: 1 },
      false,
      77
    );
    const buffered = repo.bootBuffers_.get(path.toString())!;
    expect(buffered).to.have.length(1);
    expect((buffered[0] as { tag: number }).tag).to.equal(77);
    release();
    await flushAsync();
    expect(repo.bootBuffers_.size).to.equal(0);
  });

  it('buffers server pushes that beat the cached base, then replays them', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { a: 1 });
    // Make the restore hold after the manifest: stub restoreForListen to
    // fire onManifest immediately and resolve the record only when told.
    const manager = repo.persistence_!;
    const realRestore = manager.restoreForListen.bind(manager);
    let releaseRecord: () => void = () => {};
    const recordGate = new Promise<void>(resolve => {
      releaseRecord = resolve;
    });
    manager.restoreForListen = (pathString, onManifest) => {
      // Delegate for the real manifest+record, but delay the resolution.
      return realRestore(pathString, onManifest).then(async result => {
        await recordGate;
        return result;
      });
    };

    const realQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      realQuery,
      stubRegistration()
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    // The server answers BEFORE the base applied: a range merge for 'a',
    // then a normal overwrite under the root. Both must hold.
    repoOnDataUpdateForTest(repo, path.toString() + '/b', 7, false, null);
    expect(repo.bootBuffers_.get(path.toString())!.length).to.equal(1);
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );

    releaseRecord();
    await flushAsync();
    // Base applied, buffer drained in order: cached {a:1} + buffered b=7.
    expect(repo.bootBuffers_.size).to.equal(0);
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)?.val()
    ).to.deep.equal({ a: 1, b: 7 });
  });

  it('a missing range after a seeded listen restarts exactly once cold', async () => {
    const { repo, query, path, hashFn, onComplete, calls, data } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { a: 1 });
    const manifest = data.get('test-repo|' + path.toString()) as {
      ranges: Array<{ recordId: string }>;
    };
    data.delete(
      'test-repo|' + path.toString() + '#range:' + manifest.ranges[0].recordId
    );
    const realQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      path,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      realQuery,
      stubRegistration()
    );

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));
    await flushAsync();
    expect(calls).to.deep.equal(['listen', 'unlisten', 'listen']);
    expect(repo.bootBuffers_.size).to.equal(0);
  });

  it('a validation miss attaches exactly one cold listen', async () => {
    const { repo, query, hashFn, onComplete, calls, hashFns } =
      makeListenHarness();
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      // Missing/mismatched/malformed chunks, storage failure, or an idle
      // timeout all take the same cold-listen fallback.
      restoreForListen: () =>
        Promise.resolve({ record: null, reason: 'missing' }),
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;

    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(hashFns[0]()).to.equal('');
  });

  it('publishes one restored outcome through certification', async () => {
    const { repo, query, path, hashFn, onComplete, serverCallbacks } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { cached: true });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const outcomes: ListenOutcome[] = [];
    const unsubscribe = repoOnListenOutcome(repo, path.toString(), outcome => {
      outcomes.push(outcome);
    });
    await flushAsync();
    expect(outcomes[0]).to.deep.equal({
      mode: 'restored',
      certified: false,
      bytes: 0,
      reason: undefined
    });
    serverCallbacks[0]('ok', { bytes: 321, dataReceived: false });
    await flushAsync();
    expect(outcomes[1]).to.deep.equal({
      mode: 'restored',
      certified: true,
      bytes: 321,
      reason: undefined
    });
    unsubscribe();
  });

  it('switches restored cyan to fallback amber on a full server replacement', async () => {
    const {
      repo,
      query,
      path,
      hashFn,
      onComplete,
      serverCallbacks,
      serverProgress
    } = makeListenHarness();
    await persistHarnessRoot(repo, path, { cached: true });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    await flushAsync();
    expect(outcomes.at(-1)?.mode).to.equal('restored');

    serverProgress[0]({
      bytes: 500,
      hadHash: true,
      hadCompoundHash: true,
      dataReceived: true,
      rangeMerged: false
    });
    expect(outcomes.at(-1)).to.deep.equal({
      mode: 'fallback',
      certified: false,
      bytes: 500,
      reason: undefined
    });

    serverCallbacks[0]('ok', {
      bytes: 507,
      dataReceived: true,
      rangeMerged: false
    });
    expect(outcomes.at(-1)?.mode).to.equal('fallback');
    expect(outcomes.at(-1)?.certified).to.equal(true);
  });

  it('keeps restored cyan while range merges arrive incrementally', async () => {
    const { repo, query, path, hashFn, onComplete, serverProgress } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { cached: true });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    await flushAsync();
    serverProgress[0]({
      bytes: 123,
      hadHash: true,
      hadCompoundHash: true,
      dataReceived: true,
      rangeMerged: true
    });
    expect(outcomes.at(-1)).to.deep.equal({
      mode: 'restored',
      certified: false,
      bytes: 123,
      reason: undefined
    });
  });

  it('publishes corruption as a full fallback, not a normal cold miss', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repo.persistence_ = {
      track: () => {},
      restoreForListen: () =>
        Promise.resolve({ record: null, reason: 'corrupt' }),
      isPersistentPath: () => true,
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    await flushAsync();
    expect(outcomes[0]).to.deep.equal({
      mode: 'fallback',
      certified: false,
      bytes: 0,
      reason: 'corrupt'
    });
  });

  it('publishes a cold miss with its reason', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    await flushAsync();
    expect(outcomes[0]).to.deep.equal({
      mode: 'cold',
      certified: false,
      bytes: 0,
      reason: 'missing'
    });
  });

  it('removes the outcome when the listen stops', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(repo.listenOutcomes_.has(path.toString())).to.equal(true);
    repoStopServerListen(repo, query, null);
    expect(repo.listenOutcomes_.has(path.toString())).to.equal(false);
  });

  it('repo deletion cancels every pending persisted restore', () => {
    const { repo } = makeListenHarness();
    const pending = { cancelled: false };
    repo.pendingSeedRestores_.set('/large/root', pending);
    repoCancelPendingSeedRestores(repo);
    expect(pending.cancelled).to.equal(true);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
  });

  it('repo disposal cancels restores and clears outcomes together', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    const pending = repo.pendingSeedRestores_.get(path.toString())!;
    let disposed = false;
    repo.persistence_ = {
      dispose: () => {
        disposed = true;
      }
    } as PersistenceManager;
    repoDispose(repo);
    await flushAsync();
    expect(pending.cancelled).to.equal(true);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
    expect(repo.listenOutcomes_.size).to.equal(0);
    expect(disposed).to.equal(true);
  });

  it('clears active outcome observers on repo deletion', async () => {
    const { repo, query, path, hashFn, onComplete } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(repo.listenOutcomes_.size).to.equal(1);
    repoClearListenOutcomes(repo);
    expect(repo.listenOutcomes_.size).to.equal(0);
  });
});

describe('stale restore vs live server data', () => {
  it('a restore that loses the race does not clobber certified data', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
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
      bootBuffers_: new Map(),
      listenOutcomes_: new Map(),
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

describe('persistence auth scope', () => {
  it('does not persist or replay until an auth scope is explicitly configured', async () => {
    const shared = makeFakeIndexedDB();
    const manager = new PersistenceManager('test-repo', shared.factory);
    const path = new Path('public/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ value: 1 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(shared.data.size).to.equal(0);
    expect((await manager.restoreForListen(path.toString())).reason).to.equal(
      'auth'
    );
  });
  it('never restores a cache written by another authenticated user', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('private/root');
    const writer = scopedManager('test-repo', shared.factory);
    writer.setAuthScope('user-a');
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ secret: 'a' }));
    await writer.flushNow(path.toString());
    await flushAsync();

    const reader = scopedManager('test-repo', shared.factory);
    reader.setAuthScope('user-b');
    expect(await restoreForTest(reader, path.toString())).to.equal(null);
    reader.setAuthScope('user-a');
    expect(
      (await restoreForTest(reader, path.toString()))!.node.val()
    ).to.deep.equal({
      secret: 'a'
    });
  });

  it('drops an in-flight restore when the authenticated user changes', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('private/restore-switch');
    const writer = scopedManager('test-repo', shared.factory);
    writer.setAuthScope('user-a');
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ secret: 'a' }));
    await writer.flushNow(path.toString());
    await flushAsync();

    const reader = scopedManager('test-repo', shared.factory);
    reader.setAuthScope('user-a');
    reader.track(path.toString());
    const restoring = reader.restoreForListen(path.toString());
    reader.setAuthScope('user-b');
    const result = await restoring;
    expect(result.record).to.equal(null);
    expect(result.reason).to.equal('auth');
  });

  it('never relabels an in-flight write after the auth scope changes', async () => {
    let changed = false;
    const shared = makeFakeIndexedDB({
      onPut: key => {
        if (!changed && key.includes('#range:')) {
          changed = true;
          manager.setAuthScope('user-b');
        }
      }
    });
    const path = new Path('private/in-flight');
    const manager = scopedManager('test-repo', shared.factory);
    manager.setAuthScope('user-a');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ owner: 'a' }));
    await manager.flushNow(path.toString());
    await flushAsync();

    const reader = scopedManager('test-repo', shared.factory);
    reader.setAuthScope('user-b');
    expect(await restoreForTest(reader, path.toString())).to.equal(null);
    reader.setAuthScope('user-a');
    expect(
      (await restoreForTest(reader, path.toString()))!.node.val()
    ).to.deep.equal({
      owner: 'a'
    });
  });
});

describe('persistence restore scheduling', () => {
  it('defers cold writes until the restore wave drains', async () => {
    const shared = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', shared.factory);
    const path = new Path('cold/write');
    manager.track(path.toString());
    const internals = manager as unknown as {
      activeRestoreCount_: number;
      writesDeferredUntilRestores_: Set<string>;
      flushWritesDeferredUntilRestores_: () => void;
      queues_: Map<string, Promise<void>>;
    };
    internals.activeRestoreCount_ = 1;
    manager.serverCacheUpdated(path, nodeFromJSON({ fresh: true }));
    expect(
      internals.writesDeferredUntilRestores_.has(path.toString())
    ).to.equal(true);
    expect(internals.queues_.size).to.equal(0);

    internals.activeRestoreCount_ = 0;
    internals.flushWritesDeferredUntilRestores_();
    await internals.queues_.get(path.toString());
    await flushAsync();
    const restored = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(restored?.node.val()).to.deep.equal({ fresh: true });
  });

  it('bounds concurrent IndexedDB restores like Androids serialized runloop', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory, true, 100);
    let active = 0;
    let peak = 0;
    (manager as unknown as { readRecord_: () => Promise<null> }).readRecord_ =
      async () => {
        active++;
        peak = Math.max(peak, active);
        await new Promise(resolve => setTimeout(resolve, 5));
        active--;
        return null;
      };
    const paths = Array.from({ length: 20 }, (_, i) => `/queued/${i}`);
    paths.forEach(path => manager.track(path));
    await Promise.all(paths.map(path => manager.restoreForListen(path)));
    expect(peak).to.equal(4);
  });
});

describe('persistence diagnostics', () => {
  it('records bounded store/restore outcomes for debugging cache misses', async () => {
    persistenceStats.events.length = 0;
    const shared = makeFakeIndexedDB();
    const path = new Path('diag/root');
    const writer = scopedManager('test-repo', shared.factory);
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ ok: true }));
    await writer.flushNow(path.toString());
    const reader = scopedManager('test-repo', shared.factory);
    reader.track(path.toString());
    await reader.restoreForListen(path.toString());
    expect(
      persistenceStats.events.map(event => event.event)
    ).to.include.members(['stored', 'restore-hit']);
    expect(persistenceStats.events.length).to.be.at.most(100);
  });
});

describe('DataSnapshot restored-value semantics', () => {
  it('returns a fresh value so caller mutation cannot corrupt later reads', () => {
    const node = nodeFromJSON({ nested: { value: 1 } });
    const snapshot = new DataSnapshot(
      node,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      PRIORITY_INDEX
    );
    const first = snapshot.val() as { nested: { value: number } };
    first.nested.value = 99;
    expect(snapshot.val()).to.deep.equal({ nested: { value: 1 } });
    expect(node.getChild(new Path('nested/value')).val()).to.equal(1);
  });
});

describe('getPersistedValue', () => {
  function makeDatabaseWithPersistence(): {
    db: unknown;
    manager: PersistenceManager;
  } {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const repo = { persistence_: manager };
    const db = {
      _checkNotDeleted: () => {},
      _repoInternal: repo
      // getModularInstance returns objects without _delegate untouched.
    };
    return { db, manager };
  }

  it('reads only the exact persisted listener root', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('users/alice');
    manager.track(root.toString());
    manager.serverCacheUpdated(
      root,
      nodeFromJSON({ settings: { theme: 'dark' }, name: 'alice' })
    );
    await manager.flushNow(root.toString());
    await flushAsync();

    expect(await getPersistedValue(db as never, '/users/alice')).to.deep.equal({
      settings: { theme: 'dark' },
      name: 'alice'
    });
    // A tiny exact-path peek may safely project from the already-restored
    // covering root without starting another ancestor decode.
    expect(
      await getPersistedValue(db as never, '/users/alice/settings/theme')
    ).to.equal('dark');
    expect(await getPersistedValue(db as never, '/users/bob')).to.equal(null);
  });

  it('pre-auth peeks require the expected authenticated scope', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('private/root');
    manager.setAuthScope('user-a');
    manager.track(root.toString());
    manager.serverCacheUpdated(root, nodeFromJSON({ secret: true }));
    await manager.flushNow(root.toString());
    await flushAsync();

    expect(
      await getPersistedValue(db as never, '/private/root', 'user-b')
    ).to.equal(null);
    expect(
      await getPersistedValue(db as never, '/private/root', 'user-a')
    ).to.deep.equal({ secret: true });
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

    expect(await getPersistedValue(db as never, '/prio/root')).to.deep.equal({
      a: 42,
      b: 'x'
    });
  });

  it('rejects persistence reconfiguration after the Database starts', () => {
    const { db } = makeDatabaseWithPersistence();
    (db as any)._instanceStarted = true;
    expect(() => setPersistenceEnabled(db as never, false)).to.throw(
      /before the first Database operation/i
    );
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

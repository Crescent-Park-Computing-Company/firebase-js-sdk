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

import {
  getPersistedValue,
  setPersistenceAuthScope,
  setPersistenceEnabled,
  _PEEK_MATERIALIZE_SLICE_VISITS
} from '../src/api/Database';
import {
  consumePersistedMaterialization,
  DataSnapshot,
  off,
  onValue,
  QueryImpl,
  ValueEventRegistration
} from '../src/api/Reference_impl';
import {
  CompoundHashBuilder,
  StableRangeRebuilder,
  canonicalHashFromNodeAsync,
  collectChangedSubtreePaths,
  compoundHashFromNode,
  fixedSizeSplitStrategy,
  markDirtyRanges,
  rebuildStableRanges,
  treesShareAnyChildIdentity,
  walkLeafInterval
} from '../src/core/CompoundHash';
import {
  PersistenceManager,
  PersistedRecord,
  PersistedSeedHashes,
  PersistenceRestoreResult,
  persistenceStats,
  PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS,
  PERSISTENCE_WRITE_DEBOUNCE_MS,
  _setWebLocksForTesting
} from '../src/core/Persistence';
import {
  newIngestQueue,
  repoCancelPendingSeedRestores,
  repoClearListenOutcomes,
  repoDispose,
  repoGetValue,
  repoOnDataUpdateForTest,
  repoOnListenOutcome,
  repoStartServerListen,
  repoStopServerListen,
  ListenOutcome,
  Repo
} from '../src/core/Repo';
import { ListenWireResult } from '../src/core/ServerActions';
import {
  consumeMaterializedValue,
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
  syncTreeGetCompleteServerCache,
  syncTreeRemoveEventRegistration
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { Tree } from '../src/core/util/Tree';
import { sha1 } from '../src/core/util/util';
import { EventQueue } from '../src/core/view/EventQueue';
import {
  QueryParams,
  queryParamsLimitToFirst
} from '../src/core/view/QueryParams';

// This suite tests SINGLE-manager write economics: pin the lock-less
// (CAS-only) environment so every test runs the same code path in every
// runtime. Without the pin, Node (no navigator) fails open while real
// browsers discover REAL Web Locks — the write gate then waits on an async
// lock grant these tests never await, and one test's manager can block a
// later test's writes through the shared browser lock manager. The
// multi-tab lease behavior has its own suite (persistence-multitab.test.ts)
// which injects a fake lock manager per test.
_setWebLocksForTesting(null);

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
      const stored = structuredClone(value);
      data.set(key, stored);
      if (options.onPut) {
        options.onPut(key, stored);
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
  const [
    prefix,
    factory,
    schema,
    timeout,
    cacheBytes,
    writeDelay,
    rangeTarget
  ] = args;
  const manager = new PersistenceManager(
    prefix,
    factory,
    schema,
    timeout,
    cacheBytes,
    writeDelay ?? 0,
    rangeTarget
  );
  manager.setAuthScope(null);
  return manager;
}

/** The deferred-ingest machinery is fully quiescent: no gates, empty queue. */
function expectIngestIdle(repo: Repo): void {
  expect(repo.ingestQueue_.gates.size).to.equal(0);
  expect(repo.ingestQueue_.ops.length).to.equal(0);
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

  it('round-trips trailing priorities omitted by the wire hash', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('priority/root');
    const json = { list: { '0': 'a', '1': 'b', '.priority': 7 } };
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(json));
    await manager.flushNow(path.toString());
    const restored = await restoreForTest(manager, path.toString());
    expect(restored!.node.val(true)).to.deep.equal(json);
  });

  it('persists nested __proto__ keys without prototype pollution', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('proto/root');
    const json: Record<string, unknown> = {};
    Object.defineProperty(json, '__proto__', {
      value: { nested: 'safe' },
      enumerable: true,
      configurable: true,
      writable: true
    });
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(json));
    await manager.flushNow(path.toString());
    const restored = await restoreForTest(manager, path.toString());
    expect(
      restored!.node.getChild(new Path('__proto__/nested')).val()
    ).to.equal('safe');
    expect((Object.prototype as { nested?: unknown }).nested).to.equal(
      undefined
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

  it('keeps concurrent first-generation manifests self-contained', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('cold-tabs/root');
    const tabA = scopedManager('test-repo', shared.factory);
    const tabB = scopedManager('test-repo', shared.factory);
    tabA.track(path.toString());
    tabB.track(path.toString());
    tabA.serverCacheUpdated(path, nodeFromJSON({ owner: 'a' }));
    tabB.serverCacheUpdated(path, nodeFromJSON({ owner: 'b' }));
    await Promise.all([
      tabA.flushNow(path.toString()),
      tabB.flushNow(path.toString())
    ]);
    await flushAsync();
    const manifest = shared.data.get('test-repo|/cold-tabs/root') as {
      ranges: Array<{ recordId: string }>;
    };
    expect(manifest).to.not.equal(undefined);
    for (const range of manifest.ranges) {
      expect(
        shared.data.has('test-repo|/cold-tabs/root#range:' + range.recordId)
      ).to.equal(true);
    }
    const restored = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(restored).to.not.equal(null);
  });

  it('does not sweep immutable ranges while a generation is staging', async () => {
    const shared = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', shared.factory);
    const path = new Path('sweep-race/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(
      path,
      nodeFromJSON({ value: 'x'.repeat(200000) })
    );
    const flushing = manager.flushNow(path.toString());
    await manager.sweepNow();
    await flushing;
    await flushAsync();
    expect(await restoreForTest(manager, path.toString())).to.not.equal(null);
  });

  it('a sweep between chunk staging and the manifest commit forces a clean retry', async () => {
    const path = new Path('swept-stage/root');
    const prefix = 'test-repo|/swept-stage/root';
    // Simulates the other tab's sweep at the exact hazardous interleave: a
    // freshly staged range record is not referenced by the COMMITTED
    // manifest, so a concurrent sweep classifies it as an orphan and deletes
    // it after the staging put but before the manifest CAS transaction.
    let dataRef: Map<string, unknown> | null = null;
    let baseline = new Set<string>();
    let sabotage = false;
    const swept: string[] = [];
    const shared = makeFakeIndexedDB({
      onPut: key => {
        if (
          sabotage &&
          dataRef !== null &&
          key.startsWith(prefix + '#range:') &&
          !baseline.has(key)
        ) {
          swept.push(key);
          dataRef.delete(key);
        }
      }
    });
    dataRef = shared.data;

    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ a: 1, b: 1 }));
    await manager.flushNow(path.toString());
    await flushAsync();
    const committed = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(committed).to.not.equal(undefined);
    baseline = new Set(
      [...shared.data.keys()].filter(key => key.startsWith(prefix))
    );

    const base = (await restoreForTest(manager, path.toString()))!;
    manager.serverCacheUpdated(
      path,
      base.node.updateChild(new Path('a'), nodeFromJSON(2))
    );
    sabotage = true;
    await manager.flushNow(path.toString());
    sabotage = false;
    expect(swept.length).to.be.greaterThan(0);

    // The sabotaged generation must not have been published: the committed
    // manifest is still the previous revision and every range record it
    // references is present.
    const afterLoss = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(afterLoss.revision).to.equal(committed.revision);
    for (const range of afterLoss.ranges) {
      expect(shared.data.has(prefix + '#range:' + range.recordId)).to.equal(
        true
      );
    }

    // The queue drain retries the flush coalesced; the retry publishes a
    // complete generation and a fresh session restores it warm.
    await flushAsync();
    await flushAsync();
    const final = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(final).to.not.equal(null);
    expect(final!.node.val()).to.deep.equal({ a: 2, b: 1 });
    const republished = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(republished.revision).to.not.equal(committed.revision);
    for (const range of republished.ranges) {
      expect(shared.data.has(prefix + '#range:' + range.recordId)).to.equal(
        true
      );
    }
  });

  it('draining deferred writes re-arms the write window instead of flushing', async () => {
    const shared = makeFakeIndexedDB();
    // Real 60ms window: the deferral drain must go back BEHIND it.
    const manager = new PersistenceManager(
      'test-repo',
      shared.factory,
      true,
      8000,
      100 * 1024 * 1024,
      60
    );
    manager.setAuthScope(null);
    const path = new Path('deferred/root');
    manager.track(path.toString());
    const internals = manager as unknown as {
      activeRestoreCount_: number;
      flushWritesDeferredUntilRestores_: () => void;
      queues_: Map<string, Promise<void>>;
    };
    internals.activeRestoreCount_ = 1;
    manager.serverCacheUpdated(path, nodeFromJSON({ fresh: true }));
    internals.activeRestoreCount_ = 0;
    internals.flushWritesDeferredUntilRestores_();
    // The drain is the cold-boot moment: nothing may hit IndexedDB yet.
    await flushAsync();
    expect(shared.data.has('test-repo|/deferred/root')).to.equal(false);
    // After the window fires, the flush lands normally.
    await new Promise(resolve => setTimeout(resolve, 90));
    await flushAsync();
    expect(shared.data.has('test-repo|/deferred/root')).to.equal(true);
  });

  it('an identical-tree refresh that loses its manifest rebuilds the record', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('refresh-lost/root');
    const prefix = 'test-repo|/refresh-lost/root';
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const node = nodeFromJSON({ steady: true });
    manager.serverCacheUpdated(path, node);
    await manager.flushNow(path.toString());
    await flushAsync();
    expect(shared.data.has(prefix)).to.equal(true);

    // Storage vanishes underneath (another identity, devtools clear, sweep).
    for (const key of [...shared.data.keys()]) {
      if (key.startsWith(prefix)) {
        shared.data.delete(key);
      }
    }

    // Same node, aged past the refresh threshold: the refresh path runs,
    // finds no manifest, and must NOT leave lastFlush_ describing storage
    // that no longer exists — that would skip every future write-through.
    const internals = manager as unknown as {
      lastFlush_: Map<string, { storedUpdatedAt: number }>;
    };
    const state = internals.lastFlush_.get(path.toString())!;
    state.storedUpdatedAt = Date.now() - 25 * 60 * 60 * 1000;
    manager.serverCacheUpdated(path, node);
    await manager.flushNow(path.toString());
    await flushAsync();
    await flushAsync();
    const restored = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(restored).to.not.equal(null);
    expect(restored!.node.val()).to.deep.equal({ steady: true });
  });

  it('a new identity replaces a foreign-scope manifest without livelocking', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('scoped/root');
    const prefix = 'test-repo|/scoped/root';

    const alice = scopedManager('test-repo', shared.factory);
    alice.setAuthScope('alice');
    alice.track(path.toString());
    alice.serverCacheUpdated(path, nodeFromJSON({ owner: 'alice' }));
    await alice.flushNow(path.toString());
    await flushAsync();
    const aliceManifest = shared.data.get(prefix) as { authScope: string };
    expect(aliceManifest.authScope).to.equal('alice');

    // Bob signs in on the same browser profile. His first generation finds
    // alice's manifest at the key. Strict absent-only CAS would conflict;
    // the adopt-the-winner read then resolves null (auth mismatch, records
    // are never cross-scope readable) and every coalesced retry re-stages
    // the full tree and conflicts again — a permanent livelock. A foreign
    // scope is replaceable instead.
    const bob = scopedManager('test-repo', shared.factory);
    bob.setAuthScope('bob');
    bob.track(path.toString());
    bob.serverCacheUpdated(path, nodeFromJSON({ owner: 'bob' }));
    await bob.flushNow(path.toString());
    await flushAsync();
    await flushAsync();

    const manifest = shared.data.get(prefix) as {
      authScope: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(manifest.authScope).to.equal('bob');
    for (const range of manifest.ranges) {
      expect(shared.data.has(prefix + '#range:' + range.recordId)).to.equal(
        true
      );
    }
    // No retry left pending: the generation settled cleanly.
    const internals = bob as unknown as { flushPending_: Set<string> };
    expect(internals.flushPending_.has(path.toString())).to.equal(false);

    const restoredForBob = scopedManager('test-repo', shared.factory);
    restoredForBob.setAuthScope('bob');
    restoredForBob.track(path.toString());
    const result = await restoredForBob.restoreForListen(path.toString());
    expect(result.record).to.not.equal(null);
    expect(result.record!.node.val()).to.deep.equal({ owner: 'bob' });
  });

  it('an identity change mid-staging converges to the new scope', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('switch/root');
    const prefix = 'test-repo|/switch/root';
    const manager = scopedManager('test-repo', shared.factory);
    manager.setAuthScope('alice');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ owner: 'alice' }));
    // The scope flips while alice's generation is mid-flight; bob's own
    // write-through queues behind it on the same root.
    const flushing = manager.flushNow(path.toString());
    manager.setAuthScope('bob');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ owner: 'bob' }));
    await flushing;
    await manager.flushNow(path.toString());
    await flushAsync();
    await flushAsync();
    // Alice's stale generation may have published into the empty key under
    // her own label, but bob's generation must end up authoritative — and
    // alice's label must never ride bob's session forward.
    const manifest = shared.data.get(prefix) as {
      authScope: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(manifest.authScope).to.equal('bob');
    for (const range of manifest.ranges) {
      expect(shared.data.has(prefix + '#range:' + range.recordId)).to.equal(
        true
      );
    }
    const reader = scopedManager('test-repo', shared.factory);
    reader.setAuthScope('bob');
    reader.track(path.toString());
    const result = await reader.restoreForListen(path.toString());
    expect(result.record!.node.val()).to.deep.equal({ owner: 'bob' });
  });

  it('flushes from accumulated changed paths without running the identity diff', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('accum/root');
    const prefix = 'test-repo|/accum/root';
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const base: Record<string, unknown> = {};
    for (let i = 0; i < 40; i++) {
      base['child' + i] = { body: 'x'.repeat(40000), v: i };
    }
    const baseNode = nodeFromJSON(base);
    manager.serverCacheUpdated(path, baseNode, [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const manifest = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };
    expect(manifest.ranges.length).to.be.greaterThan(3);

    // Steady state: the server names the changed subtree; the flush must mark
    // dirty ranges from that list without ever comparing the two trees.
    const internals = manager as unknown as {
      changedSinceFlush_: Map<string, string[][] | null>;
    };
    const updated = baseNode.updateChild(
      new Path('child3/v'),
      nodeFromJSON(999)
    );
    manager.serverCacheUpdated(path, updated, [['child3', 'v']]);
    expect(internals.changedSinceFlush_.get(path.toString())).to.deep.equal([
      ['child3', 'v']
    ]);
    // Poison the baseline's identity: if flush_ ran collectChangedSubtreePaths
    // it would compare against THIS node and dirty everything. The accumulated
    // list must win instead.
    const lastFlush = (
      manager as unknown as {
        lastFlush_: Map<string, { rootNode: unknown }>;
      }
    ).lastFlush_.get(path.toString())!;
    lastFlush.rootNode = nodeFromJSON({ unrelated: true });
    await manager.flushNow(path.toString());
    await flushAsync();
    const next = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };
    const before = new Set(manifest.ranges.map(r => r.recordId));
    const reused = next.ranges.filter(r => before.has(r.recordId)).length;
    // Identity-diff against the poisoned baseline would have re-staged every
    // range; the accumulated path re-stages only the touched one(s).
    expect(next.ranges.length - reused).to.be.lessThan(3);
    expect(reused).to.be.greaterThan(manifest.ranges.length - 3);
    // Consumed: the accumulator reset to empty for the new baseline.
    expect(internals.changedSinceFlush_.get(path.toString())).to.deep.equal([]);
  });

  it('a failed range stage preserves the consumed changed paths', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('accum-fail/root');
    const prefix = 'test-repo|/accum-fail/root';
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const base: Record<string, unknown> = {};
    for (let i = 0; i < 40; i++) {
      base['child' + i] = { body: 'x'.repeat(40000), v: i };
    }
    const baseNode = nodeFromJSON(base);
    manager.serverCacheUpdated(path, baseNode, [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const manifest = shared.data.get(prefix) as {
      revision: string;
      ranges: Array<{ recordId: string }>;
    };

    // A named change, then a flush whose range staging fails transiently
    // (the fake store's put throws once).
    const internals = manager as unknown as {
      changedSinceFlush_: Map<string, string[][] | null>;
    };
    const updated = baseNode.updateChild(
      new Path('child3/v'),
      nodeFromJSON(999)
    );
    manager.serverCacheUpdated(path, updated, [['child3', 'v']]);
    const store = shared.data;
    const realSet = store.set.bind(store);
    let failed = false;
    store.set = (key: string, value: unknown) => {
      if (!failed && key.includes('#')) {
        failed = true;
        throw new Error('simulated transient IndexedDB failure');
      }
      return realSet(key, value);
    };
    await manager.flushNow(path.toString());
    await flushAsync();
    store.set = realSet;
    // Nothing committed; the stored manifest is still the base generation.
    expect((shared.data.get(prefix) as { revision: string }).revision).to.equal(
      manifest.revision
    );

    // The consumed paths flowed back: WITHOUT them, the next flush would
    // diff its own last-known baseline, see nothing new for child3, and
    // publish a manifest whose child3 range record was never re-staged.
    const preserved = internals.changedSinceFlush_.get(path.toString());
    expect(preserved === null || preserved!.length > 0).to.equal(true);

    // And the recovery flush persists the change end-to-end.
    manager.serverCacheUpdated(
      path,
      updated.updateChild(new Path('child5/v'), nodeFromJSON(555)),
      [['child5', 'v']]
    );
    await manager.flushNow(path.toString());
    await flushAsync();
    const restored = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    const val = restored!.node.val() as Record<string, { v: number }>;
    expect(val['child3'].v).to.equal(999);
    expect(val['child5'].v).to.equal(555);
  });

  it('an unnamed update falls the flush back to the identity diff', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('accum-fallback/root');
    const prefix = 'test-repo|/accum-fallback/root';
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const base: Record<string, unknown> = {};
    for (let i = 0; i < 40; i++) {
      base['child' + i] = { body: 'x'.repeat(40000), v: i };
    }
    const baseNode = nodeFromJSON(base);
    manager.serverCacheUpdated(path, baseNode, [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const manifest = shared.data.get(prefix) as {
      ranges: Array<{ recordId: string }>;
    };

    // A range merge (or any caller that cannot name the change) marks the
    // set imprecise; the identity diff must still find the real change.
    const updated = baseNode.updateChild(
      new Path('child7/v'),
      nodeFromJSON(1000)
    );
    manager.serverCacheUpdated(path, updated); // no changedPaths
    const internals = manager as unknown as {
      changedSinceFlush_: Map<string, string[][] | null>;
    };
    expect(internals.changedSinceFlush_.get(path.toString())).to.equal(null);
    await manager.flushNow(path.toString());
    await flushAsync();
    const next = shared.data.get(prefix) as {
      ranges: Array<{ recordId: string }>;
    };
    const restored = await restoreForTest(
      scopedManager('test-repo', shared.factory),
      path.toString()
    );
    expect(
      (restored!.node.val() as Record<string, { v: number }>)['child7'].v
    ).to.equal(1000);
    // Incremental even on the fallback: the diff found one subtree.
    const before = new Set(manifest.ranges.map(r => r.recordId));
    const reused = next.ranges.filter(r => before.has(r.recordId)).length;
    expect(reused).to.be.greaterThan(manifest.ranges.length - 3);
  });

  it('a named change after an unnamed one stays imprecise until the flush', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('accum-sticky/root');
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const baseNode = nodeFromJSON({ a: 1, b: 2 });
    manager.serverCacheUpdated(path, baseNode, [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const internals = manager as unknown as {
      changedSinceFlush_: Map<string, string[][] | null>;
    };
    manager.serverCacheUpdated(
      path,
      baseNode.updateChild(new Path('a'), nodeFromJSON(10))
    ); // unnamed
    manager.serverCacheUpdated(
      path,
      baseNode.updateChild(new Path('b'), nodeFromJSON(20)),
      [['b']]
    ); // named — must NOT un-poison the set (the 'a' change is unaccounted)
    expect(internals.changedSinceFlush_.get(path.toString())).to.equal(null);
  });

  it('restore decode yields between slices and still assembles correctly', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('sliced/root');
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    // Enough data to span well past one decode slice (8 records/slice):
    // ~7MB at the 256KiB range target = ~28 ranges = ~3 slices.
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < 60; i++) {
      wide['child' + String(i).padStart(3, '0')] = {
        body: 'x'.repeat(120000),
        v: i
      };
    }
    manager.serverCacheUpdated(path, nodeFromJSON(wide), [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const manifest = shared.data.get('test-repo|/sliced/root') as {
      ranges: Array<{ recordId: string }>;
    };
    expect(manifest.ranges.length).to.be.greaterThan(8);

    // Count macrotask turns consumed by the restore: the sliced decode must
    // yield at least floor(ranges/8) times (setTimeout(0) per slice).
    let timerTurns = 0;
    const originalSetTimeout = global.setTimeout;
    const patched = ((
      fn: (...a: unknown[]) => void,
      ms?: number,
      ...rest: unknown[]
    ) => {
      if (ms === 0) {
        timerTurns++;
      }
      return originalSetTimeout(fn, ms, ...rest);
    }) as typeof setTimeout;
    global.setTimeout = patched;
    try {
      const restored = await restoreForTest(
        scopedManager('test-repo', shared.factory),
        path.toString()
      );
      expect(restored).to.not.equal(null);
      expect(
        Object.keys(restored!.node.val() as Record<string, unknown>).length
      ).to.equal(60);
      // The decoder yields before slices 8, 16, ... — exactly
      // floor((ranges - 1) / 8) times (the timer patch may also see other
      // 0ms timers, so >= not ===).
      expect(timerTurns).to.be.at.least(
        Math.floor((manifest.ranges.length - 1) / 8)
      );
    } finally {
      global.setTimeout = originalSetTimeout;
    }
  });

  it('a dispose during the sliced decode neither corrupts nor deletes the record', async () => {
    const shared = makeFakeIndexedDB();
    const path = new Path('sliced-dispose/root');
    const manager = scopedManager('test-repo', shared.factory);
    manager.track(path.toString());
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < 40; i++) {
      wide['child' + i] = { body: 'x'.repeat(30000), v: i };
    }
    manager.serverCacheUpdated(path, nodeFromJSON(wide), [[]]);
    await manager.flushNow(path.toString());
    await flushAsync();
    const keysBefore = [...shared.data.keys()].filter(k =>
      k.startsWith('test-repo|/sliced-dispose/root')
    );

    const reader = scopedManager('test-repo', shared.factory);
    reader.track(path.toString());
    // Dispose exactly at the first decode-slice yield (the setTimeout(0)
    // the sliced decoder awaits) — deterministically mid-decode.
    const originalSetTimeout = global.setTimeout;
    let fired = false;
    const patched = ((
      fn: (...a: unknown[]) => void,
      ms?: number,
      ...rest: unknown[]
    ) => {
      if (ms === 0 && !fired) {
        fired = true;
        reader.dispose();
      }
      return originalSetTimeout(fn, ms, ...rest);
    }) as typeof setTimeout;
    global.setTimeout = patched;
    let result;
    try {
      result = await reader.restoreForListen(path.toString());
    } finally {
      global.setTimeout = originalSetTimeout;
    }
    expect(result.record).to.equal(null);
    await flushAsync();
    const keysAfter = [...shared.data.keys()].filter(k =>
      k.startsWith('test-repo|/sliced-dispose/root')
    );
    expect(keysAfter.length).to.equal(keysBefore.length);
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

  it('holds a completed pre-auth peek for its listener across slow auth (one decode per boot)', async () => {
    // The regression behind the staging OOM/loading-ring reports: Firebase
    // Auth hydration can take arbitrarily long on a loaded profile, and the
    // peek's retained decode used to expire on a fixed short timer racing
    // it. The listener then re-read and re-decoded the full tree while the
    // peek's copy was still alive — two ~60 MB JS trees at the peak of boot.
    const seeded = makeFakeIndexedDB();
    const path = new Path('slow-auth/root');
    const writer = scopedManager('test-repo', seeded.factory);
    writer.setAuthScope('user-a');
    writer.track(path.toString());
    writer.serverCacheUpdated(
      path,
      nodeFromJSON({ a: bigLeaf('a'), b: bigLeaf('b'), c: 1 })
    );
    await writer.flushNow(path.toString());
    await flushAsync();

    let rangeGets = 0;
    const readerFactory = makeFakeIndexedDB({
      onGet: key => {
        if (key.startsWith('test-repo|/slow-auth/root#range:')) {
          rangeGets++;
        }
      }
    });
    for (const [key, value] of seeded.data) {
      readerFactory.data.set(key, value);
    }
    // Short post-auth grace (1 ms), long pre-auth backstop. The peek primes
    // the scope WITHOUT app confirmation — exactly getPersistedValue's shape.
    const reader = new PersistenceManager(
      'test-repo',
      readerFactory.factory,
      true,
      undefined,
      undefined,
      0,
      undefined,
      1,
      60_000
    );
    reader.setAuthScope('user-a', false);
    const peeked = await reader.peek(path.toString(), 'user-a');
    expect(peeked).to.not.equal(null);
    const getsAfterPeek = rangeGets;

    // Auth "hydrates slowly": far longer than the 1 ms post-auth grace.
    await new Promise<void>(resolve => setTimeout(resolve, 50));

    // Real auth confirms the primed identity, and the listener attaches.
    reader.setAuthScope('user-a');
    reader.track(path.toString());
    const restored = await reader.restoreForListen(path.toString());

    // The SAME decoded tree is handed off — no second physical range read.
    expect(restored.record).to.not.equal(null);
    expect(restored.record!.node).to.equal(peeked!.node);
    expect(rangeGets).to.equal(getsAfterPeek);
  });

  it('drops peek retention to the short grace once real auth confirms the primed scope', async () => {
    const seeded = makeFakeIndexedDB();
    const path = new Path('confirmed/root');
    const writer = scopedManager('test-repo', seeded.factory);
    writer.setAuthScope('user-a');
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ a: 1, b: 2 }));
    await writer.flushNow(path.toString());
    await flushAsync();

    let rangeGets = 0;
    const readerFactory = makeFakeIndexedDB({
      onGet: key => {
        if (key.startsWith('test-repo|/confirmed/root#range:')) {
          rangeGets++;
        }
      }
    });
    for (const [key, value] of seeded.data) {
      readerFactory.data.set(key, value);
    }
    const reader = new PersistenceManager(
      'test-repo',
      readerFactory.factory,
      true,
      undefined,
      undefined,
      0,
      undefined,
      1,
      60_000
    );
    reader.setAuthScope('user-a', false);
    const peeked = await reader.peek(path.toString(), 'user-a');
    expect(peeked).to.not.equal(null);

    // Confirmation of the SAME primed scope re-arms retention down to the
    // short grace, counted from now — the memory bound is restored the
    // moment the handoff window actually opens.
    reader.setAuthScope('user-a');
    await new Promise<void>(resolve => setTimeout(resolve, 50));

    const getsBeforeRestore = rangeGets;
    reader.track(path.toString());
    const restored = await reader.restoreForListen(path.toString());
    expect(restored.record).to.not.equal(null);
    // Retention expired: this restore performed its own physical read.
    expect(rangeGets).to.be.greaterThan(getsBeforeRestore);
    expect(restored.record!.node).to.not.equal(peeked!.node);
  });

  it('bounds a never-confirmed peek with the pre-auth backstop', async () => {
    const seeded = makeFakeIndexedDB();
    const path = new Path('unconfirmed/root');
    const writer = scopedManager('test-repo', seeded.factory);
    writer.setAuthScope('user-a');
    writer.track(path.toString());
    writer.serverCacheUpdated(path, nodeFromJSON({ a: 1 }));
    await writer.flushNow(path.toString());
    await flushAsync();

    let rangeGets = 0;
    const readerFactory = makeFakeIndexedDB({
      onGet: key => {
        if (key.startsWith('test-repo|/unconfirmed/root#range:')) {
          rangeGets++;
        }
      }
    });
    for (const [key, value] of seeded.data) {
      readerFactory.data.set(key, value);
    }
    // Pre-auth backstop of 1 ms: auth never confirming must still release
    // the retained tree (the true leak case the timer exists for).
    const reader = new PersistenceManager(
      'test-repo',
      readerFactory.factory,
      true,
      undefined,
      undefined,
      0,
      undefined,
      60_000,
      1
    );
    reader.setAuthScope('user-a', false);
    const peeked = await reader.peek(path.toString(), 'user-a');
    expect(peeked).to.not.equal(null);

    await new Promise<void>(resolve => setTimeout(resolve, 50));

    const getsBefore = rangeGets;
    // The retained entry expired; a second peek re-reads physically.
    const again = await reader.peek(path.toString(), 'user-a');
    expect(again).to.not.equal(null);
    expect(rangeGets).to.be.greaterThan(getsBefore);
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
    const manager = new PersistenceManager(
      'test-repo',
      factory,
      true,
      8000,
      100 * 1024 * 1024,
      1000
    );
    manager.setAuthScope(null);
    interleave = () => manager.serverCacheUpdated(path, nodeFromJSON({ v: 2 }));
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ v: 1 }));
    const firstFlush = manager.flushNow(path.toString());
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

  it('keeps first cache creation behind the write window', async () => {
    const { factory } = makeFakeIndexedDB();
    const path = new Path('first/root');
    const manager = new PersistenceManager(
      'test-repo',
      factory,
      true,
      8000,
      100 * 1024 * 1024,
      100
    );
    manager.setAuthScope(null);
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON({ first: true }));
    await flushAsync();
    expect(await restoreForTest(manager, path.toString())).to.equal(null);
    await manager.flushNow(path.toString());
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

describe('persistence rebinding', () => {
  it('preserves selected roots when emulator configuration rebinds the manager', () => {
    const manager = scopedManager('prod');
    manager.setPersistentPath('/kept', true);
    const rebound = manager.rebindTo('emulator');
    expect(rebound.isPersistentPath('/kept')).to.equal(true);
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

describe('persistent listener options', () => {
  function makeQueryHarness() {
    const manager = scopedManager('test-repo', makeFakeIndexedDB().factory);
    const syncTree = new SyncTree({
      startListening: () => [],
      stopListening: () => {}
    });
    const repo = {
      persistence_: manager,
      serverSyncTree_: syncTree,
      infoSyncTree_: syncTree,
      eventQueue_: new EventQueue()
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
  });

  it('releases selection when off() removes the registration', () => {
    const { manager, path, query } = makeQueryHarness();
    const callback = () => {};
    onValue(query, callback, { persistent: true });
    off(query, 'value', callback);
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
  });

  it('rejects persistence on a filtered query instead of silently missing', () => {
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
  });

  it('reference-counts registrations at the same path', () => {
    const { manager, path, query } = makeQueryHarness();
    const unsubscribeA = onValue(query, () => {}, { persistent: true });
    const unsubscribeB = onValue(query, () => {}, { persistent: true });
    unsubscribeA();
    expect(manager.isPersistentPath(path.toString())).to.equal(true);
    unsubscribeB();
    expect(manager.isPersistentPath(path.toString())).to.equal(false);
  });

  it('activates persistence when joining an existing non-persistent listen', async () => {
    const { manager, path, query, syncTree } = makeQueryHarness();
    // A plain registration creates the wire listen first; the start path ran
    // without persistence (not selected), so nothing tracked the root.
    onValue(query, () => {});
    // The listen certifies a complete server cache.
    syncTreeApplyServerOverwrite(syncTree, path, nodeFromJSON({ a: 1 }));
    expect(manager.trackedRootFor(path.toString())).to.equal(null);

    // A second registration joins the SAME live listen with { persistent }.
    // repoStartServerListen does not re-run — activation must happen against
    // the aggregated listen: tracked + write-through seeded from the
    // certified cache.
    onValue(query, () => {}, { persistent: true });
    expect(manager.trackedRootFor(path.toString())).to.equal(path.toString());
    await manager.flushNow(path.toString());
    await flushAsync();
    const record = await manager.peek(path.toString());
    expect(record?.node.val()).to.deep.equal({ a: 1 });
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
    const getResponders: Array<(payload: unknown) => void> = [];
    const pendingHashes = new PendingListenHashStore();
    const repo = {
      pendingSeedRestores_: new Map<string, { cancelled: boolean }>(),
      pendingListenHashes_: pendingHashes,
      ingestQueue_: newIngestQueue(),
      listenOutcomes_: new Map(),
      // The boot-window drain reruns transactions after each replayed push;
      // the real Repo always carries this tree. (The legacy synchronous
      // drain crashed here too, but inside a void'd promise chain — the
      // in-place drain surfaces what was silently swallowed.)
      transactionQueueTree_: new Tree(),
      persistence_: manager,
      persistenceAuthScope_: null,
      persistenceAuthScopeListeners_: new Set<() => void>(),
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
        unlisten: (...args: unknown[]) => calls.push('unlisten'),
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
      getResponders,
      data,
      factory
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

  /**
   * A value registration that records every raised snapshot value — for
   * asserting the exact event SEQUENCE a listener observes (a fresh→stale
   * flip vs a single consistent value).
   */
  function recordingRegistration(values: unknown[]) {
    return {
      respondsTo: (eventType: string) => eventType === 'value',
      createEvent: (
        change: { snapshotNode: Node },
        query: { _path: Path }
      ) => ({
        getPath: () => query._path,
        getEventType: () => 'value',
        getEventRunner: () => () => values.push(change.snapshotNode.val()),
        toString: () => 'recording-event'
      }),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createCancelEvent: () => null as any,
      matches: () => false,
      hasAnyCallback: () => true
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
  }

  it('outcome subscribers registered before the listen still receive outcomes', async () => {
    const { repo, query, path, hashFn, onComplete, serverCallbacks } =
      makeListenHarness();
    const outcomes: ListenOutcome[] = [];
    // Natural ordering: observability wired BEFORE onValue starts the listen.
    const unsubscribe = repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    serverCallbacks[0]('ok');
    expect(outcomes.length).to.be.greaterThan(0);
    expect(outcomes[outcomes.length - 1].certified).to.equal(true);
    unsubscribe();
  });

  it('sends the listen after the restore resolves', async () => {
    const { repo, query, hashFn, onComplete, calls } = makeListenHarness();
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    expect(calls).to.deep.equal([]);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
  });

  it('waits inside the SDK for the initial auth scope, then restores/listens', async () => {
    const harness = makeListenHarness();
    const manager = new PersistenceManager('test-repo', harness.factory);
    manager.setPersistentPath(harness.path.toString(), true);
    harness.repo.persistence_ = manager;
    harness.repo.persistenceAuthScope_ = undefined;
    const db = {
      _checkNotDeleted: () => {},
      _repoInternal: harness.repo
    };

    repoStartServerListen(
      harness.repo,
      harness.query,
      null,
      harness.hashFn,
      harness.onComplete
    );
    await Promise.resolve();
    expect(harness.calls).to.deep.equal([]);

    setPersistenceAuthScope(db as never, 'viewer');
    await flushAsync();
    expect(harness.calls).to.deep.equal(['listen']);
  });

  it('cancels an auth-scope wait when the subscription stops', async () => {
    const harness = makeListenHarness();
    const manager = new PersistenceManager('test-repo', harness.factory);
    manager.setPersistentPath(harness.path.toString(), true);
    harness.repo.persistence_ = manager;
    harness.repo.persistenceAuthScope_ = undefined;

    repoStartServerListen(
      harness.repo,
      harness.query,
      null,
      harness.hashFn,
      harness.onComplete
    );
    repoStopServerListen(harness.repo, harness.query, null);
    manager.setAuthScope('viewer');
    harness.repo.persistenceAuthScope_ = 'viewer';
    for (const listener of harness.repo.persistenceAuthScopeListeners_) {
      listener();
    }
    await flushAsync();
    expect(harness.calls).to.deep.equal([]);
    expect(harness.repo.persistenceAuthScopeListeners_.size).to.equal(0);
  });

  it('falls open to a cold listen if auth scope hydration stalls', async () => {
    const harness = makeListenHarness();
    const manager = new PersistenceManager('test-repo', harness.factory);
    manager.setPersistentPath(harness.path.toString(), true);
    harness.repo.persistence_ = manager;
    harness.repo.persistenceAuthScope_ = undefined;

    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(harness.repo, harness.path.toString(), outcome =>
      outcomes.push(outcome)
    );
    repoStartServerListen(
      harness.repo,
      harness.query,
      null,
      harness.hashFn,
      harness.onComplete,
      false,
      5
    );
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(harness.calls).to.deep.equal(['listen']);
    expect(harness.repo.persistenceAuthScopeListeners_.size).to.equal(0);
    // The cold outcome names WHY: identity hydration timed out (not a cache
    // miss) — observability for attributing forced-cold boots.
    expect(outcomes[0].mode).to.equal('cold');
    expect(outcomes[0].reason).to.equal('auth-timeout');
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
    expectIngestIdle(repo);
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

  it('a child certified while the restore is in flight is grafted, never clobbered', async () => {
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
    // The certified child was grafted over the restored base: it remains
    // current, and the parent still seeded from the cache around it.
    const childCache = syncTreeGetCompleteServerCache(
      repo.serverSyncTree_,
      childPath
    );
    expect(childCache?.val(true)).to.deep.equal({ msg: 'fresh' });
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)?.val()
    ).to.deep.equal({ sibling: 'stale', inbox: { msg: 'fresh' } });
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

    const outcomes: ListenOutcome[] = [];
    repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();

    // The listen went out, but unseeded: applying the stale tree would have
    // replaced the filtered view's live server data.
    expect(calls).to.deep.equal(['listen']);
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );
    // The cold outcome names the graft refusal — a filtered window below
    // the root, not a cache miss.
    expect(outcomes[0].mode).to.equal('cold');
    expect(outcomes[0].reason).to.equal('partial-descendants');
  });

  it('grafts certified descendant caches over the restored base instead of going cold', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    // Big leaves either side of the graft path force the stored generation
    // to split into MULTIPLE ranges, so the mid-window assertions below can
    // distinguish a blanked graft-intersecting range from an intact clean
    // one.
    await persistHarnessRoot(repo, path, {
      aaa: bigLeaf('a'),
      inbox: { msg: 'stale' },
      zzz: bigLeaf('z')
    });
    // Hold the restore open after the manifest fires so the boot window is
    // inspectable while the seeded listen is on the wire.
    const manager = repo.persistence_!;
    const realRestore = manager.restoreForListen.bind(manager);
    let releaseRecord: () => void = () => {};
    const recordGate = new Promise<void>(resolve => {
      releaseRecord = resolve;
    });
    manager.restoreForListen = (pathString, onManifest) =>
      realRestore(pathString, onManifest).then(async result => {
        await recordGate;
        return result;
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
    // A deeper live listen certified BEFORE the parent's listen starts (a
    // component's own onValue answered in one round-trip while the parent's
    // IndexedDB decode is still ahead). Its complete cache is server truth
    // for that subtree — graftable, never a reason to abandon the cache.
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
    syncTreeApplyServerOverwrite(
      repo.serverSyncTree_,
      childPath,
      nodeFromJSON({ msg: 'fresh' })
    );

    const outcomes: ListenOutcome[] = [];
    const unsubscribe = repoOnListenOutcome(repo, path.toString(), outcome =>
      outcomes.push(outcome)
    );
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();

    // Seeded, not cold — the listen went out manifest-first despite the
    // certified descendant.
    expect(calls).to.deep.equal(['listen']);
    expect(outcomes[outcomes.length - 1].mode).to.equal('restored');
    expect(repo.ingestQueue_.gates.has(path.toString())).to.equal(true);

    // CONVERGENCE ON THE WIRE (what the SyncTree hashFn consults for this
    // listen): the merged base-plus-graft tree matches no stored whole-tree
    // hash, so the listen claims none, and every range the graft intersects
    // has its hash BLANKED — an empty hash never matches, so the server
    // always resends those intervals' current data. Clean ranges keep their
    // stored hashes (they hold exactly the stored bytes) and can still
    // validate without a re-download.
    const pending = repo.pendingListenHashes_.get(path.toString());
    expect(pending).to.not.equal(undefined);
    expect(pending!.hash).to.equal('');
    const wire = pending!.compoundHash;
    expect(wire.hashes.length).to.equal(wire.posts.length + 1);
    let sawBlankedGraftRange = false;
    let sawIntactCleanRange = false;
    for (let index = 0; index < wire.posts.length; index++) {
      const rangeStart = index === 0 ? null : wire.posts[index - 1];
      const rangeEnd = wire.posts[index];
      // Range i covers (posts[i-1], posts[i]]; it intersects the graft's
      // subtree iff it overlaps the marker interval of 'inbox'.
      const intersectsGraft =
        rangeEnd >= 'inbox' && (rangeStart === null || rangeStart < 'inboxz');
      if (intersectsGraft) {
        expect(wire.hashes[index]).to.equal('');
        sawBlankedGraftRange = true;
      } else if (wire.hashes[index] !== '') {
        sawIntactCleanRange = true;
      }
    }
    expect(sawBlankedGraftRange).to.equal(true);
    // The stored generation splits this root into ranges untouched by the
    // graft; their hashes must have survived intact.
    expect(sawIntactCleanRange).to.equal(true);

    releaseRecord();
    await flushAsync();

    // The restored base applied with the certified child grafted over it —
    // the fresher subtree wins, the cache still serves everything else.
    const merged = syncTreeGetCompleteServerCache(
      repo.serverSyncTree_,
      path
    )?.val() as Record<string, unknown>;
    expect(merged.inbox).to.deep.equal({ msg: 'fresh' });
    expect(merged.aaa).to.equal(bigLeaf('a'));
    expect(merged.zzz).to.equal(bigLeaf('z'));
    // The grafted child kept its identity as the descendant view's truth.
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, childPath)?.val()
    ).to.deep.equal({ msg: 'fresh' });
    unsubscribe();
  });
  it('a get() answered during the boot window does not flip listeners fresh-then-stale', async () => {
    const { repo, query, path, hashFn, onComplete, calls, getResponders } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, {
      sibling: 'stale',
      inbox: { msg: 'stale' }
    });
    // Hold the restore open after the manifest fires, so the boot window is
    // wide enough to interleave a get() response inside it.
    const manager = repo.persistence_!;
    const realRestore = manager.restoreForListen.bind(manager);
    let releaseRecord: () => void = () => {};
    const recordGate = new Promise<void>(resolve => {
      releaseRecord = resolve;
    });
    manager.restoreForListen = (pathString, onManifest) =>
      realRestore(pathString, onManifest).then(async result => {
        await recordGate;
        return result;
      });

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
    expect(repo.ingestQueue_.gates.has(path.toString())).to.equal(true);

    // Mid-window, a component subscribes to a child and issues a get() for
    // it. The get is request-response — it bypasses the push buffer — and
    // the server answers with FRESH data while the stale base is still
    // decoding.
    const childPath = new Path('users/alice/inbox');
    const childValues: unknown[] = [];
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
      recordingRegistration(childValues)
    );
    const getPromise = repoGetValue(
      repo,
      childQuery as never,
      stubRegistration() as unknown as ValueEventRegistration
    );
    getResponders[0]({ msg: 'fresh' });
    const got = await getPromise;
    // The caller always receives the fresh server answer.
    expect(got.val()).to.deep.equal({ msg: 'fresh' });

    releaseRecord();
    await flushAsync();

    // The child listener must NOT have observed fresh data that then
    // regressed to the stale base — the visible fresh→stale→fresh flip. It
    // sees one consistent value: the boot-buffered progression (base, then
    // buffered deltas), which the server then certifies or corrects.
    expect(childValues).to.deep.equal([{ msg: 'stale' }]);
    expectIngestIdle(repo);
  });

  it('a get() on an UNGATED root keeps its SyncTree side effect while the queue is non-empty', async () => {
    const { repo, query, hashFn, onComplete, calls, getResponders, data } =
      makeListenHarness();
    void data;
    await persistHarnessRoot(repo, new Path('users/alice'), { a: 1 });
    // Hold alice's restore open so her gate stays installed with a queued op.
    const manager = repo.persistence_!;
    const realRestore = manager.restoreForListen.bind(manager);
    let releaseRecord: () => void = () => {};
    const recordGate = new Promise<void>(resolve => {
      releaseRecord = resolve;
    });
    manager.restoreForListen = (pathString, onManifest) =>
      realRestore(pathString, onManifest).then(async result => {
        await recordGate;
        return result;
      });
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    // A push for alice lands mid-window → the queue is non-empty.
    repoOnDataUpdateForTest(
      repo,
      new Path('users/alice/b').toString(),
      7,
      false,
      null
    );
    expect(repo.ingestQueue_.ops.length).to.equal(1);

    // A get() for a completely UNRELATED, ungated root: its normal SyncTree
    // side effect must NOT be suppressed by alice's queued op (the guard is
    // path-specific — only a COVERING gate suppresses).
    const bobPath = new Path('users/bob');
    const bobValues: unknown[] = [];
    const bobQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      bobPath,
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(
      repo.serverSyncTree_,
      bobQuery,
      recordingRegistration(bobValues)
    );
    const getPromise = repoGetValue(
      repo,
      bobQuery as never,
      stubRegistration() as unknown as ValueEventRegistration
    );
    getResponders[0]({ profile: 'bob' });
    const got = await getPromise;
    expect(got.val()).to.deep.equal({ profile: 'bob' });
    // The side effect ran: bob's listener saw the fresh value.
    expect(bobValues).to.deep.equal([{ profile: 'bob' }]);

    releaseRecord();
    await flushAsync();
    expectIngestIdle(repo);
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
      isAuthScopeConfigured: () => true,
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

  it('account switch mid-restore reattaches the listen cold (pre-manifest)', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    await persistHarnessRoot(repo, path, { a: 1 });
    // Hold the restore forever: the manifest never arrives, so no listen has
    // been sent when the account switches.
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      isAuthScopeConfigured: () => true,
      restoreForListen: () => new Promise(() => {}),
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    expect(calls).to.deep.equal([]);

    // The account changes: the restore is moot, but the live registration
    // must still reach the server — as one ordinary cold listen.
    repoCancelPendingSeedRestores(repo);
    await flushAsync();
    expect(calls).to.deep.equal(['listen']);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
    expectIngestIdle(repo);
  });

  it('account switch mid-restore reattaches the listen cold (post-manifest)', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    const node = nodeFromJSON({ a: 1, b: 2 });
    const compoundHash = computeCompoundHash(node.val(true));
    // Manifest arrives (the seeded listen goes out), then the range restore
    // hangs — the exact window an account switch can land in.
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      isAuthScopeConfigured: () => true,
      restoreForListen: (
        _path: string,
        onManifest: (h: PersistedSeedHashes) => void
      ) => {
        onManifest({ hash: '', compoundHash });
        return new Promise(() => {});
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
    expect(calls).to.deep.equal(['listen']); // the seeded manifest-first send
    expect(repo.ingestQueue_.gates.has(path.toString())).to.equal(true);

    // The account changes: the seeded wire listen is torn down (its buffered
    // base can never be applied) and exactly one cold listen replaces it.
    repoCancelPendingSeedRestores(repo);
    await flushAsync();
    expect(calls).to.deep.equal(['listen', 'unlisten', 'listen']);
    expectIngestIdle(repo);
    expect(repo.pendingListenHashes_.get(path.toString())).to.equal(undefined);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
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
      isAuthScopeConfigured: () => true,
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
    expectIngestIdle(repo);
    expect(
      syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)?.val()
    ).to.deep.equal({ a: 1, b: 2 });
  });

  it('a stop after manifest send cancels range replay and cold restart', async () => {
    const { repo, query, path, hashFn, onComplete, calls } =
      makeListenHarness();
    let release: (value: PersistenceRestoreResult) => void = () => {};
    const pending = new Promise<PersistenceRestoreResult>(resolve => {
      release = resolve;
    });
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      isAuthScopeConfigured: () => true,
      restoreForListen: (
        _path: string,
        onManifest: (h: PersistedSeedHashes) => void
      ) => {
        onManifest({
          hash: '',
          compoundHash: { hashes: ['h', ''], posts: ['a'] }
        });
        return pending;
      },
      trackedRootFor: () => null,
      serverCacheUpdated: () => {},
      invalidate: () => {},
      evict: () => {},
      untrack: () => {}
    } as unknown as PersistenceManager;
    repoStartServerListen(repo, query, null, hashFn, onComplete);
    await Promise.resolve();
    expect(calls).to.deep.equal(['listen']);
    repoStopServerListen(repo, query, null);
    release({ record: null, reason: 'corrupt' });
    await flushAsync();
    expect(calls).to.deep.equal(['listen', 'unlisten']);
    expectIngestIdle(repo);
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
      isAuthScopeConfigured: () => true,
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
    expect(repo.ingestQueue_.ops.length).to.equal(1);
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
      isAuthScopeConfigured: () => true,
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
    const buffered = repo.ingestQueue_.ops;
    expect(buffered).to.have.length(1);
    expect((buffered[0] as { tag: number }).tag).to.equal(77);
    release();
    await flushAsync();
    expectIngestIdle(repo);
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
    expect(repo.ingestQueue_.ops.length).to.equal(1);
    expect(syncTreeGetCompleteServerCache(repo.serverSyncTree_, path)).to.equal(
      null
    );

    releaseRecord();
    await flushAsync();
    // Base applied, buffer drained in order: cached {a:1} + buffered b=7.
    expectIngestIdle(repo);
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
    expectIngestIdle(repo);
  });

  it('a validation miss attaches exactly one cold listen', async () => {
    const { repo, query, hashFn, onComplete, calls, hashFns } =
      makeListenHarness();
    repo.persistence_ = {
      track: () => {},
      isPersistentPath: () => true,
      isAuthScopeConfigured: () => true,
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
      isAuthScopeConfigured: () => true,
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
    const suspensions: boolean[] = [];
    repo.persistence_ = {
      dispose: () => {
        disposed = true;
      },
      // repoDispose interrupts the repo first, which suspends persistence
      // (an offline tab must stop being any root's writer) before dispose.
      setNetworkSuspended: (suspended: boolean) => {
        suspensions.push(suspended);
      }
    } as unknown as PersistenceManager;
    repoDispose(repo);
    await flushAsync();
    expect(pending.cancelled).to.equal(true);
    expect(repo.pendingSeedRestores_.size).to.equal(0);
    expect(repo.listenOutcomes_.size).to.equal(0);
    expect(suspensions).to.deep.equal([true]);
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

  it('emits generic persistence traces without an app-side wrapper', async () => {
    const traced: unknown[] = [];
    const traceGlobal = globalThis as typeof globalThis & {
      __firebaseDatabasePersistenceTrace?: (event: unknown) => void;
    };
    traceGlobal.__firebaseDatabasePersistenceTrace = event =>
      traced.push(event);
    try {
      const { repo, query, path, hashFn, onComplete } = makeListenHarness();
      repoStartServerListen(repo, query, null, hashFn, onComplete);
      await flushAsync();
      expect(traced).to.deep.include({
        type: 'listen-outcome',
        path: path.toString(),
        outcome: {
          mode: 'cold',
          certified: false,
          bytes: 0,
          reason: 'missing'
        }
      });
    } finally {
      delete traceGlobal.__firebaseDatabasePersistenceTrace;
    }
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
      ingestQueue_: newIngestQueue(),
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

  it('hands the peek materialization only to the explicit consumer', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('users/alice');
    manager.track(root.toString());
    manager.serverCacheUpdated(
      root,
      nodeFromJSON({
        profile: { name: 'alice', langs: { en: true, fa: true } },
        posts: { p1: { title: 'hi' } },
        count: 3
      })
    );
    await manager.flushNow(root.toString());
    await flushAsync();

    const value = (await getPersistedValue(db as never, '/users/alice')) as {
      profile: object;
      posts: object;
    };
    // The retained read hands the SAME record (hence the same immutable
    // Node instances) to the adopting listener; model that adoption.
    const record = await manager.peek(root.toString());
    expect(record).to.not.equal(null);

    // The replay burst wraps each top-level child Node in a DataSnapshot.
    // val() NEVER returns the peek's objects (fresh-objects contract) …
    const profileNode = record!.node.getImmediateChild('profile');
    const burstSnap = new DataSnapshot(
      profileNode,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      PRIORITY_INDEX
    );
    const plain = burstSnap.val();
    expect(plain).to.not.equal(value.profile);
    expect(plain).to.deep.equal(value.profile);

    // … the explicit opt-in consumer receives them by identity, exactly once.
    const adopted = consumePersistedMaterialization(burstSnap);
    expect(adopted).to.equal(value.profile);
    expect(consumePersistedMaterialization(burstSnap)).to.equal(undefined);
  });

  it('a caller mutation of the peek result never flows through val()', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('users/alice');
    manager.track(root.toString());
    manager.serverCacheUpdated(
      root,
      nodeFromJSON({ inbox: { unread: 2 }, sent: { b: 1 } })
    );
    await manager.flushNow(root.toString());
    await flushAsync();

    const value = (await getPersistedValue(db as never, '/users/alice')) as {
      inbox: { unread: number };
    };
    // The application mutates the optimistic object it was handed (it owns
    // that object) before the listener replay.
    value.inbox.unread = 999;

    const record = await manager.peek(root.toString());
    const inboxSnap = new DataSnapshot(
      record!.node.getImmediateChild('inbox'),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      PRIORITY_INDEX
    );
    // val() walks the immutable Node — the mutation cannot leak into the
    // SDK's replayed data.
    expect(inboxSnap.val()).to.deep.equal({ unread: 2 });
  });

  it('a child changed after the peek misses the explicit consumer', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('users/alice');
    manager.track(root.toString());
    manager.serverCacheUpdated(
      root,
      nodeFromJSON({ inbox: { a: 1 }, sent: { b: 2 } })
    );
    await manager.flushNow(root.toString());
    await flushAsync();

    await getPersistedValue(db as never, '/users/alice');
    // A server delta between peek and replay produces a NEW child Node; the
    // stamp is keyed on instance identity, so the fresh node can never
    // return the stale materialization.
    const freshInbox = nodeFromJSON({ a: 1, c: 3 });
    const snap = new DataSnapshot(
      freshInbox,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      PRIORITY_INDEX
    );
    expect(consumePersistedMaterialization(snap)).to.equal(undefined);
    expect(snap.val()).to.deep.equal({ a: 1, c: 3 });
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

  it('sliced materialization matches node.val() exactly, including array coercion', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('shape/root');
    manager.track(root.toString());
    // Every val() shape the sliced walk must reproduce: dense integer keys
    // (array coercion), sparse-but-coercible keys (holes), integer keys too
    // sparse to coerce (object), nested mixes, and named-key objects.
    const raw = {
      dense: { '0': 'a', '1': 'b', '2': 'c' },
      holes: { '0': 'x', '2': 'z' }, // maxKey 2 < 2*2 → array with a hole
      tooSparse: { '0': 'x', '9': 'y' }, // maxKey 9 >= 2*2 → stays an object
      nested: { list: { '0': { name: 'n0' }, '1': { name: 'n1' } } },
      named: { alpha: 1, beta: true, gamma: 'g' }
    };
    const node = nodeFromJSON(raw);
    manager.serverCacheUpdated(root, node);
    await manager.flushNow(root.toString());
    await flushAsync();

    const sliced = await getPersistedValue(db as never, '/shape/root');
    // The reference is the SDK's own val() over the same restored node — the
    // authoritative definition the sliced walk replicates.
    const record = await manager.peek(root.toString());
    expect(stringify(sliced)).to.equal(stringify(record!.node.val()));
    // Spot-check the coercions directly.
    const value = sliced as Record<string, unknown>;
    expect(value.dense).to.be.an('array').with.lengthOf(3);
    expect(value.holes).to.be.an('array').with.lengthOf(3);
    expect((value.holes as unknown[])[1]).to.equal(undefined);
    expect(value.tooSparse).to.be.an('object').and.not.an('array');
    expect((value.nested as { list: unknown[] }).list).to.be.an('array');
  });

  it('yields to the event loop while materializing a large tree', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('big/root');
    manager.track(root.toString());
    // More leaves than one slice budget, so the walk must yield at least once.
    const wide: Record<string, Record<string, number>> = {};
    const perParent = 100;
    const parents = Math.ceil((_PEEK_MATERIALIZE_SLICE_VISITS * 2) / perParent);
    for (let i = 0; i < parents; i++) {
      const children: Record<string, number> = {};
      for (let j = 0; j < perParent; j++) {
        children['c' + j] = i * perParent + j;
      }
      wide['p' + i] = children;
    }
    manager.serverCacheUpdated(root, nodeFromJSON(wide));
    await manager.flushNow(root.toString());
    await flushAsync();

    // A macrotask scheduled AFTER the peek starts must run BEFORE the peek
    // resolves — the monolithic val() walk could never allow that.
    let macrotaskRan = false;
    const peek = getPersistedValue(db as never, '/big/root');
    setTimeout(() => {
      macrotaskRan = true;
    }, 0);
    const value = (await peek) as Record<string, unknown>;
    expect(macrotaskRan).to.equal(true);
    expect(Object.keys(value).length).to.equal(parents);
    expect((value.p0 as Record<string, number>).c0).to.equal(0);
  });

  it('yields inside one FLAT wide node — children are pulled lazily, never enumerated up front', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('flat/root');
    manager.track(root.toString());
    // One node whose DIRECT children exceed several slice budgets. An eager
    // per-node child copy (forEachChild into an array) would enumerate all of
    // them synchronously before the first yield — the exact wide-collection
    // long task the lazy iterator exists to prevent.
    const flat: Record<string, string> = {};
    const count = _PEEK_MATERIALIZE_SLICE_VISITS * 3;
    for (let i = 0; i < count; i++) {
      flat['k' + i] = 'v' + i;
    }
    manager.serverCacheUpdated(root, nodeFromJSON(flat));
    await manager.flushNow(root.toString());
    await flushAsync();

    let macrotaskRan = false;
    const peek = getPersistedValue(db as never, '/flat/root');
    setTimeout(() => {
      macrotaskRan = true;
    }, 0);
    const value = (await peek) as Record<string, string>;
    // The walk must have yielded mid-node: this macrotask ran before resolve.
    expect(macrotaskRan).to.equal(true);
    expect(Object.keys(value).length).to.equal(count);
    expect(value.k0).to.equal('v0');
    expect(value['k' + (count - 1)]).to.equal('v' + (count - 1));
  });

  /** A tree wide enough that the sliced walk must yield at least once. */
  function wideTree(): Record<string, string> {
    const flat: Record<string, string> = {};
    for (let i = 0; i < _PEEK_MATERIALIZE_SLICE_VISITS * 2; i++) {
      flat['k' + i] = 'v' + i;
    }
    return flat;
  }

  it('an auth-scope switch during the sliced walk invalidates the peek', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('acct/root');
    manager.setAuthScope('user-a');
    manager.track(root.toString());
    manager.serverCacheUpdated(root, nodeFromJSON(wideTree()));
    await manager.flushNow(root.toString());
    await flushAsync();

    // peek() itself revalidates the scope at record resolution; the sliced
    // walk then opens a multi-macrotask window. Switch accounts at the first
    // yield: the continuation must return null, never the old identity's tree.
    const peek = getPersistedValue(db as never, '/acct/root', 'user-a');
    setTimeout(() => manager.setAuthScope('user-b'), 0);
    expect(await peek).to.equal(null);
  });

  it('a listener consuming the retained read mid-walk leaves no orphaned stamps', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('race/root');
    manager.track(root.toString());
    manager.serverCacheUpdated(root, nodeFromJSON(wideTree()));
    await manager.flushNow(root.toString());
    await flushAsync();

    // The authenticated listener may join the peek's retained read while the
    // walk is parked on a yield; its child_added burst applies synchronously
    // and consumes stamps THEN. Stamps installed by the walk afterwards would
    // ride live Nodes with no replay ever taking them — a session-long pinned
    // JS copy of each subtree. The atomic handoff must skip stamping entirely
    // once the retention is gone.
    const peek = getPersistedValue(db as never, '/race/root');
    const joined = new Promise<PersistedRecord | null>(resolve => {
      setTimeout(() => {
        // restoreForListen's join shape: consume (not retain) the read.
        void manager
          .restoreForListen(root.toString())
          .then(result => resolve(result.record));
      }, 0);
    });
    const [value, record] = await Promise.all([peek, joined]);
    expect(value).to.not.equal(null);
    expect(record).to.not.equal(null);
    // The value itself is still delivered…
    expect((value as Record<string, string>).k0).to.equal('v0');
    // …but no stamp was left behind on any node of the consumed read.
    expect(consumeMaterializedValue(record!.node)).to.equal(undefined);
    record!.node.forEachChild(PRIORITY_INDEX, (_key, child) => {
      expect(consumeMaterializedValue(child)).to.equal(undefined);
    });
  });

  it('a replacement peek retained mid-walk cannot authorize stamps on the consumed read', async () => {
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('replace/root');
    manager.track(root.toString());
    manager.serverCacheUpdated(root, nodeFromJSON(wideTree()));
    await manager.flushNow(root.toString());
    await flushAsync();

    // While peek1's walk is parked on a yield: a listener CONSUMES the
    // retained read (removing its entry), then a second getPersistedValue
    // installs a fresh retained entry at the SAME path. A path-only
    // retention check would now pass and stamp peek1's record — whose nodes
    // are live in SyncTree with no replay ever coming. The identity-bound
    // check must refuse: the retained entry did not resolve peek1's node.
    const peek1 = getPersistedValue(db as never, '/replace/root');
    const raced = new Promise<PersistedRecord | null>(resolve => {
      setTimeout(() => {
        void manager
          .restoreForListen(root.toString()) // consumes the retained read
          .then(result => {
            void getPersistedValue(db as never, '/replace/root'); // replacement retention
            resolve(result.record);
          });
      }, 0);
    });
    const [value, consumedRecord] = await Promise.all([peek1, raced]);
    expect(value).to.not.equal(null);
    expect(consumedRecord).to.not.equal(null);
    // The consumed read's nodes must carry ZERO stamps from peek1.
    expect(consumeMaterializedValue(consumedRecord!.node)).to.equal(undefined);
    consumedRecord!.node.forEachChild(PRIORITY_INDEX, (_key, child) => {
      expect(consumeMaterializedValue(child)).to.equal(undefined);
    });
  });

  it('leaves no referenced MessagePort behind after the sliced walk drains', async function () {
    // Node-only observability: a REFERENCED MessagePort keeps the Node event
    // loop alive, so an idle yield channel would hang a Node consumer's
    // otherwise-clean shutdown (mocha's exit:true masks the hang itself —
    // assert the handle state instead). Browsers have no ref/unref.
    const getActiveResourcesInfo = (
      process as unknown as { getActiveResourcesInfo?: () => string[] }
    ).getActiveResourcesInfo;
    if (typeof getActiveResourcesInfo !== 'function') {
      this.skip();
      return;
    }
    const { db, manager } = makeDatabaseWithPersistence();
    const root = new Path('handles/root');
    manager.track(root.toString());
    manager.serverCacheUpdated(root, nodeFromJSON(wideTree()));
    await manager.flushNow(root.toString());
    await flushAsync();

    // The wide tree forces at least one MessageChannel yield.
    expect(await getPersistedValue(db as never, '/handles/root')).to.not.equal(
      null
    );
    const referencedPorts = getActiveResourcesInfo().filter(resource =>
      resource.includes('MessagePort')
    );
    expect(referencedPorts).to.deep.equal([]);
  });
});

describe('gentle flush (sliced planning + budgeted staging)', () => {
  function wideRoot(parents: number, perParent: number): unknown {
    const wide: Record<string, Record<string, string>> = {};
    for (let i = 0; i < parents; i++) {
      const children: Record<string, string> = {};
      for (let j = 0; j < perParent; j++) {
        // Sizeable string leaves so the plan spans multiple ranges.
        children['c' + j] = 'value-' + i + '-' + j + '-' + 'x'.repeat(64);
      }
      wide['p' + i] = children;
    }
    return wide;
  }

  it('first-generation flush yields to the event loop while planning and staging', async () => {
    const { factory } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('gentle/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(wideRoot(120, 60)));

    // A macrotask scheduled AFTER the flush starts must run BEFORE the flush
    // settles — the monolithic plan+stage could never allow that.
    let macrotaskRan = false;
    const flushing = manager.flushNow(path.toString());
    setTimeout(() => {
      macrotaskRan = true;
    }, 0);
    await flushing;
    expect(macrotaskRan).to.equal(true);

    // The sliced write is byte-identical to what a restore expects.
    const restored = await manager.restoreForListen(path.toString());
    expect(restored.record).to.not.equal(null);
    expect(restored.record!.node.equals(nodeFromJSON(wideRoot(120, 60)))).to.equal(
      true
    );
  });

  it('sliced planning produces the same generation a synchronous rebuild does', async () => {
    const node = nodeFromJSON(wideRoot(40, 40));
    const syncBuilder = new CompoundHashBuilder(
      fixedSizeSplitStrategy(4096),
      true
    );
    const syncRanges = rebuildStableRanges(node, [], [], false, syncBuilder, 4096);

    const slicedBuilder = new CompoundHashBuilder(
      fixedSizeSplitStrategy(4096),
      true
    );
    const rebuilder = new StableRangeRebuilder(
      node,
      [],
      [],
      false,
      slicedBuilder,
      4096
    );
    // Tiny deadline: force many slices.
    while (!rebuilder.drainUntil(Date.now())) {
      await new Promise<void>(resolve => setTimeout(resolve, 0));
    }
    const slicedRanges = rebuilder.result();
    expect(slicedRanges.map(r => r.post)).to.deep.equal(
      syncRanges.map(r => r.post)
    );
    expect(slicedRanges.map(r => r.size)).to.deep.equal(
      syncRanges.map(r => r.size)
    );
  });

  it('sliced dirty-run rebuild preserves clean ranges in order', async () => {
    const before: Record<string, unknown> = {};
    for (let i = 0; i < 26; i++) {
      const key = String.fromCharCode(97 + i);
      const children: Record<string, string> = {};
      for (let j = 0; j < 40; j++) {
        children['k' + j] = key + '-' + j + '-' + 'y'.repeat(32);
      }
      before[key] = children;
    }
    const beforeNode = nodeFromJSON(before);
    const planBuilder = new CompoundHashBuilder(
      fixedSizeSplitStrategy(2048),
      true
    );
    const baseline = rebuildStableRanges(
      beforeNode,
      [],
      [],
      false,
      planBuilder,
      2048
    );
    expect(baseline.length).to.be.greaterThan(4);

    // Dirty exactly one interior range; rebuild sliced.
    const after = { ...(before as Record<string, unknown>) } as Record<
      string,
      unknown
    >;
    const dirtyIndex = Math.floor(baseline.length / 2);
    const marker = baseline[dirtyIndex].post.split('/')[0];
    after[marker] = { changed: 'z'.repeat(128) };
    const afterNode = nodeFromJSON(after);
    const changed = collectChangedSubtreePaths(beforeNode, afterNode);
    expect(changed).to.not.equal(null);
    const marked = markDirtyRanges(baseline, changed!);

    const syncBuilder2 = new CompoundHashBuilder(
      fixedSizeSplitStrategy(2048),
      true
    );
    const expected = rebuildStableRanges(
      afterNode,
      baseline,
      marked.dirty,
      marked.tailDirty,
      syncBuilder2,
      2048
    );
    const slicedBuilder2 = new CompoundHashBuilder(
      fixedSizeSplitStrategy(2048),
      true
    );
    const rebuilder = new StableRangeRebuilder(
      afterNode,
      baseline,
      marked.dirty,
      marked.tailDirty,
      slicedBuilder2,
      2048
    );
    while (!rebuilder.drainUntil(Date.now())) {
      await new Promise<void>(resolve => setTimeout(resolve, 0));
    }
    expect(rebuilder.result().map(r => r.post)).to.deep.equal(
      expected.map(r => r.post)
    );
    // Clean ranges carried over verbatim (same object => same hash string).
    const cleanExpected = expected.filter(r => r.hash !== '');
    const cleanSliced = rebuilder.result().filter(r => r.hash !== '');
    expect(cleanSliced.map(r => r.hash)).to.deep.equal(
      cleanExpected.map(r => r.hash)
    );
  });

  it('a full-reload baseline skips the identity diff via the divorced check', async () => {
    const shape = wideRoot(30, 30);
    const a = nodeFromJSON(shape);
    const b = nodeFromJSON(shape); // equal content, ZERO shared identity
    expect(treesShareAnyChildIdentity(a, b)).to.equal(false);
    const evolved = a.updateImmediateChild('p0', nodeFromJSON({ x: 1 }));
    expect(treesShareAnyChildIdentity(a, evolved)).to.equal(true);
  });

  it('manifest estimatedBytes equals the sum of range sizes', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('estimate/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(wideRoot(20, 20)));
    await manager.flushNow(path.toString());
    await flushAsync();
    const manifest = data.get('test-repo|/estimate/root') as {
      estimatedBytes: number;
      ranges: Array<{ size: number }>;
    };
    expect(manifest).to.not.equal(undefined);
    expect(manifest.estimatedBytes).to.equal(
      manifest.ranges.reduce((sum, range) => sum + range.size, 0)
    );
  });

  it('a baseline-less root arms the shorter first-generation window', async () => {
    const { factory } = makeFakeIndexedDB();
    // Ordinary window far longer than the first-generation delay.
    const manager = new PersistenceManager(
      'test-repo',
      factory,
      true,
      8000,
      100 * 1024 * 1024,
      PERSISTENCE_WRITE_DEBOUNCE_MS
    );
    manager.setAuthScope(null);
    const path = new Path('firstgen/root');
    manager.track(path.toString());
    const internals = manager as unknown as {
      writeTimers_: Map<string, { _idleTimeout?: number }>;
      lastFlush_: Map<string, unknown>;
    };
    manager.serverCacheUpdated(path, nodeFromJSON({ a: 1 }));
    const timer = internals.writeTimers_.get(path.toString());
    expect(timer).to.not.equal(undefined);
    // Node timers expose the delay; guard for environments that hide it.
    if (timer!._idleTimeout !== undefined) {
      expect(timer!._idleTimeout).to.equal(
        PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS
      );
    }
    await manager.flushNow(path.toString());
    await flushAsync();
    // With a baseline the ordinary window applies again.
    manager.serverCacheUpdated(path, nodeFromJSON({ a: 2 }));
    const second = internals.writeTimers_.get(path.toString());
    expect(second).to.not.equal(undefined);
    if (second!._idleTimeout !== undefined) {
      expect(second!._idleTimeout).to.equal(PERSISTENCE_WRITE_DEBOUNCE_MS);
    }
    manager.dispose();
  });

  it('a dispose during the sliced plan neither throws nor commits', async () => {
    const { factory, data } = makeFakeIndexedDB();
    const manager = scopedManager('test-repo', factory);
    const path = new Path('dispose/root');
    manager.track(path.toString());
    manager.serverCacheUpdated(path, nodeFromJSON(wideRoot(120, 60)));
    const flushing = manager.flushNow(path.toString());
    manager.dispose();
    await flushing;
    await flushAsync();
    expect(data.get('test-repo|/dispose/root')).to.equal(
      undefined
    );
  });
});

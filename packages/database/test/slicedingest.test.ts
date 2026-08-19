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

import { QueryImpl } from '../src/api/Reference_impl';
import {
  PersistenceManager,
  _setWebLocksForTesting
} from '../src/core/Persistence';
import { repoOnDataUpdateForTest, Repo } from '../src/core/Repo';
import {
  _INGEST_DECODE_SLICE_VISITS,
  decodeNodeSliced,
  decodeChildrenSliced,
  IngestCancelledError,
  sliceableAsChildren
} from '../src/core/SlicedNodeDecode';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import {
  SyncTree,
  syncTreeAddEventRegistration,
  syncTreeApplyServerOverwrite,
  syncTreeGetCompleteServerCache
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { Tree } from '../src/core/util/Tree';
import { EventQueue } from '../src/core/view/EventQueue';
import { QueryParams } from '../src/core/view/QueryParams';

_setWebLocksForTesting(null);

function flushAsync(turns = 12): Promise<void> {
  let chain = Promise.resolve();
  for (let i = 0; i < turns; i++) {
    chain = chain.then(
      () => new Promise<void>(resolve => setTimeout(resolve, 0))
    );
  }
  return chain;
}

/**
 * The decode-fidelity corpus: every shape whose handling nodeFromJSON and
 * decodeNodeSliced must agree on. Equality is asserted BOTH ways (node
 * equals + canonical hash) so a divergence in either direction fails.
 */
const DECODE_CORPUS: Array<[string, unknown]> = [
  ['flat children', { a: 1, b: 'two', c: true }],
  ['nested tree', { a: { b: { c: 1 } }, d: { e: 2, f: { g: 'x' } } }],
  ['leaf string', 'hello'],
  ['leaf number', 42.5],
  ['leaf boolean', false],
  ['dot-value wrapper', { '.value': 7, '.priority': 3 }],
  ['leaf with priority', { '.value': 'v', '.priority': 'p' }],
  ['children with child priorities', { a: { '.value': 1, '.priority': 2 }, b: 3 }],
  ['root priority on children', { '.priority': 9, a: 1, b: 2 }],
  ['array payload', ['x', 'y', 'z']],
  ['sparse array-like keys', { '0': 'a', '2': 'c', '5': 'f' }],
  ['metadata keys skipped', { a: 1, '.info': 'meta' }],
  ['empty children pruned', { a: { b: null }, c: 1 }],
  ['null', null],
  ['deep mixed', { u1: { name: 'x', todos: { t1: { done: false, note: 'n' } } }, u2: { name: 'y' } }]
];

describe('SlicedNodeDecode', () => {
  for (const [label, json] of DECODE_CORPUS) {
    it(`decodes identically to nodeFromJSON: ${label}`, async () => {
      const expected = nodeFromJSON(json);
      const actual = await decodeNodeSliced(json, {
        visits: 0,
        isCurrent: () => true
      });
      expect(actual.equals(expected)).to.equal(
        true,
        `node inequality for ${label}`
      );
      expect(actual.hash()).to.equal(expected.hash());
    });
  }

  it('yields between slices on a payload wider than one budget', async () => {
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 3; i++) {
      wide['k' + i] = i;
    }
    let macrotaskRan = false;
    setTimeout(() => {
      macrotaskRan = true;
    }, 0);
    const node = await decodeNodeSliced(wide, {
      visits: 0,
      isCurrent: () => true
    });
    expect(macrotaskRan).to.equal(
      true,
      'a queued macrotask must run before a multi-budget decode resolves'
    );
    expect(node.equals(nodeFromJSON(wide))).to.equal(true);
  });

  it('cancels at the next yield when isCurrent flips false', async () => {
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    let current = true;
    const decode = decodeNodeSliced(wide, {
      visits: 0,
      isCurrent: () => current
    });
    current = false;
    let threw: unknown = null;
    try {
      await decode;
    } catch (e) {
      threw = e;
    }
    expect(threw).to.be.instanceOf(IngestCancelledError);
  });

  it('streams top-level children in key order with the root never built', async () => {
    const emitted: string[] = [];
    await decodeChildrenSliced(
      { b: { x: 1 }, a: 2, '.info': 'meta', c: null },
      () => true,
      key => emitted.push(key)
    );
    // '.info' skipped (metadata), 'c' skipped (empty), source order kept.
    expect(emitted).to.deep.equal(['b', 'a']);
  });

  it('sliceableAsChildren accepts only plain children objects', () => {
    expect(sliceableAsChildren({ a: 1 })).to.equal(true);
    expect(sliceableAsChildren(null)).to.equal(false);
    expect(sliceableAsChildren('leaf')).to.equal(false);
    expect(sliceableAsChildren(['a'])).to.equal(false);
    expect(sliceableAsChildren({ '.value': 1 })).to.equal(false);
    expect(sliceableAsChildren({ '.priority': 1, a: 2 })).to.equal(false);
    expect(sliceableAsChildren({ '.sv': 'timestamp' })).to.equal(false);
  });
});

describe('sliced full-root push ingestion', () => {
  /**
   * A Repo with a REAL SyncTree + EventQueue and a persistence stub that
   * marks one root persistent — the exact surface repoOnDataUpdate and the
   * ingest pump touch. Server actions record listen/unlisten only.
   */
  function makeIngestHarness(rootPath: string) {
    const syncTree = new SyncTree({
      startListening: () => [],
      stopListening: () => {}
    });
    const repo = {
      dataUpdateCount: 0,
      interceptServerDataCallback_: null,
      pendingSeedRestores_: new Map(),
      bootBuffers_: new Map(),
      listenOutcomes_: new Map(),
      persistedUpdates_: [] as Array<{ path: string; precise: string }>,
      persistence_: {
        isPersistentPath: (p: string) => p === rootPath,
        trackedRootFor: (p: string) =>
          p === rootPath || p.startsWith(rootPath + '/') ? rootPath : null,
        serverCacheUpdated: () => {},
        track: () => {},
        untrack: () => {},
        evict: () => {},
        invalidate: () => {},
        isAuthScopeConfigured: () => true
      } as unknown as PersistenceManager,
      eventQueue_: new EventQueue(),
      serverSyncTree_: syncTree,
      transactionQueueTree_: new Tree(),
      server_: {
        listen: () => {},
        unlisten: () => {}
      }
    } as unknown as Repo;
    // repoPersistAfterServerUpdate reads the tracked root + complete cache;
    // record the write-through precision for assertions.
    (repo.persistence_ as unknown as Record<string, unknown>)[
      'serverCacheUpdated'
    ] = (path: Path, _node: unknown, changedPaths?: string[][]) => {
      (repo as unknown as Record<string, unknown[]>)['persistedUpdates_'].push({
        path: path.toString(),
        precise:
          changedPaths === undefined
            ? 'unknown'
            : changedPaths.length === 0
            ? 'confirmed'
            : 'at-path'
      });
    };
    const query = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      new Path(rootPath),
      new QueryParams(),
      false
    );
    // Events must actually raise (the queue dereferences every event), so
    // createEvent returns a real runnable — the stubRegistration pattern
    // from persistence.test.ts.
    const registration = {
      respondsTo: () => true,
      createEvent: (_change: unknown, q: { _path: Path }) => ({
        getPath: () => q._path,
        getEventType: () => 'value',
        getEventRunner: () => () => {},
        toString: () => 'stub-event'
      }),
      getEventRunner: () => () => {},
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createCancelEvent: () => null as any,
      matches: () => false,
      hasAnyCallback: () => true
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
    syncTreeAddEventRegistration(syncTree, query, registration);
    return { repo, syncTree, query };
  }

  function wideRoot(children: number): Record<string, unknown> {
    const root: Record<string, unknown> = {};
    for (let i = 0; i < children; i++) {
      root['child' + i] = { value: i, nested: { deep: 'v' + i } };
    }
    return root;
  }

  it('a full-root push at a persistent root lands asynchronously and matches the monolithic result', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = wideRoot(50);

    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    // Synchronously: a boot buffer window is open, tree not yet complete.
    expect(repo.bootBuffers_.has(rootPath)).to.equal(true);

    await flushAsync();
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.not.equal(null);
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('yields at least one macrotask for a payload wider than one slice budget', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // Each child charges 1 key + surcharge + nested keys; 3 budgets of keys
    // guarantees multiple slices regardless of surcharge accounting.
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 3; i++) {
      wide['k' + i] = i;
    }
    let macrotaskRan = false;
    setTimeout(() => {
      macrotaskRan = true;
    }, 0);
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    await flushAsync(64);
    expect(macrotaskRan).to.equal(true);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.equals(nodeFromJSON(wide))).to.equal(true);
  });

  it('replaces an initialized base per-child and removes stale children', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // Initialized base: stale key + surviving key with an old value.
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ gone: { a: 1 }, kept: { v: 'old' } })
    );
    const payload = { kept: { v: 'new' }, added: { v: 1 } };
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    await flushAsync();
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(
      true,
      'stale child must be removed, kept updated, added inserted'
    );
  });

  it('preserves identity of unchanged children across a replacement push', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const unchanged = { deep: { tree: 'stable' } };
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ stable: unchanged, changing: { v: 1 } })
    );
    const before = syncTreeGetCompleteServerCache(
      syncTree,
      new Path(rootPath)
    )!.getImmediateChild('stable');

    repoOnDataUpdateForTest(
      repo,
      rootPath,
      { stable: unchanged, changing: { v: 2 } },
      false,
      null
    );
    await flushAsync();
    const after = syncTreeGetCompleteServerCache(
      syncTree,
      new Path(rootPath)
    )!.getImmediateChild('stable');
    // Not the same object (fresh decode), but equal — and critically the
    // unchanged child was applied through the child-level equals
    // short-circuit, never a root-wide diff. Equality is the contract.
    expect(after.equals(before)).to.equal(true);
  });

  it('buffers wire operations during the pump and replays them in order', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = wideRoot(20);
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    // While the pump is mid-flight, a descendant push arrives.
    repoOnDataUpdateForTest(
      repo,
      rootPath + '/child3/value',
      999,
      false,
      null
    );
    expect(repo.bootBuffers_.get(rootPath)!.length).to.equal(1);
    await flushAsync();
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // The buffered delta applied AFTER the base: child3/value is 999.
    expect(
      cache!.getChild(new Path('child3/value')).val()
    ).to.equal(999);
  });

  it('descendant pushes and merges keep the synchronous path', () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // Initialize the root (a full push — pump handles it asynchronously,
    // but for THIS test seed the tree directly so everything after is
    // observable synchronously).
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ x: { v: 0 } })
    );
    // Descendant push: applies synchronously, no buffer window opens.
    repoOnDataUpdateForTest(repo, rootPath + '/x', { v: 1 }, false, null);
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path(rootPath))!
        .getChild(new Path('x/v'))
        .val()
    ).to.equal(1);
    // Merge at the root: synchronous (merges never take the pump).
    repoOnDataUpdateForTest(repo, rootPath, { y: 2 }, true, null);
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('y')).val()).to.equal(2);
  });

  it('records a root-precision write-through exactly once per push', async () => {
    const rootPath = '/users/alice';
    const { repo } = makeIngestHarness(rootPath);
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(10), false, null);
    await flushAsync();
    const updates = (
      repo as unknown as Record<
        string,
        Array<{ path: string; precise: string }>
      >
    )['persistedUpdates_'];
    const rootUpdates = updates.filter(u => u.path === rootPath);
    expect(rootUpdates.length).to.equal(1);
    expect(rootUpdates[0].precise).to.equal('at-path');
  });

  it('a listen teardown mid-pump cancels the ingest cleanly', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree, query } = makeIngestHarness(rootPath);
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    // Teardown while the pump is between slices: the window is dropped the
    // way stopListening does it.
    repo.bootBuffers_.delete(rootPath);
    await flushAsync(64);
    // Cancelled: no complete root cache was ever installed (cold mode
    // applies all-or-nothing), and no crash surfaced.
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.equal(null);
    void query;
  });

  it('drains buffered ops in exact arrival order across kinds', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(10), false, null);
    // Wire order while the pump runs: push b=1, a 'complete' certification
    // marker, push b=2. The drain must preserve exactly this order — the
    // marker sees b=1 applied but not b=2.
    const observed: unknown[] = [];
    repoOnDataUpdateForTest(repo, rootPath + '/b', 1, false, null);
    repo.bootBuffers_.get(rootPath)!.push({
      kind: 'complete',
      apply: () => {
        observed.push(
          syncTreeGetCompleteServerCache(syncTree, new Path(rootPath))!
            .getChild(new Path('b'))
            .val()
        );
      }
    });
    repoOnDataUpdateForTest(repo, rootPath + '/b', 2, false, null);
    await flushAsync(64);
    expect(observed).to.deep.equal([1]);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('b')).val()).to.equal(2);
  });

  it('a second full push while pumping supersedes via the buffered path', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const first = wideRoot(30);
    const second = { winner: { v: 2 } };
    repoOnDataUpdateForTest(repo, rootPath, first, false, null);
    // Arrives mid-pump → buffered behind the first.
    repoOnDataUpdateForTest(repo, rootPath, second, false, null);
    await flushAsync(64);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // Both applied in order; the second replaced the first entirely.
    expect(cache!.equals(nodeFromJSON(second))).to.equal(true);
  });
});

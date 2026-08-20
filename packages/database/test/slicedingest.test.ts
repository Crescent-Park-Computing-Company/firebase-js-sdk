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
import {
  repoDropIngestWindowForTest,
  repoOnConnectStatusForTest,
  repoOnDataUpdateForTest,
  Repo
} from '../src/core/Repo';
import {
  _INGEST_DECODE_SLICE_VISITS,
  decodeNodeSliced,
  decodeFullRootSliced,
  IngestCancelledError,
  sliceableAsChildren
} from '../src/core/SlicedNodeDecode';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { SnapshotHolder } from '../src/core/SnapshotHolder';
import {
  newSparseSnapshotTree,
  sparseSnapshotTreeForget,
  sparseSnapshotTreeRemember
} from '../src/core/SparseSnapshotTree';
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
  [
    'children with child priorities',
    { a: { '.value': 1, '.priority': 2 }, b: 3 }
  ],
  ['root priority on children', { '.priority': 9, a: 1, b: 2 }],
  ['array payload', ['x', 'y', 'z']],
  ['sparse array-like keys', { '0': 'a', '2': 'c', '5': 'f' }],
  ['metadata keys skipped', { a: 1, '.info': 'meta' }],
  ['empty children pruned', { a: { b: null }, c: 1 }],
  ['null', null],
  [
    'deep mixed',
    {
      u1: { name: 'x', todos: { t1: { done: false, note: 'n' } } },
      u2: { name: 'y' }
    }
  ],
  // Legal child names that shadow Object.prototype members: JSON.parse makes
  // them own string properties, so any direct obj.hasOwnProperty(...) call
  // in an enumeration loop would invoke user data and throw.
  [
    'prototype-shadowing keys',
    JSON.parse(
      '{"hasOwnProperty": {"x": 1}, "toString": 2, "constructor": {"y": 3}, "a": 4}'
    )
  ]
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

  it('assembles a full root identical to nodeFromJSON, skipping metadata and empties', async () => {
    const payload = { b: { x: 1 }, a: 2, '.info': 'meta', c: null };
    const assembled = await decodeFullRootSliced(payload, null, () => true);
    expect(assembled.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('grafts structurally equal children from the base by IDENTITY', async () => {
    const base = nodeFromJSON({
      stable: { deep: { tree: 'unchanged' } },
      changing: { v: 1 }
    });
    const assembled = await decodeFullRootSliced(
      { stable: { deep: { tree: 'unchanged' } }, changing: { v: 2 } },
      base,
      () => true
    );
    // The unchanged child is the base's OBJECT (===), not merely equal —
    // this is what keeps the single atomic overwrite's diff O(changed).
    expect(assembled.getImmediateChild('stable')).to.equal(
      base.getImmediateChild('stable')
    );
    expect(
      assembled.getImmediateChild('changing').equals(nodeFromJSON({ v: 2 }))
    ).to.equal(true);
  });

  it('does not graft when content differs or the key is absent from the base', async () => {
    const base = nodeFromJSON({ a: { v: 1 } });
    const assembled = await decodeFullRootSliced(
      { a: { v: 2 }, b: { fresh: true } },
      base,
      () => true
    );
    expect(assembled.getImmediateChild('a')).to.not.equal(
      base.getImmediateChild('a')
    );
    expect(
      assembled.equals(nodeFromJSON({ a: { v: 2 }, b: { fresh: true } }))
    ).to.equal(true);
  });

  it('decodes a payload with prototype-shadowing keys without invoking them', async () => {
    const payload = JSON.parse(
      '{"hasOwnProperty": {"x": 1}, "a": 2}'
    ) as Record<string, unknown>;
    const assembled = await decodeFullRootSliced(payload, null, () => true);
    expect(assembled.equals(nodeFromJSON(payload))).to.equal(true);
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
      onDisconnect_: newSparseSnapshotTree(),
      infoData_: new SnapshotHolder(),
      infoSyncTree_: new SyncTree({
        startListening: () => [],
        stopListening: () => {}
      }),
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
        isAuthScopeConfigured: () => true,
        authGeneration: () => 1
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
    // The graft preserves IDENTITY (===), not merely equality: unchanged
    // children keep their live object across a replacement push, so the
    // single overwrite's diff and every downstream memoized consumer
    // short-circuit on identity.
    expect(after).to.equal(before);
  });

  it('buffers wire operations during the pump and replays them in order', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = wideRoot(20);
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    // While the pump is mid-flight, a descendant push arrives.
    repoOnDataUpdateForTest(repo, rootPath + '/child3/value', 999, false, null);
    expect(repo.bootBuffers_.get(rootPath)!.length).to.equal(1);
    await flushAsync();
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // The buffered delta applied AFTER the base: child3/value is 999.
    expect(cache!.getChild(new Path('child3/value')).val()).to.equal(999);
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

  it('a replacement push is ATOMIC to listeners: one value event, no mixed states', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ a: { v: 'old' }, b: { v: 'old' } })
    );
    // A recording value listener on the root: every raised snapshot value
    // is captured, so a partially replaced root would show up as an
    // intermediate mixed state.
    const values: unknown[] = [];
    const recordingQuery = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      new Path(rootPath),
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(syncTree, recordingQuery, {
      respondsTo: (eventType: string) => eventType === 'value',
      createEvent: (
        change: { snapshotNode: { val: () => unknown } },
        q: { _path: Path }
      ) => ({
        getPath: () => q._path,
        getEventType: () => 'value',
        getEventRunner: () => () => values.push(change.snapshotNode.val()),
        toString: () => 'recording-event'
      }),
      getEventRunner: () => () => {},
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      createCancelEvent: () => null as any,
      matches: () => false,
      hasAnyCallback: () => true
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    values.length = 0; // drop the registration's initial event, if any

    const payload = { a: { v: 'new' }, c: { v: 'added' } };
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    await flushAsync(64);
    // Exactly one coherent transition: never an intermediate root where
    // only SOME children were replaced (the per-child-apply failure mode).
    expect(values).to.deep.equal([payload]);
  });

  it('a throwing buffered op does not orphan the window or stop the drain', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(10), false, null);
    // While the pump runs: a poison custom op, then a healthy descendant push.
    repo.bootBuffers_.get(rootPath)!.push({
      kind: 'complete',
      apply: () => {
        throw new Error('poison op');
      }
    });
    repoOnDataUpdateForTest(repo, rootPath + '/child3/value', 999, false, null);
    await flushAsync(64);
    // The poison op was contained: the later op still applied, and the
    // window came down (no orphaned buffer swallowing future updates).
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('child3/value')).val()).to.equal(999);
    // And the repo still advances: a post-drain push applies immediately.
    repoOnDataUpdateForTest(repo, rootPath + '/child4/value', 1, false, null);
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path(rootPath))!
        .getChild(new Path('child4/value'))
        .val()
    ).to.equal(1);
  });

  it('a payload with prototype-shadowing keys ingests through the sliced path', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = JSON.parse(
      '{"hasOwnProperty": {"x": 1}, "constructor": {"y": 2}, "a": 3}'
    ) as Record<string, unknown>;
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    await flushAsync();
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.not.equal(null);
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('a disconnect mid-pump applies onDisconnect writes AFTER the buffered push (wire order)', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // The onDisconnect contract: when the connection drops, child9 = 'gone'.
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status'),
      nodeFromJSON('gone')
    );
    const payload = wideRoot(12);
    repoOnDataUpdateForTest(repo, rootPath, payload, false, null);
    // Connection drops while the decoder is mid-flight. Legacy applied the
    // run immediately; the sliced pump must instead order it AFTER the push
    // it time-shifted — otherwise the finished push overwrites the run's
    // writes with the pre-disconnect snapshot (wire order inverted).
    repoOnConnectStatusForTest(repo, false);
    await flushAsync(64);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.not.equal(null);
    // The push landed AND the later-ordered onDisconnect write survives it.
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
    expect(cache!.getChild(new Path('child3/value')).val()).to.equal(3);
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
  });

  it('distinct disconnects are distinct frozen runs, each applying its own registrations', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // First disconnect: only child9 is registered.
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status'),
      nodeFromJSON('gone-1')
    );
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(6), false, null);
    repoOnConnectStatusForTest(repo, false);
    // Reconnect; a NEW registration lands before the pump drains. It must
    // fire with the SECOND disconnect only — never retroactively with the
    // first (its tree was frozen at the first disconnect).
    repoOnConnectStatusForTest(repo, true);
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child8/status'),
      nodeFromJSON('gone-2')
    );
    repoOnConnectStatusForTest(repo, false);
    const markers = repo.bootBuffers_
      .get(rootPath)!
      .filter(op => op.kind === 'disconnect');
    expect(markers.length).to.equal(2);
    await flushAsync(64);
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // Both runs applied, in order, each from its own frozen tree.
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone-1');
    expect(cache!.getChild(new Path('child8/status')).val()).to.equal('gone-2');
  });

  it('a cancel acked after the disconnect cannot rewrite the frozen run', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status'),
      nodeFromJSON('gone')
    );
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(6), false, null);
    // Disconnect freezes the run; then a cancel ack lands (server processed
    // an onDisconnectCancel on the new connection) BEFORE the pump drains.
    // Legacy semantics: the cancel was acknowledged AFTER this disconnect
    // fired, so it affects the next disconnect — not this one.
    repoOnConnectStatusForTest(repo, false);
    sparseSnapshotTreeForget(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status')
    );
    await flushAsync(64);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
    // And the (now-empty) live tree means the NEXT disconnect fires nothing:
    repoOnConnectStatusForTest(repo, false);
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
  });

  it('an auth-scope switch mid-pump cancels the ingest (no cross-account apply)', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // A generation-bumping persistence stub: the pump captures the value at
    // start and stands down when it moves (the account-switch signal).
    let generation = 1;
    (repo.persistence_ as unknown as Record<string, unknown>)[
      'authGeneration'
    ] = () => generation;
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    // The switch lands between decode slices.
    generation = 2;
    await flushAsync(64);
    // The prior account's payload was never applied, and the window did not
    // leak (the driver's teardown owns it even when superseded).
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path(rootPath))
    ).to.equal(null);
    expect(repo.bootBuffers_.has(rootPath)).to.equal(false);
  });

  it('a window torn down while holding a disconnect marker still runs it', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    // Initialize so the onDisconnect write has a tree to land in.
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ child9: { status: 'up' } })
    );
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status'),
      nodeFromJSON('gone')
    );
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    repoOnConnectStatusForTest(repo, false);
    // Teardown mid-pump (the stop-listen shape): the buffered pushes are
    // moot, but the repo-global onDisconnect run must not be lost with them.
    repo.bootBuffers_.delete(rootPath);
    // Directly dropping the entry (as SDK teardown paths now do via
    // repoDropIngestWindow) is simulated by the driver's own finally here:
    // the pump notices at its next yield and its teardown flushes the run.
    await flushAsync(64);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
  });

  it('with two open windows, the frozen run fires exactly once — after the LAST holder releases', async () => {
    const rootA = '/users/alice';
    const rootB = '/users/bob';
    const { repo, syncTree } = makeIngestHarness(rootA);
    // Second persistent root on the same repo/harness.
    const persistence = repo.persistence_ as unknown as {
      isPersistentPath: (p: string) => boolean;
    };
    persistence.isPersistentPath = (p: string) => p === rootA || p === rootB;
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootA + '/child9/status'),
      nodeFromJSON('gone')
    );
    // Window A pumps; window B is a manually installed boot window that will
    // be torn down (the stop-listen shape), not drained.
    repoOnDataUpdateForTest(repo, rootA, wideRoot(6), false, null);
    repo.bootBuffers_.set(rootB, []);
    repoOnConnectStatusForTest(repo, false);
    const markersA = repo.bootBuffers_
      .get(rootA)!
      .filter(op => op.kind === 'disconnect');
    const markersB = repo.bootBuffers_
      .get(rootB)!
      .filter(op => op.kind === 'disconnect');
    expect(markersA.length).to.equal(1);
    expect(markersB.length).to.equal(1);
    // Window A drains first — the run must NOT fire yet (B still holds it):
    await flushAsync(64);
    expect(repo.bootBuffers_.has(rootA)).to.equal(false);
    const midCache = syncTreeGetCompleteServerCache(syncTree, new Path(rootA));
    expect(midCache!.getChild(new Path('child9/status')).isEmpty()).to.equal(
      true
    );
    // B tears down (repoStopServerListen shape → repoDropIngestWindow): the
    // LAST holder released — the run fires now, exactly once.
    repoDropIngestWindowForTest(repo, rootB);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootA));
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
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

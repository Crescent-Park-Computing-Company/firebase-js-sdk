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
  _INGEST_WIRE_BYTES_THRESHOLD,
  newIngestQueue,
  repoLiftIngestGateForTest,
  repoOnConnectStatusForTest,
  repoOnDataUpdateForTest,
  repoOnRangeMergeUpdateForTest,
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

/** Quiescent: no gates, empty queue. */
function expectIngestIdle(repo: Repo): void {
  expect(repo.ingestQueue_.gates.size).to.equal(0);
  expect(repo.ingestQueue_.ops.length).to.equal(0);
}

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
      ingestQueue_: newIngestQueue(),
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
    /**
     * Attach a stub value listener at another path (a view must exist for
     * the SyncTree to retain server data there).
     */
    const listenAt = (path: string) => {
      const q = new QueryImpl(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        null as any,
        new Path(path),
        new QueryParams(),
        false
      );
      syncTreeAddEventRegistration(syncTree, q, registration);
    };
    return { repo, syncTree, query, listenAt };
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
    expect(repo.ingestQueue_.gates.has(rootPath)).to.equal(true);

    await flushAsync();
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.not.equal(null);
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('engages for wire-form paths (no leading slash), as the server delivers them', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = wideRoot(50);

    // The wire delivers server-form paths ('users/alice'; '' for the root),
    // while persistence roots are registered canonically via
    // Path.toString() ('/users/alice'). The pump must engage regardless of
    // which form arrives — this is the production format.
    repoOnDataUpdateForTest(repo, 'users/alice', payload, false, null);
    // The gate must be keyed canonically: repoGetValue's gate lookup
    // compares canonical query paths against gate keys.
    expect(repo.ingestQueue_.gates.has(rootPath)).to.equal(true);

    await flushAsync();
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache).to.not.equal(null);
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('engages for a wire-form push at the database root (empty string)', async () => {
    const rootPath = '/';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    const payload = wideRoot(10);

    // The server addresses the database root as ''; canonical form is '/'.
    repoOnDataUpdateForTest(repo, '', payload, false, null);
    expect(repo.ingestQueue_.gates.has(rootPath)).to.equal(true);

    await flushAsync();
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('queues deferred wire-form operations under canonical path keys', async () => {
    const rootPath = '/users/alice';
    const { repo } = makeIngestHarness(rootPath);

    // First push gates the stream; deliver both op kinds in wire form
    // while the gate holds. Every queued op must carry the canonical key:
    // the drain's re-entry eligibility and gate-coverage checks compare
    // path strings, so a raw wire form in the queue would silently fall
    // back to the monolithic apply.
    repoOnDataUpdateForTest(repo, 'users/alice', wideRoot(5), false, null);
    expect(repo.ingestQueue_.gates.has(rootPath)).to.equal(true);
    repoOnDataUpdateForTest(repo, 'users/alice/child0', { v: 2 }, false, null);
    repoOnRangeMergeUpdateForTest(
      repo,
      'users/alice',
      [{ m: { child1: { value: 1 } } }],
      null
    );
    const kinds = repo.ingestQueue_.ops.map(op => op.kind);
    expect(kinds).to.deep.equal(['data', 'rm']);
    for (const op of repo.ingestQueue_.ops) {
      if (op.kind === 'data' || op.kind === 'rm') {
        expect(op.pathString.startsWith('/')).to.equal(
          true,
          `queued ${op.kind} op must be canonically keyed, got ${op.pathString}`
        );
      }
    }
    await flushAsync();
    expectIngestIdle(repo);
  });

  it('a giant push at an UNREGISTERED path takes the sliced pump (size-based eligibility)', async () => {
    // The harness registers only rootPath as persistent; this push lands on
    // a sibling path no one registered — the production shape of a giant
    // listen answer for a component's own listener (or a re-listen racing a
    // remount's deregistration). Path registration must not be the only
    // gate: above the wire-size threshold the pump engages anyway.
    const rootPath = '/users/alice';
    const { repo, syncTree, listenAt } = makeIngestHarness(rootPath);
    const otherPath = '/users/bob';
    listenAt(otherPath);
    const payload = wideRoot(50);

    repoOnDataUpdateForTest(
      repo,
      'users/bob',
      payload,
      false,
      null,
      _INGEST_WIRE_BYTES_THRESHOLD
    );
    expect(repo.ingestQueue_.gates.has(otherPath)).to.equal(true);

    await flushAsync();
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(otherPath));
    expect(cache).to.not.equal(null);
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('a small push at an unregistered path keeps the synchronous path', () => {
    const rootPath = '/users/alice';
    const { repo, syncTree, listenAt } = makeIngestHarness(rootPath);
    listenAt('/users/bob');
    const payload = { v: 1 };

    repoOnDataUpdateForTest(
      repo,
      'users/bob',
      payload,
      false,
      null,
      _INGEST_WIRE_BYTES_THRESHOLD - 1
    );
    // Applied synchronously: no gate, tree already complete.
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(
      syncTree,
      new Path('/users/bob')
    );
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('a giant push with UNKNOWN wire size (0) keeps the synchronous path off registered roots', () => {
    // Transports that do not report bytes (long-poll, REST) pass 0; the
    // size-based catch-all must not fire on an unknown size.
    const rootPath = '/users/alice';
    const { repo, syncTree, listenAt } = makeIngestHarness(rootPath);
    listenAt('/users/bob');
    const payload = wideRoot(10);

    repoOnDataUpdateForTest(repo, 'users/bob', payload, false, null, 0);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(
      syncTree,
      new Path('/users/bob')
    );
    expect(cache!.equals(nodeFromJSON(payload))).to.equal(true);
  });

  it('a deferred giant push at an unregistered path re-enters the pump from the drain', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree, listenAt } = makeIngestHarness(rootPath);
    listenAt('/users/bob');

    // Gate the stream with a registered-root ingest, then deliver a giant
    // push for an unregistered sibling while the gate holds. The drain's
    // re-entry must honor the recorded wire size.
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(5), false, null);
    expect(repo.ingestQueue_.gates.has(rootPath)).to.equal(true);
    // Wide enough that the sliced decode must yield at least once, so the
    // re-entered gate is observable across turns (the monolithic fallback
    // never gates at all).
    const bobWide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      bobWide['k' + i] = i;
    }
    const payload = bobWide;
    repoOnDataUpdateForTest(
      repo,
      'users/bob',
      payload,
      false,
      null,
      _INGEST_WIRE_BYTES_THRESHOLD * 2
    );
    expect(repo.ingestQueue_.ops.length).to.equal(1);

    // The drain must RE-ENTER the sliced pump for the queued giant push: a
    // gate for its path forms (the monolithic fallback never gates). Poll
    // across turns — the first ingest and the drain both yield.
    let sawBobGate = false;
    for (let i = 0; i < 64 && !sawBobGate; i++) {
      await flushAsync(1);
      sawBobGate = repo.ingestQueue_.gates.has('/users/bob');
    }
    expect(sawBobGate).to.equal(
      true,
      'queued giant push must re-enter the sliced pump (gate observed)'
    );

    await flushAsync(32);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(
      syncTree,
      new Path('/users/bob')
    );
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
    expect(repo.ingestQueue_.ops.length).to.equal(1);
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
    expectIngestIdle(repo);
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path(rootPath))!
        .getChild(new Path('x/v'))
        .val()
    ).to.equal(1);
    // Merge at the root: synchronous (merges never take the pump).
    repoOnDataUpdateForTest(repo, rootPath, { y: 2 }, true, null);
    expectIngestIdle(repo);
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

  it('drains buffered ops in exact arrival order across kinds', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(10), false, null);
    // Wire order while the pump runs: push b=1, a 'complete' certification
    // marker, push b=2. The drain must preserve exactly this order — the
    // marker sees b=1 applied but not b=2.
    const observed: unknown[] = [];
    repoOnDataUpdateForTest(repo, rootPath + '/b', 1, false, null);
    repo.ingestQueue_.ops.push({
      kind: 'complete',
      generation: repo.ingestQueue_.generation,
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
    repo.ingestQueue_.ops.push({
      kind: 'complete',
      generation: repo.ingestQueue_.generation,
      apply: () => {
        throw new Error('poison op');
      }
    });
    repoOnDataUpdateForTest(repo, rootPath + '/child3/value', 999, false, null);
    await flushAsync(64);
    // The poison op was contained: the later op still applied, and the
    // window came down (no orphaned buffer swallowing future updates).
    expectIngestIdle(repo);
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
    expectIngestIdle(repo);
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
    // Reconnect; a NEW registration lands before the queue drains. It must
    // fire with the SECOND disconnect only — never retroactively with the
    // first (its tree was frozen at the first disconnect).
    repoOnConnectStatusForTest(repo, true);
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child8/status'),
      nodeFromJSON('gone-2')
    );
    repoOnConnectStatusForTest(repo, false);
    const queuedRuns = repo.ingestQueue_.ops.filter(
      op => op.kind === 'disconnect'
    );
    expect(queuedRuns.length).to.equal(2);
    await flushAsync(64);
    expectIngestIdle(repo);
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
    expectIngestIdle(repo);
  });

  it('a gate lifted while a disconnect run is queued still fires it, exactly once', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
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
    // Stop-listen mid-decode: the gate lifts. The queued run is repo-global
    // state and must survive the teardown — the queue is never dropped with
    // a gate — and fire exactly once when the drain reaches it.
    repoLiftIngestGateForTest(repo, rootPath);
    await flushAsync(64);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
  });

  it('a post-disconnect push never overtakes the queued run — and is not clobbered by it', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/child9/status'),
      nodeFromJSON('gone')
    );
    // A sliced ingest holds the gate...
    repoOnDataUpdateForTest(repo, rootPath, wideRoot(6), false, null);
    // ...the connection drops (run queues behind the push)...
    repoOnConnectStatusForTest(repo, false);
    // ...and a POST-disconnect (reconnect) push arrives for the same field
    // the run writes. Wire order: push(base) < disconnect < push(fresh).
    repoOnDataUpdateForTest(
      repo,
      rootPath + '/child9/status',
      'fresh-after-reconnect',
      false,
      null
    );
    const kinds = repo.ingestQueue_.ops.map(op => op.kind);
    // The queue holds them in exact wire order: disconnect BEFORE the push.
    expect(kinds).to.deep.equal(['disconnect', 'data']);
    await flushAsync(64);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // The post-reconnect push wins: it applied AFTER the run, never before.
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal(
      'fresh-after-reconnect'
    );
  });

  it('ops on an ungated root queue behind a pending disconnect instead of overtaking it', async () => {
    const rootA = '/users/alice';
    const rootB = '/users/bob';
    const { repo, syncTree } = makeIngestHarness(rootA);
    const persistence = repo.persistence_ as unknown as {
      isPersistentPath: (p: string) => boolean;
    };
    persistence.isPersistentPath = (p: string) => p === rootA || p === rootB;
    // Register B's view so its pushes land in SyncTree.
    const queryB = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      new Path(rootB),
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(syncTree, queryB, {
      respondsTo: () => true,
      createEvent: (_c: unknown, q: { _path: Path }) => ({
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
    } as any);
    // Initialize B's root view (an uninitialized view drops descendant ops).
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootB),
      nodeFromJSON({ flag: 'initial' })
    );
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootB + '/flag'),
      nodeFromJSON('from-disconnect')
    );
    // A's ingest gates the queue; the disconnect queues its run.
    repoOnDataUpdateForTest(repo, rootA, wideRoot(6), false, null);
    repoOnConnectStatusForTest(repo, false);
    // A post-disconnect push lands on B — a root with NO gate of its own.
    // It must still queue BEHIND the disconnect run (single global FIFO),
    // not apply immediately and be clobbered when the run drains.
    repoOnDataUpdateForTest(repo, rootB + '/flag', 'fresh', false, null);
    expect(repo.ingestQueue_.ops.map(op => op.kind)).to.deep.equal([
      'disconnect',
      'data'
    ]);
    await flushAsync(64);
    expectIngestIdle(repo);
    const cacheB = syncTreeGetCompleteServerCache(syncTree, new Path(rootB));
    expect(cacheB!.getChild(new Path('flag')).val()).to.equal('fresh');
  });

  it('teardown during a sliced ingest fires a queued disconnect run exactly ONCE (round-4a repro)', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ counter: { v: 0 } })
    );
    // A second (double-released) run would land AFTER the post-disconnect
    // push below and clobber it back — the visible round-4a signature.
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/ranAt'),
      nodeFromJSON('run-1')
    );
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    repoOnConnectStatusForTest(repo, false);
    // Wire order: disconnect < this push. If the run fired TWICE (the
    // round-4a teardown+finally double release), the second run would land
    // AFTER this push and clobber it back to 'run-1'.
    repoOnDataUpdateForTest(
      repo,
      rootPath + '/ranAt',
      'post-disconnect-write',
      false,
      null
    );
    // Production teardown path (stop-listen shape), then the ingest's own
    // finally — the round-4a double-release sequence.
    repoLiftIngestGateForTest(repo, rootPath);
    await flushAsync(64);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    expect(cache!.getChild(new Path('ranAt')).val()).to.equal(
      'post-disconnect-write'
    );
  });

  it('a stop-listen mid-ingest supersedes the decode: its payload never applies over the drained stream', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
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
    repoLiftIngestGateForTest(repo, rootPath);
    await flushAsync(64);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // The run fired; the superseded wide payload did NOT apply over it
    // (isCurrent went false when the gate token was lifted).
    expect(cache!.getChild(new Path('child9/status')).val()).to.equal('gone');
    expect(cache!.getChild(new Path('k0')).isEmpty()).to.equal(true);
  });

  it('an auth-scope switch drops queued account-bound ops; disconnect runs still fire (CWE-200 repro)', async () => {
    const rootPath = '/users/alice';
    const { repo, syncTree } = makeIngestHarness(rootPath);
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootPath),
      nodeFromJSON({ inbox: { msg: 'account-A-old' } })
    );
    sparseSnapshotTreeRemember(
      repo.onDisconnect_,
      new Path(rootPath + '/presence'),
      nodeFromJSON('offline')
    );
    let generation = 1;
    (repo.persistence_ as unknown as Record<string, unknown>)[
      'authGeneration'
    ] = () => generation;
    const wide: Record<string, unknown> = {};
    for (let i = 0; i < _INGEST_DECODE_SLICE_VISITS * 2; i++) {
      wide['k' + i] = i;
    }
    // Account A's full push gates the queue; a descendant push (A's bytes)
    // and a disconnect land behind it.
    repoOnDataUpdateForTest(repo, rootPath, wide, false, null);
    repoOnDataUpdateForTest(
      repo,
      rootPath + '/inbox/msg',
      'account-A-secret',
      false,
      null
    );
    repoOnConnectStatusForTest(repo, false);
    // The account switch: the app path bumps BOTH signals (the persistence
    // manager's own generation and, via repoCancelPendingSeedRestores, the
    // queue generation).
    generation = 2;
    repo.ingestQueue_.generation++;
    await flushAsync(64);
    expectIngestIdle(repo);
    const cache = syncTreeGetCompleteServerCache(syncTree, new Path(rootPath));
    // A's queued bytes were DROPPED — never surfaced under the new scope...
    expect(cache!.getChild(new Path('inbox/msg')).val()).to.equal(
      'account-A-old'
    );
    // ...while the repo-global disconnect run still fired.
    expect(cache!.getChild(new Path('presence')).val()).to.equal('offline');
  });

  it('a get() on an ungated root keeps its SyncTree side effect while another root has queued ops', async () => {
    const rootPath = '/users/alice';
    const { repo } = makeIngestHarness(rootPath);
    // Queue is non-empty (an op for alice), but bob has NO gate: get()'s
    // guard must be path-specific — only a covering gate suppresses the
    // side effect.
    repo.ingestQueue_.ops.push({
      kind: 'complete',
      generation: repo.ingestQueue_.generation,
      apply: () => {}
    });
    // Import-free structural probe: the guard's predicate is what decides.
    // (repoGetValue itself needs a live server_.get; asserting the predicate
    // boundary keeps this test at the same layer as the other gate tests.)
    expect(repo.ingestQueue_.gates.has('/users/bob')).to.equal(false);
    // The wire-op predicate DOES defer for bob (global order)...
    repoOnDataUpdateForTest(repo, '/users/bob/x', 1, false, null);
    expect(
      repo.ingestQueue_.ops.some(
        op => op.kind === 'data' && op.pathString === '/users/bob/x'
      )
    ).to.equal(true);
    await flushAsync(64);
    expectIngestIdle(repo);
  });

  it('an unrelated root arriving during an ingest with an EMPTY queue still defers (round-6 repro)', async () => {
    const rootA = '/users/alice';
    const rootB = '/users/bob';
    const { repo, syncTree } = makeIngestHarness(rootA);
    const persistence = repo.persistence_ as unknown as {
      isPersistentPath: (p: string) => boolean;
    };
    persistence.isPersistentPath = (p: string) => p === rootA || p === rootB;
    // B has a live, registered view (a complete server cache needs a
    // registration — an unregistered overwrite leaves no view behind).
    const queryB = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      new Path(rootB),
      new QueryParams(),
      false
    );
    syncTreeAddEventRegistration(syncTree, queryB, {
      respondsTo: () => true,
      createEvent: (_c: unknown, q: { _path: Path }) => ({
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
    } as any);
    syncTreeApplyServerOverwrite(
      syncTree,
      new Path(rootB),
      nodeFromJSON({ x: 'initial' })
    );
    // A's sliced ingest starts: gate installed, queue EMPTY — the exact
    // window the path-scoped predicate missed.
    repoOnDataUpdateForTest(repo, rootA, wideRoot(20), false, null);
    expect(repo.ingestQueue_.gates.has(rootA)).to.equal(true);
    expect(repo.ingestQueue_.ops.length).to.equal(0);
    // B's push arrives. Wire order: A's full push < B's push. It must NOT
    // apply immediately (that would invert cross-root write order), and an
    // ELIGIBLE full push for B must not start a concurrent ingest.
    repoOnDataUpdateForTest(repo, rootB + '/x', 'after-A', false, null);
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path(rootB))!
        .getChild(new Path('x'))
        .val()
    ).to.equal('initial');
    expect(repo.ingestQueue_.ops.length).to.equal(1);
    // An eligible FULL push for B mid-ingest: queued, not a second gate.
    repoOnDataUpdateForTest(repo, rootB, { x: 'full-B' }, false, null);
    expect(repo.ingestQueue_.gates.size).to.equal(1);
    await flushAsync(64);
    expectIngestIdle(repo);
    // Everything landed, in wire order: A's root, then B's ops.
    const cacheA = syncTreeGetCompleteServerCache(syncTree, new Path(rootA));
    expect(cacheA).to.not.equal(null);
    const cacheB = syncTreeGetCompleteServerCache(syncTree, new Path(rootB));
    expect(cacheB!.getChild(new Path('x')).val()).to.equal('full-B');
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

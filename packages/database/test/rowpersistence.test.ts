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
  compoundHashFromNode,
  simpleSizeSplitStrategy
} from '../src/core/CompoundHash';
import { RowPersistenceManager } from '../src/core/RowPersistence';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';

import { makeFakeIdb, flushMicrotasks, wait } from './helpers/fakeIdb';

/** A manager with instant write windows for tests. */
function makeManager(
  idb: IDBFactory,
  options: {
    webLocks?: null;
    writeDelayMs?: number;
    splitThreshold?: number;
  } = {}
): RowPersistenceManager {
  return new RowPersistenceManager(
    'test-repo',
    idb,
    options.webLocks ?? null, // Node: no Web Locks -> always writer
    options.writeDelayMs ?? 1,
    options.writeDelayMs ?? 1,
    options.splitThreshold ?? 512,
    30000,
    300000,
    1000,
    1 << 20
  );
}

describe('RowPersistenceManager', () => {
  it('flushes a first generation and restores it', async () => {
    const idb = makeFakeIdb();
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON({
      docs: { a: 'x'.repeat(300), b: 'y'.repeat(300) },
      todos: { t1: { text: 'buy milk', done: false } }
    });
    manager.serverCacheUpdated(new Path('/ws'), tree);
    await wait(30);
    await flushMicrotasks();

    const reader = makeManager(idb);
    reader.setAuthScope('alice');
    reader.setPersistentPath('/ws', true);
    reader.track('/ws');
    const restored = await reader.restoreForListen('/ws');
    expect(restored.node).to.not.equal(null);
    expect(restored.node!.equals(tree)).to.equal(true);
    manager.dispose();
    reader.dispose();
  });

  it('peek returns the cached tree pre-auth and retains for the listener', async () => {
    const idb = makeFakeIdb();
    const writer = makeManager(idb);
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    const tree = nodeFromJSON({ a: 1, b: 'two' });
    writer.serverCacheUpdated(new Path('/ws'), tree);
    await wait(30);
    await flushMicrotasks();
    writer.dispose();

    const manager = makeManager(idb);
    // Pre-auth prime (getPersistedValue): scope expected but unconfirmed.
    manager.setAuthScope('alice', false);
    const peeked = await manager.peek('/ws', 'alice');
    expect(peeked).to.not.equal(null);
    expect(peeked!.node.val()).to.deep.equal({ a: 1, b: 'two' });
    expect(manager.hasRetainedPeek('/ws', peeked!.node)).to.equal(true);

    // The listener's restore consumes the SAME decode (node identity).
    const restored = await manager.restoreForListen('/ws');
    expect(restored.node).to.equal(peeked!.node);
    expect(manager.hasRetainedPeek('/ws', peeked!.node)).to.equal(false);
    manager.dispose();
  });

  it('a different auth scope never sees another account cache', async () => {
    const idb = makeFakeIdb();
    const writer = makeManager(idb);
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    writer.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ secret: 1 }));
    await wait(30);
    await flushMicrotasks();
    writer.dispose();

    const manager = makeManager(idb);
    manager.setAuthScope('bob');
    const peeked = await manager.peek('/ws', 'bob');
    expect(peeked).to.equal(null);
    const restored = await manager.restoreForListen('/ws');
    expect(restored.node).to.equal(null);
    manager.dispose();
  });

  it('scope switch mid-peek invalidates the resolved read', async () => {
    const idb = makeFakeIdb();
    const writer = makeManager(idb);
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    writer.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 1 }));
    await wait(30);
    await flushMicrotasks();
    writer.dispose();

    const manager = makeManager(idb);
    manager.setAuthScope('alice', false);
    const peekPromise = manager.peek('/ws', 'alice');
    manager.setAuthScope('bob'); // switch before the read resolves
    const peeked = await peekPromise;
    expect(peeked).to.equal(null);
    manager.dispose();
  });

  it('incremental flush rewrites only the dirty row', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const log = { puts: [] as string[], deletes: [] as string[] };
    const idb = makeFakeIdb(shared, log);
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    // Two branches large enough to split into separate rows.
    const v1 = nodeFromJSON({
      left: { a: 'x'.repeat(600), b: 'x'.repeat(600) },
      right: { c: 'y'.repeat(600), d: 'y'.repeat(600) }
    });
    manager.serverCacheUpdated(new Path('/ws'), v1);
    await wait(30);
    await flushMicrotasks();
    // Restore to load the row index (as a booted client would).
    const restored = await manager.restoreForListen('/ws');
    expect(restored.node).to.not.equal(null);

    // Change one leaf under 'left'; only left-subtree rows may rewrite.
    log.puts.length = 0;
    log.deletes.length = 0;
    const v2 = nodeFromJSON({
      left: { a: 'CHANGED', b: 'x'.repeat(600) },
      right: { c: 'y'.repeat(600), d: 'y'.repeat(600) }
    });
    manager.serverCacheUpdated(new Path('/ws'), v2, [['left', 'a']]);
    await wait(30);
    await flushMicrotasks();

    // Write economics: only rows under the dirty path's row boundary were
    // touched (plus the meta stamp) — never the clean 'right' subtree.
    const rowPuts = log.puts.filter(p => p.startsWith('rows:'));
    expect(rowPuts.length).to.be.greaterThan(0);
    for (const put of rowPuts) {
      expect(put).to.include('\u0001left\u0001');
    }
    for (const del of log.deletes.filter(d => d.startsWith('rows:'))) {
      expect(del).to.include('\u0001left\u0001');
    }

    const reader = makeManager(idb);
    reader.setAuthScope('alice');
    const after = await reader.peek('/ws', 'alice');
    expect(after!.node.equals(v2)).to.equal(true);
    manager.dispose();
    reader.dispose();
  });

  it('computeListenHashes matches compoundHashFromNode over the stored tree', async () => {
    const idb = makeFakeIdb();
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON({
      docs: Object.fromEntries(
        Array.from({ length: 30 }, (_, i) => ['d' + i, 'text-'.repeat(20) + i])
      ),
      meta: { count: 30 }
    });
    manager.serverCacheUpdated(new Path('/ws'), tree);
    await wait(30);
    await flushMicrotasks();

    const hashes = await manager.computeListenHashes('/ws');
    expect(hashes).to.not.equal(null);
    expect(hashes!.hash).to.equal('');
    const expected = compoundHashFromNode(tree, simpleSizeSplitStrategy(tree));
    expect(hashes!.compoundHash.posts).to.deep.equal(expected.posts);
    expect(hashes!.compoundHash.hashes).to.deep.equal(expected.hashes);
    manager.dispose();
  });

  it('meta commits last: rows without meta read as no cache (torn first gen)', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    writer.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ a: 1 }));
    await wait(30);
    await flushMicrotasks();
    writer.dispose();

    // Simulate the crash-mid-stage state: rows present, meta missing.
    shared.get('meta')!.clear();

    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    expect(await reader.peek('/ws', 'alice')).to.equal(null);
    const restored = await reader.restoreForListen('/ws');
    expect(restored.node).to.equal(null);
    reader.dispose();
  });

  it('two managers on shared storage: last writer wins, both restore', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const idbA = makeFakeIdb(shared);
    const idbB = makeFakeIdb(shared);
    const a = makeManager(idbA);
    const b = makeManager(idbB);
    a.setAuthScope('alice');
    b.setAuthScope('alice');
    for (const m of [a, b]) {
      m.setPersistentPath('/ws', true);
      m.track('/ws');
    }
    const treeA = nodeFromJSON({ from: 'a', n: 1 });
    const treeB = nodeFromJSON({ from: 'b', n: 2 });
    a.serverCacheUpdated(new Path('/ws'), treeA);
    b.serverCacheUpdated(new Path('/ws'), treeB);
    await wait(40);
    await flushMicrotasks();

    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const result = await reader.peek('/ws', 'alice');
    expect(result).to.not.equal(null);
    // One of the two trees, intact — never a torn mix at the row level
    // (both writers wrote whole generations here).
    const val = result!.node.val() as { from: string };
    expect(['a', 'b']).to.include(val.from);
    a.dispose();
    b.dispose();
    reader.dispose();
  });

  it('evict removes the stored cache', async () => {
    const idb = makeFakeIdb();
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ a: 1 }));
    await wait(30);
    await flushMicrotasks();
    manager.evict(new Path('/ws'));
    await flushMicrotasks();

    const reader = makeManager(idb);
    reader.setAuthScope('alice');
    const result = await reader.peek('/ws', 'alice');
    expect(result).to.equal(null);
    manager.dispose();
    reader.dispose();
  });

  it('network suspension defers flushes until resume', async () => {
    const idb = makeFakeIdb();
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.setNetworkSuspended(true);
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ a: 1 }));
    await wait(30);
    await flushMicrotasks();
    const reader = makeManager(idb);
    reader.setAuthScope('alice');
    expect(await reader.peek('/ws', 'alice')).to.equal(null);

    manager.setNetworkSuspended(false);
    await wait(30);
    await flushMicrotasks();
    const reader2 = makeManager(idb);
    reader2.setAuthScope('alice');
    const result = await reader2.peek('/ws', 'alice');
    expect(result).to.not.equal(null);
    manager.dispose();
    reader.dispose();
    reader2.dispose();
  });
});

describe('RowPersistenceManager property: generational round-trips', () => {
  it('8 seeds x 5 reload/mutate/flush generations: exact tree + hash equality', async function () {
    this.timeout(30000);
    for (let seedBase = 1; seedBase <= 8; seedBase++) {
      let seed = seedBase * 0x9e3779b9;
      const rand = (): number => {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;
        return (seed >>> 0) / 0xffffffff;
      };
      const randKey = (): string =>
        rand() < 0.3
          ? String(Math.floor(rand() * 20))
          : 'k' + Math.floor(rand() * 40);
      const randLeaf = (): unknown =>
        rand() < 0.4
          ? Math.floor(rand() * 1e6)
          : 's'.repeat(Math.floor(rand() * 40)) + Math.floor(rand() * 10);
      const randTree = (depth: number): unknown => {
        if (depth === 0 || rand() < 0.3) {
          return randLeaf();
        }
        const children: Record<string, unknown> = {};
        const n = 1 + Math.floor(rand() * 6);
        for (let i = 0; i < n; i++) {
          children[randKey()] = randTree(depth - 1);
        }
        return children;
      };

      const shared = new Map<string, Map<string, unknown>>();
      // Tiny split threshold forces multi-row layouts and boundary churn.
      let current = nodeFromJSON(randTree(3));
      {
        const writer = makeManager(makeFakeIdb(shared), { splitThreshold: 96 });
        writer.setAuthScope('u');
        writer.setPersistentPath('/r', true);
        writer.track('/r');
        writer.serverCacheUpdated(new Path('/r'), current);
        await wait(20);
        await flushMicrotasks();
        writer.dispose();
      }

      for (let gen = 0; gen < 5; gen++) {
        const manager = makeManager(makeFakeIdb(shared), { splitThreshold: 96 });
        manager.setAuthScope('u');
        manager.setPersistentPath('/r', true);
        manager.track('/r');
        // Reload: stored tree must equal the last flushed tree exactly.
        const restored = await manager.restoreForListen('/r');
        expect(restored.node, `seed ${seedBase} gen ${gen} restore`).to.not.equal(null);
        expect(
          restored.node!.equals(current),
          `seed ${seedBase} gen ${gen} tree equality`
        ).to.equal(true);
        // Stored-rows hash must equal the assembled tree's own hash.
        const hashes = await manager.computeListenHashes('/r');
        expect(hashes, `seed ${seedBase} gen ${gen} hashes`).to.not.equal(null);
        const expected = compoundHashFromNode(
          restored.node!,
          simpleSizeSplitStrategy(restored.node!)
        );
        expect(hashes!.compoundHash.hashes).to.deep.equal(expected.hashes);
        expect(hashes!.compoundHash.posts).to.deep.equal(expected.posts);

        // Mutate 1-3 random paths (precise dirt), sometimes a whole-root
        // rewrite (unknown dirt), flush, next generation reloads it.
        if (rand() < 0.25) {
          current = nodeFromJSON(randTree(3));
          manager.serverCacheUpdated(new Path('/r'), current);
        } else {
          const mutations = 1 + Math.floor(rand() * 3);
          const dirty: string[][] = [];
          for (let m = 0; m < mutations; m++) {
            const segs = [randKey()];
            if (rand() < 0.5) {
              segs.push(randKey());
            }
            const sub = nodeFromJSON(rand() < 0.2 ? null : randTree(2));
            current = current.updateChild(new Path(segs.join('/')), sub);
            dirty.push(segs);
          }
          if (current.isEmpty()) {
            current = nodeFromJSON({ keep: 1 });
            manager.serverCacheUpdated(new Path('/r'), current);
          } else {
            manager.serverCacheUpdated(new Path('/r'), current, dirty);
          }
        }
        await wait(20);
        await flushMicrotasks();
        manager.dispose();
      }
    }
  });
});

describe('RowPersistenceManager sweep', () => {
  it('removes expired roots and orphan rows, keeps live roots', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    // Live root written normally.
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/live', true);
    writer.track('/live');
    writer.serverCacheUpdated(new Path('/live'), nodeFromJSON({ ok: 1 }));
    await wait(30);
    await flushMicrotasks();
    writer.dispose();

    // An expired root (meta 40 days old) and an orphan row (no meta).
    const sep = '\u0001';
    shared
      .get('meta')!
      .set('bob' + sep + '/old' + sep, {
        updatedAt: Date.now() - 40 * 24 * 60 * 60 * 1000,
        formatVersion: 1
      });
    shared.get('rows')!.set('bob' + sep + '/old' + sep, '{"stale":1}');
    shared.get('rows')!.set('carol' + sep + '/orphan' + sep, '{"torn":1}');

    // A manager with an instant sweep delay: restore triggers the sweep.
    const manager = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      null,
      1,
      1,
      512,
      30000,
      300000,
      1000,
      1 << 20,
      20000,
      30 * 24 * 60 * 60 * 1000
    );
    // Shrink the sweep delay via the timer being real: patch is overkill —
    // call the private path through restore then wait past the delay is too
    // slow for tests, so invoke the sweep directly.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (manager as any).sweep_();
    manager.setAuthScope('alice');
    expect(shared.get('rows')!.has('carol' + sep + '/orphan' + sep)).to.equal(
      false
    );
    expect(shared.get('meta')!.has('bob' + sep + '/old' + sep)).to.equal(false);
    expect(shared.get('rows')!.has('bob' + sep + '/old' + sep)).to.equal(false);
    const kept = await manager.peek('/live', 'alice');
    expect(kept!.node.val()).to.deep.equal({ ok: 1 });
    manager.dispose();
  });
});

describe('RowPersistenceManager writer lease (Web Locks)', () => {
  /** A fake Web Locks manager: exclusive queued grants per name. */
  function makeFakeLocks() {
    const queues = new Map<
      string,
      Array<{ grant: () => void }>
    >();
    const held = new Set<string>();
    const tryGrantNext = (name: string): void => {
      if (held.has(name)) {
        return;
      }
      const queue = queues.get(name);
      if (queue === undefined || queue.length === 0) {
        return;
      }
      held.add(name);
      queue.shift()!.grant();
    };
    return {
      request: (
        name: string,
        _options: { mode: 'exclusive' },
        callback: (lock: unknown) => Promise<unknown>
      ): Promise<unknown> => {
        return new Promise(resolveRequest => {
          const entry = {
            grant: () => {
              void Promise.resolve()
                .then(() => callback({}))
                .then(() => {
                  held.delete(name);
                  resolveRequest(undefined);
                  tryGrantNext(name);
                });
            }
          };
          if (!queues.has(name)) {
            queues.set(name, []);
          }
          queues.get(name)!.push(entry);
          tryGrantNext(name);
        });
      }
    };
  }

  function lockedManager(
    idb: IDBFactory,
    locks: ReturnType<typeof makeFakeLocks>
  ): RowPersistenceManager {
    return new RowPersistenceManager(
      'test-repo',
      idb,
      locks,
      1,
      1,
      512,
      30000,
      300000,
      1000,
      1 << 20
    );
  }

  it('only the lock holder writes; a follower holds its dirt in memory', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const locks = makeFakeLocks();
    const a = lockedManager(makeFakeIdb(shared), locks);
    const b = lockedManager(makeFakeIdb(shared), locks);
    a.setAuthScope('alice');
    b.setAuthScope('alice');
    for (const m of [a, b]) {
      m.setPersistentPath('/ws', true);
      m.track('/ws');
    }
    await flushMicrotasks();
    // a queued first -> holder. b's write must NOT reach storage.
    b.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ from: 'b' }));
    await wait(30);
    await flushMicrotasks();
    expect(shared.get('meta')?.size ?? 0).to.equal(0);

    // a's write lands.
    a.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ from: 'a' }));
    await wait(30);
    await flushMicrotasks();
    expect(shared.get('meta')!.size).to.equal(1);
    a.dispose();
    b.dispose();
  });

  it('the lock hands off on dispose and the successor flushes its pending dirt', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const locks = makeFakeLocks();
    const a = lockedManager(makeFakeIdb(shared), locks);
    const b = lockedManager(makeFakeIdb(shared), locks);
    a.setAuthScope('alice');
    b.setAuthScope('alice');
    for (const m of [a, b]) {
      m.setPersistentPath('/ws', true);
      m.track('/ws');
    }
    await flushMicrotasks();
    // b (follower) accumulates dirt.
    b.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ from: 'b', v: 2 }));
    await wait(30);
    await flushMicrotasks();
    expect(shared.get('meta')?.size ?? 0).to.equal(0);

    // a dies -> the UA (fake) grants b's queued request -> b flushes.
    a.dispose();
    await wait(30);
    await flushMicrotasks();
    expect(shared.get('meta')!.size).to.equal(1);

    const reader = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      null,
      1,
      1,
      512,
      30000,
      300000,
      1000,
      1 << 20
    );
    reader.setAuthScope('alice');
    const result = await reader.peek('/ws', 'alice');
    expect(result!.node.val()).to.deep.equal({ from: 'b', v: 2 });
    b.dispose();
    reader.dispose();
  });
});

describe('generation-bound listen hashes', () => {
  it('a foreign commit between restore and hash downgrades the claim (gen mismatch)', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    writer.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 1 }));
    await writer.flushNow('/ws');
    await flushMicrotasks();
    writer.dispose();

    const manager = makeManager(makeFakeIdb(shared));
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const restored = await manager.restoreForListen('/ws');
    expect(restored.node).to.not.equal(null);

    // A second tab commits a NEWER generation before this tab hashes.
    const foreign = makeManager(makeFakeIdb(shared));
    foreign.setAuthScope('alice');
    foreign.setPersistentPath('/ws', true);
    foreign.track('/ws');
    foreign.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 2 }));
    await foreign.flushNow('/ws');
    await flushMicrotasks();
    foreign.dispose();

    // Hashing the foreign rows would certify bytes this tab does not hold.
    const hashes = await manager.computeListenHashes('/ws');
    expect(hashes).to.equal(null);
    manager.dispose();
  });

  it('hash succeeds when the stored generation is the one this manager restored', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    writer.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 1 }));
    await writer.flushNow('/ws');
    await flushMicrotasks();
    writer.dispose();

    const manager = makeManager(makeFakeIdb(shared));
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    await manager.restoreForListen('/ws');
    const hashes = await manager.computeListenHashes('/ws');
    expect(hashes).to.not.equal(null);
    manager.dispose();
  });
});

describe('overlapping tracked roots', () => {
  it('trackedRootsFor returns ancestors and descendants of the update path', () => {
    const manager = makeManager(makeFakeIdb());
    manager.setAuthScope('alice');
    for (const p of ['/a', '/a/b/c', '/x']) {
      manager.setPersistentPath(p, true);
      manager.track(p);
    }
    expect(manager.trackedRootsFor('/a/b').sort()).to.deep.equal([
      '/a',
      '/a/b/c'
    ]);
    expect(manager.trackedRootsFor('/x/y')).to.deep.equal(['/x']);
    expect(manager.trackedRootsFor('/unrelated')).to.deep.equal([]);
    manager.dispose();
  });
});

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
import { splitNodeIntoRows } from '../src/core/RowStore';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';

import { makeFakeIdb, flushMicrotasks, wait } from './helpers/fakeIdb';

/** A manager with instant write windows for tests. */
/**
 * Grants every request immediately and exclusively — the single-tab
 * environment. Tests that need contention build their own fake (see the
 * writer-lease suite); tests for the LOCKLESS environment pass
 * `webLocks: null` explicitly (claims are then declined by design).
 */
function makeAlwaysGrantedLocks(): {
  request: (
    name: string,
    options: { mode: 'exclusive' },
    callback: (lock: unknown) => Promise<unknown>
  ) => Promise<unknown>;
} {
  return {
    request: (_name, _options, callback) =>
      Promise.resolve().then(() => callback({}))
  };
}

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
    'webLocks' in options ? options.webLocks ?? null : makeAlwaysGrantedLocks(),
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

  it('an identity-equal update skips the flush; a change swaps one fresh generation', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const log = { puts: [] as string[], deletes: [] as string[] };
    const idb = makeFakeIdb(shared, log);
    const manager = makeManager(idb);
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const v1 = nodeFromJSON({
      left: { a: 'x'.repeat(600) },
      right: { c: 'y'.repeat(600) }
    });
    manager.serverCacheUpdated(new Path('/ws'), v1);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const putsAfterFirst = log.puts.length;

    // Same node identity (a certification, a no-op update): NO writes.
    manager.serverCacheUpdated(new Path('/ws'), v1);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    expect(log.puts.length).to.equal(putsAfterFirst);

    // A real change: one fresh generation replaces the old one (old gen's
    // chunks deleted in the swap transaction).
    const v2 = nodeFromJSON({
      left: { a: 'CHANGED' },
      right: { c: 'y'.repeat(600) }
    });
    manager.serverCacheUpdated(new Path('/ws'), v2, [['left', 'a']]);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const after = await reader.peek('/ws', 'alice');
    expect(after!.node.equals(v2)).to.equal(true);
    // Exactly one live generation remains in the chunk store.
    const gens = new Set(
      [...shared.get('chunks')!.keys()].map(k => k.split('\u0001')[2])
    );
    expect(gens.size).to.equal(1);
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
        const manager = makeManager(makeFakeIdb(shared), {
          splitThreshold: 96
        });
        manager.setAuthScope('u');
        manager.setPersistentPath('/r', true);
        manager.track('/r');
        // Reload: stored tree must equal the last flushed tree exactly.
        const restored = await manager.restoreForListen('/r');
        expect(
          restored.node,
          `seed ${seedBase} gen ${gen} restore`
        ).to.not.equal(null);
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
  it('removes expired roots and aged orphan generations, keeps live ones', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/live', true);
    writer.track('/live');
    writer.serverCacheUpdated(new Path('/live'), nodeFromJSON({ ok: 1 }));
    await writer.flushNow('/live');
    await flushMicrotasks();
    writer.dispose();

    const sep = '\u0001';
    // An expired root: meta 40 days old + its chunk.
    shared.get('meta')!.set('S' + sep + '/old' + sep, {
      gen: 'g1',
      chunkCount: 1,
      updatedAt: Date.now() - 40 * 24 * 60 * 60 * 1000,
      formatVersion: 3
    });
    shared
      .get('chunks')!
      .set('S' + sep + '/old' + sep + 'g1' + sep + '000000', '[]');
    // An AGED orphan generation of the live root (crashed write from long
    // ago — gen id's base36 time prefix far in the past).
    const oldGen = (Date.now() - 60 * 60 * 1000).toString(36) + 'zzzz';
    const liveMetaKey = [...shared.get('meta')!.keys()].find(
      k => k.indexOf('/live') >= 0
    )!;
    shared.get('chunks')!.set(liveMetaKey + oldGen + sep + '000000', '[]');
    // A FRESH orphan generation (an in-flight write): must survive.
    const freshGen = Date.now().toString(36) + 'ffff';
    shared.get('chunks')!.set(liveMetaKey + freshGen + sep + '000000', '[]');

    const manager = makeManager(makeFakeIdb(shared));
    manager.setAuthScope('alice');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (manager as any).sweep_();
    expect(shared.get('meta')!.has('S' + sep + '/old' + sep)).to.equal(false);
    expect(
      shared
        .get('chunks')!
        .has('S' + sep + '/old' + sep + 'g1' + sep + '000000')
    ).to.equal(false);
    expect(
      shared.get('chunks')!.has(liveMetaKey + oldGen + sep + '000000')
    ).to.equal(false);
    expect(
      shared.get('chunks')!.has(liveMetaKey + freshGen + sep + '000000')
    ).to.equal(true);
    const kept = makeManager(makeFakeIdb(shared));
    kept.setAuthScope('alice');
    const peeked = await kept.peek('/live', 'alice');
    expect(peeked!.node.val()).to.deep.equal({ ok: 1 });
    manager.dispose();
    kept.dispose();
  });
});

describe('RowPersistenceManager writer lease (Web Locks)', () => {
  /** A fake Web Locks manager: exclusive queued grants per name. */
  /**
   * Spec-accurate fake Web Locks: exclusive queued grants per name, and
   * `ifAvailable: true` resolves IMMEDIATELY — callback(null) when the lock
   * is held or contended, callback(lock) when free — never queues. This is
   * the semantics the two-phase acquisition depends on (probe decides now,
   * blocking request queues for succession).
   */
  function makeFakeLocks() {
    const queues = new Map<string, Array<{ grant: () => void }>>();
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
        options: { mode: 'exclusive'; ifAvailable?: boolean },
        callback: (lock: unknown | null) => Promise<unknown>
      ): Promise<unknown> => {
        if (options.ifAvailable === true) {
          if (held.has(name)) {
            // Contended: decide immediately with null, never queue.
            return Promise.resolve().then(() => callback(null));
          }
          held.add(name);
          return Promise.resolve()
            .then(() => callback({}))
            .then(result => {
              held.delete(name);
              tryGrantNext(name);
              return result;
            });
        }
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

describe('transaction atomicity (buffered fake)', () => {
  it('a dispose mid-whole-root-flush leaves a complete generation or none — never torn', async () => {
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
    // Whole-root rewrite; dispose while the txn's puts are queued. The
    // abort path must leave the v1 generation exactly as it was.
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 2 }));
    const flushing = manager.flushNow('/ws');
    manager.dispose();
    await flushing.catch(() => {});
    await flushMicrotasks();

    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const peeked = await reader.peek('/ws', 'alice');
    // Staging is meta-deleted-first / meta-written-last: the cache is
    // either a COMPLETE generation or ABSENT — never a torn mix. A dispose
    // mid-stage may cost the cache (cold next boot), never correctness.
    if (peeked !== null) {
      const v = (peeked.node.val() as { v: number }).v;
      expect([1, 2]).to.include(v);
    }
    reader.dispose();
  });

  it('another connection never observes a half-committed generation', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const writer = makeManager(makeFakeIdb(shared), { splitThreshold: 64 });
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    // Multi-row generation (small threshold forces several puts).
    const tree = nodeFromJSON({
      a: 'x'.repeat(200),
      b: 'y'.repeat(200),
      c: 'z'.repeat(200)
    });
    writer.serverCacheUpdated(new Path('/ws'), tree);
    const flushing = writer.flushNow('/ws');
    // While the flush's txn is buffered (not yet committed), a reader must
    // see NO cache at all — never some rows without meta or vice versa.
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const early = await reader.peek('/ws', 'alice');
    expect(early).to.equal(null);
    await flushing;
    await flushMicrotasks();
    const late = makeManager(makeFakeIdb(shared));
    late.setAuthScope('alice');
    const after = await late.peek('/ws', 'alice');
    expect(after).to.not.equal(null);
    expect(after!.node.equals(tree)).to.equal(true);
    writer.dispose();
    reader.dispose();
    late.dispose();
  });
});

describe('untrack/re-track race', () => {
  it('a synchronous re-registration during untrack keeps the root tracked', async () => {
    const manager = makeManager(makeFakeIdb());
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    // Remove + re-add in one stack (React effect cleanup then setup).
    manager.setPersistentPath('/ws', false);
    manager.untrack('/ws');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    await flushMicrotasks();
    await wait(10);
    await flushMicrotasks();
    expect(manager.trackedPaths()).to.deep.equal(['/ws']);
    // And the revived root still works end to end.
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ ok: 1 }));
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const restored = await manager.restoreForListen('/ws');
    expect(restored.node).to.not.equal(null);
    manager.dispose();
  });

  it('a plain untrack with no re-registration still tears down', async () => {
    const manager = makeManager(makeFakeIdb());
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.setPersistentPath('/ws', false);
    manager.untrack('/ws');
    await flushMicrotasks();
    await wait(10);
    await flushMicrotasks();
    expect(manager.trackedPaths()).to.deep.equal([]);
    manager.dispose();
  });
});

describe('iterative whole-root staging', () => {
  it('the chunked snapshot stores exactly the splitNodeIntoRows rows, across chunks', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    // ~1KB chunk budget forces multiple chunks at a 96B split threshold.
    const manager = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      makeAlwaysGrantedLocks(),
      1,
      1,
      96,
      30000,
      300000,
      1000,
      1024
    );
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON({
      a: { x: 'q'.repeat(300), y: 'r'.repeat(300) },
      b: 's'.repeat(400),
      c: { d: { e: 't'.repeat(350) }, f: 1 },
      g: 'plain',
      h: { i: 'u'.repeat(300), j: 'v'.repeat(300) }
    });
    manager.serverCacheUpdated(new Path('/ws'), tree);
    await manager.flushNow('/ws');
    await flushMicrotasks();

    expect(shared.get('chunks')!.size).to.be.greaterThan(1);
    // Concatenated chunk rows === the direct splitter's rows.
    const meta = [...shared.get('meta')!.values()][0] as { gen: string };
    const chunkKeys = [...shared.get('chunks')!.keys()]
      .filter(k => k.indexOf(meta.gen) >= 0)
      .sort();
    const rows: Array<[string[], string]> = [];
    for (const k of chunkKeys) {
      const parsed = JSON.parse(
        shared.get('chunks')!.get(k) as string
      ) as Array<[string[], string]>;
      for (const r of parsed) {
        rows.push(r);
      }
    }
    const expected = splitNodeIntoRows([], tree, 96);
    expect(rows).to.deep.equal(expected);

    const reader = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      makeAlwaysGrantedLocks(),
      1,
      1,
      96,
      30000,
      300000,
      1000,
      1024
    );
    reader.setAuthScope('alice');
    const restored = await reader.restoreForListen('/ws');
    expect(restored.node!.equals(tree)).to.equal(true);
    manager.dispose();
    reader.dispose();
  });
});

describe('boot-claim protection (amber regression)', () => {
  it('a listen certification (changedPaths=[]) marks nothing dirty', async () => {
    const manager = makeManager(makeFakeIdb());
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON({ a: 1 });
    manager.serverCacheUpdated(new Path('/ws'), tree);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    expect(manager.hasPendingDirt('/ws')).to.equal(false);
    // The certification write-through: nothing dirty, claim stays sound.
    manager.serverCacheUpdated(new Path('/ws'), tree, []);
    expect(manager.hasPendingDirt('/ws')).to.equal(false);
    const hashes = await manager.computeListenHashes('/ws');
    expect(hashes).to.not.equal(null);
    manager.dispose();
  });

  it('a root-covering changed path routes to whole-root staging, not the single-txn incremental', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const manager = makeManager(makeFakeIdb(shared));
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 1 }));
    await manager.flushNow('/ws');
    await flushMicrotasks();
    // A full push write-through names the ROOT ('at-path' at the root):
    // changedPaths=[[]] must take the batched whole-root path (dirty=null).
    const v2 = nodeFromJSON({ v: 2, extra: 'x'.repeat(100) });
    manager.serverCacheUpdated(new Path('/ws'), v2, [[]]);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const restored = await reader.peek('/ws', 'alice');
    expect(restored!.node.equals(v2)).to.equal(true);
    manager.dispose();
    reader.dispose();
  });
});

describe('claims require cross-tab exclusion', () => {
  it('no Web Locks -> every tab writes complete generations and claims stay sound', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const manager = makeManager(makeFakeIdb(shared), { webLocks: null });
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.serverCacheUpdated(new Path('/ws'), nodeFromJSON({ v: 1 }));
    await manager.flushNow('/ws');
    await flushMicrotasks();
    // v3 generations are complete-or-invisible, so a claim is sound even
    // without cross-tab exclusion: it describes this manager's own
    // committed generation, which IS its live cache.
    const hashes = await manager.computeListenHashes('/ws');
    expect(hashes).to.not.equal(null);
    manager.dispose();
  });
});

describe('deletion vs in-flight staging', () => {
  it('an evict during a multi-batch stage never leaves a torn store', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    // Tiny batch budget forces many staging transactions.
    const manager = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      makeAlwaysGrantedLocks(),
      1,
      1,
      96,
      30000,
      300000,
      1000,
      512
    );
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON(
      Object.fromEntries(
        Array.from({ length: 30 }, (_, i) => ['k' + i, 'x'.repeat(120) + i])
      )
    );
    manager.serverCacheUpdated(new Path('/ws'), tree);
    const staging = manager.flushNow('/ws');
    // Evict mid-stage: deleteRoot_ awaits the active flush, so the store
    // ends either fully deleted or a complete generation — never a mix.
    manager.evict(new Path('/ws'));
    await staging.catch(() => {});
    await wait(20);
    await flushMicrotasks();
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const peeked = await reader.peek('/ws', 'alice');
    if (peeked !== null) {
      expect(peeked.node.equals(tree)).to.equal(true);
    }
    manager.dispose();
    reader.dispose();
  });

  it('a foreign tab replacing the staging marker aborts the stage', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const manager = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      makeAlwaysGrantedLocks(),
      1,
      1,
      96,
      30000,
      300000,
      1000,
      256
    );
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const tree = nodeFromJSON(
      Object.fromEntries(
        Array.from({ length: 40 }, (_, i) => ['k' + i, 'y'.repeat(150) + i])
      )
    );
    manager.serverCacheUpdated(new Path('/ws'), tree);
    const staging = manager.flushNow('/ws');
    // A foreign deleteRoot_ (another tab's evict) clears marker + rows
    // while this stage is between batches.
    await wait(2);
    const metaStore = shared.get('meta');
    if (metaStore !== undefined) {
      metaStore.clear();
    }
    await staging.catch(() => {});
    await wait(20);
    await flushMicrotasks();
    // The stage must NOT have completed a generation over the foreign
    // deletion: either nothing is stored, or (if the marker clear landed
    // before the first batch) a complete self-consistent generation.
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const peeked = await reader.peek('/ws', 'alice');
    if (peeked !== null) {
      expect(peeked.node.equals(tree)).to.equal(true);
    }
    manager.dispose();
    reader.dispose();
  });
});

describe('sweep vs concurrent commits (round-3)', () => {
  it('a generation committed while the sweep runs keeps all its chunks', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    const sep = '\u0001';
    // Seed an EXPIRED root so the sweep has work.
    shared.set('meta', new Map());
    shared.set('chunks', new Map());
    shared.get('meta')!.set('S' + sep + '/dead' + sep, {
      gen: 'g0',
      chunkCount: 1,
      updatedAt: Date.now() - 40 * 24 * 60 * 60 * 1000,
      formatVersion: 3
    });
    shared
      .get('chunks')!
      .set('S' + sep + '/dead' + sep + 'g0' + sep + '000000', '[]');

    const sweeper = makeManager(makeFakeIdb(shared));
    sweeper.setAuthScope('alice');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const sweeping = (sweeper as any).sweep_();
    const writer = makeManager(makeFakeIdb(shared));
    writer.setAuthScope('alice');
    writer.setPersistentPath('/ws', true);
    writer.track('/ws');
    const tree = nodeFromJSON({ fresh: 'data' });
    writer.serverCacheUpdated(new Path('/ws'), tree);
    const flushing = writer.flushNow('/ws');
    await Promise.all([sweeping, flushing]);
    await flushMicrotasks();

    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('alice');
    const peeked = await reader.peek('/ws', 'alice');
    expect(peeked).to.not.equal(null);
    expect(peeked!.node.equals(tree)).to.equal(true);
    expect(shared.get('meta')!.has('S' + sep + '/dead' + sep)).to.equal(false);
    sweeper.dispose();
    writer.dispose();
    reader.dispose();
  });
});

describe('deletion vs auth switch (round-3)', () => {
  it('an evict awaited across a scope switch never deletes the new scope cache', async () => {
    const shared = new Map<string, Map<string, unknown>>();
    // Bob has a valid cache.
    const bobWriter = makeManager(makeFakeIdb(shared));
    bobWriter.setAuthScope('bob');
    bobWriter.setPersistentPath('/ws', true);
    bobWriter.track('/ws');
    const bobTree = nodeFromJSON({ owner: 'bob' });
    bobWriter.serverCacheUpdated(new Path('/ws'), bobTree);
    await bobWriter.flushNow('/ws');
    await flushMicrotasks();
    bobWriter.dispose();

    // Alice: start a multi-batch stage, evict mid-flight, then switch to
    // Bob before the awaited deletion runs.
    const manager = new RowPersistenceManager(
      'test-repo',
      makeFakeIdb(shared),
      makeAlwaysGrantedLocks(),
      1,
      1,
      96,
      30000,
      300000,
      1000,
      512
    );
    manager.setAuthScope('alice');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    manager.serverCacheUpdated(
      new Path('/ws'),
      nodeFromJSON(
        Object.fromEntries(
          Array.from({ length: 30 }, (_, i) => ['k' + i, 'x'.repeat(120) + i])
        )
      )
    );
    const staging = manager.flushNow('/ws');
    manager.evict(new Path('/ws'));
    manager.setAuthScope('bob');
    await staging.catch(() => {});
    await wait(30);
    await flushMicrotasks();

    // Bob's cache survived: the deletion belonged to Alice's namespace.
    const reader = makeManager(makeFakeIdb(shared));
    reader.setAuthScope('bob');
    const peeked = await reader.peek('/ws', 'bob');
    expect(peeked).to.not.equal(null);
    expect(peeked!.node.equals(bobTree)).to.equal(true);
    manager.dispose();
    reader.dispose();
  });
});

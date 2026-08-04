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
import { PersistentConnection } from '../src/core/PersistentConnection';
import { RepoInfo } from '../src/core/RepoInfo';
import {
  computeCanonicalHash,
  computeCompoundHash,
  buildSeedNode,
  ListenHashFn,
  ServerCacheSeedStore
} from '../src/core/ServerCacheSeed';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { RangeMerge } from '../src/core/snap/RangeMerge';
import {
  SyncTree,
  syncTreeAddEventRegistration,
  syncTreeApplyServerRangeMerges,
  syncTreeGetCompleteServerCache
} from '../src/core/SyncTree';
import { Path } from '../src/core/util/Path';
import { Event } from '../src/core/view/Event';
import {
  EventRegistration,
  QueryContext
} from '../src/core/view/EventRegistration';
import { QueryParams } from '../src/core/view/QueryParams';

interface CapturedListen {
  pathString: string;
  hashFn: ListenHashFn;
  onComplete: (status: string) => Event[];
}

function makeSyncTree(): {
  syncTree: SyncTree;
  listens: CapturedListen[];
  seeds: ServerCacheSeedStore;
} {
  const listens: CapturedListen[] = [];
  const seeds = new ServerCacheSeedStore();
  const syncTree = new SyncTree({
    startListening: (query, tag, hashFn, onComplete) => {
      listens.push({
        pathString: query._path.toString(),
        hashFn,
        onComplete
      });
      return [];
    },
    stopListening: () => {},
    takeServerCacheSeed: pathString => seeds.take(pathString)
  });
  return { syncTree, listens, seeds };
}

interface CapturedChange {
  type?: string;
  snapshotNode?: { val(exportFormat?: boolean): unknown };
}

function makeEventRegistration(): EventRegistration {
  return {
    respondsTo: () => true,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    createEvent: (change: unknown) => ({ change } as any),
    getEventRunner: () => () => {},
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    createCancelEvent: () => null as any,
    matches: () => false,
    hasAnyCallback: () => true
  };
}

function defaultQueryAt(pathString: string): QueryContext {
  return new QueryImpl(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    null as any,
    new Path(pathString),
    new QueryParams(),
    false
  );
}

function changesOf(events: Event[]): CapturedChange[] {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return events.map(event => (event as any).change as CapturedChange);
}

describe('Server cache seeding', () => {
  it('an unseeded listen sends the empty hash and no compound hash', () => {
    const { syncTree, listens } = makeSyncTree();
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('unseeded/path'),
      makeEventRegistration()
    );
    expect(listens).to.have.length(1);
    expect(listens[0].hashFn()).to.equal('');
    expect(listens[0].hashFn.compoundHash?.()).to.equal(undefined);
  });

  it('a seeded listen carries the stamped hash without recomputing it', () => {
    const json = { a: 'x', b: { c: 1 } };
    const hash = computeCanonicalHash(json);
    const { syncTree, listens, seeds } = makeSyncTree();
    seeds.set('seeded/path', json, hash);
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('seeded/path'),
      makeEventRegistration()
    );
    expect(listens).to.have.length(1);
    expect(listens[0].hashFn()).to.equal(hash);
  });

  it('a seeded listen exposes the stamped compound hash', () => {
    const json = { a: 'x', b: { c: 1 } };
    const compoundHash = computeCompoundHash(json);
    const { syncTree, listens, seeds } = makeSyncTree();
    seeds.set('seeded/ch', json, computeCanonicalHash(json), compoundHash);
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('seeded/ch'),
      makeEventRegistration()
    );
    expect(listens[0].hashFn.compoundHash?.()).to.deep.equal(compoundHash);
  });

  it('the stamped hash equals what an unstamped node computes', () => {
    const json = { s: 'text', n: 1.5, deep: { b: true } };
    expect(computeCanonicalHash(json)).to.equal(nodeFromJSON(json).hash());
  });

  it('a seed raises no value event until the server confirms', () => {
    const { syncTree, seeds } = makeSyncTree();
    seeds.set('quiet/path', { a: 1 }, computeCanonicalHash({ a: 1 }));
    const initial = syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('quiet/path'),
      makeEventRegistration()
    );
    // The incomplete seed must not surface a value event — that is what
    // onValue callbacks fire on; only server confirmation may produce it.
    const valueEvents = changesOf(initial).filter(
      change => change && change.type === 'value'
    );
    expect(valueEvents).to.have.length(0);
  });

  it('listen-complete promotes the seed to a server-certified value event', () => {
    const { syncTree, listens, seeds } = makeSyncTree();
    seeds.set('confirmed/path', { a: 1 }, computeCanonicalHash({ a: 1 }));
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('confirmed/path'),
      makeEventRegistration()
    );
    // Server certified our hash: 'ok' with no data.
    const events = listens[0].onComplete('ok');
    const valueEvent = changesOf(events).find(
      change => change && change.type === 'value'
    );
    expect(valueEvent).to.not.equal(undefined);
    expect(valueEvent!.snapshotNode!.val(true)).to.deep.equal({ a: 1 });
  });

  it('a seed is consumed at most once and only for its exact path', () => {
    const seeds = new ServerCacheSeedStore();
    seeds.set('once/path', { a: 1 });
    expect(seeds.take('other/path')).to.equal(undefined);
    expect(seeds.take('/once/path')).to.not.equal(undefined);
    expect(seeds.take('/once/path')).to.equal(undefined);
  });

  it('seeds are scoped to their store — one instance cannot steal another', () => {
    const first = makeSyncTree();
    const second = makeSyncTree();
    first.seeds.set('shared/path', { a: 1 }, computeCanonicalHash({ a: 1 }));

    // The second instance listens to the same path first: no theft.
    syncTreeAddEventRegistration(
      second.syncTree,
      defaultQueryAt('shared/path'),
      makeEventRegistration()
    );
    expect(second.listens[0].hashFn()).to.equal('');

    // The first instance still holds its seed.
    syncTreeAddEventRegistration(
      first.syncTree,
      defaultQueryAt('shared/path'),
      makeEventRegistration()
    );
    expect(first.listens[0].hashFn()).to.equal(computeCanonicalHash({ a: 1 }));
  });

  it('a filtered query does not consume the seed', () => {
    const { syncTree, seeds } = makeSyncTree();
    seeds.set('filtered/path', { a: 1 });
    const params = new QueryParams();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (params as any).limitSet_ = true;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (params as any).limit_ = 5;
    const query = new QueryImpl(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      null as any,
      new Path('filtered/path'),
      params,
      false
    );
    syncTreeAddEventRegistration(syncTree, query, makeEventRegistration());
    // The seed must still be there for a later default listen.
    expect(seeds.take('filtered/path')).to.not.equal(undefined);
  });

  it('a seeded (uncertified) cache is not reported as complete server cache', () => {
    const { syncTree, listens, seeds } = makeSyncTree();
    seeds.set('seeded/gate', { a: 1 }, computeCanonicalHash({ a: 1 }));
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('seeded/gate'),
      makeEventRegistration()
    );
    // Uncertified: the persistence write-through must see nothing here, or
    // seeded bytes would be re-persisted as server truth.
    expect(
      syncTreeGetCompleteServerCache(syncTree, new Path('seeded/gate'))
    ).to.equal(null);
    // Server certifies -> now it is complete.
    listens[0].onComplete('ok');
    const cache = syncTreeGetCompleteServerCache(
      syncTree,
      new Path('seeded/gate')
    );
    expect(cache).to.not.equal(null);
    expect(cache!.val(true)).to.deep.equal({ a: 1 });
  });

  it('an empty seed does not poison the shared empty-node singleton', () => {
    const seeded = buildSeedNode({
      json: null,
      hash: 'HASH_SHOULD_NOT_STICK'
    });
    expect(seeded.isEmpty()).to.equal(true);
    expect(nodeFromJSON(null).hash()).to.equal('');
  });

  it('a range merge against the seeded cache yields the reconciled tree', () => {
    const cached = {
      bar: 'bar-value',
      foo: { a: { 'deep-a-1': 1, 'deep-a-2': 2 }, b: 'b', c: 'c', d: 'd' },
      quu: 'quu-value'
    };
    const { syncTree, seeds } = makeSyncTree();
    seeds.set(
      'rm/path',
      cached,
      computeCanonicalHash(cached),
      computeCompoundHash(cached)
    );
    syncTreeAddEventRegistration(
      syncTree,
      defaultQueryAt('rm/path'),
      makeEventRegistration()
    );

    const merges = [
      new RangeMerge(
        new Path('foo/a/deep-a-1'),
        new Path('foo/c'),
        nodeFromJSON({
          foo: {
            a: { 'deep-a-2': 'new-a-2', 'deep-a-3': 3 },
            'b-2': 'new-b',
            c: 'new-c'
          }
        })
      )
    ];
    const events = syncTreeApplyServerRangeMerges(
      syncTree,
      new Path('rm/path'),
      merges
    );
    const valueEvent = changesOf(events).find(
      change => change && change.type === 'value'
    );
    expect(valueEvent).to.not.equal(undefined);
    expect(valueEvent!.snapshotNode!.val(true)).to.deep.equal({
      bar: 'bar-value',
      foo: {
        a: { 'deep-a-1': 1, 'deep-a-2': 'new-a-2', 'deep-a-3': 3 },
        'b-2': 'new-b',
        c: 'new-c',
        d: 'd'
      },
      quu: 'quu-value'
    });
  });

  it('a range merge for a path with no view is ignored', () => {
    const { syncTree } = makeSyncTree();
    const events = syncTreeApplyServerRangeMerges(
      syncTree,
      new Path('nobody/here'),
      [new RangeMerge(null, null, nodeFromJSON({ a: 1 }))]
    );
    expect(events).to.deep.equal([]);
  });
});

describe('PersistentConnection compound-hash wire protocol', () => {
  function makeConnection(rmCalls: unknown[][]): PersistentConnection {
    return new PersistentConnection(
      new RepoInfo(
        'test.example.com',
        true,
        'test',
        false,
        undefined,
        undefined,
        false,
        false
      ),
      'app-id',
      () => {},
      () => {},
      () => {},
      {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        addTokenChangeListener: () => {},
        removeTokenChangeListener: () => {}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any,
      {
        addTokenChangeListener: () => {},
        removeTokenChangeListener: () => {}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any,
      undefined,
      (pathString, ranges, tag) => {
        rmCalls.push([pathString, ranges, tag]);
      }
    );
  }

  interface SentRequest {
    action: string;
    body: { [k: string]: unknown };
  }

  function captureRequests(connection: PersistentConnection): SentRequest[] {
    const sent: SentRequest[] = [];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (connection as any).sendRequest = (
      action: string,
      body: { [k: string]: unknown }
    ) => {
      sent.push({ action, body });
    };
    return sent;
  }

  it('sendListen_ attaches ch when the hashFn carries a compound hash', () => {
    const connection = makeConnection([]);
    const sent = captureRequests(connection);

    const json = { a: 'x', b: 'y' };
    const compoundHash = computeCompoundHash(json);
    const hashFn: ListenHashFn = () => computeCanonicalHash(json);
    hashFn.compoundHash = () => compoundHash;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (connection as any).sendListen_({
      onComplete: () => [],
      hashFn,
      query: defaultQueryAt('some/path'),
      tag: null
    });

    expect(sent).to.have.length(1);
    expect(sent[0].action).to.equal('q');
    expect(sent[0].body['h']).to.equal(computeCanonicalHash(json));
    expect(sent[0].body['ch']).to.deep.equal({
      hs: compoundHash.hashes,
      ps: compoundHash.posts
    });
  });

  it('sendListen_ omits ch when the cache carries no compound hash', () => {
    const connection = makeConnection([]);
    const sent = captureRequests(connection);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (connection as any).sendListen_({
      onComplete: () => [],
      hashFn: (() => '') as ListenHashFn,
      query: defaultQueryAt('some/path'),
      tag: null
    });

    expect(sent).to.have.length(1);
    expect(sent[0].body['h']).to.equal('');
    expect('ch' in sent[0].body).to.equal(false);
  });

  it('onDataPush_ dispatches rm pushes to the range-merge callback', () => {
    const rmCalls: unknown[][] = [];
    const connection = makeConnection(rmCalls);
    const ranges = [{ s: 'a/b', e: 'a/z', m: { c: 1 } }];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (connection as any).onDataPush_('rm', { p: 'some/path', d: ranges, t: 42 });
    expect(rmCalls).to.deep.equal([['some/path', ranges, 42]]);
  });

  it('onDataPush_ ignores rm when no callback is registered', () => {
    const connection = new PersistentConnection(
      new RepoInfo(
        'test.example.com',
        true,
        'test',
        false,
        undefined,
        undefined,
        false,
        false
      ),
      'app-id',
      () => {},
      () => {},
      () => {},
      {
        addTokenChangeListener: () => {},
        removeTokenChangeListener: () => {}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any,
      {
        addTokenChangeListener: () => {},
        removeTokenChangeListener: () => {}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any
    );
    expect(() =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (connection as any).onDataPush_('rm', { p: 'x', d: [] })
    ).to.not.throw();
  });
});

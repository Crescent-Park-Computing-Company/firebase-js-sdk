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
import { compoundHashFromNode } from '../src/core/CompoundHash';
import { PersistentConnection } from '../src/core/PersistentConnection';
import { RepoInfo } from '../src/core/RepoInfo';
import { ListenHashFn, SeedCompoundHash } from '../src/core/ServerCacheSeed';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';
import { QueryContext } from '../src/core/view/EventRegistration';
import { QueryParams } from '../src/core/view/QueryParams';

function computeCanonicalHash(json: unknown): string {
  return nodeFromJSON(json).hash();
}

function computeCompoundHash(json: unknown): SeedCompoundHash {
  const hash = compoundHashFromNode(nodeFromJSON(json));
  return { hashes: hash.hashes, posts: hash.posts };
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

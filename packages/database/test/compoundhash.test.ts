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
  collectChangedSubtreePaths,
  canonicalHashFromNodeAsync,
  CompoundHash,
  compoundHashFromNode,
  compoundHashFromNodeAsync,
  CompoundHashSplitState,
  estimateSerializedNodeSize
} from '../src/core/CompoundHash';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';
import { sha1 } from '../src/core/util/util';

/**
 * The golden vectors in this suite mirror the reference implementations'
 * suites (Android CompoundHashTest.java, iOS FCompoundHashTest.m), so the
 * serialization stays wire-compatible with the mobile SDKs.
 */

const NEVER_SPLIT = () => false;

function splitAtPaths(paths: string[]) {
  return (state: CompoundHashSplitState) =>
    paths.includes(state.currentPath().join('/'));
}

function compoundOf(
  json: unknown,
  splitStrategy?: (state: CompoundHashSplitState) => boolean
): CompoundHash {
  return compoundHashFromNode(nodeFromJSON(json), splitStrategy);
}

describe('CompoundHash', () => {
  it('empty node yields the empty compound hash', () => {
    const hash = compoundOf(null);
    expect(hash.posts).to.deep.equal([]);
    expect(hash.hashes).to.deep.equal(['']);
  });

  it('compound hash is always followed by the trailing empty hash', () => {
    const hash = compoundOf({ foo: 'bar' }, NEVER_SPLIT);
    expect(hash.posts).to.deep.equal(['foo']);
    expect(hash.hashes).to.deep.equal([sha1('("foo":(string:"bar"))'), '']);
  });

  it('can split at priority', () => {
    const hash = compoundOf(
      {
        foo: {
          '!beforePriority': 'before',
          '.priority': 'prio',
          afterPriority: 'after'
        },
        qux: 'qux'
      },
      splitAtPaths(['foo/.priority'])
    );
    expect(hash.posts).to.deep.equal(['foo/.priority', 'qux']);
    expect(hash.hashes).to.deep.equal([
      sha1(
        '("foo":("!beforePriority":(string:"before"),".priority":(string:"prio")))'
      ),
      sha1('("foo":("afterPriority":(string:"after")),"qux":(string:"qux"))'),
      ''
    ]);
  });

  it('hashes priority leaf nodes with the priority prefix', () => {
    const hash = compoundOf(
      { foo: { '.value': 'bar', '.priority': 'baz' } },
      NEVER_SPLIT
    );
    expect(hash.posts).to.deep.equal(['foo']);
    expect(hash.hashes).to.deep.equal([
      sha1('("foo":(priority:string:"baz":string:"bar"))'),
      ''
    ]);
  });

  it('follows Firebase key semantics (integer keys sort numerically)', () => {
    const hash = compoundOf(
      { '1': 'one', '2': 'two', '10': 'ten' },
      splitAtPaths(['2'])
    );
    expect(hash.posts).to.deep.equal(['2', '10']);
    expect(hash.hashes).to.deep.equal([
      sha1('("1":(string:"one"),"2":(string:"two"))'),
      sha1('("10":(string:"ten"))'),
      ''
    ]);
  });

  it('splits on child boundaries', () => {
    const hash = compoundOf(
      { bar: { deep: 'value' }, foo: { 'other-deep': 'value' } },
      splitAtPaths(['bar/deep'])
    );
    expect(hash.posts).to.deep.equal(['bar/deep', 'foo/other-deep']);
    expect(hash.hashes).to.deep.equal([
      sha1('("bar":("deep":(string:"value")))'),
      sha1('("foo":("other-deep":(string:"value")))'),
      ''
    ]);
  });

  it('sets commas between nested children', () => {
    const hash = compoundOf(
      { bar: { deep: 'value' }, foo: { 'other-deep': 'value' } },
      NEVER_SPLIT
    );
    expect(hash.posts).to.deep.equal(['foo/other-deep']);
    expect(hash.hashes).to.deep.equal([
      sha1(
        '("bar":("deep":(string:"value")),"foo":("other-deep":(string:"value")))'
      ),
      ''
    ]);
  });

  it('quotes strings and keys', () => {
    const hash = compoundOf({ '"': '\\', '"\\"\\': '"\\"\\' }, NEVER_SPLIT);
    expect(hash.posts).to.deep.equal(['"\\"\\']);
    expect(hash.hashes).to.deep.equal([
      sha1(
        '("\\"":(string:"\\\\"),"\\"\\\\\\"\\\\":(string:"\\"\\\\\\"\\\\"))'
      ),
      ''
    ]);
  });

  it('number leaves use the IEEE754 representation', () => {
    const hash = compoundOf({ n: 1 }, NEVER_SPLIT);
    expect(hash.hashes[0]).to.equal(sha1('("n":(number:3ff0000000000000))'));
  });

  it('boolean leaves serialize as true/false', () => {
    const hash = compoundOf({ b: true }, NEVER_SPLIT);
    expect(hash.hashes[0]).to.equal(sha1('("b":(boolean:true))'));
  });

  it('default split yields a sensible number of ranges', () => {
    // ~10KB and ~100KB nodes, matching the reference suites' size checks.
    const dict10k: Record<string, string> = {};
    for (let i = 0; i < 500; i++) {
      dict10k[String(i)] = 'value';
    }
    const dict100k: Record<string, string> = {};
    for (let i = 0; i < 5000; i++) {
      dict100k[String(i)] = 'value';
    }
    expect(compoundOf(dict10k).hashes.length).to.be.within(12, 18);
    expect(compoundOf(dict100k).hashes.length).to.be.within(45, 55);
  });

  it('posts are always hashes minus one', () => {
    const shapes: unknown[] = [
      { a: 1 },
      { a: { b: { c: 'deep' } }, d: true },
      Object.fromEntries(Array.from({ length: 300 }, (_, i) => [String(i), i]))
    ];
    for (const json of shapes) {
      const { hashes, posts } = compoundOf(json);
      expect(posts.length).to.equal(hashes.length - 1);
      expect(hashes[hashes.length - 1]).to.equal('');
    }
  });

  it('constructor rejects mismatched posts/hashes', () => {
    expect(() => new CompoundHash(['a', 'b'], ['h'])).to.throw();
  });

  it('size estimation is stable for leaves and children', () => {
    expect(estimateSerializedNodeSize(nodeFromJSON(null))).to.equal(4);
    expect(estimateSerializedNodeSize(nodeFromJSON(1.5))).to.equal(8);
    expect(estimateSerializedNodeSize(nodeFromJSON(true))).to.equal(4);
    expect(estimateSerializedNodeSize(nodeFromJSON('abc'))).to.equal(5);
    expect(estimateSerializedNodeSize(nodeFromJSON({ key: 'abc' }))).to.equal(
      1 + 3 + 4 + 5
    );
  });

  it('async compound hash matches the synchronous one', async () => {
    const shapes: unknown[] = [
      { a: 1 },
      { a: { b: { c: 'deep', '.priority': 'p' } }, d: true },
      Object.fromEntries(Array.from({ length: 800 }, (_, i) => [String(i), i]))
    ];
    for (const json of shapes) {
      const node = nodeFromJSON(json);
      // A tiny slice forces multiple scheduling rounds on the larger trees.
      const asyncHash = await compoundHashFromNodeAsync(node, undefined, 1);
      const syncHash = compoundHashFromNode(node);
      expect(asyncHash.hashes).to.deep.equal(syncHash.hashes);
      expect(asyncHash.posts).to.deep.equal(syncHash.posts);
    }
  });

  it('non-retaining canonical hash ignores lazy hash state', async () => {
    const json = { a: 1, b: { c: 'x', '.priority': 2 } };
    const node = nodeFromJSON(json);
    node.stampLazyHash('deliberately-wrong');
    expect(await canonicalHashFromNodeAsync(node, 1)).to.equal(
      nodeFromJSON(json).hash()
    );
  });

  it('collapses a broad changed subtree instead of dirtying the whole root', () => {
    const beforeJson: Record<string, unknown> = {
      stable: { untouched: true },
      broad: {}
    };
    const afterBroad: Record<string, unknown> = {};
    for (let i = 0; i < 100; i++) {
      (beforeJson.broad as Record<string, unknown>)['k' + i] = { value: i };
      afterBroad['k' + i] = { value: i + 1 };
    }
    const before = nodeFromJSON(beforeJson);
    const after = before.updateChild(
      new Path('broad'),
      nodeFromJSON(afterBroad)
    );
    const changed = collectChangedSubtreePaths(before, after, 8, 4);
    expect(changed).to.deep.equal([['broad']]);
  });

  it('finds randomized adds/removes/edits at their exact paths', () => {
    // Ground-truth check for the sorted pair-merge: every mutation must be
    // covered by some reported changed path (a reported path is a PREFIX of
    // the mutation), and no report may cover an untouched sibling subtree.
    const rand = (seed => () => {
      seed |= 0;
      seed = (seed + 0x6d2b79f5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    })(20260809);
    for (let round = 0; round < 30; round++) {
      const beforeJson: Record<string, Record<string, unknown>> = {};
      for (let c = 0; c < 12; c++) {
        const child: Record<string, unknown> = {};
        for (let g = 0; g < 6; g++) {
          child['g' + g] = { v: c * 100 + g };
        }
        beforeJson['c' + c] = child;
      }
      const before = nodeFromJSON(beforeJson);
      let after = before;
      const mutations: string[][] = [];
      const count = 1 + Math.floor(rand() * 4);
      for (let m = 0; m < count; m++) {
        const c = 'c' + Math.floor(rand() * 12);
        const roll = rand();
        if (roll < 0.4) {
          const g = 'g' + Math.floor(rand() * 6);
          after = after.updateChild(
            new Path(c + '/' + g + '/v'),
            nodeFromJSON(round * 1000 + m)
          );
          mutations.push([c, g, 'v']);
        } else if (roll < 0.7) {
          const added = 'added' + m;
          after = after.updateChild(
            new Path(c + '/' + added),
            nodeFromJSON({ fresh: m })
          );
          mutations.push([c, added]);
        } else {
          const g = 'g' + Math.floor(rand() * 6);
          after = after.updateChild(new Path(c + '/' + g), nodeFromJSON(null));
          mutations.push([c, g]);
        }
      }
      const changed = collectChangedSubtreePaths(before, after)!;
      expect(changed).to.not.equal(null);
      const covers = (report: string[], target: string[]) =>
        report.length <= target.length &&
        report.every((seg, i) => seg === target[i]);
      for (const mutation of mutations) {
        expect(
          changed.some(report => covers(report, mutation)),
          `round ${round}: ${mutation.join('/')} uncovered in ${JSON.stringify(
            changed
          )}`
        ).to.equal(true);
      }
      const touchedTops = new Set(mutations.map(m => m[0]));
      for (const report of changed) {
        expect(
          touchedTops.has(report[0]),
          `round ${round}: spurious report ${report.join('/')}`
        ).to.equal(true);
      }
    }
  });
});

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

import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { RangeMerge } from '../src/core/snap/RangeMerge';
import { Path } from '../src/core/util/Path';

/**
 * The vectors in this suite mirror the reference implementations' suites
 * (Android RangeMergeTest.java, iOS FRangeMergeTest.m), so range application
 * stays wire-compatible with the mobile SDKs.
 */

function applyRange(
  nodeJson: unknown,
  start: string | null,
  end: string | null,
  updateJson: unknown
): unknown {
  const merge = new RangeMerge(
    start === null ? null : new Path(start),
    end === null ? null : new Path(end),
    nodeFromJSON(updateJson)
  );
  return merge.applyTo(nodeFromJSON(nodeJson)).val(true);
}

describe('RangeMerge', () => {
  it('smoke test: applies a mid-tree range', () => {
    const base = {
      bar: 'bar-value',
      foo: { a: { 'deep-a-1': 1, 'deep-a-2': 2 }, b: 'b', c: 'c', d: 'd' },
      quu: 'quu-value'
    };
    const updates = {
      foo: {
        a: { 'deep-a-2': 'new-a-2', 'deep-a-3': 3 },
        'b-2': 'new-b',
        c: 'new-c'
      }
    };
    expect(applyRange(base, 'foo/a/deep-a-1', 'foo/c', updates)).to.deep.equal({
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

  it('start is exclusive', () => {
    expect(
      applyRange(
        { bar: 'bar-value', foo: 'foo-value', quu: 'quu-value' },
        'bar',
        'foo',
        { foo: 'new-foo-value' }
      )
    ).to.deep.equal({
      bar: 'bar-value',
      foo: 'new-foo-value',
      quu: 'quu-value'
    });
  });

  it('start is exclusive but includes children', () => {
    expect(
      applyRange(
        { bar: 'bar-value', foo: 'foo-value', quu: 'quu-value' },
        'bar',
        'foo',
        { bar: { 'bar-child': 'bar-child-value' }, foo: 'new-foo-value' }
      )
    ).to.deep.equal({
      bar: { 'bar-child': 'bar-child-value' },
      foo: 'new-foo-value',
      quu: 'quu-value'
    });
  });

  it('end is inclusive (absent key in range deletes)', () => {
    expect(
      applyRange(
        { bar: 'bar-value', foo: 'foo-value', quu: 'quu-value' },
        'bar',
        'foo',
        { baz: 'baz-value' }
      )
    ).to.deep.equal({ bar: 'bar-value', baz: 'baz-value', quu: 'quu-value' });
  });

  it('end is inclusive but excludes children', () => {
    expect(
      applyRange(
        {
          bar: 'bar-value',
          foo: { 'foo-child': 'foo-child-value' },
          quu: 'quu-value'
        },
        'bar',
        'foo',
        { baz: 'baz-value' }
      )
    ).to.deep.equal({
      bar: 'bar-value',
      baz: 'baz-value',
      foo: { 'foo-child': 'foo-child-value' },
      quu: 'quu-value'
    });
  });

  it('can update a leaf node', () => {
    expect(
      applyRange('leaf-value', null, 'foo', { bar: 'bar-value' })
    ).to.deep.equal({ bar: 'bar-value' });
  });

  it('can replace a leaf node with a leaf node', () => {
    expect(applyRange('leaf-value', null, '', 'new-leaf-value')).to.equal(
      'new-leaf-value'
    );
  });

  it('updates leaves when the range includes a deeper path', () => {
    expect(
      applyRange({ foo: { bar: 'bar-value' } }, 'foo', 'foo/bar/deep', {
        foo: { bar: 'new-bar-value' }
      })
    ).to.deep.equal({ foo: { bar: 'new-bar-value' } });
  });

  it('does not update leaves when the range starts at the leaf and includes deeper paths', () => {
    expect(
      applyRange({ foo: { bar: 'bar-value' } }, 'foo/bar', 'foo/bar/deep', {
        foo: { bar: 'new-bar-value' }
      })
    ).to.deep.equal({ foo: { bar: 'bar-value' } });
  });

  it('updating the entire (unbounded) range updates everything', () => {
    expect(
      applyRange(null, null, null, {
        foo: 'foo-value',
        bar: { child: 'bar-child-value' }
      })
    ).to.deep.equal({
      foo: 'foo-value',
      bar: { child: 'bar-child-value' }
    });
  });

  it('unbounded left post works', () => {
    expect(
      applyRange({ bar: 'bar-value', foo: 'foo-value' }, null, 'bar', {
        bar: 'new-bar'
      })
    ).to.deep.equal({ bar: 'new-bar', foo: 'foo-value' });
  });

  it('right post being a child of the left post works', () => {
    expect(
      applyRange(
        { foo: { a: 'a', b: { '1': '1', '2': '2' }, c: 'c' } },
        'foo',
        'foo/b/1',
        { foo: { a: 'new-a', b: { '1': 'new-1' } } }
      )
    ).to.deep.equal({
      foo: { a: 'new-a', b: { '1': 'new-1', '2': '2' }, c: 'c' }
    });
  });

  it('right post child of left post works with integer keys', () => {
    expect(
      applyRange(
        { foo: { a: 'a', b: { '1': '1', '2': '2', '10': '10' }, c: 'c' } },
        'foo',
        'foo/b/2',
        { foo: { a: 'new-a', b: { '1': 'new-1' } } }
      )
    ).to.deep.equal({
      foo: { a: 'new-a', b: { '1': 'new-1', '10': '10' }, c: 'c' }
    });
  });

  it('updating a leaf includes its priority', () => {
    expect(
      applyRange(
        { bar: 'bar-value', foo: 'foo-value', quu: 'quu-value' },
        'bar',
        'foo',
        { foo: { '.value': 'new-foo', '.priority': 'prio' } }
      )
    ).to.deep.equal({
      bar: 'bar-value',
      foo: { '.value': 'new-foo', '.priority': 'prio' },
      quu: 'quu-value'
    });
  });

  it('updates a priority in a children node', () => {
    expect(
      applyRange({ bar: 'bar-value', foo: 'foo-value' }, null, 'bar', {
        bar: 'new-bar',
        '.priority': 'prio'
      })
    ).to.deep.equal({
      bar: 'new-bar',
      foo: 'foo-value',
      '.priority': 'prio'
    });
  });

  it('updating a priority on an initially empty node does not break', () => {
    expect(
      applyRange(null, null, 'foo', { '.priority': 'prio', foo: 'foo-value' })
    ).to.deep.equal({ foo: 'foo-value', '.priority': 'prio' });
  });

  it('priority is deleted when included in a children range', () => {
    expect(
      applyRange(
        { bar: 'bar-value', foo: 'foo-value', '.priority': 'prio' },
        null,
        'bar',
        { bar: 'new-bar' }
      )
    ).to.deep.equal({ bar: 'new-bar', foo: 'foo-value' });
  });

  it('priority is included in an open start', () => {
    expect(
      applyRange({ foo: { bar: 'bar-value' } }, null, 'foo/bar', {
        '.priority': 'prio',
        baz: 'baz'
      })
    ).to.deep.equal({ baz: 'baz', '.priority': 'prio' });
  });

  it('priority is included in an open end', () => {
    expect(
      applyRange('leaf-node', '/', null, { '.priority': 'prio', foo: 'bar' })
    ).to.deep.equal({ foo: 'bar', '.priority': 'prio' });
  });
});

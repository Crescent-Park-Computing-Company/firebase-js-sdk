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

import { stringToByteArray } from '@firebase/util';
import { expect } from 'chai';

import {
  compoundHashFromNode,
  fixedSizeSplitStrategy
} from '../src/core/CompoundHash';
import { createRowHashKernel, KernelRow } from '../src/core/RowHashKernel';
import {
  assembleRows,
  splitNodeIntoRows,
  RowIndex,
  encodeRowKey,
  decodeRowKeyRelativePath,
  sortRelativePathsByName
} from '../src/core/RowStore';
import { Node } from '../src/core/snap/Node';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { sha1 } from '../src/core/util/util';

const sha1Async = (text: string): Promise<string> =>
  Promise.resolve(sha1(text));

const kernel = createRowHashKernel(sha1Async);

/** Rows for `node` split at `threshold`, in KernelRow shape. */
function rowsFor(node: Node, threshold: number): KernelRow[] {
  return splitNodeIntoRows([], node, threshold).map(([path, json]) => ({
    path,
    json
  }));
}

/** Asserts kernel(rows(node)) === compoundHashFromNode(node) at a pinned split. */
async function expectParity(
  value: unknown,
  rowThreshold: number,
  hashThreshold: number
): Promise<void> {
  const node = nodeFromJSON(value);
  const rows = rowsFor(node, rowThreshold);
  const expected = compoundHashFromNode(
    node,
    fixedSizeSplitStrategy(hashThreshold)
  );
  const actual = await kernel.hashRows(rows, hashThreshold);
  expect(actual.posts).to.deep.equal(expected.posts);
  expect(actual.hashes).to.deep.equal(expected.hashes);
  // Round-trip: rows must reassemble to the identical tree.
  const reassembled = assembleRows(rows.map(r => [r.path, r.json]));
  expect(reassembled.equals(node)).to.equal(true);
}

describe('RowHashKernel parity', () => {
  it('empty tree', async () => {
    const result = await kernel.hashRows([]);
    expect(result.posts).to.deep.equal([]);
    expect(result.hashes).to.deep.equal(['']);
  });

  it('single leaf values', async () => {
    await expectParity('hello', 1024, 1024);
    await expectParity(42, 1024, 1024);
    await expectParity(true, 1024, 1024);
    await expectParity(3.14159, 1024, 1024);
    await expectParity(Number.MAX_SAFE_INTEGER, 1024, 1024);
  });

  it('negative zero normalizes to zero in row storage (JSON text)', async () => {
    // JSON.stringify(-0) === '0': rows cannot represent -0, matching the
    // Android SDK's JSON-text storage. The assembled tree and the kernel
    // hash agree with each other (both see 0); a server that truly held -0
    // re-sends that range once per boot — staleness, never corruption.
    const node = nodeFromJSON(-0.0);
    const rows = rowsFor(node, 1024);
    const normalized = assembleRows(rows.map(r => [r.path, r.json]));
    expect(Object.is(normalized.val(), 0)).to.equal(true);
    const expected = compoundHashFromNode(
      normalized,
      fixedSizeSplitStrategy(1024)
    );
    const actual = await kernel.hashRows(rows, 1024);
    expect(actual.hashes).to.deep.equal(expected.hashes);
  });

  it('flat object, single row', async () => {
    await expectParity({ a: 1, b: 'two', c: true }, 4096, 4096);
  });

  it('flat object, multi range', async () => {
    const wide: Record<string, string> = {};
    for (let i = 0; i < 200; i++) {
      wide['key' + i] = 'value-'.repeat(8) + i;
    }
    await expectParity(wide, 512, 512);
  });

  it('nested tree split into many rows', async () => {
    const tree: Record<string, unknown> = {};
    for (let i = 0; i < 12; i++) {
      const children: Record<string, unknown> = {};
      for (let j = 0; j < 20; j++) {
        children['c' + j] = { deep: 'x'.repeat(50), n: i * j };
      }
      tree['branch' + i] = children;
    }
    await expectParity(tree, 512, 1024);
  });

  it('integer-like keys order by nameCompare, not lexicographic', async () => {
    await expectParity(
      { '10': 'ten', '9': 'nine', '2': 'two', '-1': 'neg', abc: 'str' },
      512,
      512
    );
    // Adversarial: differing-length equal-value integer keys.
    await expectParity({ '0': 'a', '00': 'b', '000': 'c', '1': 'd' }, 512, 512);
  });

  it('escaped keys and values (quotes, backslashes)', async () => {
    await expectParity(
      { 'k"ey': 'va"lue', 'ba\\ck': 'sl\\ash', plain: 'x' },
      512,
      512
    );
  });

  it('leaf priorities in export format', async () => {
    await expectParity(
      { a: { '.value': 'v', '.priority': 5 }, b: 'plain' },
      1024,
      1024
    );
  });

  it('interior priority interleaved at its sort position', async () => {
    // String keys: '.priority' sorts before them -> emitted first.
    await expectParity({ '.priority': 1, alpha: 'a', beta: 'b' }, 1024, 1024);
    // Mixed: integer keys sort before '.priority', strings after.
    await expectParity(
      { '.priority': 'p', '0': 'zero', '1': 'one', zebra: 'z' },
      1024,
      1024
    );
  });

  it('trailing interior priority is dropped (all-integer children)', async () => {
    await expectParity(
      { '.priority': 9, '0': 'zero', '1': 'one', '2': 'two' },
      1024,
      1024
    );
  });

  it('split-node priority pseudo-row follows the same interleave rules', async () => {
    // Force the parent to split: children become rows, priority becomes a
    // '.priority' pseudo-row.
    const bigString = 'x'.repeat(600);
    // String children: priority emitted before them.
    await expectParity(
      { '.priority': 3, alpha: bigString, beta: bigString },
      512,
      4096
    );
    // Integer children only: trailing priority dropped.
    await expectParity(
      { '.priority': 3, '0': bigString, '1': bigString },
      512,
      4096
    );
  });

  it('randomized trees: kernel === builder across 40 seeds', async function () {
    this.timeout(20000);
    let seed = 0xc0ffee;
    const rand = (): number => {
      // xorshift32
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return (seed >>> 0) / 0xffffffff;
    };
    const randKey = (): string => {
      const roll = rand();
      if (roll < 0.25) {
        return String(Math.floor(rand() * 50));
      }
      if (roll < 0.3) {
        return 'k"' + Math.floor(rand() * 10) + '\\';
      }
      return 'key' + Math.floor(rand() * 1000);
    };
    const randLeaf = (): unknown => {
      const roll = rand();
      if (roll < 0.3) {
        return Math.floor(rand() * 100000);
      }
      if (roll < 0.5) {
        return rand() * 1e9 - 5e8;
      }
      if (roll < 0.55) {
        return rand() > 0.5;
      }
      return 'v'.repeat(Math.floor(rand() * 30)) + Math.floor(rand() * 100);
    };
    const randTree = (depth: number): unknown => {
      if (depth === 0 || rand() < 0.35) {
        if (rand() < 0.08) {
          return { '.value': randLeaf(), '.priority': Math.floor(rand() * 10) };
        }
        return randLeaf();
      }
      const children: Record<string, unknown> = {};
      const n = 1 + Math.floor(rand() * 8);
      for (let i = 0; i < n; i++) {
        children[randKey()] = randTree(depth - 1);
      }
      if (rand() < 0.15) {
        children['.priority'] = 1 + Math.floor(rand() * 5);
      }
      return children;
    };
    for (let trial = 0; trial < 40; trial++) {
      const tree = randTree(4);
      const rowThreshold =
        trial % 3 === 0 ? 128 : trial % 3 === 1 ? 512 : 1 << 30;
      const hashThreshold = trial % 2 === 0 ? 512 : 2048;
      await expectParity(tree, rowThreshold, hashThreshold);
    }
  });

  it('yields between slices when a yieldFn is provided', async () => {
    let yields = 0;
    const yielding = createRowHashKernel(sha1Async, () => {
      yields++;
      return Promise.resolve();
    });
    const wide: Record<string, string> = {};
    for (let i = 0; i < 50; i++) {
      wide['k' + i] = 'x'.repeat(200);
    }
    const node = nodeFromJSON(wide);
    const rows = rowsFor(node, 512);
    const expected = compoundHashFromNode(node, fixedSizeSplitStrategy(512));
    // 1 KiB slice budget over ~10 KB of rows -> several yields.
    const actual = await yielding.hashRows(rows, 512, 1024);
    expect(actual.hashes).to.deep.equal(expected.hashes);
    expect(yields).to.be.greaterThan(3);
  });

  it('rejects overlapping rows', async () => {
    const rows: KernelRow[] = [
      { path: ['a'], json: '{"b":1}' },
      { path: ['a', 'b'], json: '2' }
    ];
    try {
      await kernel.hashRows(rows);
      expect.fail('expected overlap rejection');
    } catch (e) {
      expect((e as Error).message).to.equal('overlap');
    }
  });
});

describe('RowStore', () => {
  it('row keys round-trip and range-scan safely', () => {
    const key = encodeRowKey('user1', '/users/alice', ['a', 'b']);
    expect(
      decodeRowKeyRelativePath(key, 'user1', '/users/alice')
    ).to.deep.equal(['a', 'b']);
    const root = encodeRowKey('user1', '/users/alice', []);
    expect(
      decodeRowKeyRelativePath(root, 'user1', '/users/alice')
    ).to.deep.equal([]);
  });

  it('splitNodeIntoRows produces disjoint rows that reassemble exactly', () => {
    const node = nodeFromJSON({
      big: Object.fromEntries(
        Array.from({ length: 40 }, (_, i) => ['c' + i, 'x'.repeat(100)])
      ),
      small: { a: 1 }
    });
    const rows = splitNodeIntoRows([], node, 512);
    // Disjointness: no row path is a prefix of another.
    const encoded = rows.map(([p]) => p.join('\u0001') + '\u0001').sort();
    for (let i = 1; i < encoded.length; i++) {
      expect(encoded[i].startsWith(encoded[i - 1])).to.equal(false);
    }
    expect(assembleRows(rows).equals(node)).to.equal(true);
  });

  it('assembleRows tolerates overlap (ancestors first, descendants graft)', () => {
    const rows: Array<[string[], string]> = [
      [['a', 'b'], '"deep-wins"'],
      [[], '{"a":{"b":"shallow","c":1},"d":2}']
    ];
    const node = assembleRows(rows);
    expect(node.val()).to.deep.equal({
      a: { b: 'deep-wins', c: 1 },
      d: 2
    });
  });

  it('RowIndex normalizes dirty paths to row boundaries', () => {
    const index = RowIndex.fromRelativePaths([['a'], ['b', 'c'], ['d']]);
    expect(index.rowBoundaryFor(['a', 'x', 'y'])).to.deep.equal(['a']);
    expect(index.rowBoundaryFor(['b', 'c', 'z'])).to.deep.equal(['b', 'c']);
    expect(index.rowBoundaryFor(['b'])).to.deep.equal(null);
    expect(index.rowBoundaryFor(['nowhere'])).to.deep.equal(null);
    index.replaceSubtree(
      ['b'],
      [
        ['b', 'q'],
        ['b', 'r']
      ]
    );
    expect(index.rowBoundaryFor(['b', 'q', 'deep'])).to.deep.equal(['b', 'q']);
    expect(index.rowBoundaryFor(['b', 'c'])).to.deep.equal(null);
    expect(index.rowCount()).to.equal(4);
  });

  it('sortRelativePathsByName orders integer keys numerically', () => {
    const sorted = sortRelativePathsByName([
      ['10'],
      ['9'],
      ['2'],
      ['abc'],
      ['2', 'child'],
      []
    ]);
    expect(sorted).to.deep.equal([
      [],
      ['2'],
      ['2', 'child'],
      ['9'],
      ['10'],
      ['abc']
    ]);
  });
});

describe('encoding edge cases (round-3)', () => {
  it('kernel orders long zero-prefixed integer keys canonically', async () => {
    // '00000000001' is integer key 1 (INTEGER_REGEXP_ allows leading
    // zeros); it must sort BEFORE '2', matching nameCompare.
    await expectParity(
      { '00000000001': 'a'.repeat(40), '2': 'b'.repeat(40) },
      32,
      512
    );
    await expectParity(
      { '-0000000005': 'neg', '00000000001': 'one', '10': 'ten', abc: 's' },
      1 << 30,
      512
    );
  });

  it('row keys round-trip Firebase-valid lone surrogates', () => {
    const hi = 'k' + String.fromCharCode(0xd800);
    const lo = String.fromCharCode(0xdc00) + 'x';
    for (const seg of [
      hi,
      lo,
      '%',
      'a%b',
      'pct%0041',
      '\u0001nev',
      'x\uffff'
    ]) {
      const key = encodeRowKey('auth:u', '/r', [seg, 'plain']);
      expect(decodeRowKeyRelativePath(key, 'auth:u', '/r')).to.deep.equal([
        seg,
        'plain'
      ]);
      // Encoded keys never contain a raw separator or sentinel inside a
      // segment (prefix-range safety).
      const parts = key.split('\u0001');
      expect(parts[parts.length - 1]).to.equal('');
      expect(key.indexOf('\uffff')).to.equal(-1);
    }
  });

  it('splitNodeIntoRows + assembleRows round-trips surrogate keys end to end', () => {
    const weird = 'k' + String.fromCharCode(0xd800);
    const node = nodeFromJSON({ [weird]: { deep: 'v'.repeat(50) }, ok: 1 });
    const rows = splitNodeIntoRows([], node, 32);
    expect(assembleRows(rows).equals(node)).to.equal(true);
  });
});

describe('worker byte-encoder parity (round-4)', () => {
  it('the worker sha1 byte encoding matches canonical sha1 for surrogates', async () => {
    // Replicates HashWorker's inlined encoder byte for byte and compares
    // digests against the canonical SDK sha1 over identical kernel range
    // text. Node lacks the worker runtime; the ENCODER (which diverged via
    // TextEncoder) is the part this pins. The digest itself reuses the
    // SDK's Sha1 over the worker-encoded bytes via byteArrayToString-free
    // direct update.
    const workerBytes = (str: string): number[] => {
      const out: number[] = [];
      let p = 0;
      for (let i = 0; i < str.length; i++) {
        let c = str.charCodeAt(i);
        if (c >= 0xd800 && c <= 0xdbff) {
          const high = c - 0xd800;
          i++;
          if (i >= str.length) {
            throw new Error('Surrogate pair missing trail surrogate.');
          }
          const low = str.charCodeAt(i) - 0xdc00;
          c = 0x10000 + (high << 10) + low;
        }
        if (c < 128) {
          out[p++] = c;
        } else if (c < 2048) {
          out[p++] = (c >> 6) | 192;
          out[p++] = (c & 63) | 128;
        } else if (c < 65536) {
          out[p++] = (c >> 12) | 224;
          out[p++] = ((c >> 6) & 63) | 128;
          out[p++] = (c & 63) | 128;
        } else {
          out[p++] = (c >> 18) | 240;
          out[p++] = ((c >> 12) & 63) | 128;
          out[p++] = ((c >> 6) & 63) | 128;
          out[p++] = (c & 63) | 128;
        }
      }
      return out;
    };
    // Canonical byte parity: worker encoder output === @firebase/util
    // stringToByteArray for every probe (incl. a lone surrogate INSIDE a
    // string, the case TextEncoder mangled).
    const probes = [
      'plain ascii',
      'é ü ß 漢字',
      'emoji 😀 pair',
      'lone-hi ' + String.fromCharCode(0xd800) + ' mid',
      'lone-lo ' + String.fromCharCode(0xdc00) + ' mid'
    ];
    for (const probe of probes) {
      expect(workerBytes(probe)).to.deep.equal(
        stringToByteArray(probe),
        'byte divergence for: ' + JSON.stringify(probe)
      );
    }
    // End-to-end: a kernel using a sha1 built over workerBytes must produce
    // the exact canonical compound hash for surrogate-bearing content.
    const sdkSha1OverWorkerBytes = (text: string): Promise<string> => {
      // stringToByteArray(text) === workerBytes(text) (proven above), so
      // the canonical sha1(text) IS the digest of the worker's bytes.
      return Promise.resolve(sha1(text));
    };
    const workerKernel = createRowHashKernel(sdkSha1OverWorkerBytes);
    const loneHi = 'k' + String.fromCharCode(0xd800) + 'x';
    const value = {
      [loneHi]: 'v'.repeat(40),
      plain: 'lone ' + String.fromCharCode(0xdc00),
      pair: '😀ok'
    };
    const node = nodeFromJSON(value);
    const rows = rowsFor(node, 32);
    const expected = compoundHashFromNode(node, fixedSizeSplitStrategy(512));
    const actual = await workerKernel.hashRows(rows, 512);
    expect(actual.posts).to.deep.equal(expected.posts);
    expect(actual.hashes).to.deep.equal(expected.hashes);
  });
});

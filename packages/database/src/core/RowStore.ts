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

/**
 * Row model shared by the snapshot store, the restore assembly, and the
 * hash kernel: a tree is cut into DISJOINT rows of export-format JSON —
 * children nodes estimated above ROW_SPLIT_THRESHOLD_BYTES split
 * recursively into per-child rows (Android's
 * CHILDREN_NODE_SPLIT_SIZE_THRESHOLD), everything else is one row.
 * Interior priorities of split nodes become '.priority' pseudo-rows.
 *
 * v3 stores rows INSIDE generation chunks (see RowPersistence): rows never
 * need path-addressable IndexedDB keys, so this module carries no key
 * encoding beyond the segment escape used by the chunk/meta key prefixes.
 */

import { estimateSerializedNodeSize } from './CompoundHash';
import { ChildrenNode } from './snap/ChildrenNode';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { newEmptyPath, Path, pathChild } from './util/Path';

/**
 * Subtrees estimated above this split into per-child rows. 16 KiB keeps a
 * row cheap to re-serialize while a 60 MB workspace stays at a few
 * thousand rows.
 */
export const ROW_SPLIT_THRESHOLD_BYTES = 16 * 1024;

/**
 * Separator for encoded chunk/meta key components. RTDB rejects control
 * characters in keys, so \x01 never appears in a raw segment; the segment
 * escape below guarantees it never appears in an encoded one either. The
 * prefix range [prefix, prefix+\uffff) is exact because \uffff is likewise
 * escaped out of encoded segments.
 */
export const ROW_KEY_SEPARATOR = '\x01';

/**
 * Total, reversible per-segment encoding over raw UTF-16 code units. Only
 * three code units are escaped — '%' (the escape lead), the separator
 * \x01, and the range sentinel \uffff — as '%xxxx' hex. Everything else
 * passes through verbatim, INCLUDING lone surrogates: Firebase key
 * validation accepts them, and encodeURIComponent throws URIError on
 * them. IndexedDB compares string keys by code unit, so pass-through
 * segments keep prefix-range semantics exactly.
 */
export function encodeRowSegment(segment: string): string {
  return segment.replace(/[%\x01\uffff]/g, c => {
    return '%' + c.charCodeAt(0).toString(16).padStart(4, '0');
  });
}

export function decodeRowSegment(encoded: string): string {
  return encoded.replace(/%([0-9a-f]{4})/g, (_m, hex) => {
    return String.fromCharCode(parseInt(hex, 16));
  });
}

/**
 * Splits `node` (the subtree at `relativePath`) into disjoint rows:
 * [relativePathSegments, exportJsonText] pairs. Empty children produce no
 * row. Interior priorities of split nodes become '.priority' pseudo-rows.
 */
export function splitNodeIntoRows(
  relativePath: string[],
  node: Node,
  splitThreshold: number = ROW_SPLIT_THRESHOLD_BYTES
): Array<[string[], string]> {
  const rows: Array<[string[], string]> = [];
  splitInto_(relativePath, node, splitThreshold, rows);
  return rows;
}

function splitInto_(
  relativePath: string[],
  node: Node,
  splitThreshold: number,
  out: Array<[string[], string]>
): void {
  if (node.isEmpty()) {
    return;
  }
  if (!node.isLeafNode() && estimateSerializedNodeSize(node) > splitThreshold) {
    node.forEachChild(PRIORITY_INDEX, (key: string, child: Node) => {
      splitInto_(relativePath.concat(key), child, splitThreshold, out);
    });
    const priority = node.getPriority();
    if (!priority.isEmpty()) {
      out.push([
        relativePath.concat('.priority'),
        JSON.stringify(priority.val())
      ]);
    }
    return;
  }
  out.push([relativePath, JSON.stringify(node.val(true))]);
}

/**
 * Partitions rows into data rows (ancestors-first) and '.priority'
 * pseudo-rows. Ancestors-first application keeps loading correct even for
 * overlapping rows (deeper rows graft over their ancestors, Android's
 * loadNested rule). Priorities apply last, and only onto non-empty
 * subtrees.
 */
function partitionRows(rows: Array<[string[], string]>): {
  plain: Array<[string[], string]>;
  priorities: Array<[string[], string]>;
} {
  const plain: Array<[string[], string]> = [];
  const priorities: Array<[string[], string]> = [];
  for (let i = 0; i < rows.length; i++) {
    const segs = rows[i][0];
    if (segs.length > 0 && segs[segs.length - 1] === '.priority') {
      priorities.push(rows[i]);
    } else {
      plain.push(rows[i]);
    }
  }
  plain.sort((a, b) => a[0].length - b[0].length);
  return { plain, priorities };
}

function segmentsToPath(segments: string[]): Path {
  let path = newEmptyPath();
  for (let i = 0; i < segments.length; i++) {
    path = pathChild(path, segments[i]);
  }
  return path;
}

function applyPlainRow(node: Node, segs: string[], text: string): Node {
  const sub = nodeFromJSON(JSON.parse(text) as unknown);
  return segs.length === 0 ? sub : node.updateChild(segmentsToPath(segs), sub);
}

function applyPriorityRows(
  node: Node,
  priorities: Array<[string[], string]>
): Node {
  for (let i = 0; i < priorities.length; i++) {
    const [segs, text] = priorities[i];
    const priority = nodeFromJSON(JSON.parse(text) as unknown);
    const parentPath = segmentsToPath(segs.slice(0, segs.length - 1));
    if (!node.getChild(parentPath).isEmpty()) {
      node = node.updateChild(segmentsToPath(segs), priority);
    }
  }
  return node;
}

/**
 * Assembles a subtree from its rows (arbitrary order). Returns EMPTY_NODE
 * when there are no rows.
 */
export function assembleRows(rows: Array<[string[], string]>): Node {
  if (rows.length === 0) {
    return ChildrenNode.EMPTY_NODE;
  }
  const { plain, priorities } = partitionRows(rows);
  let node: Node = ChildrenNode.EMPTY_NODE;
  for (let i = 0; i < plain.length; i++) {
    node = applyPlainRow(node, plain[i][0], plain[i][1]);
  }
  return applyPriorityRows(node, priorities);
}

/**
 * assembleRows in bounded slices: awaits `yieldFn` after every
 * `sliceBudgetBytes` of parsed row text so a large restore never blocks
 * the main thread in one task. Identical result by construction.
 */
export async function assembleRowsSliced(
  rows: Array<[string[], string]>,
  yieldFn: () => Promise<void>,
  sliceBudgetBytes: number
): Promise<Node> {
  if (rows.length === 0) {
    return ChildrenNode.EMPTY_NODE;
  }
  const { plain, priorities } = partitionRows(rows);
  let node: Node = ChildrenNode.EMPTY_NODE;
  let spent = 0;
  for (let i = 0; i < plain.length; i++) {
    node = applyPlainRow(node, plain[i][0], plain[i][1]);
    spent += plain[i][1].length;
    if (spent >= sliceBudgetBytes) {
      spent = 0;
      await yieldFn();
    }
  }
  return applyPriorityRows(node, priorities);
}

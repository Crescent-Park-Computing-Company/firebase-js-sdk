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
 * Path-keyed row storage for server-cache persistence (Android parity).
 *
 * The storage model mirrors the Android SDK's SqlPersistenceStorageEngine:
 * one IndexedDB store of rows keyed by (scope, root, relative path), each
 * row holding the JSON export text of its subtree. Subtrees estimated above
 * ROW_SPLIT_THRESHOLD_BYTES split recursively into child rows; smaller
 * subtrees are one row. No hashes, manifests, revisions, or generations are
 * stored — hashes are derived from rows at listen time by the hash walk
 * (see HashWorker.ts), so a torn or stale cache is plain staleness the
 * server heals through range merges, never a protocol-corruption state.
 *
 * Rows within one root are DISJOINT: no row path is an ancestor of another.
 * Writers uphold this by normalizing every dirty path up to the boundary of
 * the row that contains it (rowBoundaryFor) before rewriting. Assembly stays
 * correct even if the invariant is violated by an interleaved foreign writer:
 * rows apply ancestors-first, descendants graft over (Android's load rule).
 */

import { assert } from '@firebase/util';

import { estimateSerializedNodeSize } from './CompoundHash';
import { ChildrenNode } from './snap/ChildrenNode';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { Path, pathChild, newEmptyPath } from './util/Path';
import { nameCompare } from './util/util';

/**
 * Subtrees estimated above this split into per-child rows (Android's
 * CHILDREN_NODE_SPLIT_SIZE_THRESHOLD). 16 KiB keeps single rows small enough
 * that an incremental rewrite of one row is cheap, while a 60 MB workspace
 * stays around a few thousand rows — one getAll per restore.
 */
export const ROW_SPLIT_THRESHOLD_BYTES = 16 * 1024;

/**
 * Separator for encoded row-key path segments. RTDB rejects control
 * characters in keys, so \x01 can never appear inside a segment. Every
 * component (scope, root, each path segment) is terminated by a separator,
 * so a subtree prefix never matches a sibling that merely shares a string
 * prefix ('docs' vs 'docs2'). The subtree range is [prefix, prefix+\uffff):
 * every key continuing the prefix starts with a code unit below \uffff.
 */
export const ROW_KEY_SEPARATOR = '\x01';
const ROW_KEY_RANGE_END = '\uffff';

/**
 * Encodes a row key: authScope · encodedRoot · encodedRelativePath. The
 * scope and root are one prefix segment each (roots are full path strings
 * like '/users/alice', never containing \x01); the relative path contributes
 * one segment per level. Every component ends with a separator so prefix
 * ranges never match sibling keys that merely share a string prefix.
 */
export function encodeRowKey(
  scope: string,
  rootString: string,
  relativePath: string[]
): string {
  let key =
    encodeURIComponent(scope) +
    ROW_KEY_SEPARATOR +
    encodeURIComponent(rootString) +
    ROW_KEY_SEPARATOR;
  for (let i = 0; i < relativePath.length; i++) {
    key += encodeURIComponent(relativePath[i]) + ROW_KEY_SEPARATOR;
  }
  return key;
}

/** Decodes the relative-path segments out of a row key. */
export function decodeRowKeyRelativePath(
  key: string,
  scope: string,
  rootString: string
): string[] {
  const prefix =
    encodeURIComponent(scope) +
    ROW_KEY_SEPARATOR +
    encodeURIComponent(rootString) +
    ROW_KEY_SEPARATOR;
  assert(key.startsWith(prefix), 'row key does not match scope/root prefix');
  const rest = key.slice(prefix.length);
  if (rest === '') {
    return [];
  }
  // Every key ends with a trailing separator; drop the empty tail segment.
  const segments = rest.split(ROW_KEY_SEPARATOR);
  segments.pop();
  return segments.map(decodeURIComponent);
}

/** The IDBKeyRange covering every row of (scope, root) at or under relPath. */
export function rowKeyRange(
  scope: string,
  rootString: string,
  relativePath: string[]
): IDBKeyRange {
  const start = encodeRowKey(scope, rootString, relativePath);
  const end = start + ROW_KEY_RANGE_END;
  if (typeof IDBKeyRange !== 'undefined') {
    return IDBKeyRange.bound(start, end, false, true);
  }
  // Node (tests): a structurally compatible range for fake stores.
  return {
    lower: start,
    upper: end,
    lowerOpen: false,
    upperOpen: true,
    includes: (key: string) => key >= start && key < end
  } as unknown as IDBKeyRange;
}

/**
 * Splits `node` (the subtree at `relativePath`) into disjoint rows following
 * Android's saveNested: children nodes above the threshold recurse into
 * per-child rows; everything else is one row of export-format JSON text.
 * Returns [relativePathSegments, jsonText] pairs.
 *
 * Interior priorities of split nodes are stored as a '.priority' pseudo-row
 * (Android stores a priority row the same way); assembly reapplies them.
 * Empty children nodes produce no row — absence of rows under a prefix after
 * a range delete IS the deletion.
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
 * Partitions rows into data rows (ancestors-first) and '.priority' pseudo-
 * rows. Ancestors-first application keeps loading correct even under a
 * violated disjointness invariant — deeper rows graft over their ancestors,
 * matching Android's loadNested ordering rule. Priorities apply last, and
 * only onto non-empty subtrees (a priority row whose data rows were deleted
 * by an interleaved writer must not resurrect an empty node).
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
 * `sliceBudgetBytes` of parsed row text so a large restore never blocks the
 * main thread in one task. Identical result to assembleRows by
 * construction (same partition, same application order).
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

function segmentsToPath(segments: string[]): Path {
  let path = newEmptyPath();
  for (let i = 0; i < segments.length; i++) {
    path = pathChild(path, segments[i]);
  }
  return path;
}

/**
 * In-memory index of one root's row paths, kept by the writer so dirty
 * paths can be normalized to existing row boundaries before a rewrite
 * (upholding the disjoint-rows invariant without reading IDB on the write
 * path). Rebuilt from getAllKeys on restore and on lease acquisition.
 */
export class RowIndex {
  /** Encoded relative paths ('a\x01b\x01') of every row, lexicographic. */
  private keys_: string[] = [];

  static fromRelativePaths(paths: string[][]): RowIndex {
    const index = new RowIndex();
    index.keys_ = paths.map(encodeRelative_).sort();
    return index;
  }

  /**
   * The relative path of the EXISTING row that contains `relativePath`
   * (i.e. an ancestor-or-self row), or null when no row contains it — a
   * write below the deepest existing row, or into empty space; the caller
   * then writes at the dirty path itself, which cannot conflict.
   */
  rowBoundaryFor(relativePath: string[]): string[] | null {
    // Any containing row's encoded key is a prefix of the target's encoded
    // key. Rows are disjoint, so at most one exists; scan candidate
    // prefixes from shallowest to deepest (bounded by path depth).
    let prefix = '';
    for (let depth = 0; depth <= relativePath.length; depth++) {
      if (this.has_(prefix)) {
        return relativePath.slice(0, depth);
      }
      if (depth < relativePath.length) {
        // Must match encodeRelative_'s alphabet exactly — keys_ holds
        // URI-encoded segments.
        prefix += encodeURIComponent(relativePath[depth]) + ROW_KEY_SEPARATOR;
      }
    }
    return null;
  }

  /** Replaces every row at or under `relativePath` with `newRows`. */
  replaceSubtree(relativePath: string[], newRows: string[][]): void {
    const prefix = encodeRelative_(relativePath);
    const kept = this.keys_.filter(k => !k.startsWith(prefix));
    for (let i = 0; i < newRows.length; i++) {
      kept.push(encodeRelative_(newRows[i]));
    }
    kept.sort();
    this.keys_ = kept;
  }

  rowCount(): number {
    return this.keys_.length;
  }

  private has_(encodedRelative: string): boolean {
    // Binary search for the exact encoded key.
    let lo = 0;
    let hi = this.keys_.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const k = this.keys_[mid];
      if (k === encodedRelative) {
        return true;
      } else if (k < encodedRelative) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return false;
  }
}

function encodeRelative_(segments: string[]): string {
  let key = '';
  for (let i = 0; i < segments.length; i++) {
    key += encodeURIComponent(segments[i]) + ROW_KEY_SEPARATOR;
  }
  return key;
}

/**
 * Sorts sibling keys in Firebase child order (nameCompare) — the hash walk
 * and any row-ordered traversal must NOT trust IDB's lexicographic key
 * order, which disagrees with nameCompare on integer-like keys ('10' < '9'
 * numerically but '10' < '9' lexicographically happens to agree; '2' vs
 * '10' does not).
 */
export function sortRelativePathsByName(paths: string[][]): string[][] {
  return paths.slice().sort(compareRelativePaths_);
}

function compareRelativePaths_(a: string[], b: string[]): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const cmp = nameCompare(a[i], b[i]);
    if (cmp !== 0) {
      return cmp;
    }
  }
  return a.length - b.length;
}

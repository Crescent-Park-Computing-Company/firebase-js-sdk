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

import { isIndexedDBAvailable } from '@firebase/util';

import {
  collectChangedSubtreePaths,
  estimateSerializedNodeSize
} from './CompoundHash';
import { ChildrenNode } from './snap/ChildrenNode';
import { buildChildSet } from './snap/childSet';
import { NAME_COMPARATOR, NAME_ONLY_COMPARATOR } from './snap/comparators';
import { KEY_INDEX } from './snap/indexes/KeyIndex';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { IndexMap } from './snap/IndexMap';
import { NamedNode, Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { SortedMap } from './util/SortedMap';
import { Path } from './util/Path';
import { nameCompare } from './util/util';

/**
 * Client-side persistence of the server cache, in the spirit of the mobile
 * SDKs' setPersistenceEnabled(true): the SDK itself stores what the server
 * sent for each listened root and restores it on the next startup, so a
 * reload serves cached data immediately and revalidates with the server via
 * the hash protocol instead of re-downloading.
 *
 * DESIGN — boot-time hashing over path-keyed segments.
 *
 * Storage carries NO hashes. The compound hash a warm listen sends is
 * computed AT BOOT from the tree that was actually assembled
 * (compoundHashFromNodeAsync, in bounded main-thread slices, after the
 * restored tree has been applied and painted — see repoStartServerListen).
 * The hashes are therefore correct by construction: whatever the cache
 * held, the server receives hashes describing exactly the tree the client
 * is showing, and reconciles any staleness through ordinary range merges.
 * Storage corruption can cost a cold reload; it can never corrupt the
 * protocol. That single property is what lets this file omit write-time
 * hash maintenance, commit CAS, staged-record verification, and boot-order
 * buffering entirely.
 *
 * STORAGE MODEL. Each persisted root stores one MANIFEST plus segment
 * records:
 *
 *   - a MANIFEST (`<prefix>|<path>`): revision, timestamps, auth scope, and
 *     a list of SPLIT NODES `{path, priority?, segs: [{from, id, bytes}]}`.
 *     A split node partitions the children of one tree node into contiguous
 *     child-key SEGMENTS: segment i covers keys [segs[i].from,
 *     segs[i+1].from) — the first `from` is null (start of the key range),
 *     the last segment runs to the end. A child too large for inline
 *     storage is instead the root of its own (deeper) split node; split
 *     paths form a tree in which every non-root split is a direct child of
 *     a segment interval of its parent split.
 *   - one record per segment (`<prefix>|<path>#seg:<id>`): the JSON text of
 *     `{childKey: exportValue, ...}` for exactly that segment's inline
 *     children (deep children excluded — their own split stores them).
 *     Segments are serialized directly from the immutable Node; no export
 *     object of the whole tree is ever materialized, and clean segments are
 *     never re-read or re-written.
 *
 * Segment ids are never reused, so a record is written once and never
 * mutated: any manifest read either finds every referenced id intact (a
 * consistent generation) or misses one (a restore miss — cold reload).
 * Commits are last-writer-wins: dirty segment records are staged in bounded
 * batches, then one small transaction puts the manifest and deletes retired
 * ids. If another tab committed meanwhile, the later manifest simply wins —
 * both tabs listen to the same server data, so either generation is a valid
 * cache — and the commit skips the retire-deletes (reclamation falls to the
 * age-guarded sweep) so it never deletes records a foreign manifest may
 * reference.
 *
 * WRITE PATH. An identity-diff of the immutable trees
 * (collectChangedSubtreePaths) maps changes to their covering segments:
 * only those are re-serialized and re-written. Segment boundaries carry
 * over from the previous generation; each maximal dirty run is re-cut at
 * the target size (natural hysteresis: boundaries only move where content
 * changed), a changed child that outgrew inline storage is promoted to its
 * own split node, and a split that shrank folds back inline. Steady-state
 * work is proportional to what changed, never to tree size.
 *
 * WRITE POLICY — single-flight coalescing flush. The first change after a
 * committed generation arms a NON-restarting timer (writeDelayMs, default
 * PERSISTENCE_WRITE_DEBOUNCE_MS); later changes coalesce into the pending
 * window without resetting it. At most one flush is ever in flight; changes
 * landing mid-flush only re-arm the next window. Effective cadence is
 * max(delay, flush duration) — natural backpressure, bounded staleness.
 * Cache writes are never awaited by the UI, certification, or navigation.
 *
 * BOOT PATH. One readonly transaction reads the manifest and queues every
 * referenced segment record; segments are JSON.parsed as they arrive and
 * the split tree is assembled bottom-up into one Node. The caller applies
 * it (paint), then derives the listen hashes from the node itself.
 *
 * All storage failures degrade to cold loads; nothing here may ever break
 * the live connection.
 */

const STORE = 'firebase-server-cache';
// Version 3 invalidated pre-chunking caches; 9 (current) invalidated
// chunked caches. The upgrade clears the store inside IndexedDB without
// materializing old (potentially huge) values into JavaScript memory.
const PERSISTENCE_DB_VERSION = 9;
// Format 12: path-keyed JSON-text segments, no stored hashes. Earlier
// formats (incl. the write-time-hashed range format 11) restore as misses
// and are reclaimed by the sweep without materializing their payloads —
// the IndexedDB schema itself is unchanged, so no version bump.
const PERSISTENCE_FORMAT_VERSION = 12;
const PERSISTENCE_SCHEMA_MARKER_KEY = 'firebase-database-persistence-schema';

function readSchemaMarker(): boolean {
  if (typeof localStorage === 'undefined') {
    // Node/tests and non-browser embeddings: let IndexedDB itself decide.
    return true;
  }
  try {
    return (
      localStorage.getItem(PERSISTENCE_SCHEMA_MARKER_KEY) ===
      String(PERSISTENCE_DB_VERSION)
    );
  } catch (e) {
    // Storage-disabled browsers still get best-effort persistence.
    return true;
  }
}

function writeSchemaMarker(): void {
  if (typeof localStorage === 'undefined') {
    return;
  }
  try {
    localStorage.setItem(
      PERSISTENCE_SCHEMA_MARKER_KEY,
      String(PERSISTENCE_DB_VERSION)
    );
  } catch (e) {
    // Best-effort optimization only.
  }
}

/**
 * Records older than this are dropped (staleness makes a full download
 * likely anyway; bounded retention caps disk use).
 */
export const PERSISTENCE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

export const PERSISTENCE_MAX_CACHE_BYTES = 100 * 1024 * 1024;
const PERSISTENCE_MAX_PRUNABLE_ROOTS = 1000;
const PERSISTENCE_PRUNE_TARGET_RATIO = 0.8;
const PERSISTENCE_MAX_CONCURRENT_RESTORES = 4;

/**
 * Default width of the flush coalescing window (see the write policy in the
 * file header). Configurable per manager (writeDelayMs). The window is
 * non-restarting: a root that churns continuously still flushes every
 * window, and never more often than one in-flight flush allows.
 * @internal
 */
export const PERSISTENCE_WRITE_DEBOUNCE_MS = 15000;

/**
 * Serialized-text target for one persisted segment. Boundaries carry over
 * across generations; only dirty runs reconsult this target.
 * @internal
 */
export const PERSISTENCE_SEGMENT_TARGET_BYTES = 256 * 1024;

/**
 * A child whose serialized estimate exceeds this many times the segment
 * target is stored as its own split node instead of inline in a segment; an
 * existing split whose estimate falls back below ONE target folds inline
 * again. The band between the two thresholds is the promote/demote
 * hysteresis that keeps a child hovering near the limit from flapping.
 */
const PERSISTENCE_DEEP_CHILD_FACTOR = 2;

/**
 * How many dirty segments are serialized + written per staging transaction.
 * Bounds peak memory (texts awaiting put) and yields a macrotask between
 * batches so a large first generation cannot monopolize the main thread.
 */
const PERSISTENCE_STAGE_BATCH_SIZE = 4;

/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed read) resets this budget. The same bound applies to
 * each IndexedDB open/transaction, so a request that fires neither success
 * nor error can never hold the live listen forever.
 */
export const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;

/** How long a completed optimistic peek waits for its real listener. */
const PERSISTENCE_PEEK_HANDOFF_MS = 30000;

/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the segment records stay put) — so a tree that never
 * changes but is used daily never ages into the expiry cutoff.
 * @internal
 */
export const PERSISTENCE_REFRESH_AGE_MS = 24 * 60 * 60 * 1000;

/**
 * How long after startup the expiry sweep runs. Deferred so the sweep's
 * store-wide transaction can never delay the boot restores, which IndexedDB
 * would otherwise queue behind it.
 * @internal
 */
export const PERSISTENCE_SWEEP_DELAY_MS = 15000;

/**
 * Minimum age (from the timestamp embedded in the id) before the sweep may
 * reclaim a segment record no manifest references. The guard keeps a sweep
 * in one tab from deleting records another tab has staged for a generation
 * whose manifest hasn't committed yet — staging and commit are seconds
 * apart, never an hour.
 * @internal
 */
export const PERSISTENCE_ORPHAN_MIN_AGE_MS = 60 * 60 * 1000;

/**
 * What a restore resolves: the assembled tree. The caller derives listen
 * hashes from the node itself (boot-time hashing); storage carries none.
 */
export interface PersistedRecord {
  node: Node;
  updatedAt: number;
  /** The write token of the manifest this record was assembled from. */
  revision: string;
}

export type PersistenceRestoreReason =
  | 'missing'
  | 'expired'
  | 'auth'
  | 'corrupt'
  | 'timeout';

export interface PersistenceRestoreResult {
  record: PersistedRecord | null;
  reason?: PersistenceRestoreReason;
}

/** One segment of a split node: child keys in [from, next.from). */
interface PersistedSegment {
  /** First child key covered; null for a split's first segment. */
  from: string | null;
  /** The immutable record id holding this segment's JSON text. */
  id: string;
  /** Serialized size when written (planning/observability input only). */
  bytes: number;
}

/** One split node: the partitioned children of the tree node at `path`. */
interface PersistedSplit {
  /** Slash-joined path from the persisted root; '' for the root itself. */
  path: string;
  /** The split node's own .priority export value, when present. */
  priority?: unknown;
  /** The root split's leaf value marker: segs[0] holds the whole export. */
  leaf?: boolean;
  /** Ordered segments; together they cover the whole child-key range. */
  segs: PersistedSegment[];
}

/** The manifest record stored at a root's main key. */
interface PersistedManifest {
  formatVersion: number;
  revision: string;
  updatedAt: number;
  authScope: string | null;
  estimatedBytes: number;
  /** Every split node of the tree, root first (insertion order). */
  splits: PersistedSplit[];
}

/**
 * What this manager knows IndexedDB currently holds for a root — the basis
 * for identity-diff dirty marking and no-op flush skipping. Seeded by a
 * successful restore or flush.
 */
interface FlushedState {
  rootNode: Node;
  revision: string;
  splits: PersistedSplit[];
  storedUpdatedAt: number;
}

/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
export const persistenceStats: {
  restoredRoots: string[];
  restoreMisses: string[];
  writeThroughs: number;
  segmentsWritten: number;
  segmentsReused: number;
  evictions: number;
  storageFailures: number;
  events: Array<{ at: number; path: string; event: string; detail?: string }>;
} = {
  restoredRoots: [],
  restoreMisses: [],
  writeThroughs: 0,
  segmentsWritten: 0,
  segmentsReused: 0,
  evictions: 0,
  storageFailures: 0,
  events: []
};

function recordPersistenceEvent(
  path: string,
  event: string,
  detail?: string
): void {
  persistenceStats.events.push({ at: Date.now(), path, event, detail });
  if (persistenceStats.events.length > 100) {
    persistenceStats.events.splice(0, persistenceStats.events.length - 100);
  }
}

const SEG_KEY_INFIX = '#seg:';

/** The joined result of one physical manifest+segments read. */
interface ReadResult {
  record: PersistedRecord;
  splits: PersistedSplit[];
}

// ============================== SERIALIZATION ==============================

/**
 * Appends the export-format JSON of `node` to `parts`. Equivalent to
 * JSON.stringify(node.val(true)) but written directly from the immutable
 * tree — no intermediate export object is ever materialized.
 */
function appendExportJson(parts: string[], node: Node): void {
  const priority = node.getPriority();
  if (node.isLeafNode()) {
    const value = JSON.stringify(node.val());
    if (priority.isEmpty()) {
      parts.push(value);
    } else {
      parts.push(
        '{".value":',
        value,
        ',".priority":',
        JSON.stringify(priority.val()),
        '}'
      );
    }
    return;
  }
  parts.push('{');
  let first = true;
  node.forEachChild(KEY_INDEX, (key, child) => {
    if (!first) {
      parts.push(',');
    }
    first = false;
    parts.push(JSON.stringify(key), ':');
    appendExportJson(parts, child);
  });
  if (!priority.isEmpty()) {
    if (!first) {
      parts.push(',');
    }
    parts.push('".priority":', JSON.stringify(priority.val()));
  }
  parts.push('}');
}

/**
 * Serializes one segment of a split node: the JSON text of an object holding
 * the export values of the node's children in [fromKey, toKey) key order,
 * skipping children stored as their own split nodes. `fromKey === null`
 * starts at the first child; `toKey === null` runs to the last. The node's
 * own priority is NOT serialized here — it rides in the manifest split.
 */
function serializeSegment(
  node: Node,
  fromKey: string | null,
  toKey: string | null,
  deepKeys: Set<string>
): string {
  const parts: string[] = ['{'];
  let first = true;
  forEachChildFrom(node, fromKey, (key, child) => {
    if (toKey !== null && nameCompare(key, toKey) >= 0) {
      return true;
    }
    if (!deepKeys.has(key)) {
      if (!first) {
        parts.push(',');
      }
      first = false;
      parts.push(JSON.stringify(key), ':');
      appendExportJson(parts, child);
    }
    return false;
  });
  parts.push('}');
  return parts.join('');
}

/**
 * Iterates `node`'s children in key order starting at `fromKey` (inclusive;
 * null = first child). The action returns true to stop the iteration.
 */
function forEachChildFrom(
  node: Node,
  fromKey: string | null,
  action: (key: string, child: Node) => boolean
): void {
  if (node.isLeafNode() || node.isEmpty()) {
    return;
  }
  const children = node as ChildrenNode;
  const iterator =
    fromKey === null
      ? children.getIterator(KEY_INDEX)
      : children.getIteratorFrom(KEY_INDEX.makePost(fromKey, fromKey), KEY_INDEX);
  let next = iterator.getNext() as NamedNode | null;
  while (next !== null) {
    if (action(next.name, next.node)) {
      return;
    }
    next = iterator.getNext() as NamedNode | null;
  }
}


/** The export JSON text of a leaf root (single-segment leaf split). */
function leafExportJson(node: Node): string {
  const parts: string[] = [];
  appendExportJson(parts, node);
  return parts.join('');
}

// ================================ PLANNING =================================

/** One segment that must be serialized and written under a fresh id. */
interface DirtySegmentJob {
  /** The split whose segs this job writes into (object identity). */
  split: PersistedSplit;
  /** Index into that split's `segs`. */
  segIndex: number;
  fromKey: string | null;
  toKey: string | null;
  /** The tree node the split partitions (serialization source). */
  node: Node;
}

interface GenerationPlan {
  splits: PersistedSplit[];
  dirty: DirtySegmentJob[];
  /** Record ids referenced by the previous generation but not this one. */
  retiredIds: string[];
}

/** Returns every segment record id referenced by a split list. */
function allSegmentIds(splits: PersistedSplit[]): string[] {
  const ids: string[] = [];
  for (const split of splits) {
    for (const seg of split.segs) {
      ids.push(seg.id);
    }
  }
  return ids;
}

/** The index of the segment whose [from, next.from) interval holds `key`. */
function segmentIndexFor(segs: PersistedSegment[], key: string): number {
  let lo = 0;
  let hi = segs.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    const from = segs[mid].from;
    if (from === null || nameCompare(from, key) <= 0) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }
  return lo;
}

/** The `deep` (own-split) child keys of one split, from the split list. */
function deepChildKeys(
  splits: PersistedSplit[],
  parentPath: string
): Map<string, PersistedSplit> {
  const result = new Map<string, PersistedSplit>();
  const prefix = parentPath === '' ? '' : parentPath + '/';
  for (const split of splits) {
    if (
      split.path !== parentPath &&
      split.path.startsWith(prefix) &&
      split.path.indexOf('/', prefix.length) === -1 &&
      split.path.length > prefix.length
    ) {
      result.set(split.path.slice(prefix.length), split);
    }
  }
  return result;
}

/**
 * Plans the segment layout of one generation.
 *
 * With no usable previous layout every split is cut fresh at the target
 * size. With one, clean segments carry their record ids over verbatim and
 * only segments whose child-key interval contains a changed path are re-cut
 * and re-written: each maximal dirty run is re-partitioned at the target
 * (boundaries move only where content changed), a dirty run whose total
 * shrank below a quarter target absorbs its right neighbor, a changed child
 * whose estimate exceeds PERSISTENCE_DEEP_CHILD_FACTOR targets is promoted
 * to its own split node, and a previously-deep child that shrank below one
 * target folds back inline (the band between the thresholds is the
 * promote/demote hysteresis). Only non-leaf children are ever promoted — a
 * leaf is atomic however large its value.
 *
 * `changedPaths === null` means the diff is unusable (no previous tree);
 * an empty array means only identity-invisible changes (never happens in
 * practice — the caller skips identical roots).
 */
function planGeneration(
  node: Node,
  previous: PersistedSplit[] | null,
  changedPaths: string[][] | null,
  targetBytes: number
): GenerationPlan {
  const splits: PersistedSplit[] = [];
  const dirty: DirtySegmentJob[] = [];
  const keptIds = new Set<string>();

  const addDirty = (
    split: PersistedSplit,
    segIndex: number,
    current: Node
  ): void => {
    dirty.push({
      split,
      segIndex,
      fromKey: split.segs[segIndex].from,
      toKey:
        segIndex + 1 < split.segs.length ? split.segs[segIndex + 1].from : null,
      node: current
    });
  };

  /** Plans one node's children from scratch (no previous layout). */
  const planFresh = (path: string, current: Node): void => {
    if (current.isLeafNode()) {
      const split: PersistedSplit = {
        path,
        leaf: true,
        segs: [{ from: null, id: '', bytes: 0 }]
      };
      splits.push(split);
      addDirty(split, 0, current);
      return;
    }
    const split: PersistedSplit = { path, segs: [] };
    if (!current.getPriority().isEmpty()) {
      split.priority = current.getPriority().val();
    }
    splits.push(split);
    const deep: Array<[string, Node]> = [];
    let segFrom: string | null = null;
    let segBytes = 0;
    current.forEachChild(KEY_INDEX, (key, child) => {
      const estimate = estimateSerializedNodeSize(child) + key.length + 4;
      if (
        !child.isLeafNode() &&
        estimate > targetBytes * PERSISTENCE_DEEP_CHILD_FACTOR
      ) {
        deep.push([key, child]);
        return;
      }
      if (segBytes > 0 && segBytes + estimate > targetBytes) {
        split.segs.push({ from: segFrom, id: '', bytes: 0 });
        segFrom = key;
        segBytes = 0;
      }
      segBytes += estimate;
    });
    split.segs.push({ from: segFrom, id: '', bytes: 0 });
    for (let i = 0; i < split.segs.length; i++) {
      addDirty(split, i, current);
    }
    // Parents precede children in the manifest (assembly relies only on
    // paths, but keeping the invariant makes manifests debuggable).
    for (const [key, child] of deep) {
      planFresh(path === '' ? key : path + '/' + key, child);
    }
  };

  /**
   * Re-partitions the child-key interval [runFrom, runTo) of `current` at
   * the target size, appending fresh (dirty) segments to `split`.
   */
  const recutRun = (
    split: PersistedSplit,
    current: Node,
    runFrom: string | null,
    runTo: string | null,
    deepNow: Set<string>
  ): void => {
    const startIndex = split.segs.length;
    let segFrom: string | null = runFrom;
    let segBytes = 0;
    forEachChildFrom(current, runFrom, (key, child) => {
      if (runTo !== null && nameCompare(key, runTo) >= 0) {
        return true;
      }
      if (!deepNow.has(key)) {
        const estimate = estimateSerializedNodeSize(child) + key.length + 4;
        if (segBytes > 0 && segBytes + estimate > targetBytes) {
          split.segs.push({ from: segFrom, id: '', bytes: 0 });
          segFrom = key;
          segBytes = 0;
        }
        segBytes += estimate;
      }
      return false;
    });
    // Always emit at least one segment so the interval stays covered.
    split.segs.push({ from: segFrom, id: '', bytes: 0 });
    // NOT addDirty: the run's LAST segment must stop at runTo. addDirty
    // derives toKey from the next entry in segs, and the clean segments
    // that follow this run are appended only after recutRun returns — a
    // null there would serialize past the run boundary into content the
    // carried-over segments already own (duplicated children).
    for (let i = startIndex; i < split.segs.length; i++) {
      dirty.push({
        split,
        segIndex: i,
        fromKey: split.segs[i].from,
        toKey:
          i + 1 < split.segs.length ? split.segs[i + 1].from : runTo,
        node: current
      });
    }
  };

  /** Keeps an untouched deep split — and its whole subtree — verbatim. */
  const carryOverSubtree = (
    prev: PersistedSplit,
    prevAll: PersistedSplit[]
  ): void => {
    splits.push(prev);
    for (const seg of prev.segs) {
      keptIds.add(seg.id);
    }
    for (const child of deepChildKeys(prevAll, prev.path).values()) {
      carryOverSubtree(child, prevAll);
    }
  };

  /**
   * Plans one previously-split node. `changes` holds the changed paths
   * RELATIVE to this split; an empty element means this whole node changed.
   */
  const planExisting = (
    path: string,
    current: Node,
    prev: PersistedSplit,
    prevAll: PersistedSplit[],
    changes: string[][]
  ): void => {
    if (current.isLeafNode() || prev.leaf === true) {
      // Shape changed at the root of this split (or a leaf value changed):
      // re-plan the subtree from scratch; its old records retire.
      planFresh(path, current);
      return;
    }
    const split: PersistedSplit = { path, segs: [] };
    if (!current.getPriority().isEmpty()) {
      split.priority = current.getPriority().val();
    }
    splits.push(split);

    const wholeDirty = changes.some(c => c.length === 0);
    const prevDeep = deepChildKeys(prevAll, path);

    // Route changes: through a still-deep child -> recurse; anything else ->
    // the covering segment goes dirty.
    const childChanges = new Map<string, string[][]>();
    if (!wholeDirty) {
      for (const change of changes) {
        const key = change[0];
        let scoped = childChanges.get(key);
        if (scoped === undefined) {
          scoped = [];
          childChanges.set(key, scoped);
        }
        scoped.push(change.slice(1));
      }
    }

    // Decide each previously-deep child: keep deep (recurse), or fold
    // inline (demote) into the covering parent segment.
    const deepNow = new Set<string>();
    const dirtyKeys: string[] = [];
    for (const [key, childPrev] of prevDeep) {
      const childNode = current.getImmediateChild(key);
      const childPath = path === '' ? key : path + '/' + key;
      const keepDeep =
        !childNode.isLeafNode() &&
        !childNode.isEmpty() &&
        estimateSerializedNodeSize(childNode) > targetBytes;
      if (keepDeep) {
        deepNow.add(key);
        const scoped = wholeDirty ? [[]] : childChanges.get(key) ?? [];
        childChanges.delete(key);
        if (scoped.length > 0) {
          planExisting(childPath, childNode, childPrev, prevAll, scoped);
        } else {
          carryOverSubtree(childPrev, prevAll);
        }
      } else {
        childChanges.delete(key);
        dirtyKeys.push(key);
      }
    }

    // Remaining changed keys are inline children; a changed inline child
    // that outgrew the promote threshold becomes its own split node.
    for (const key of childChanges.keys()) {
      const childNode = current.getImmediateChild(key);
      if (
        !childNode.isLeafNode() &&
        !childNode.isEmpty() &&
        estimateSerializedNodeSize(childNode) >
          targetBytes * PERSISTENCE_DEEP_CHILD_FACTOR
      ) {
        deepNow.add(key);
        planFresh(path === '' ? key : path + '/' + key, childNode);
      }
      dirtyKeys.push(key);
    }

    // Map dirty keys onto the previous segment boundaries, then re-cut each
    // maximal dirty run and carry every clean segment over.
    const prevSegs = prev.segs;
    const segDirty = new Array<boolean>(prevSegs.length).fill(wholeDirty);
    if (!wholeDirty) {
      for (const key of dirtyKeys) {
        segDirty[segmentIndexFor(prevSegs, key)] = true;
      }
    }
    let i = 0;
    while (i < prevSegs.length) {
      if (!segDirty[i]) {
        split.segs.push(prevSegs[i]);
        keptIds.add(prevSegs[i].id);
        i++;
        continue;
      }
      let j = i;
      while (j < prevSegs.length && segDirty[j]) {
        j++;
      }
      // Absorb-right hysteresis: a shrunken run merges with its clean right
      // neighbor instead of surviving as an ever-smaller fragment.
      if (
        j < prevSegs.length &&
        prevSegs.slice(i, j).reduce((sum, seg) => sum + seg.bytes, 0) <
          targetBytes / 4
      ) {
        j++;
      }
      recutRun(
        split,
        current,
        prevSegs[i].from,
        j < prevSegs.length ? prevSegs[j].from : null,
        deepNow
      );
      i = j;
    }
  };

  if (previous === null || changedPaths === null) {
    planFresh('', node);
  } else {
    const rootPrev = previous.find(split => split.path === '');
    if (rootPrev === undefined) {
      planFresh('', node);
    } else {
      planExisting('', node, rootPrev, previous, changedPaths);
    }
  }

  const retiredIds: string[] = [];
  if (previous !== null) {
    for (const id of allSegmentIds(previous)) {
      if (!keptIds.has(id)) {
        retiredIds.push(id);
      }
    }
  }
  return { splits, dirty, retiredIds };
}

// ================================ ASSEMBLY =================================

/**
 * Builds a ChildrenNode from named children (mirroring nodeFromJSON's
 * balanced-tree construction, including the priority index when any child
 * carries one) plus the node's own priority export value.
 */
function buildChildrenNode(children: NamedNode[], priorityValue: unknown): Node {
  const priority =
    priorityValue === undefined || priorityValue === null
      ? ChildrenNode.EMPTY_NODE
      : nodeFromJSON(priorityValue);
  if (children.length === 0) {
    return ChildrenNode.EMPTY_NODE;
  }
  let childrenHavePriority = false;
  for (const child of children) {
    if (!child.node.getPriority().isEmpty()) {
      childrenHavePriority = true;
      break;
    }
  }
  const childSet = buildChildSet(
    children.slice(),
    NAME_ONLY_COMPARATOR,
    namedNode => namedNode.name,
    NAME_COMPARATOR
  ) as SortedMap<string, Node>;
  if (childrenHavePriority) {
    const sortedChildSet = buildChildSet(
      children.slice(),
      PRIORITY_INDEX.getCompare()
    );
    return new ChildrenNode(
      childSet,
      priority,
      new IndexMap(
        { '.priority': sortedChildSet },
        { '.priority': PRIORITY_INDEX }
      )
    );
  }
  return new ChildrenNode(childSet, priority, IndexMap.Default);
}

/**
 * Assembles the restored root from the manifest's split tree and each
 * segment's already-built child Nodes. Deepest splits first, grafting each
 * built split into its parent's child list.
 */
function assembleTree(
  manifest: PersistedManifest,
  segChildren: Map<string, NamedNode[]>,
  leafNodes: Map<string, Node>
): Node {
  const byDepth = manifest.splits
    .slice()
    .sort(
      (a, b) =>
        (b.path === '' ? 0 : b.path.split('/').length) -
        (a.path === '' ? 0 : a.path.split('/').length)
    );
  const built = new Map<string, Node>();
  for (const split of byDepth) {
    if (split.leaf === true) {
      const leaf = leafNodes.get(split.segs[0].id);
      if (leaf === undefined) {
        throw new Error('Persisted leaf segment missing');
      }
      built.set(split.path, leaf);
      continue;
    }
    const children: NamedNode[] = [];
    for (const seg of split.segs) {
      const segNodes = segChildren.get(seg.id);
      if (segNodes === undefined) {
        throw new Error('Persisted segment missing');
      }
      for (const child of segNodes) {
        children.push(child);
      }
    }
    for (const [key, childSplit] of deepChildKeys(
      manifest.splits,
      split.path
    )) {
      const child = built.get(childSplit.path);
      if (child !== undefined && !child.isEmpty()) {
        children.push(new NamedNode(key, child));
      }
    }
    // buildChildSet requires name-sorted input (it builds the balanced tree
    // positionally). Segments arrive in key order, but grafted deep children
    // belong in the middle of the range.
    children.sort((a, b) => nameCompare(a.name, b.name));
    built.set(split.path, buildChildrenNode(children, split.priority));
  }
  const root = built.get('');
  if (root === undefined) {
    throw new Error('Persisted manifest has no root split');
  }
  return root;
}

export class PersistenceManager {
  private db_: Promise<IDBDatabase | null> | null = null;
  /** Roots explicitly selected by the application (keepSynced semantics). */
  private persistentRoots_ = new Map<string, number>();
  /** Active selected roots currently flowing through persistence. */
  private trackedRoots_ = new Set<string>();
  /**
   * Latest server tree per root. Revisions come from a single manager-wide
   * counter, so no revision is ever reissued — an in-flight flush can never
   * collide with a tree that arrived after its root was evicted and
   * re-tracked.
   */
  private latest_ = new Map<
    string,
    { node: Node; revision: string; authScope: string | null }
  >();
  /**
   * What IndexedDB currently holds per root (see FlushedState) — the basis
   * for identity-diff dirty marking and no-op flushes.
   */
  private lastFlush_ = new Map<string, FlushedState>();
  /**
   * Distinguishes this manager's revisions and record ids from every other
   * tab's and session's — numeric counters restart at zero on reload.
   */
  private instanceId_ = Math.random().toString(36).slice(2, 10);
  private writeCounter_ = 0;
  private segmentCounter_ = 0;
  /**
   * The single-flight coalescing window per root: `writeTimers_` holds the
   * pending (non-restarting) window; `flushPending_` marks a change that
   * landed while the root's queue was busy flushing — exactly one follow-up
   * flush runs when the queue drains, however many changes landed meanwhile.
   */
  private writeTimers_ = new Map<string, ReturnType<typeof setTimeout>>();
  private flushPending_ = new Set<string>();
  /** In-flight storage operations per root (see enqueue_). */
  private queues_ = new Map<string, Promise<void>>();
  /**
   * One physical IndexedDB decode per root. The pre-auth peek and the
   * authenticated listener often overlap; without coalescing they each read
   * the segment records and rebuilt the same large Node tree concurrently.
   */
  private activeReads_ = new Map<
    string,
    {
      promise: Promise<ReadResult | null>;
      progress: Set<() => void>;
      retainAfterResolve: boolean;
      cleanupTimer: ReturnType<typeof setTimeout> | null;
    }
  >();
  private restoreReasons_ = new Map<string, PersistenceRestoreReason>();
  private activeRestoreCount_ = 0;
  private restoreQueue_: Array<() => void> = [];
  private writesDeferredUntilRestores_ = new Set<string>();
  private sweepTimer_: ReturnType<typeof setTimeout> | null = null;
  private sweepInFlight_: Promise<void> | null = null;
  private disposed_ = false;
  private authScope_: string | null = null;
  private authScopeConfigured_ = false;
  private authGeneration_ = 0;

  setAuthScope(scope: string | null): boolean {
    const changed = !this.authScopeConfigured_ || scope !== this.authScope_;
    this.authScopeConfigured_ = true;
    if (!changed) {
      return false;
    }
    this.authGeneration_++;
    for (const timer of this.writeTimers_.values()) {
      clearTimeout(timer);
    }
    this.writeTimers_.clear();
    this.flushPending_.clear();
    this.writesDeferredUntilRestores_.clear();
    this.latest_.clear();
    this.lastFlush_.clear();
    this.activeReads_.clear();
    this.restoreReasons_.clear();
    this.authScope_ = scope;
    recordPersistenceEvent(
      '*',
      'auth-scope-change',
      scope ? 'signed-in' : 'signed-out'
    );
    return true;
  }

  constructor(
    private prefix_: string,
    private idbFactory_: IDBFactory | null = isIndexedDBAvailable()
      ? indexedDB
      : null,
    private schemaKnownCurrent_: boolean = readSchemaMarker(),
    private operationTimeoutMs_: number = PERSISTENCE_RESTORE_TIMEOUT_MS,
    private cacheMaxBytes_: number = PERSISTENCE_MAX_CACHE_BYTES,
    private writeDelayMs_: number = PERSISTENCE_WRITE_DEBOUNCE_MS,
    private segmentTargetBytes_: number = PERSISTENCE_SEGMENT_TARGET_BYTES
  ) {
    if (!this.schemaKnownCurrent_) {
      // Do not put the cold server listen behind a potentially slow Safari
      // version-change transaction. Migration runs in the background; restore
      // APIs return a cache miss synchronously for this boot.
      void this.open_();
    }
  }

  rebindTo(prefix: string): PersistenceManager {
    const scope = this.authScope_;
    const selectedRoots = [...this.persistentRoots_];
    this.dispose();
    const rebound = new PersistenceManager(
      prefix,
      this.idbFactory_,
      this.schemaKnownCurrent_,
      this.operationTimeoutMs_,
      this.cacheMaxBytes_,
      this.writeDelayMs_,
      this.segmentTargetBytes_
    );
    if (this.authScopeConfigured_) {
      rebound.setAuthScope(scope);
    }
    for (const [pathString, count] of selectedRoots) {
      rebound.persistentRoots_.set(pathString, count);
    }
    return rebound;
  }

  setPersistentPath(pathString: string, enabled: boolean): void {
    const current = this.persistentRoots_.get(pathString) ?? 0;
    if (enabled) {
      this.persistentRoots_.set(pathString, current + 1);
    } else if (current <= 1) {
      this.persistentRoots_.delete(pathString);
      this.untrack(pathString);
    } else {
      this.persistentRoots_.set(pathString, current - 1);
    }
  }

  isPersistentPath(pathString: string): boolean {
    return this.persistentRoots_.has(pathString);
  }

  /**
   * Marks a root as persistence-managed; write-throughs only run for
   * tracked roots (and their descendants' updates).
   */
  track(pathString: string): void {
    this.trackedRoots_.add(pathString);
  }

  /**
   * The root's last listen stopped. When a live tracked ancestor covers the
   * root, its record — which contains this subtree and keeps flushing — is
   * the one future sessions should restore, so the child's own record is
   * deleted rather than left to shadow it. Otherwise any pending
   * write-through is flushed so IndexedDB holds the final tree for the next
   * session. Either way the in-memory copies are released — only live
   * listens need them.
   */
  untrack(pathString: string): void {
    if (!this.trackedRoots_.has(pathString)) {
      return;
    }
    this.trackedRoots_.delete(pathString);
    const timer = this.writeTimers_.get(pathString);
    if (timer) {
      clearTimeout(timer);
      this.writeTimers_.delete(pathString);
    }
    this.flushPending_.delete(pathString);
    if (this.trackedRootFor(pathString) !== null) {
      this.latest_.delete(pathString);
      this.lastFlush_.delete(pathString);
      void this.enqueue_(pathString, () => this.deleteRecord_(pathString));
      return;
    }
    // Release the tree only after the final flush settles (flush_ reads
    // latest_ when it runs). Skip the delete if the root was re-tracked
    // meanwhile — the new listen owns the entry now. flushNow never rejects
    // today, but cleanup on both callbacks keeps this leak-proof either way.
    const release = () => {
      if (!this.trackedRoots_.has(pathString)) {
        this.latest_.delete(pathString);
        this.lastFlush_.delete(pathString);
      }
    };
    void this.flushNow(pathString).then(release, release);
  }

  /**
   * The nearest (deepest) tracked root at-or-above `pathString`, or null.
   */
  trackedRootFor(pathString: string): string | null {
    let best: string | null = null;
    for (const root of this.trackedRoots_) {
      if (
        pathString === root ||
        (pathString.length > root.length &&
          pathString.startsWith(root === '/' ? root : root + '/'))
      ) {
        if (best === null || root.length > best.length) {
          best = root;
        }
      }
    }
    return best;
  }

  private open_(): Promise<IDBDatabase | null> {
    if (this.db_) {
      return this.db_;
    }
    this.db_ = this.openAtVersion_(undefined)
      .then(db => {
        if (db === null) {
          return null;
        }
        // One-time migration away from older cache formats. Close and upgrade
        // BEFORE any get(): clearing in the version change transaction drops
        // the old values inside IndexedDB, without structured-cloning them
        // into the WebKit heap (which is exactly what crashed large legacy
        // accounts during restore).
        if (db.version < PERSISTENCE_DB_VERSION) {
          db.close();
          return this.openAtVersion_(PERSISTENCE_DB_VERSION);
        }
        if (db.objectStoreNames.contains(STORE)) {
          return db;
        }
        const nextVersion = db.version + 1;
        db.close();
        return this.openAtVersion_(nextVersion);
      })
      .then(db => {
        if (db !== null && !db.objectStoreNames.contains(STORE)) {
          persistenceStats.storageFailures++;
          db.close();
          return null;
        }
        if (db !== null) {
          this.schemaKnownCurrent_ = true;
          writeSchemaMarker();
        }
        return db;
      });
    void this.db_.then(db => {
      if (db !== null && !this.disposed_ && this.sweepTimer_ === null) {
        this.sweepTimer_ = setTimeout(() => {
          void this.sweepExpired_();
        }, PERSISTENCE_SWEEP_DELAY_MS);
        (this.sweepTimer_ as { unref?: () => void }).unref?.();
      }
    });
    return this.db_;
  }

  private openAtVersion_(
    version: number | undefined
  ): Promise<IDBDatabase | null> {
    return new Promise(resolve => {
      if (!this.idbFactory_) {
        resolve(null);
        return;
      }
      let settled = false;
      const finish = (db: IDBDatabase | null, failed = false) => {
        if (settled) {
          db?.close();
          return;
        }
        settled = true;
        clearTimeout(timer);
        if (failed) {
          persistenceStats.storageFailures++;
        }
        resolve(db);
      };
      const timer = setTimeout(() => {
        // Some WebKit IndexedDB requests fire neither success nor error. A
        // cache miss is always safer than blocking the network listen.
        finish(null, true);
      }, this.operationTimeoutMs_);
      try {
        const req =
          version === undefined
            ? this.idbFactory_.open('firebase-database-persistence')
            : this.idbFactory_.open('firebase-database-persistence', version);
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains(STORE)) {
            db.createObjectStore(STORE);
          } else if (version === PERSISTENCE_DB_VERSION) {
            // Clear in-IDB: no old record is cloned into JS memory.
            req.transaction!.objectStore(STORE).clear();
          }
        };
        req.onsuccess = () => {
          const db = req.result;
          if (settled) {
            // An open that completed after the timeout must not leak a
            // connection or block a future schema upgrade.
            db.close();
            return;
          }
          db.onversionchange = () => db.close();
          finish(db);
        };
        req.onerror = () => finish(null, true);
        req.onblocked = () => finish(null);
      } catch (e) {
        finish(null, true);
      }
    });
  }

  private key_(pathString: string): string {
    return this.prefix_ + '|' + pathString;
  }

  /**
   * A fresh, never-reused segment record id. The leading base36 wall clock
   * is what the sweep's orphan age guard reads (see sweepExpired_).
   */
  private newSegmentId_(): string {
    return (
      Date.now().toString(36) +
      '.' +
      this.instanceId_ +
      '.' +
      (++this.segmentCounter_).toString(36)
    );
  }

  /**
   * Runs `body` against the object store in a transaction of the given mode
   * and resolves with what `body` chose to deliver (via its `done` callback)
   * once the transaction completes. Every failure path — no database, a
   * throwing store call, an aborted transaction — resolves `fallback` and
   * counts one storageFailure (except when IndexedDB is absent altogether,
   * which is a supported cold-load configuration, not a failure).
   */
  private withStore_<T>(
    mode: IDBTransactionMode,
    fallback: T,
    body: (
      store: IDBObjectStore,
      done: (value: T) => void,
      progress: () => void
    ) => void,
    onProgress: () => void = () => {}
  ): Promise<T> {
    return this.open_().then(
      db =>
        new Promise<T>(resolve => {
          if (!db) {
            resolve(fallback);
            return;
          }
          let settled = false;
          let timer: ReturnType<typeof setTimeout> | null = null;
          const finish = (value: T, failed = false) => {
            if (settled) {
              return;
            }
            settled = true;
            if (timer !== null) {
              clearTimeout(timer);
            }
            if (failed) {
              persistenceStats.storageFailures++;
            }
            resolve(value);
          };
          try {
            const tx = db.transaction(STORE, mode);
            let value = fallback;
            const arm = () => {
              if (settled) {
                return;
              }
              if (timer !== null) {
                clearTimeout(timer);
              }
              timer = setTimeout(() => {
                // Abort a stalled transaction so an abandoned cache read
                // cannot keep buffering indefinitely.
                try {
                  tx.abort();
                } catch (e) {
                  // It may have completed between the timer firing and abort.
                }
                finish(fallback, true);
              }, this.operationTimeoutMs_);
              onProgress();
            };
            body(
              tx.objectStore(STORE),
              v => {
                value = v;
              },
              arm
            );
            arm();
            tx.oncomplete = () => finish(value);
            tx.onabort = tx.onerror = () => finish(fallback, true);
          } catch (e) {
            finish(fallback, true);
          }
        })
    );
  }

  /**
   * Coalesced physical read: one manifest+segments decode per root however
   * many callers (pre-auth peek, authenticated listener) overlap on it.
   */
  private readRecord_(
    pathString: string,
    onProgress: () => void = () => {},
    retainAfterResolve = false,
    expectedAuthScope: string | null = this.authScope_
  ): Promise<ReadResult | null> {
    const active = this.activeReads_.get(pathString);
    if (active) {
      active.progress.add(onProgress);
      // Joining an already-progressing read is itself progress.
      onProgress();
      if (retainAfterResolve) {
        active.retainAfterResolve = true;
      } else if (active.retainAfterResolve) {
        // A real listener consumes the optimistic peek's completed read. The
        // returned promise still owns the result; the manager no longer needs
        // a second retained handle to it.
        active.retainAfterResolve = false;
        if (active.cleanupTimer !== null) {
          clearTimeout(active.cleanupTimer);
          this.activeReads_.delete(pathString);
        }
      }
      return active.promise;
    }
    const progress = new Set<() => void>([onProgress]);
    const emitProgress = () => {
      for (const callback of progress) {
        callback();
      }
    };
    const entry: {
      promise: Promise<ReadResult | null>;
      progress: Set<() => void>;
      retainAfterResolve: boolean;
      cleanupTimer: ReturnType<typeof setTimeout> | null;
    } = {
      promise: Promise.resolve(null),
      progress,
      retainAfterResolve,
      cleanupTimer: null
    };
    const promise = this.readRecordOnce_(
      pathString,
      emitProgress,
      expectedAuthScope
    );
    entry.promise = promise;
    this.activeReads_.set(pathString, entry);
    const release = () => {
      entry.progress.clear();
      if (this.activeReads_.get(pathString) !== entry) {
        return;
      }
      if (!entry.retainAfterResolve) {
        this.activeReads_.delete(pathString);
        return;
      }
      entry.cleanupTimer = setTimeout(() => {
        if (this.activeReads_.get(pathString) === entry) {
          this.activeReads_.delete(pathString);
        }
      }, PERSISTENCE_PEEK_HANDOFF_MS);
    };
    void promise.then(release, release);
    return promise;
  }

  /**
   * One physical restore. A single readonly transaction reads the manifest
   * and queues every referenced segment get; each segment's JSON text is
   * parsed and its children built as results arrive (bounded work per
   * event-loop turn by IndexedDB's own request cadence), and the split tree
   * is assembled once the transaction completes.
   */
  private readRecordOnce_(
    pathString: string,
    onProgress: () => void,
    expectedAuthScope: string | null = this.authScope_
  ): Promise<ReadResult | null> {
    const key = this.key_(pathString);
    interface Assembly {
      manifest: PersistedManifest;
      segChildren: Map<string, NamedNode[]>;
      leafNodes: Map<string, Node>;
    }
    let assembly: Assembly | null = null;
    let failed = false;
    return this.withStore_<boolean>(
      'readonly',
      false,
      (store, done, progress) => {
        const manifestReq = store.get(key);
        manifestReq.onsuccess = () => {
          progress();
          const manifest =
            (manifestReq.result as PersistedManifest | undefined) ?? null;
          if (manifest === null) {
            done(false);
            return;
          }
          const structurallyValid =
            manifest.formatVersion === PERSISTENCE_FORMAT_VERSION &&
            typeof manifest.revision === 'string' &&
            typeof manifest.updatedAt === 'number' &&
            Array.isArray(manifest.splits) &&
            manifest.splits.length > 0 &&
            manifest.splits.every(
              split =>
                split !== null &&
                typeof split === 'object' &&
                typeof split.path === 'string' &&
                Array.isArray(split.segs) &&
                split.segs.length > 0 &&
                split.segs.every(
                  seg =>
                    seg !== null &&
                    typeof seg === 'object' &&
                    typeof seg.id === 'string' &&
                    seg.id.length > 0 &&
                    (seg.from === null || typeof seg.from === 'string')
                )
            );
          if (!structurallyValid) {
            this.restoreReasons_.set(pathString, 'corrupt');
            done(false);
            return;
          }
          if (manifest.authScope !== expectedAuthScope) {
            this.restoreReasons_.set(pathString, 'auth');
            done(false);
            return;
          }
          if (manifest.updatedAt < Date.now() - PERSISTENCE_MAX_AGE_MS) {
            this.restoreReasons_.set(pathString, 'expired');
            done(false);
            return;
          }
          const current: Assembly = {
            manifest,
            segChildren: new Map(),
            leafNodes: new Map()
          };
          assembly = current;
          const leafIds = new Set<string>();
          for (const split of manifest.splits) {
            if (split.leaf === true) {
              leafIds.add(split.segs[0].id);
            }
          }
          let remaining = 0;
          for (const id of allSegmentIds(manifest.splits)) {
            remaining++;
            const req = store.get(key + SEG_KEY_INFIX + id);
            req.onsuccess = () => {
              progress();
              if (!failed) {
                const text = req.result as string | undefined;
                if (typeof text !== 'string') {
                  failed = true;
                  this.restoreReasons_.set(pathString, 'corrupt');
                } else {
                  try {
                    // Parse and build Nodes NOW, one segment per success
                    // callback: the expensive work interleaves with the
                    // remaining gets instead of forming one giant post-
                    // transaction pause, and the text becomes collectable
                    // immediately.
                    const parsed = JSON.parse(text) as unknown;
                    if (leafIds.has(id)) {
                      current.leafNodes.set(id, nodeFromJSON(parsed));
                    } else {
                      if (
                        parsed === null ||
                        typeof parsed !== 'object' ||
                        Array.isArray(parsed)
                      ) {
                        throw new Error('segment is not an object');
                      }
                      const record = parsed as Record<string, unknown>;
                      const children: NamedNode[] = [];
                      for (const childKey of Object.keys(record)) {
                        const child = nodeFromJSON(record[childKey]);
                        if (!child.isEmpty()) {
                          children.push(new NamedNode(childKey, child));
                        }
                      }
                      current.segChildren.set(id, children);
                    }
                  } catch (e) {
                    failed = true;
                    this.restoreReasons_.set(pathString, 'corrupt');
                  }
                }
              }
              remaining--;
              if (remaining === 0) {
                done(!failed);
              }
            };
          }
        };
      },
      onProgress
    ).then(ok => {
      if (!ok || assembly === null || failed) {
        const reason = this.restoreReasons_.get(pathString);
        if (reason === 'corrupt' || reason === 'expired') {
          // Best-effort cleanup. Auth/missing misses must not delete another
          // identity's otherwise valid cache record.
          void this.deleteRecord_(pathString);
        }
        return null;
      }
      try {
        const node = assembleTree(
          assembly.manifest,
          assembly.segChildren,
          assembly.leafNodes
        );
        if (node.isEmpty()) {
          this.restoreReasons_.set(pathString, 'corrupt');
          void this.deleteRecord_(pathString);
          return null;
        }
        return {
          record: {
            node,
            updatedAt: assembly.manifest.updatedAt,
            revision: assembly.manifest.revision
          },
          splits: assembly.manifest.splits
        };
      } catch (e) {
        this.restoreReasons_.set(pathString, 'corrupt');
        void this.deleteRecord_(pathString);
        return null;
      }
    });
  }

  private deleteRecord_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, store => {
      store.delete(key);
      // Segment records and legacy sidecars share the '#' suffix namespace.
      // Range-delete where the platform has IDBKeyRange; cursor-walk
      // otherwise (Node, test fakes) — key-only, no values.
      if (typeof IDBKeyRange !== 'undefined') {
        try {
          store.delete(
            IDBKeyRange.bound(
              key + '#',
              key + '#' + String.fromCharCode(0xffff)
            )
          );
          return;
        } catch (e) {
          // Fall through to the cursor walk.
        }
      }
      try {
        const req = store.openCursor();
        req.onsuccess = () => {
          const cursor = req.result as IDBCursor | null;
          if (!cursor) {
            return;
          }
          if (
            typeof cursor.key === 'string' &&
            cursor.key.startsWith(key + '#')
          ) {
            cursor.delete();
          }
          cursor.continue();
        };
      } catch (e) {
        // Sidecar cleanup is best-effort; the sweep reclaims leftovers.
      }
    });
  }

  private withRestoreSlot_<T>(work: () => Promise<T>): Promise<T> {
    const run = () => {
      this.activeRestoreCount_++;
      return work().finally(() => {
        this.activeRestoreCount_--;
        const next = this.restoreQueue_.shift();
        if (next) {
          next();
        } else if (this.activeRestoreCount_ === 0) {
          this.flushWritesDeferredUntilRestores_();
        }
      });
    };
    if (this.activeRestoreCount_ < PERSISTENCE_MAX_CONCURRENT_RESTORES) {
      return run();
    }
    return new Promise<T>((resolve, reject) => {
      this.restoreQueue_.push(() => {
        if (this.disposed_) {
          resolve(null as T);
          return;
        }
        void run().then(resolve, reject);
      });
    });
  }

  /**
   * Projects an exact-path peek from a covering root that is already restored
   * or actively restoring in this manager. This never starts a large ancestor
   * read just to answer a tiny token lookup; it only reuses work the app is
   * already paying for, preserving the exact-root fast path on direct boots.
   */
  private peekFromCoveringRead_(
    pathString: string,
    expectedAuthScope: string | null
  ): Promise<PersistedRecord | null> | null {
    if (!this.authScopeConfigured_ || expectedAuthScope !== this.authScope_) {
      return null;
    }
    let bestRoot: string | null = null;
    let source: Promise<ReadResult | null> | null = null;
    for (const [root, read] of this.activeReads_) {
      if (
        pathString !== root &&
        (root === '/' || pathString.startsWith(root + '/')) &&
        (bestRoot === null || root.length > bestRoot.length)
      ) {
        bestRoot = root;
        source = read.promise;
      }
    }
    for (const [root, state] of this.lastFlush_) {
      if (
        pathString !== root &&
        (root === '/' || pathString.startsWith(root + '/')) &&
        (bestRoot === null || root.length > bestRoot.length)
      ) {
        bestRoot = root;
        source = Promise.resolve({
          record: {
            node: state.rootNode,
            updatedAt: state.storedUpdatedAt,
            revision: state.revision
          },
          splits: state.splits
        });
      }
    }
    if (bestRoot === null || source === null) {
      return null;
    }
    const relative =
      bestRoot === '/'
        ? pathString.replace(/^\/+/, '')
        : pathString.slice(bestRoot.length).replace(/^\/+/, '');
    return source.then(result => {
      if (result === null) {
        return null;
      }
      const node = result.record.node.getChild(new Path(relative));
      return node.isEmpty()
        ? null
        : {
            node,
            updatedAt: result.record.updatedAt,
            revision: result.record.revision
          };
    });
  }

  /**
   * Exact-root optimistic peek. The completed assembly is retained briefly
   * so the authenticated listener consumes the same immutable Node instead
   * of reconstructing the root twice during boot.
   */
  peek(
    pathString: string,
    expectedAuthScope: string | null = this.authScope_
  ): Promise<PersistedRecord | null> {
    if (
      this.disposed_ ||
      !this.schemaKnownCurrent_ ||
      !this.authScopeConfigured_
    ) {
      recordPersistenceEvent(
        pathString,
        'peek-miss',
        this.disposed_
          ? 'disposed'
          : !this.authScopeConfigured_
          ? 'auth-scope-unconfigured'
          : 'schema-migration'
      );
      return Promise.resolve(null);
    }
    const authGeneration = this.authGeneration_;
    const covering = this.peekFromCoveringRead_(pathString, expectedAuthScope);
    if (covering !== null) {
      return covering.then(record =>
        authGeneration === this.authGeneration_ &&
        expectedAuthScope === this.authScope_
          ? record
          : null
      );
    }
    return this.withRestoreSlot_(() =>
      this.raceRestoreTimeout_(
        onProgress =>
          this.readRecord_(
            pathString,
            onProgress,
            true,
            expectedAuthScope
          ).then(result => {
            recordPersistenceEvent(
              pathString,
              result ? 'peek-hit' : 'peek-miss'
            );
            return result === null ? null : result.record;
          }),
        this.operationTimeoutMs_,
        () => recordPersistenceEvent(pathString, 'peek-idle-timeout')
      )
    ).then(record =>
      authGeneration === this.authGeneration_ &&
      expectedAuthScope === this.authScope_
        ? record
        : null
    );
  }

  /**
   * Listener restore with an idle (no-progress) bound. Resolves with the
   * assembled tree; the caller applies it and derives the listen hashes
   * from the node itself (see repoStartServerListen).
   */
  restoreForListen(pathString: string): Promise<PersistenceRestoreResult> {
    this.restoreReasons_.delete(pathString);
    const authGeneration = this.authGeneration_;
    const expectedAuthScope = this.authScope_;
    if (
      this.disposed_ ||
      !this.schemaKnownCurrent_ ||
      !this.authScopeConfigured_
    ) {
      const reason: PersistenceRestoreReason = this.authScopeConfigured_
        ? 'missing'
        : 'auth';
      this.restoreReasons_.set(pathString, reason);
      recordPersistenceEvent(
        pathString,
        'restore-miss',
        this.disposed_
          ? 'disposed'
          : !this.authScopeConfigured_
          ? 'auth-scope-unconfigured'
          : 'schema-migration'
      );
      return Promise.resolve({ record: null, reason });
    }
    return this.withRestoreSlot_(() =>
      this.raceRestoreTimeout_(
        onProgress =>
          this.readRecord_(
            pathString,
            onProgress,
            false,
            expectedAuthScope
          ).then(result => {
            if (
              result === null ||
              authGeneration !== this.authGeneration_ ||
              expectedAuthScope !== this.authScope_ ||
              this.disposed_ ||
              !this.trackedRoots_.has(pathString)
            ) {
              return null;
            }
            this.lastFlush_.set(pathString, {
              rootNode: result.record.node,
              revision: result.record.revision,
              splits: result.splits,
              storedUpdatedAt: result.record.updatedAt
            });
            persistenceStats.restoredRoots.push(pathString);
            return result.record;
          }),
        this.operationTimeoutMs_,
        () => {
          this.restoreReasons_.set(pathString, 'timeout');
          recordPersistenceEvent(pathString, 'restore-idle-timeout');
        }
      )
    ).then(record => {
      if (
        authGeneration !== this.authGeneration_ ||
        expectedAuthScope !== this.authScope_
      ) {
        this.restoreReasons_.set(pathString, 'auth');
        record = null;
      }
      const reason = record
        ? undefined
        : this.restoreReasons_.get(pathString) ?? 'missing';
      recordPersistenceEvent(
        pathString,
        record ? 'restore-hit' : 'restore-miss',
        reason
      );
      return record ? { record } : { record: null, reason };
    });
  }

  /**
   * Bounds a read by an IDLE (no-progress) timeout. The factory form lets
   * segment restores reset the timer after every completed request; callers
   * that pass an already-started Promise retain the old total-time bound.
   */
  private raceRestoreTimeout_<T>(
    readOrStart:
      | Promise<T | null>
      | ((onProgress: () => void) => Promise<T | null>),
    timeoutMs = this.operationTimeoutMs_,
    onTimeout: () => void = () => {}
  ): Promise<T | null> {
    let timer: ReturnType<typeof setTimeout>;
    let settled = false;
    let timeoutResolve: (value: null) => void = () => {};
    const timeout = new Promise<null>(resolve => {
      timeoutResolve = resolve;
    });
    const arm = () => {
      if (settled) {
        return;
      }
      clearTimeout(timer);
      timer = setTimeout(() => {
        onTimeout();
        timeoutResolve(null);
      }, timeoutMs);
    };
    const read =
      typeof readOrStart === 'function' ? readOrStart(arm) : readOrStart;
    arm();
    return Promise.race([read, timeout]).then(result => {
      settled = true;
      clearTimeout(timer);
      return result;
    });
  }

  private flushWritesDeferredUntilRestores_(): void {
    const paths = [...this.writesDeferredUntilRestores_];
    this.writesDeferredUntilRestores_.clear();
    for (const pathString of paths) {
      // The deferral must not bypass the write window: draining the restore
      // wave IS the cold-boot moment (LCP, initial render). Re-arm the same
      // non-restarting window a direct write-through would have entered.
      this.armWriteWindow_(pathString);
    }
  }

  /** Arms the non-restarting single-flight write window for a root. */
  private armWriteWindow_(pathString: string): void {
    if (!this.writeTimers_.has(pathString)) {
      this.writeTimers_.set(
        pathString,
        setTimeout(() => {
          this.writeTimers_.delete(pathString);
          this.scheduleFlush_(pathString);
        }, this.writeDelayMs_)
      );
    }
  }

  serverCacheUpdated(path: Path, node: Node): void {
    if (this.disposed_ || !this.authScopeConfigured_) {
      return;
    }
    const pathString = path.toString();
    if (!this.trackedRoots_.has(pathString)) {
      return;
    }
    const prev = this.lastFlush_.get(pathString);
    if (
      prev &&
      prev.rootNode === node &&
      Date.now() - prev.storedUpdatedAt < PERSISTENCE_REFRESH_AGE_MS
    ) {
      return;
    }
    this.latest_.set(pathString, {
      node,
      revision: this.instanceId_ + '-' + (++this.writeCounter_).toString(36),
      authScope: this.authScope_
    });
    // IndexedDB serializes readwrite transactions for this object store. A
    // cold root must not begin a large write while another selected root is
    // still restoring, or the restore can hit its idle timeout behind its own
    // write-through. Android gets this ordering from one persistence runloop;
    // the web manager reproduces it explicitly.
    if (this.activeRestoreCount_ > 0 || this.restoreQueue_.length > 0) {
      this.writesDeferredUntilRestores_.add(pathString);
      return;
    }
    // Every generation, including the first, enters the non-restarting
    // window. Cache creation is an optional accelerator and must not compete
    // with the cold page's initial render/LCP. The flush reads
    // latest_ when it runs, so it always writes the newest tree.
    this.armWriteWindow_(pathString);
  }

  /**
   * Enqueues a flush unless the root's queue is still working — then one
   * flush is marked pending and enqueued when the queue drains. Without the
   * mark, a root whose flush takes longer than the window would queue
   * flushes faster than they complete, unboundedly. This is the
   * single-flight guarantee: at most one flush in flight per root, effective
   * cadence max(writeDelayMs, flush duration).
   */
  private scheduleFlush_(pathString: string): void {
    if (this.queues_.has(pathString)) {
      this.flushPending_.add(pathString);
      return;
    }
    void this.enqueue_(pathString, () => this.flush_(pathString));
  }

  /** Drop an unusable persisted record but keep the live root tracked. */
  invalidate(path: Path): void {
    const pathString = path.toString();
    this.latest_.delete(pathString);
    this.lastFlush_.delete(pathString);
    recordPersistenceEvent(pathString, 'invalidate', 'corrupt-or-incompatible');
    void this.enqueue_(pathString, () => this.deleteRecord_(pathString));
  }

  /**
   * The viewer lost access to a root: a cached copy must not outlive the
   * access that produced it, and the root leaves write-through tracking
   * entirely — SyncTree never calls stopListening for server-revoked
   * listens, so nothing else would ever untrack it.
   */
  evict(path: Path): void {
    const pathString = path.toString();
    this.trackedRoots_.delete(pathString);
    this.latest_.delete(pathString);
    this.lastFlush_.delete(pathString);
    this.flushPending_.delete(pathString);
    const timer = this.writeTimers_.get(pathString);
    if (timer) {
      clearTimeout(timer);
      this.writeTimers_.delete(pathString);
    }
    persistenceStats.evictions++;
    recordPersistenceEvent(pathString, 'evict', 'permission-or-revocation');
    // Through the queue: a flush already running for this root finishes its
    // writes first, then the delete removes them — never the reverse.
    void this.enqueue_(pathString, () => this.deleteRecord_(pathString));
  }

  dispose(): void {
    this.disposed_ = true;
    for (const timer of this.writeTimers_.values()) {
      clearTimeout(timer);
    }
    this.writeTimers_.clear();
    if (this.sweepTimer_ !== null) {
      clearTimeout(this.sweepTimer_);
    }
    this.flushPending_.clear();
    const queuedRestores = this.restoreQueue_;
    this.restoreQueue_ = [];
    for (const resume of queuedRestores) {
      resume();
    }
    for (const read of this.activeReads_.values()) {
      if (read.cleanupTimer !== null) {
        clearTimeout(read.cleanupTimer);
      }
    }
    this.activeReads_.clear();
    this.persistentRoots_.clear();
    this.latest_.clear();
    this.lastFlush_.clear();
    void this.db_?.then(db => db?.close());
  }

  /**
   * Test seam: forces a pending flush window to fire now.
   */
  flushNow(pathString: string): Promise<void> {
    const timer = this.writeTimers_.get(pathString);
    if (timer) {
      clearTimeout(timer);
      this.writeTimers_.delete(pathString);
    }
    this.flushPending_.delete(pathString);
    return this.enqueue_(pathString, () => this.flush_(pathString));
  }

  /**
   * Chains an operation onto the root's queue. One writer per root at a
   * time: a flush's segments and manifest commit before the next flush or
   * delete for that root starts — no cross-operation races to reason about
   * WITHIN a tab. (Cross-tab writers are last-writer-wins by design; see
   * the file header.)
   */
  private enqueue_(pathString: string, op: () => Promise<void>): Promise<void> {
    const next = (this.queues_.get(pathString) ?? Promise.resolve()).then(op);
    // Settle-or-not, the chain must continue; storage failures are already
    // absorbed (and counted) inside withStore_.
    const settled = next.catch(() => {});
    this.queues_.set(pathString, settled);
    void settled.then(() => {
      if (this.queues_.get(pathString) === settled) {
        this.queues_.delete(pathString);
        if (this.flushPending_.delete(pathString) && !this.disposed_) {
          void this.enqueue_(pathString, () => this.flush_(pathString));
        }
      }
    });
    return next;
  }

  /**
   * One generation: identity-diff against the last known stored tree maps
   * the change set to dirty segments; only those are re-serialized (direct
   * Node -> JSON text, no export objects) and written under fresh ids in
   * bounded batches. Clean segment records carry over verbatim and are
   * never read or cloned. The commit is one small last-writer-wins
   * transaction: manifest put + retired-id deletes. No hashing anywhere —
   * the next boot derives listen hashes from whatever tree it assembles.
   */
  private flush_(pathString: string): Promise<void> {
    if (this.sweepInFlight_ !== null) {
      return this.sweepInFlight_.then(() => this.flush_(pathString));
    }
    const entry = this.latest_.get(pathString);
    if (!entry || this.disposed_ || !this.authScopeConfigured_) {
      return Promise.resolve();
    }
    const { node, revision, authScope } = entry;
    const prev = this.lastFlush_.get(pathString);
    const now = Date.now();
    if (
      prev &&
      prev.rootNode === node &&
      now - prev.storedUpdatedAt < PERSISTENCE_REFRESH_AGE_MS
    ) {
      return Promise.resolve();
    }
    const key = this.key_(pathString);
    if (node.isEmpty()) {
      return this.deleteRecord_(pathString).then(() => {
        this.lastFlush_.delete(pathString);
      });
    }
    if (prev && prev.rootNode === node) {
      // Content-identical: refresh only the manifest timestamp. The revision
      // guard prevents a stale tab from refreshing a superseded generation.
      return this.withStore_<boolean>(
        'readwrite',
        false,
        (store, done, progress) => {
          const req = store.get(key);
          req.onsuccess = () => {
            progress();
            const current = req.result as PersistedManifest | undefined;
            if (current && current.revision === prev.revision) {
              const put = store.put({ ...current, updatedAt: now }, key);
              put.onsuccess = progress;
              done(true);
            } else {
              done(false);
            }
          };
        }
      ).then(ok => {
        if (this.disposed_) {
          return;
        }
        if (ok) {
          this.lastFlush_.set(pathString, { ...prev, storedUpdatedAt: now });
          return;
        }
        // The stored generation is gone (another tab's manifest, a sweep, or
        // manual storage clearing). lastFlush_ no longer describes storage;
        // left in place, every future identical-node write-through would
        // skip against it and the root would stay unpersisted for the whole
        // session. Resync from storage and rebuild once.
        return this.readRecord_(pathString).then(winner => {
          if (this.disposed_) {
            return;
          }
          if (winner !== null) {
            this.lastFlush_.set(pathString, {
              rootNode: winner.record.node,
              revision: winner.record.revision,
              splits: winner.splits,
              storedUpdatedAt: winner.record.updatedAt
            });
          } else {
            this.lastFlush_.delete(pathString);
          }
          this.flushPending_.add(pathString);
        });
      });
    }

    // Identity-diff against the exact generation this tab restored or
    // committed. An unusable diff — the first generation, or a path-budget
    // overflow (collectChangedSubtreePaths returns null) — plans the whole
    // tree from scratch.
    let previousSplits: PersistedSplit[] | null = null;
    let changed: string[][] | null = null;
    if (prev && prev.splits.length > 0) {
      changed = collectChangedSubtreePaths(prev.rootNode, node);
      previousSplits = changed === null ? null : prev.splits;
    }

    let plan: GenerationPlan;
    try {
      plan = planGeneration(
        node,
        previousSplits,
        changed,
        this.segmentTargetBytes_
      );
    } catch (e) {
      persistenceStats.storageFailures++;
      recordPersistenceEvent(pathString, 'flush-plan-error');
      return this.deleteRecord_(pathString).then(() => {
        this.lastFlush_.delete(pathString);
      });
    }

    // Assign fresh immutable ids to every dirty segment up front; the
    // manifest is complete before any byte is written.
    for (const job of plan.dirty) {
      job.split.segs[job.segIndex].id = this.newSegmentId_();
    }

    const stageBatch = (jobs: DirtySegmentJob[]): Promise<void> => {
      const puts: Array<{ id: string; text: string }> = [];
      for (const job of jobs) {
        const seg = job.split.segs[job.segIndex];
        const deep = new Set(
          deepChildKeys(plan.splits, job.split.path).keys()
        );
        const text =
          job.split.leaf === true
            ? leafExportJson(job.node)
            : serializeSegment(job.node, job.fromKey, job.toKey, deep);
        seg.bytes = text.length;
        puts.push({ id: seg.id, text });
      }
      return this.withStore_<boolean>(
        'readwrite',
        false,
        (store, done, progress) => {
          for (const put of puts) {
            const req = store.put(put.text, key + SEG_KEY_INFIX + put.id);
            req.onsuccess = progress;
          }
          done(true);
        }
      ).then(stored => {
        if (!stored) {
          throw new Error('Failed to stage persisted segments');
        }
        puts.length = 0;
        // Yield a macrotask so a large first generation cannot monopolize
        // the main thread between staging transactions.
        return new Promise<void>(resolve => setTimeout(resolve, 0));
      });
    };

    const stageAll = async (): Promise<void> => {
      for (
        let i = 0;
        i < plan.dirty.length;
        i += PERSISTENCE_STAGE_BATCH_SIZE
      ) {
        if (this.disposed_) {
          return;
        }
        await stageBatch(plan.dirty.slice(i, i + PERSISTENCE_STAGE_BATCH_SIZE));
      }
    };

    return stageAll()
      .then(() => {
        if (this.disposed_) {
          return;
        }
        const manifest: PersistedManifest = {
          formatVersion: PERSISTENCE_FORMAT_VERSION,
          revision,
          updatedAt: now,
          authScope,
          estimatedBytes: estimateSerializedNodeSize(node),
          splits: plan.splits
        };
        // LAST-WRITER-WINS COMMIT. When the stored manifest is still the
        // generation this flush built on (or the slot is empty/foreign),
        // retired ids are deleted here too. When another tab interleaved,
        // the manifest still wins the slot — both tabs cache the same
        // server data — but the deletes are skipped: the loser must not
        // delete records the interleaved manifest may reference. The sweep
        // reclaims whatever ends up unreferenced, age-guarded.
        return this.withStore_<boolean>(
          'readwrite',
          false,
          (store, done, progress) => {
            const currentReq = store.get(key);
            currentReq.onsuccess = () => {
              progress();
              const current = currentReq.result as
                | PersistedManifest
                | undefined;
              const undisturbed =
                (prev === undefined && current === undefined) ||
                (prev !== undefined &&
                  current !== undefined &&
                  current.revision === prev.revision);
              if (undisturbed) {
                for (const id of plan.retiredIds) {
                  const remove = store.delete(key + SEG_KEY_INFIX + id);
                  remove.onsuccess = progress;
                }
              }
              const manifestPut = store.put(manifest, key);
              manifestPut.onsuccess = progress;
              done(true);
            };
          }
        ).then(ok => {
          if (this.disposed_ || !ok) {
            if (!ok) {
              recordPersistenceEvent(pathString, 'flush-commit-error');
            }
            return;
          }
          persistenceStats.segmentsWritten += plan.dirty.length;
          persistenceStats.segmentsReused +=
            allSegmentIds(plan.splits).length - plan.dirty.length;
          persistenceStats.writeThroughs++;
          this.lastFlush_.set(pathString, {
            rootNode: node,
            revision,
            splits: plan.splits,
            storedUpdatedAt: now
          });
          recordPersistenceEvent(
            pathString,
            'stored',
            `${allSegmentIds(plan.splits).length} segments, ${
              plan.dirty.length
            } written`
          );
        });
      })
      .catch(() => {
        persistenceStats.storageFailures++;
        recordPersistenceEvent(pathString, 'flush-stage-error');
        // Staged records are unreferenced until the manifest commits; the
        // age-guarded sweep reclaims them.
      });
  }

  sweepNow(): Promise<void> {
    if (this.sweepTimer_ !== null) {
      clearTimeout(this.sweepTimer_);
    }
    return this.sweepExpired_();
  }

  /**
   * Deletes this manager's expired records (see PERSISTENCE_MAX_AGE_MS) and
   * reclaims unreferenced segment records. Expiry is decided by each root's
   * manifest: a '#'-suffixed record carries no authority of its own and is
   * dropped exactly when its manifest is dropped, is missing, or no longer
   * references it — except that an unreferenced record younger than
   * PERSISTENCE_ORPHAN_MIN_AGE_MS is left alone, because another tab may
   * have staged it for a generation whose manifest hasn't committed yet.
   * Scoped to this manager's key range and reading keys before values, so
   * foreign records are never materialized. Best-effort: any failure leaves
   * the records for the next session's sweep.
   */
  private sweepExpired_(): Promise<void> {
    if (
      this.activeRestoreCount_ > 0 ||
      this.restoreQueue_.length > 0 ||
      this.queues_.size > 0
    ) {
      if (!this.disposed_) {
        this.sweepTimer_ = setTimeout(() => {
          void this.sweepExpired_();
        }, 5000);
        (this.sweepTimer_ as { unref?: () => void }).unref?.();
      }
      return Promise.resolve();
    }
    this.sweepTimer_ = null;
    if (this.sweepInFlight_ !== null) {
      return this.sweepInFlight_;
    }
    const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
    const orphanCutoff = Date.now() - PERSISTENCE_ORPHAN_MIN_AGE_MS;
    const prefix = this.key_('');
    let range: IDBKeyRange | undefined;
    try {
      // Not in every embedding (Node test environments) — without it the
      // cursor walks the whole store and filters by prefix in JS.
      range =
        typeof IDBKeyRange !== 'undefined'
          ? IDBKeyRange.bound(prefix, prefix + String.fromCharCode(0xffff))
          : undefined;
    } catch (e) {
      range = undefined;
    }
    const work = this.withStore_<string[]>('readonly', [], (store, done) => {
      const keys: string[] = [];
      // openKeyCursor never materializes values; the value-cursor fallback
      // (test fakes) walks values but only retains keys.
      const keyCursorStore = store as unknown as {
        openKeyCursor?: (range?: IDBKeyRange) => IDBRequest;
      };
      const req =
        typeof keyCursorStore.openKeyCursor === 'function'
          ? keyCursorStore.openKeyCursor(range)
          : store.openCursor(range);
      req.onsuccess = () => {
        const cursor = req.result as IDBCursor | null;
        if (!cursor) {
          done(keys);
          return;
        }
        if (typeof cursor.key === 'string' && cursor.key.startsWith(prefix)) {
          keys.push(cursor.key);
        }
        cursor.continue();
      };
    }).then(keys => {
      if (keys.length === 0) {
        return;
      }
      const baseKeys: string[] = [];
      const suffixedKeys: string[] = [];
      for (const key of keys) {
        if (key.indexOf('#', prefix.length) === -1) {
          baseKeys.push(key);
        } else {
          suffixedKeys.push(key);
        }
      }
      return this.withStore_<void>('readwrite', undefined, store => {
        // Decide each base record (manifests are small), then settle the
        // suffixed records against those decisions — all in one transaction,
        // so a flush cannot interleave between the read and the delete.
        const decisions = new Map<
          string,
          {
            expired: boolean;
            updatedAt: number;
            estimatedBytes: number;
            liveSegKeys: Set<string>;
          }
        >();
        let index = 0;
        const pruneLru = () => {
          const activeKeys = new Set(
            [...this.trackedRoots_].map(path => this.key_(path))
          );
          const live = [...decisions.entries()].filter(([, d]) => !d.expired);
          let totalBytes = live.reduce(
            (sum, [, d]) => sum + d.estimatedBytes,
            0
          );
          if (
            totalBytes <= this.cacheMaxBytes_ &&
            live.length <= PERSISTENCE_MAX_PRUNABLE_ROOTS
          ) {
            return;
          }
          const targetBytes =
            this.cacheMaxBytes_ * PERSISTENCE_PRUNE_TARGET_RATIO;
          const targetRoots = Math.floor(
            PERSISTENCE_MAX_PRUNABLE_ROOTS * PERSISTENCE_PRUNE_TARGET_RATIO
          );
          const candidates = live
            .filter(([key]) => !activeKeys.has(key))
            .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
          let liveRoots = live.length;
          for (const [, decision] of candidates) {
            if (totalBytes <= targetBytes && liveRoots <= targetRoots) {
              break;
            }
            decision.expired = true;
            totalBytes -= decision.estimatedBytes;
            liveRoots--;
          }
        };

        const settleSuffixed = () => {
          for (const key of suffixedKeys) {
            const base = key.slice(0, key.indexOf('#', prefix.length));
            const decision = decisions.get(base);
            const referenced =
              decision !== undefined &&
              !decision.expired &&
              decision.liveSegKeys.has(key);
            if (referenced) {
              continue;
            }
            // Unreferenced. A young segment may belong to a generation
            // another tab is staging RIGHT NOW — for a first generation
            // there is not even a manifest yet — so any '#seg:' record
            // younger than the orphan guard is left for a later sweep. Its
            // id embeds its staging wall clock. (An expired manifest's
            // segments are always older than the manifest's own cutoff-aged
            // timestamp, so this never retains expired content.) Legacy
            // suffixed records ('#range:', '#chunk:', '#hash') have no
            // '#seg:' infix and are reclaimed unconditionally.
            const infix = key.indexOf(SEG_KEY_INFIX, prefix.length);
            if (infix !== -1) {
              const id = key.slice(infix + SEG_KEY_INFIX.length);
              const stampEnd = id.indexOf('.');
              const stamp =
                stampEnd === -1 ? NaN : parseInt(id.slice(0, stampEnd), 36);
              if (!isNaN(stamp) && stamp > orphanCutoff) {
                continue;
              }
            }
            store.delete(key);
          }
        };
        const step = () => {
          if (index >= baseKeys.length) {
            pruneLru();
            for (const [key, decision] of decisions) {
              if (decision.expired) {
                persistenceStats.evictions++;
                store.delete(key);
              }
            }
            settleSuffixed();
            return;
          }
          const key = baseKeys[index++];
          const req = store.get(key);
          req.onsuccess = () => {
            const record = req.result as
              | {
                  formatVersion?: unknown;
                  updatedAt?: unknown;
                  estimatedBytes?: unknown;
                  splits?: unknown;
                }
              | undefined;
            const expired =
              !record ||
              record.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
              typeof record.updatedAt !== 'number' ||
              record.updatedAt < cutoff;
            decisions.set(key, {
              expired,
              updatedAt:
                record && typeof record.updatedAt === 'number'
                  ? record.updatedAt
                  : 0,
              estimatedBytes:
                record && typeof record.estimatedBytes === 'number'
                  ? record.estimatedBytes
                  : 0,
              liveSegKeys:
                record &&
                record.formatVersion === PERSISTENCE_FORMAT_VERSION &&
                Array.isArray(record.splits)
                  ? new Set(
                      (record.splits as PersistedSplit[]).flatMap(split =>
                        Array.isArray(split?.segs)
                          ? split.segs
                              .filter(
                                seg => typeof seg?.id === 'string'
                              )
                              .map(seg => key + SEG_KEY_INFIX + seg.id)
                          : []
                      )
                    )
                  : new Set<string>()
            });
            step();
          };
        };
        step();
      });
    });
    this.sweepInFlight_ = work.finally(() => {
      this.sweepInFlight_ = null;
    });
    return this.sweepInFlight_;
  }
}

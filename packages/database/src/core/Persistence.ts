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
  compoundHashFromNodeAsync,
  estimateSerializedNodeSize
} from './CompoundHash';
import { SeedCompoundHash } from './ServerCacheSeed';
import { ChildrenNode } from './snap/ChildrenNode';
import { KEY_INDEX } from './snap/indexes/KeyIndex';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { Path, pathParent } from './util/Path';

/**
 * Client-side persistence of the server cache, in the spirit of the mobile
 * SDKs' setPersistenceEnabled(true): the SDK itself stores what the server
 * sent for each listened root and restores it on the next startup, so a
 * reload serves cached data immediately and revalidates with the server via
 * the hash protocol (see ServerCacheSeed) instead of re-downloading.
 *
 * Web-specific shape: IndexedDB is asynchronous, so unlike Android's blocking
 * SQLite reads the restore is a promise. The Repo HOLDS each persisted root's
 * outbound listen until its restore settles (bounded below), applies the
 * restored tree as server data (raising the cached events immediately —
 * mobile persistence semantics: cached data is shown, then corrected by the
 * server when it differs), and then sends the listen carrying the restored
 * tree's hashes. An unchanged tree costs a hash handshake; a changed one
 * costs range-merge deltas; a cold root costs exactly today's full download.
 *
 * MEMORY MODEL — why storage is chunked. A root is persisted per top-level
 * listened ROOT (a complete, unfiltered listen), but never as one monolithic
 * value: serializing a large tree in one piece costs a full exported-JSON
 * copy plus a full structured clone in peak memory on every write, and the
 * mirror image on every restore — enough to OOM a memory-constrained mobile
 * tab whose live tree already occupies hundreds of MB. Instead each root is
 * stored as:
 *
 *   - a small MANIFEST record (`<prefix>|<path>`): revision, updatedAt, and
 *     the per-chunk revision join keys — no tree data;
 *   - CHUNK records (`<prefix>|<path>#c<index>`): disjoint subtrees of
 *     roughly PERSISTENCE_CHUNK_TARGET_BYTES each, as arrays of
 *     [relative path, exported JSON] entries (see planChunks);
 *   - a HASH record (`<prefix>|<path>#hash`): the tree's canonical listen
 *     hash and compound hash, recomputed after a write in idle-time slices.
 *
 * Chunking bounds peak memory on both sides — a flush serializes and clones
 * one chunk at a time, a restore parses one chunk at a time into the shared
 * immutable tree — and makes writes INCREMENTAL: nodes are immutable and
 * structurally shared, so a chunk whose subtrees are reference-identical to
 * the previously stored plan is skipped entirely. The common warm-boot flow
 * (restore, listen 'ok' certifying the unchanged tree) therefore writes
 * nothing at all, and a small change to a huge root rewrites only the chunks
 * it touched.
 *
 * Coupling stored hashes to stored trees: every write stamps the manifest
 * with a write token unique across sessions and tabs (`revision`); each
 * chunk carries the token of the write that produced it, and the manifest
 * lists the token expected of every chunk. A restore joins only when all
 * chunk tokens match the manifest (an interrupted or interleaved write
 * degrades to a miss, never to a stitched tree), and joins the hash record
 * only when ITS token matches too — a recompute pending at shutdown restores
 * WITHOUT hashes: the data still paints, the listen just goes out hashless,
 * exactly a cold load for that root.
 *
 * Records written before chunking (a single record with the exported JSON
 * inline) still restore; their next write replaces them with the chunked
 * layout.
 *
 * Storage: one IndexedDB database ('firebase-database-persistence'), one
 * object store, keyed by `<repo prefix>|<path>` plus the '#'-suffixed
 * sidecars above ('#' cannot appear in a path segment). All storage failures
 * degrade to cold loads; nothing here may ever break the live connection.
 */

const STORE = 'firebase-server-cache';
// Version 3 invalidates every cache written before per-chunk transactions.
// The upgrade clears the store inside IndexedDB without materializing the old
// (potentially huge monolithic) values into JavaScript memory.
const PERSISTENCE_DB_VERSION = 3;

/**
 * Records older than this are dropped (staleness makes a full download
 * likely anyway; bounded retention caps disk use).
 */
export const PERSISTENCE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

/**
 * How long after the last server update a root's write-through runs. The
 * flush re-serializes the chunks the update dirtied, so it is deliberately
 * coarse.
 * @internal
 */
export const PERSISTENCE_WRITE_DEBOUNCE_MS = 10000;

/**
 * A restore that hasn't settled by this budget attaches the listen unseeded
 * — persistence may add at most this much latency to a root's FIRST listen,
 * and only when IndexedDB is pathologically slow.
 */
export const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;

/**
 * Target serialized size of one chunk record. Peak transient memory of a
 * flush or restore is a few multiples of THIS (one chunk's exported JSON
 * plus its structured clone), not of the whole root. A single leaf larger
 * than the target still becomes one oversized chunk — leaves cannot split.
 * @internal
 */
export const PERSISTENCE_CHUNK_TARGET_BYTES = 1024 * 1024;

/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the chunks and hash record stay put) — so a tree that
 * never changes but is used daily never ages into the expiry cutoff.
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
 * What a restore resolves: the assembled tree, with the stored hashes joined
 * when they describe exactly this tree.
 */
export interface PersistedRecord {
  node: Node;
  hash?: string;
  compoundHash?: SeedCompoundHash;
  updatedAt: number;
  /** The write token of the manifest this record was assembled from. */
  revision: string;
}

/** The manifest record stored at a root's main key. */
interface PersistedManifest {
  revision: string;
  updatedAt: number;
  chunkCount: number;
  /**
   * The write token each chunk record must carry to belong to this
   * manifest. Chunks untouched by a flush keep their previous token, so the
   * list — not a single manifest-wide token — is the join.
   */
  chunkRevisions: string[];
}

/** The pre-chunking record shape: the whole tree inline. Restore-only. */
interface LegacyPersistedRecord {
  json: unknown;
  updatedAt: number;
  revision: string;
}

/** One chunk record: disjoint subtrees, in assembly order. */
interface PersistedChunk {
  revision: string;
  /** [path relative to the root, exported JSON of the subtree there]. */
  entries: Array<[string, unknown]>;
}

/** The hash follow-up record stored next to a manifest. */
interface PersistedHashRecord {
  hash: string;
  compoundHash: SeedCompoundHash;
  updatedAt: number;
  revision: string;
}

/** One planned chunk: the subtrees it will serialize. */
interface ChunkPlanEntry {
  relPath: string;
  node: Node;
}
type ChunkPlan = ChunkPlanEntry[];

/**
 * What this manager knows IndexedDB currently holds for a root — the state
 * that lets the next flush skip clean chunks (or skip entirely). Seeded by a
 * successful restore or flush; absent when the stored state is unknown, in
 * which case the next flush rewrites everything.
 */
interface FlushedState {
  rootNode: Node;
  revision: string;
  /** null for a restored legacy record (no chunk layout to compare). */
  plans: ChunkPlan[] | null;
  chunkRevisions: string[] | null;
  chunkCount: number | null;
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
  chunksWritten: number;
  chunksSkipped: number;
  hashRecomputes: number;
  evictions: number;
  storageFailures: number;
} = {
  restoredRoots: [],
  restoreMisses: [],
  writeThroughs: 0,
  chunksWritten: 0,
  chunksSkipped: 0,
  hashRecomputes: 0,
  evictions: 0,
  storageFailures: 0
};

const HASH_KEY_SUFFIX = '#hash';
const CHUNK_KEY_INFIX = '#c';

/**
 * Zero-padded so chunk keys sort in index order ('#c000002' < '#c000010'),
 * and below '#hash' ('c' < 'h') — one key range spans exactly a root's
 * chunk records.
 */
function chunkKeySuffix(index: number): string {
  return CHUNK_KEY_INFIX + String(index).padStart(6, '0');
}

function isLegacyRecord(
  record: PersistedManifest | LegacyPersistedRecord
): record is LegacyPersistedRecord {
  return (record as LegacyPersistedRecord).json !== undefined;
}

/**
 * Splits a tree into chunk plans of roughly PERSISTENCE_CHUNK_TARGET_BYTES
 * each: subtrees at or under the target are emitted whole (greedily binned
 * with their siblings), larger ones recurse into their children, with a
 * split node's own priority emitted as a trailing '.priority' entry.
 * Entries are disjoint and in a fixed traversal order, so applying them in
 * sequence over an empty node rebuilds the exact tree — and the same tree
 * always yields the same plan, which is what lets flushes compare plans
 * entry-by-entry against structurally shared previous trees.
 */
function planChunks(root: Node): ChunkPlan[] {
  const plans: ChunkPlan[] = [];
  let current: ChunkPlan = [];
  let currentSize = 0;
  const flushBin = () => {
    if (current.length > 0) {
      plans.push(current);
      current = [];
      currentSize = 0;
    }
  };
  const emit = (relPath: string, node: Node, size: number) => {
    if (currentSize > 0 && currentSize + size > PERSISTENCE_CHUNK_TARGET_BYTES) {
      flushBin();
    }
    current.push({ relPath, node });
    currentSize += size;
    if (currentSize >= PERSISTENCE_CHUNK_TARGET_BYTES) {
      flushBin();
    }
  };
  const walk = (relPath: string, node: Node) => {
    const size = estimateSerializedNodeSize(node);
    if (node.isLeafNode() || size <= PERSISTENCE_CHUNK_TARGET_BYTES) {
      emit(relPath, node, size);
      return;
    }
    node.forEachChild(KEY_INDEX, (key, child) => {
      walk(relPath === '' ? key : relPath + '/' + key, child);
    });
    const priority = node.getPriority();
    if (!priority.isEmpty()) {
      emit(
        relPath === '' ? '.priority' : relPath + '/.priority',
        priority,
        estimateSerializedNodeSize(priority)
      );
    }
  };
  walk('', root);
  flushBin();
  return plans;
}

function samePlan(a: ChunkPlan, b: ChunkPlan): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i].relPath !== b[i].relPath || a[i].node !== b[i].node) {
      return false;
    }
  }
  return true;
}

/**
 * The result of reading a root's stored state, before expiry filtering.
 */
interface ReadResult {
  record: PersistedRecord;
  plans: ChunkPlan[] | null;
  chunkRevisions: string[] | null;
  chunkCount: number | null;
}

/**
 * One PersistenceManager per Repo. `prefix` namespaces records so multiple
 * databases/apps sharing the page don't collide. `idbFactory` exists for
 * tests (Node has no IndexedDB); production uses the global.
 */
export class PersistenceManager {
  private db_: Promise<IDBDatabase | null> | null = null;
  /**
   * Roots that flow through persistence (complete default listens).
   */
  private trackedRoots_ = new Set<string>();
  /**
   * Latest server tree per root. Revisions come from a single manager-wide
   * counter, so no revision is ever reissued — an in-flight hash recompute
   * can never collide with a tree that arrived after its root was evicted
   * and re-tracked.
   */
  private latest_ = new Map<string, { node: Node; revision: string }>();
  /**
   * What IndexedDB currently holds per root (see FlushedState) — the basis
   * for skipping clean chunks and no-op flushes.
   */
  private lastFlush_ = new Map<string, FlushedState>();
  /**
   * Distinguishes this manager's write tokens from every other tab's and
   * session's — numeric counters restart at zero on reload, which let a new
   * data write pair up with a surviving old hash sidecar.
   */
  private instanceId_ = Math.random().toString(36).slice(2, 10);
  private writeCounter_ = 0;
  private writeTimers_ = new Map<string, ReturnType<typeof setTimeout>>();
  /**
   * Roots whose throttle fired while their queue was busy: exactly one
   * flush is re-enqueued when the queue drains, however many intervals
   * elapsed meanwhile — the queue can never grow faster than it drains.
   */
  private flushPending_ = new Set<string>();
  /** In-flight storage operations per root (see enqueue_). */
  private queues_ = new Map<string, Promise<void>>();
  private sweepTimer_: ReturnType<typeof setTimeout> | null = null;
  private disposed_ = false;

  constructor(
    private prefix_: string,
    private idbFactory_: IDBFactory | null = isIndexedDBAvailable()
      ? indexedDB
      : null
  ) {}

  /**
   * A replacement manager for a different key prefix — used when emulator
   * configuration changes the RepoInfo after persistence was enabled but
   * before the repo started (no queues or tracked roots exist yet).
   */
  rebindTo(prefix: string): PersistenceManager {
    this.dispose();
    return new PersistenceManager(prefix, this.idbFactory_);
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
    this.db_ = this.openAtVersion_(undefined).then(db => {
      if (db === null) {
        return null;
      }
      // One-time migration away from monolithic / shared-transaction cache
      // formats. Close and upgrade BEFORE any get(): clearing in the version
      // change transaction drops the old values inside IndexedDB, without
      // structured-cloning them into the WebKit heap (which is exactly what
      // crashed large legacy accounts during restore).
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
    }).then(db => {
      if (db !== null && !db.objectStoreNames.contains(STORE)) {
        persistenceStats.storageFailures++;
        db.close();
        return null;
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

  /**
   * Test seam: runs the deferred expiry sweep immediately.
   * @internal
   */
  sweepNow(): Promise<void> {
    if (this.sweepTimer_ !== null) {
      clearTimeout(this.sweepTimer_);
    }
    return this.sweepExpired_();
  }

  /**
   * Deletes this manager's expired records (see PERSISTENCE_MAX_AGE_MS).
   * Expiry is decided by each root's manifest (or legacy record): the
   * '#'-suffixed chunk and hash records carry no authority of their own and
   * are dropped exactly when their manifest is dropped, is missing (orphans
   * from an interrupted write), or no longer lists them. Scoped to this
   * manager's key range and reading keys before values, where the platform
   * allows, so foreign records are never materialized. Best-effort: any
   * failure leaves the records for the next session's sweep.
   */
  private sweepExpired_(): Promise<void> {
    const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
    const prefix = this.key_('');
    let range: IDBKeyRange | undefined;
    try {
      // Not in every embedding (Node test environments) — without it the
      // cursor walks the whole store and filters by prefix in JS.
      range =
        typeof IDBKeyRange !== 'undefined'
          ? IDBKeyRange.bound(prefix, prefix + '\uffff')
          : undefined;
    } catch (e) {
      range = undefined;
    }
    return this.withStore_<string[]>('readonly', [], (store, done) => {
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
        // Decide each base record (manifests are tiny; legacy records
        // materialize one at a time), then settle the suffixed records
        // against those decisions — all in one transaction, so a flush
        // cannot interleave between the read and the delete.
        const decisions = new Map<
          string,
          { expired: boolean; chunkCount: number }
        >();
        let index = 0;
        const settleSuffixed = () => {
          for (const key of suffixedKeys) {
            const base = key.slice(0, key.indexOf('#', prefix.length));
            const decision = decisions.get(base);
            let drop = decision === undefined || decision.expired;
            if (!drop && key.startsWith(base + CHUNK_KEY_INFIX)) {
              const chunkIndex = parseInt(
                key.slice(base.length + CHUNK_KEY_INFIX.length),
                10
              );
              if (
                !Number.isFinite(chunkIndex) ||
                chunkIndex >= decisions.get(base)!.chunkCount
              ) {
                drop = true;
              }
            }
            if (drop) {
              store.delete(key);
            }
          }
        };
        const step = () => {
          if (index >= baseKeys.length) {
            settleSuffixed();
            return;
          }
          const key = baseKeys[index++];
          const req = store.get(key);
          req.onsuccess = () => {
            const record = req.result as
              | { updatedAt?: unknown; chunkCount?: unknown }
              | undefined;
            const expired =
              !record ||
              typeof record.updatedAt !== 'number' ||
              record.updatedAt < cutoff;
            decisions.set(key, {
              expired,
              chunkCount:
                record && typeof record.chunkCount === 'number'
                  ? record.chunkCount
                  : 0
            });
            if (expired) {
              persistenceStats.evictions++;
              store.delete(key);
            }
            step();
          };
        };
        step();
      });
    });
  }

  private openAtVersion_(
    version: number | undefined
  ): Promise<IDBDatabase | null> {
    return new Promise(resolve => {
      if (!this.idbFactory_) {
        resolve(null);
        return;
      }
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
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => {
          persistenceStats.storageFailures++;
          resolve(null);
        };
        req.onblocked = () => resolve(null);
      } catch (e) {
        persistenceStats.storageFailures++;
        resolve(null);
      }
    });
  }

  private key_(pathString: string): string {
    return this.prefix_ + '|' + pathString;
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
    body: (store: IDBObjectStore, done: (value: T) => void) => void
  ): Promise<T> {
    return this.open_().then(
      db =>
        new Promise<T>(resolve => {
          if (!db) {
            resolve(fallback);
            return;
          }
          try {
            const tx = db.transaction(STORE, mode);
            let value = fallback;
            body(tx.objectStore(STORE), v => {
              value = v;
            });
            tx.oncomplete = () => resolve(value);
            tx.onabort = tx.onerror = () => {
              persistenceStats.storageFailures++;
              resolve(fallback);
            };
          } catch (e) {
            persistenceStats.storageFailures++;
            resolve(fallback);
          }
        })
    );
  }

  /**
   * Reads a root's stored state in one readonly transaction: the manifest
   * and hash records first, then — for chunked records — each chunk in
   * sequence, folded into the assembled tree as it arrives so only one
   * chunk's parsed JSON is ever held at a time. The hash record joins only
   * when its revision matches the manifest's; a chunk whose revision doesn't
   * match the manifest's expectation (an interrupted or foreign write)
   * resolves null, and the leftovers are deleted best-effort. Expired
   * records also resolve null (and are deleted best-effort).
   */
  private readRecord_(pathString: string): Promise<ReadResult | null> {
    const key = this.key_(pathString);
    // Metadata first, in a short transaction. Each chunk then gets its OWN
    // transaction: WebKit may retain every IDBRequest result until the
    // transaction closes, so issuing all chunk reads in one transaction
    // recreates the root-sized memory spike despite the chunked records.
    const metadata = this.withStore_<{
      stored: PersistedManifest | LegacyPersistedRecord;
      hashes?: PersistedHashRecord;
    } | null>('readonly', null, (store, done) => {
      const dataReq = store.get(key);
      const hashReq = store.get(key + HASH_KEY_SUFFIX);
      hashReq.onsuccess = () => {
        const stored = dataReq.result as
          | PersistedManifest
          | LegacyPersistedRecord
          | undefined;
        if (!stored) {
          done(null);
          return;
        }
        done({
          stored,
          hashes: hashReq.result as PersistedHashRecord | undefined
        });
      };
    });
    return metadata
      .then(meta => {
        if (meta === null) {
          return null;
        }
        const { stored, hashes } = meta;
        const joined =
          hashes && hashes.revision === stored.revision
            ? { hash: hashes.hash, compoundHash: hashes.compoundHash }
            : {};
        if (isLegacyRecord(stored)) {
          return {
            record: {
              node: nodeFromJSON(stored.json),
              ...joined,
              updatedAt: stored.updatedAt,
              revision: stored.revision
            },
            plans: null,
            chunkRevisions: null,
            chunkCount: null
          } as ReadResult;
        }
        const manifest = stored;
        if (
          typeof manifest.chunkCount !== 'number' ||
          manifest.chunkCount <= 0 ||
          !Array.isArray(manifest.chunkRevisions) ||
          manifest.chunkRevisions.length !== manifest.chunkCount
        ) {
          return 'mismatch' as const;
        }
        let assembled: Node = ChildrenNode.EMPTY_NODE;
        const plans: ChunkPlan[] = [];
        let chain = Promise.resolve<true | 'mismatch'>(true);
        for (let index = 0; index < manifest.chunkCount; index++) {
          chain = chain.then(status => {
            if (status === 'mismatch') {
              return status;
            }
            return this.withStore_<PersistedChunk | null>(
              'readonly',
              null,
              (store, done) => {
                const req = store.get(key + chunkKeySuffix(index));
                req.onsuccess = () =>
                  done((req.result as PersistedChunk | undefined) ?? null);
              }
            ).then(chunk => {
              if (
                !chunk ||
                chunk.revision !== manifest.chunkRevisions[index] ||
                !Array.isArray(chunk.entries)
              ) {
                return 'mismatch' as const;
              }
              const plan: ChunkPlan = [];
              for (const [relPath, json] of chunk.entries) {
                const node = nodeFromJSON(json);
                assembled = assembled.updateChild(new Path(relPath), node);
                plan.push({ relPath, node });
              }
              plans.push(plan);
              return true as const;
            });
          });
        }
        return chain.then(status =>
          status === 'mismatch'
            ? status
            : ({
                record: {
                  node: assembled,
                  ...joined,
                  updatedAt: manifest.updatedAt,
                  revision: manifest.revision
                },
                plans,
                chunkRevisions: manifest.chunkRevisions,
                chunkCount: manifest.chunkCount
              } as ReadResult)
        );
      })
      .then(result => {
        if (result === null) {
          return null;
        }
        if (result === 'mismatch') {
          persistenceStats.evictions++;
          void this.deleteRecord_(pathString);
          return null;
        }
        if (Date.now() - result.record.updatedAt > PERSISTENCE_MAX_AGE_MS) {
          persistenceStats.evictions++;
          void this.deleteRecord_(pathString);
          return null;
        }
        return result;
      });
  }

  /**
   * Deletes everything stored for a root: manifest, hash record, and every
   * chunk the manifest lists (plus, where the platform provides key ranges,
   * any orphaned chunk tail beyond it).
   */
  private deleteRecord_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, store => {
      const req = store.get(key);
      req.onsuccess = () => {
        const record = req.result as { chunkCount?: unknown } | undefined;
        const chunkCount =
          record && typeof record.chunkCount === 'number'
            ? record.chunkCount
            : 0;
        store.delete(key);
        store.delete(key + HASH_KEY_SUFFIX);
        for (let i = 0; i < chunkCount; i++) {
          store.delete(key + chunkKeySuffix(i));
        }
        if (typeof IDBKeyRange !== 'undefined') {
          try {
            // Orphans beyond the manifest's count (interrupted older
            // writes); the sweep also reclaims these eventually.
            store.delete(
              IDBKeyRange.bound(
                key + CHUNK_KEY_INFIX,
                key + CHUNK_KEY_INFIX + '\uffff'
              )
            );
          } catch (e) {
            // Key-range deletes are an optimization, never a requirement.
          }
        }
      };
    });
  }

  /**
   * Restores the persisted record for a root. Resolves null on miss, expiry,
   * storage failure, or timeout — the caller then attaches unseeded. A hit
   * also primes the flush-skip state: the store is KNOWN to hold exactly
   * this tree, so when the server certifies it unchanged (the common warm
   * boot), the follow-up write-through skips without serializing anything.
   */
  restore(pathString: string): Promise<PersistedRecord | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    const read = this.readRecord_(pathString).then(result => {
      if (result !== null && !this.disposed_) {
        this.lastFlush_.set(pathString, {
          rootNode: result.record.node,
          revision: result.record.revision,
          plans: result.plans,
          chunkRevisions: result.chunkRevisions,
          chunkCount: result.chunkCount,
          storedUpdatedAt: result.record.updatedAt
        });
      }
      return result === null ? null : result.record;
    });
    return this.raceRestoreTimeout_(read).then(record => {
      if (record) {
        persistenceStats.restoredRoots.push(pathString);
      } else {
        persistenceStats.restoreMisses.push(pathString);
      }
      return record;
    });
  }

  /**
   * The boot-peek read (see getPersistedValue): resolves the record of the
   * FRESHEST persisted ancestor of `pathString` (or of the path itself; ties
   * go to the deepest). Freshness decides because ancestors keep flushing
   * after a covered child's record froze — the deepest record is not
   * necessarily the current one. Reads the ancestor chain's manifests in one
   * transaction, then assembles only the chosen root. Expired ancestors are
   * skipped. Does not touch the restore counters or the flush-skip state —
   * a peek is not a listen restore.
   */
  restoreNearest(
    pathString: string
  ): Promise<{ root: string; record: PersistedRecord } | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    // Deepest first: the path itself, then each ancestor up to the root.
    const candidates: string[] = [];
    let path: Path | null = new Path(pathString);
    while (path !== null) {
      candidates.push(path.toString());
      path = pathParent(path);
    }
    const read = this.withStore_<Array<{ updatedAt: number } | undefined>>(
      'readonly',
      [],
      (store, done) => {
        const results: Array<{ updatedAt: number } | undefined> = new Array(
          candidates.length
        );
        let remaining = candidates.length;
        candidates.forEach((candidate, i) => {
          const req = store.get(this.key_(candidate));
          req.onsuccess = () => {
            results[i] = req.result as { updatedAt: number } | undefined;
            if (--remaining === 0) {
              done(results);
            }
          };
        });
      }
    ).then(results => {
      const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
      let best: number | null = null;
      for (let i = 0; i < results.length; i++) {
        const record = results[i];
        if (!record || typeof record.updatedAt !== 'number') {
          continue;
        }
        if (record.updatedAt < cutoff) {
          persistenceStats.evictions++;
          void this.deleteRecord_(candidates[i]);
          continue;
        }
        if (best === null || record.updatedAt > results[best]!.updatedAt) {
          best = i;
        }
      }
      if (best === null) {
        return null;
      }
      const root = candidates[best];
      return this.readRecord_(root).then(result =>
        result === null ? null : { root, record: result.record }
      );
    });
    return this.raceRestoreTimeout_(read);
  }

  /**
   * Bounds a read by PERSISTENCE_RESTORE_TIMEOUT_MS, clearing the timer as
   * soon as the read settles first (the common case — otherwise every
   * restore would pin its Repo in memory for the full budget).
   */
  private raceRestoreTimeout_<T>(read: Promise<T | null>): Promise<T | null> {
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<null>(resolve => {
      timer = setTimeout(() => resolve(null), PERSISTENCE_RESTORE_TIMEOUT_MS);
    });
    return Promise.race([read, timeout]).then(result => {
      clearTimeout(timer);
      return result;
    });
  }

  /**
   * Write-through: the server confirmed `node` as the state of the tracked
   * root `path`. Throttled per root; hashes recompute afterwards in idle
   * slices against the same revision. A tree the store is known to already
   * hold — the warm boot's listen-'ok' certifying the restored tree
   * unchanged — is skipped outright unless its stored timestamp needs a
   * refresh (see PERSISTENCE_REFRESH_AGE_MS).
   */
  serverCacheUpdated(path: Path, node: Node): void {
    if (this.disposed_) {
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
      revision: this.instanceId_ + '-' + (++this.writeCounter_).toString(36)
    });
    // Trailing throttle, NOT a resetting debounce: the timer set by the first
    // update in a burst survives later updates, so a root that churns faster
    // than the interval (a chat streaming, an editing session) still flushes
    // every interval instead of never. The flush reads latest_ when it runs,
    // so it always writes the newest tree.
    if (!this.writeTimers_.has(pathString)) {
      this.writeTimers_.set(
        pathString,
        setTimeout(() => {
          this.writeTimers_.delete(pathString);
          this.scheduleFlush_(pathString);
        }, PERSISTENCE_WRITE_DEBOUNCE_MS)
      );
    }
  }

  /**
   * Enqueues a flush unless the root's queue is still working — then one
   * flush is marked pending and enqueued when the queue drains. Without the
   * mark, a root whose flush takes longer than the throttle interval would
   * queue flushes faster than they complete, unboundedly.
   */
  private scheduleFlush_(pathString: string): void {
    if (this.queues_.has(pathString)) {
      this.flushPending_.add(pathString);
      return;
    }
    void this.enqueue_(pathString, () => this.flush_(pathString));
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
    this.latest_.clear();
    this.lastFlush_.clear();
  }

  /**
   * Test seam: forces a pending throttled flush to run now.
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
   * time: a flush's manifest, chunks, and hash record land as a couple
   * before the next flush or delete for that root starts, which is the
   * whole storage consistency argument — no cross-operation races to
   * reason about.
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

  private flush_(pathString: string): Promise<void> {
    const entry = this.latest_.get(pathString);
    if (!entry || this.disposed_) {
      return Promise.resolve();
    }
    const { node, revision } = entry;
    const prev = this.lastFlush_.get(pathString);
    const now = Date.now();
    if (
      prev &&
      prev.rootNode === node &&
      now - prev.storedUpdatedAt < PERSISTENCE_REFRESH_AGE_MS
    ) {
      // The store already holds exactly this tree, freshly enough.
      return Promise.resolve();
    }
    const key = this.key_(pathString);
    const plans = planChunks(node);
    const chunkRevisions: string[] = [];
    const dirtyIndexes: number[] = [];
    for (let i = 0; i < plans.length; i++) {
      const prevPlan =
        prev !== undefined &&
        prev.plans !== null &&
        prev.chunkRevisions !== null &&
        i < prev.plans.length
          ? prev.plans[i]
          : null;
      if (prevPlan !== null && samePlan(prevPlan, plans[i])) {
        chunkRevisions.push(prev!.chunkRevisions![i]);
      } else {
        chunkRevisions.push(revision);
        dirtyIndexes.push(i);
      }
    }
    persistenceStats.chunksWritten += dirtyIndexes.length;
    persistenceStats.chunksSkipped += plans.length - dirtyIndexes.length;
    if (
      dirtyIndexes.length === 0 &&
      prev !== undefined &&
      prev.plans !== null &&
      plans.length === prev.plans.length
    ) {
      // Content-identical to the stored state; only the timestamp is stale.
      // Rewrite the manifest alone, KEEPING the previous revision so the
      // stored hash record stays joined to it.
      const manifest: PersistedManifest = {
        revision: prev.revision,
        updatedAt: now,
        chunkCount: plans.length,
        chunkRevisions: prev.chunkRevisions!
      };
      return this.withStore_<boolean>('readwrite', false, (store, done) => {
        store.put(manifest, key);
        done(true);
      }).then(ok => {
        if (ok && !this.disposed_) {
          this.lastFlush_.set(pathString, {
            ...prev,
            rootNode: node,
            plans,
            storedUpdatedAt: now
          });
        }
      });
    }
    persistenceStats.writeThroughs++;
    // Chunks and manifest land in ONE transaction — all or nothing — with
    // each chunk serialized only when its put is issued, so peak transient
    // memory is one chunk's exported JSON plus its structured clone. The
    // tree is written hashless first — a crash before the recompute leaves
    // a restorable tree that seeds without a hash instead of nothing. The
    // hashes follow as a small separate record, and because flushes for a
    // root are serialized (enqueue_), the pair is always coupled: the hash
    // written here describes the manifest written here, even if newer
    // updates arrived while hashing.
    const manifest: PersistedManifest = {
      revision,
      updatedAt: now,
      chunkCount: plans.length,
      chunkRevisions
    };
    const prevChunkCount = prev !== undefined ? prev.chunkCount : null;
    // One transaction PER dirty chunk. WebKit may retain every put's
    // structured-clone input until its transaction closes; one transaction
    // for all chunks therefore retained a root-sized exported JSON graph and
    // defeated chunking. The manifest is committed last in a tiny transaction
    // and is the authoritative join, so a crash between chunks is a safe miss.
    let put = Promise.resolve(true);
    for (const i of dirtyIndexes) {
      put = put.then(ok => {
        if (!ok) {
          return false;
        }
        const chunk: PersistedChunk = {
          revision,
          entries: plans[i].map(e => [e.relPath, e.node.val(true)])
        };
        return this.withStore_<boolean>('readwrite', false, (store, done) => {
          store.put(chunk, key + chunkKeySuffix(i));
          done(true);
        });
      });
    }
    put = put.then(ok => {
      if (!ok) {
        return false;
      }
      return this.withStore_<boolean>('readwrite', false, (store, done) => {
        store.put(manifest, key);
        if (prevChunkCount !== null) {
          for (let i = plans.length; i < prevChunkCount; i++) {
            store.delete(key + chunkKeySuffix(i));
          }
        } else if (typeof IDBKeyRange !== 'undefined') {
          try {
            store.delete(
              IDBKeyRange.bound(
                key + chunkKeySuffix(plans.length),
                key + CHUNK_KEY_INFIX + '\uffff'
              )
            );
          } catch (e) {
            // Key-range deletes are an optimization, never a requirement.
          }
        }
        store.delete(key + HASH_KEY_SUFFIX);
        done(true);
      });
    });
    return put.then(ok => {
      if (!ok || this.disposed_) {
        return;
      }
      this.lastFlush_.set(pathString, {
        rootNode: node,
        revision,
        plans,
        chunkRevisions,
        chunkCount: plans.length,
        storedUpdatedAt: now
      });
      // A compound hash is sufficient for zero-download revalidation:
      // send an empty simple hash and let the server compare ranges. Avoiding
      // node.hash() is crucial on large roots — it permanently cached one SHA
      // string on every node, a large retained-memory jump absent on a normal
      // cold load.
      return compoundHashFromNodeAsync(node).then(compoundHash => {
        const hash = '';
        if (this.disposed_) {
          return;
        }
        persistenceStats.hashRecomputes++;
        const hashRecord: PersistedHashRecord = {
          hash,
          compoundHash: {
            hashes: compoundHash.hashes,
            posts: compoundHash.posts
          },
          updatedAt: now,
          revision
        };
        return this.withStore_<void>('readwrite', undefined, store => {
          // Another tab may have replaced the manifest while we hashed;
          // only attach the hash if the manifest still carries OUR token
          // (the read and the put share one transaction, so this is
          // atomic).
          const dataReq = store.get(key);
          dataReq.onsuccess = () => {
            const current = dataReq.result as { revision?: string } | undefined;
            if (current && current.revision === revision) {
              store.put(hashRecord, key + HASH_KEY_SUFFIX);
            }
          };
        });
      });
    });
  }
}

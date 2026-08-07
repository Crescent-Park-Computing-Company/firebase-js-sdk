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

import { base64, isIndexedDBAvailable } from '@firebase/util';

import {
  CompoundHashBuilder,
  StableRange,
  collectChangedSubtreePaths,
  estimateSerializedNodeSize,
  markDirtyRanges,
  rebuildStableRanges,
  simpleSizeSplitStrategy
} from './CompoundHash';
import { SeedCompoundHash, stampSeedValue } from './ServerCacheSeed';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { Path } from './util/Path';
import { sha1 } from './util/util';

/**
 * Client-side persistence of the server cache, in the spirit of the mobile
 * SDKs' setPersistenceEnabled(true): the SDK itself stores what the server
 * sent for each listened root and restores it on the next startup, so a
 * reload serves cached data immediately and revalidates with the server via
 * the hash protocol (see ServerCacheSeed) instead of re-downloading.
 *
 * STORAGE MODEL — one generation, two records, one transaction. Each
 * persisted root stores:
 *
 *   - a MANIFEST record (`<prefix>|<path>`): revision, timestamps, auth
 *     scope, and the root's STABLE RANGES — the compound-hash posts, hashes,
 *     and sizes describing exactly the stored tree. Tiny (~a few hundred KB
 *     for a 61MB root), reads in milliseconds.
 *   - a TREE record (`<prefix>|<path>#tree`): the full export-format plain
 *     tree, stored via structured clone — no JSON.stringify on write, no
 *     JSON.parse on read; the engine owns serialization.
 *
 * Both records are written in ONE IndexedDB transaction, so a committed
 * generation is atomic by construction: an interrupted flush leaves the
 * previous generation intact. There is no copy-on-write machinery, no
 * revision joins across records beyond the single manifest↔tree pair, and
 * no per-record checksums — a semantically stale cache is self-healing (the
 * server returns range merges for whatever differs), and a structurally
 * broken one degrades to a cache miss and a full listener.
 *
 * MANIFEST-FIRST BOOT. Because the manifest alone carries the protocol
 * hashes, a warm boot reads it first and hands the hashes to the caller
 * (see restoreForListen's onManifest) so the range listen can be sent
 * IMMEDIATELY — the server round-trip overlaps the tree record's read and
 * Node construction. A warm boot computes NO hashes.
 *
 * SELF-CONSISTENT GENERATIONS. Range hashes are maintained at write time,
 * inside the flush: an identity-diff of the immutable trees marks the
 * ranges a change dirtied, only those ranges are re-serialized and
 * re-hashed (between preserved boundary posts — see CompoundHash's stable
 * ranges), and the manifest commits with hashes that exactly describe the
 * tree record beside it. Clean ranges carry over without their bytes ever
 * being read. Stale hashes are never persisted: a stale hash could falsely
 * match a reverted server range, the one corruption the range handshake
 * cannot self-heal.
 *
 * WRITE POLICY — single-flight coalescing flush. The first change after a
 * committed generation arms a NON-restarting timer (writeDelayMs, default
 * PERSISTENCE_WRITE_DEBOUNCE_MS); later changes coalesce into the pending
 * window without resetting it. At most one flush is ever in flight; changes
 * landing mid-flush only re-arm the next window. Effective cadence is
 * max(delay, flush duration) — natural backpressure, bounded staleness.
 * Cache writes are never awaited by the UI, certification, or navigation.
 *
 * All storage failures degrade to cold loads; nothing here may ever break
 * the live connection.
 */

const STORE = 'firebase-server-cache';
// Version 3 invalidated pre-chunking caches; version 8 is current. The
// upgrade clears the store inside IndexedDB without materializing old
// (potentially huge) values into JavaScript memory.
const PERSISTENCE_DB_VERSION = 8;
// Format 10: single structured-clone tree record + stable-range manifest.
// Records written by earlier formats (chunked or monolithic) fail the
// format check, restore as a miss, and are reclaimed by the sweep.
const PERSISTENCE_FORMAT_VERSION = 10;
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
export const PERSISTENCE_WRITE_DEBOUNCE_MS = 1000;

/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed manifest or tree read) resets this budget. The same
 * bound applies to each IndexedDB open/transaction, so a request that fires
 * neither success nor error can never hold the live listen forever.
 */
export const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;

/** How long a completed optimistic peek waits for its real listener. */
const PERSISTENCE_PEEK_HANDOFF_MS = 30000;

/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the tree record stays put) — so a tree that never
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

/**
 * The protocol hashes of a committed generation, as handed to
 * restoreForListen's onManifest callback — everything a range listen needs,
 * available long before the tree record has been read.
 */
export interface PersistedSeedHashes {
  hash: string;
  compoundHash: SeedCompoundHash;
}

/** The manifest record stored at a root's main key. */
interface PersistedManifest {
  formatVersion: number;
  revision: string;
  updatedAt: number;
  authScope: string | null;
  estimatedBytes: number;
  /**
   * True when the stored tree contains no priorities anywhere — then the
   * export-format tree is byte-identical to the plain val() tree, and the
   * restored record can hand the stored object to the application by
   * reference (see stampSeedValue). Maintained inductively: the full
   * first-generation walk observes every node, and later dirty walks
   * observe every CHANGED node — priorities cannot appear in unchanged
   * subtrees.
   */
  priorityFree: boolean;
  /** The root's simple hash ('' for compound-only seeds; see ServerCacheSeed). */
  hash: string;
  /** The stable ranges describing exactly the tree record beside this. */
  ranges: StableRange[];
}

/** The tree record: the full export-format tree, structured-clone stored. */
interface PersistedTreeRecord {
  revision: string;
  tree: unknown;
}

/**
 * What this manager knows IndexedDB currently holds for a root — the basis
 * for identity-diff dirty marking and no-op flush skipping. Seeded by a
 * successful restore or flush.
 */
interface FlushedState {
  rootNode: Node;
  revision: string;
  ranges: StableRange[];
  priorityFree: boolean;
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
  rangesHashed: number;
  rangesReused: number;
  evictions: number;
  storageFailures: number;
  events: Array<{ at: number; path: string; event: string; detail?: string }>;
} = {
  restoredRoots: [],
  restoreMisses: [],
  writeThroughs: 0,
  rangesHashed: 0,
  rangesReused: 0,
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

const TREE_KEY_SUFFIX = '#tree';

function wireCompoundHashFromRanges(ranges: StableRange[]): SeedCompoundHash {
  const posts: string[] = [];
  const hashes: string[] = [];
  for (const range of ranges) {
    posts.push(range.post);
    hashes.push(range.hash);
  }
  // The empty tail hash lets the server append past the last post.
  hashes.push('');
  return { posts, hashes };
}

/**
 * Hashes the dirty ranges' serialized texts — WebCrypto where available
 * (native, off the JavaScript thread, ~40x the JS implementation), the
 * synchronous JS sha1 otherwise (Node without webcrypto, insecure contexts).
 * Falls back wholesale on any WebCrypto failure: a flush must never fail on
 * the choice of hash backend.
 */
function digestRangeTexts(texts: string[]): Promise<string[]> {
  if (texts.length === 0) {
    return Promise.resolve([]);
  }
  const subtle =
    typeof crypto !== 'undefined' &&
    typeof TextEncoder !== 'undefined' &&
    crypto.subtle &&
    typeof crypto.subtle.digest === 'function'
      ? crypto.subtle
      : null;
  if (subtle === null) {
    return Promise.resolve(texts.map(sha1));
  }
  const encoder = new TextEncoder();
  return Promise.all(
    texts.map(text =>
      subtle
        .digest('SHA-1', encoder.encode(text))
        .then(digest => base64.encodeByteArray(new Uint8Array(digest)))
    )
  ).catch(() => texts.map(sha1));
}

/** The joined result of one physical manifest+tree read. */
interface ReadResult {
  record: PersistedRecord;
  ranges: StableRange[];
  priorityFree: boolean;
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
   * Distinguishes this manager's write tokens from every other tab's and
   * session's — numeric counters restart at zero on reload, which would let
   * a new data write pair up with a write token from another manager
   * instance.
   */
  private instanceId_ = Math.random().toString(36).slice(2, 10);
  private writeCounter_ = 0;
  /**
   * The single-flight coalescing window per root: `timer` is the pending
   * (non-restarting) window; `rearm` marks a change that landed while the
   * root's queue was busy flushing — exactly one follow-up window is armed
   * when the queue drains, however many changes landed meanwhile.
   */
  private writeTimers_ = new Map<string, ReturnType<typeof setTimeout>>();
  private flushPending_ = new Set<string>();
  /** In-flight storage operations per root (see enqueue_). */
  private queues_ = new Map<string, Promise<void>>();
  /**
   * One physical IndexedDB decode per root. The pre-auth peek and the
   * authenticated listener often overlap; without coalescing they each read
   * the tree record and rebuilt the same large Node tree concurrently.
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
  private disposed_ = false;
  private authScope_: string | null = null;
  private authGeneration_ = 0;

  setAuthScope(scope: string | null): boolean {
    if (scope === this.authScope_) {
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
    private writeDelayMs_: number = PERSISTENCE_WRITE_DEBOUNCE_MS
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
    this.dispose();
    const rebound = new PersistenceManager(
      prefix,
      this.idbFactory_,
      this.schemaKnownCurrent_,
      this.operationTimeoutMs_,
      this.cacheMaxBytes_
    );
    rebound.setAuthScope(scope);
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
   * Expiry is decided by each root's manifest: the '#'-suffixed tree record
   * carries no authority of its own and is dropped exactly when its
   * manifest is dropped, is missing (an orphan), or belongs to a different
   * revision. Scoped to this manager's key range and reading keys before
   * values, where the platform allows, so foreign records are never
   * materialized. Best-effort: any failure leaves the records for the next
   * session's sweep.
   */
  private sweepExpired_(): Promise<void> {
    if (this.activeRestoreCount_ > 0 || this.restoreQueue_.length > 0) {
      if (!this.disposed_) {
        this.sweepTimer_ = setTimeout(() => {
          void this.sweepExpired_();
        }, 5000);
        (this.sweepTimer_ as { unref?: () => void }).unref?.();
      }
      return Promise.resolve();
    }
    this.sweepTimer_ = null;
    const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
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
        // Decide each base record (manifests are small), then settle the
        // suffixed records against those decisions — all in one transaction,
        // so a flush cannot interleave between the read and the delete.
        const decisions = new Map<
          string,
          {
            expired: boolean;
            updatedAt: number;
            estimatedBytes: number;
            revision: string | null;
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
            // Everything suffixed that is not the current format's live tree
            // record — legacy chunk/hash sidecars included — is reclaimed
            // with (or without) its manifest.
            const drop =
              decision === undefined ||
              decision.expired ||
              key !== base + TREE_KEY_SUFFIX;
            if (drop) {
              store.delete(key);
            }
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
                  revision?: unknown;
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
              revision:
                record && typeof record.revision === 'string'
                  ? record.revision
                  : null
            });
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
            body(tx.objectStore(STORE), v => {
              value = v;
            });
            timer = setTimeout(() => {
              // Abort the one stalled transaction so an abandoned cache read
              // cannot continue assembling a large tree beside the cold
              // network fallback.
              try {
                tx.abort();
              } catch (e) {
                // It may have completed between the timer firing and abort().
              }
              finish(fallback, true);
            }, this.operationTimeoutMs_);
            tx.oncomplete = () => finish(value);
            tx.onabort = tx.onerror = () => finish(fallback, true);
          } catch (e) {
            finish(fallback, true);
          }
        })
    );
  }

  /**
   * Reads a root's stored state: the manifest in a first short transaction —
   * validated and surfaced to `onManifest` IMMEDIATELY, so a listen carrying
   * the stored hashes can be on the wire while the tree record is still
   * loading — then the tree record, decoded into a Node and joined with the
   * manifest's hashes. A revision mismatch between the two (an interrupted
   * or foreign write; single-transaction commits make this near-impossible,
   * but the check is cheap) resolves null. Expired or format-mismatched
   * records resolve null and are deleted best-effort.
   */
  private readRecord_(
    pathString: string,
    onProgress: () => void = () => {},
    retainAfterResolve = false,
    expectedAuthScope: string | null = this.authScope_,
    onManifest: (hashes: PersistedSeedHashes) => void = () => {}
  ): Promise<ReadResult | null> {
    const active = this.activeReads_.get(pathString);
    if (active) {
      active.progress.add(onProgress);
      // Joining an already-progressing read is itself progress for this
      // caller's idle timeout. The manifest callback intentionally does not
      // replay for joiners: the first caller's listen already went out; a
      // joining listener consumes the joined record's hashes on resolve.
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
    const promise = this.readRecordOnce_(
      pathString,
      emitProgress,
      expectedAuthScope,
      onManifest
    );
    const entry = {
      promise,
      progress,
      retainAfterResolve,
      cleanupTimer: null as ReturnType<typeof setTimeout> | null
    };
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

  private readRecordOnce_(
    pathString: string,
    onProgress: () => void,
    expectedAuthScope: string | null = this.authScope_,
    onManifest: (hashes: PersistedSeedHashes) => void = () => {}
  ): Promise<ReadResult | null> {
    const key = this.key_(pathString);
    // Manifest first, in its own short transaction — it resolves in
    // milliseconds and is everything the outbound listen needs. The tree
    // record follows in a second transaction; WebKit may retain request
    // results until their transaction closes, so the large record gets a
    // transaction of its own.
    return this.withStore_<PersistedManifest | null>(
      'readonly',
      null,
      (store, done) => {
        const req = store.get(key);
        req.onsuccess = () => {
          done((req.result as PersistedManifest | undefined) ?? null);
        };
      }
    ).then(manifest => {
      onProgress();
      if (manifest === null) {
        return null;
      }
      const structurallyValid =
        manifest.formatVersion === PERSISTENCE_FORMAT_VERSION &&
        typeof manifest.revision === 'string' &&
        typeof manifest.updatedAt === 'number' &&
        typeof manifest.hash === 'string' &&
        Array.isArray(manifest.ranges) &&
        manifest.ranges.every(
          range =>
            range !== null &&
            typeof range === 'object' &&
            typeof range.post === 'string' &&
            typeof range.hash === 'string' &&
            typeof range.size === 'number'
        );
      if (!structurallyValid) {
        // Legacy format or a corrupt manifest: a miss. Reclaim best-effort;
        // the sweep also gets these eventually.
        void this.deleteRecord_(pathString);
        return null;
      }
      if (manifest.authScope !== expectedAuthScope) {
        this.restoreReasons_.set(pathString, 'auth');
        return null;
      }
      if (manifest.updatedAt < Date.now() - PERSISTENCE_MAX_AGE_MS) {
        this.restoreReasons_.set(pathString, 'expired');
        void this.deleteRecord_(pathString);
        return null;
      }
      // The manifest alone is enough to send the range listen.
      onManifest({
        hash: manifest.hash,
        compoundHash: wireCompoundHashFromRanges(manifest.ranges)
      });
      return this.withStore_<PersistedTreeRecord | null>(
        'readonly',
        null,
        (store, done) => {
          const req = store.get(key + TREE_KEY_SUFFIX);
          req.onsuccess = () => {
            done((req.result as PersistedTreeRecord | undefined) ?? null);
          };
        }
      ).then(treeRecord => {
        onProgress();
        if (
          treeRecord === null ||
          treeRecord.revision !== manifest.revision ||
          treeRecord.tree === null ||
          treeRecord.tree === undefined
        ) {
          this.restoreReasons_.set(pathString, 'corrupt');
          void this.deleteRecord_(pathString);
          return null;
        }
        try {
          const node = nodeFromJSON(treeRecord.tree);
          if (node.isEmpty()) {
            // An empty tree is never persisted (nothing to seed; the empty
            // singleton must not be stamped). Treat as corrupt.
            this.restoreReasons_.set(pathString, 'corrupt');
            void this.deleteRecord_(pathString);
            return null;
          }
          if (manifest.priorityFree) {
            // Export format === plain format for a priority-free tree: the
            // stored object IS this node's val(). Hand it to the application
            // by reference instead of re-materializing a third copy of the
            // whole tree on the first snapshot.val() (see ServerCacheSeed).
            stampSeedValue(node, treeRecord.tree);
          }
          return {
            record: {
              node,
              hash: manifest.hash,
              compoundHash: wireCompoundHashFromRanges(manifest.ranges),
              updatedAt: manifest.updatedAt,
              revision: manifest.revision
            },
            ranges: manifest.ranges,
            priorityFree: manifest.priorityFree === true
          };
        } catch (e) {
          this.restoreReasons_.set(pathString, 'corrupt');
          void this.deleteRecord_(pathString);
          return null;
        }
      });
    });
  }

  private deleteRecord_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, store => {
      store.delete(key);
      store.delete(key + TREE_KEY_SUFFIX);
      // Legacy chunk/hash sidecars from pre-blob formats share the '#'
      // suffix namespace. Range-delete where the platform has IDBKeyRange;
      // cursor-walk otherwise (Node, test fakes) — key-only, no values.
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
            cursor.key.startsWith(key + '#') &&
            cursor.key !== key + TREE_KEY_SUFFIX
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
   * Exact-root optimistic peek. The completed decode is retained briefly so
   * the authenticated listener can consume the same immutable Node instead of
   * decoding a large IndexedDB record twice during boot.
   */
  peek(
    pathString: string,
    expectedAuthScope: string | null = this.authScope_
  ): Promise<PersistedRecord | null> {
    if (this.disposed_ || !this.schemaKnownCurrent_) {
      recordPersistenceEvent(
        pathString,
        'peek-miss',
        this.disposed_ ? 'disposed' : 'schema-migration'
      );
      return Promise.resolve(null);
    }
    const authGeneration = this.authGeneration_;
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
   * Listener restore with an idle (no-progress) bound. `onManifest` fires as
   * soon as the stored generation's hashes are known — typically
   * milliseconds — letting the caller send the range listen while the tree
   * record is still being read and decoded. The callback is suppressed after
   * a timeout/miss resolution, and never fires once the returned promise has
   * settled null.
   */
  restoreForListen(
    pathString: string,
    onManifest: (hashes: PersistedSeedHashes) => void = () => {}
  ): Promise<PersistenceRestoreResult> {
    this.restoreReasons_.delete(pathString);
    const authGeneration = this.authGeneration_;
    const expectedAuthScope = this.authScope_;
    if (this.disposed_ || !this.schemaKnownCurrent_) {
      const reason: PersistenceRestoreReason = 'missing';
      this.restoreReasons_.set(pathString, reason);
      recordPersistenceEvent(
        pathString,
        'restore-miss',
        this.disposed_ ? 'disposed' : 'schema-migration'
      );
      return Promise.resolve({ record: null, reason });
    }
    let settledNull = false;
    const guardedOnManifest = (hashes: PersistedSeedHashes) => {
      if (
        !settledNull &&
        authGeneration === this.authGeneration_ &&
        !this.disposed_ &&
        this.trackedRoots_.has(pathString)
      ) {
        onManifest(hashes);
      }
    };
    return this.withRestoreSlot_(() =>
      this.raceRestoreTimeout_(
        onProgress =>
          this.readRecord_(
            pathString,
            onProgress,
            false,
            expectedAuthScope,
            guardedOnManifest
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
              ranges: result.ranges,
              priorityFree: result.priorityFree,
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
      if (record === null) {
        settledNull = true;
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
   * chunked restores reset the timer after every completed chunk; callers
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
  /**
   * Write-through: the server confirmed `node` as the state of the tracked
   * root `path`. Coalesced per root under the single-flight window (see the
   * file header): the first change arms a non-restarting timer; later
   * changes coalesce; a change landing while a flush is in flight re-arms
   * exactly one follow-up window when the queue drains. A tree the store is
   * known to already hold — the warm boot's listen-'ok' certifying the
   * restored tree unchanged — is skipped outright unless its stored
   * timestamp needs a refresh (see PERSISTENCE_REFRESH_AGE_MS).
   */
  private flushWritesDeferredUntilRestores_(): void {
    const paths = [...this.writesDeferredUntilRestores_];
    this.writesDeferredUntilRestores_.clear();
    for (const pathString of paths) {
      this.scheduleFlush_(pathString);
    }
  }

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
    // Guarantee the first complete tree immediately (a quick reload must
    // never race an arbitrary empty window), then coalesce later churn.
    if (!prev && !this.queues_.has(pathString)) {
      this.scheduleFlush_(pathString);
      return;
    }
    // The single-flight window: non-restarting, so a root that churns
    // continuously still flushes every writeDelayMs_. The flush reads
    // latest_ when it runs, so it always writes the newest tree.
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
   * time: a flush's manifest, chunks, and integrated hashes stay revision-coupled
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
  /**
   * One generation: identity-diff against the last known stored tree marks
   * the dirty ranges; only those are re-serialized (between preserved
   * boundary posts) and re-hashed; clean ranges carry over verbatim, their
   * bytes never read. The manifest (ranges + hashes) and the tree record
   * (structured-clone export tree) commit in ONE transaction, so every
   * committed generation's hashes exactly describe its stored tree — which
   * is what lets the next boot listen straight off the manifest with zero
   * hashing.
   */
  private flush_(pathString: string): Promise<void> {
    const entry = this.latest_.get(pathString);
    if (!entry || this.disposed_) {
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
      // The store already holds exactly this tree, freshly enough.
      return Promise.resolve();
    }
    const key = this.key_(pathString);
    if (node.isEmpty()) {
      // Nothing to persist; an empty cached root would seed nothing useful.
      return this.deleteRecord_(pathString).then(() => {
        this.lastFlush_.delete(pathString);
      });
    }
    if (prev && prev.rootNode === node) {
      // Content-identical to the stored state; only the timestamp is stale.
      // Rewrite the manifest alone, KEEPING the previous revision so it
      // stays joined to the stored tree record.
      return this.withStore_<boolean>('readwrite', false, (store, done) => {
        const req = store.get(key);
        req.onsuccess = () => {
          const current = req.result as PersistedManifest | undefined;
          if (current && current.revision === prev.revision) {
            store.put({ ...current, updatedAt: now }, key);
            done(true);
          }
        };
      }).then(ok => {
        if (ok && !this.disposed_) {
          this.lastFlush_.set(pathString, { ...prev, storedUpdatedAt: now });
        }
      });
    }

    // ---- Dirty marking ----
    // Identity-diff against the tree the store holds. No previous state (or
    // an overflowing diff) rebuilds everything — the first-ever generation's
    // one full walk.
    let previousRanges: StableRange[] = [];
    let dirty: boolean[] = [];
    let tailDirty = false;
    let changed: string[][] | null = null;
    if (prev && prev.ranges.length > 0) {
      changed = collectChangedSubtreePaths(prev.rootNode, node);
      if (changed !== null) {
        if (changed.length === 0) {
          // Reference inequality but structural identity (rare; e.g. a
          // rebuilt-but-equal tree): nothing is dirty, reuse everything.
          previousRanges = prev.ranges;
          dirty = new Array(prev.ranges.length).fill(false);
        } else {
          previousRanges = prev.ranges;
          const marked = markDirtyRanges(prev.ranges, changed);
          dirty = marked.dirty;
          tailDirty = marked.tailDirty;
        }
      }
    }

    // ---- Serialize + hash dirty ranges ----
    const builder = new CompoundHashBuilder(simpleSizeSplitStrategy(node));
    const dirtyTexts: string[] = [];
    builder.hashSink = (text: string) => {
      dirtyTexts.push(text);
    };
    let ranges: StableRange[];
    try {
      ranges = rebuildStableRanges(
        node,
        previousRanges,
        dirty,
        tailDirty,
        builder
      );
    } catch (e) {
      // A hashing bug must degrade to "no cached hashes for this root",
      // never break the flush queue or the live connection.
      persistenceStats.storageFailures++;
      recordPersistenceEvent(pathString, 'flush-hash-error');
      return this.deleteRecord_(pathString).then(() => {
        this.lastFlush_.delete(pathString);
      });
    }
    const dirtyCount = dirtyTexts.length;
    persistenceStats.rangesHashed += dirtyCount;
    persistenceStats.rangesReused += ranges.length - dirtyCount;
    persistenceStats.writeThroughs++;

    // The tree record's payload: the export-format plain tree. For a
    // priority-free tree this equals val() — and doubles as the app-facing
    // value (see stampSeedValue on restore). The flag is maintained
    // INDUCTIVELY: a full rebuild observes every node; an incremental flush
    // only re-checks the changed subtrees (priorities cannot appear in
    // unchanged, structurally shared subtrees). O(changed), never O(tree).
    let priorityFree: boolean;
    if (prev && changed !== null) {
      priorityFree =
        prev.priorityFree &&
        changed.every(
          path => !exportTreeHasPriority(nodeGetChild(node, path))
        );
    } else {
      priorityFree = !exportTreeHasPriority(node);
    }
    const tree = node.val(true);

    return digestRangeTexts(dirtyTexts).then(digests => {
      if (this.disposed_) {
        return;
      }
      // Deferred hashes were emitted in builder order; fill them in the same
      // order into the placeholder slots rebuildStableRanges left ''.
      let digestIndex = 0;
      for (const range of ranges) {
        if (range.hash === '') {
          range.hash = digests[digestIndex++];
        }
      }
      const manifest: PersistedManifest = {
        formatVersion: PERSISTENCE_FORMAT_VERSION,
        revision,
        updatedAt: now,
        authScope,
        estimatedBytes: estimateSerializedNodeSize(node),
        priorityFree,
        hash: '',
        ranges
      };
      const treeRecord: PersistedTreeRecord = { revision, tree };
      // ONE transaction: the generation commits atomically or not at all.
      return this.withStore_<boolean>('readwrite', false, (store, done) => {
        store.put(treeRecord, key + TREE_KEY_SUFFIX);
        store.put(manifest, key);
        done(true);
      }).then(ok => {
        if (!ok || this.disposed_) {
          return;
        }
        this.lastFlush_.set(pathString, {
          rootNode: node,
          revision,
          ranges,
          priorityFree,
          storedUpdatedAt: now
        });
        recordPersistenceEvent(
          pathString,
          'stored',
          `${ranges.length} ranges, ${dirtyCount} hashed`
        );
      });
    });
  }
}

function nodeGetChild(node: Node, path: string[]): Node {
  let current = node;
  for (const segment of path) {
    current = current.getImmediateChild(segment);
  }
  return current;
}

/**
 * Whether any node in the (sub)tree carries a priority — the one thing that
 * makes export format differ from plain format. Full trees on the first
 * generation, changed subtrees on incremental flushes (see flush_).
 */
function exportTreeHasPriority(node: Node): boolean {
  if (!node.getPriority().isEmpty()) {
    return true;
  }
  if (node.isLeafNode()) {
    return false;
  }
  let found = false;
  node.forEachChild(PRIORITY_INDEX, (key: string, child: Node) => {
    if (!found && exportTreeHasPriority(child)) {
      found = true;
    }
  });
  return found;
}

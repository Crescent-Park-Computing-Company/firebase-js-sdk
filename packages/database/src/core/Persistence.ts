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
  fixedSizeSplitStrategy,
  markDirtyRanges,
  rebuildStableRanges,
  walkLeafInterval
} from './CompoundHash';
import { SeedCompoundHash, stampSeedHashes } from './ServerCacheSeed';
import { ChildrenNode } from './snap/ChildrenNode';
import { KEY_INDEX } from './snap/indexes/KeyIndex';
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
 * STORAGE MODEL — one manifest plus immutable fixed-target range records.
 * Each persisted root stores:
 *
 *   - a MANIFEST (`<prefix>|<path>`): revision, timestamps, auth scope, and
 *     ordered stable ranges `{recordId, post, hash, size}`. It is the complete
 *     compound-listen descriptor and reads in milliseconds.
 *   - one structured-clone RANGE record per stable interval
 *     (`<prefix>|<path>#range:<recordId>`): start/end markers plus a sparse
 *     export-format fragment containing exactly that interval's leaves.
 *
 * Dirty/split/merged ranges receive new immutable ids; clean ranges keep the
 * exact prior record. New records and the manifest commit in ONE transaction.
 * An optimistic manifest-revision check prevents a stale tab from reusing a
 * different tab's records; on conflict it retries with a self-contained full
 * range generation. Retired ids are deleted in the commit, and a guarded
 * key-only GC plus the expiry sweep reclaim older crash/legacy/orphan ids.
 * No full-root val(true) or full-root structured clone occurs on a steady
 * state flush.
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
const PERSISTENCE_DB_VERSION = 9;
// Format 11: immutable fixed-target range records + one stable-range
// manifest. Earlier monolithic/chunked formats restore as misses and are
// reclaimed without materializing their payloads.
const PERSISTENCE_FORMAT_VERSION = 11;
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
 * Constant canonical-text target for one persisted/hash range. Boundaries are
 * stable across generations and only dirty runs reconsult this target. The
 * constructor accepts an override so 128/256/512 KiB can be benchmarked
 * without changing protocol code.
 * @internal
 */
export const PERSISTENCE_RANGE_TARGET_BYTES = 256 * 1024;

/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed manifest or tree read) resets this budget. The same
 * bound applies to each IndexedDB open/transaction, so a request that fires
 * neither success nor error can never hold the live listen forever.
 */
export const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;

/**
 * How long a completed optimistic peek's decoded tree stays retained for its
 * real (authenticated) listener AFTER the app has confirmed the auth scope.
 * From that moment the listener is normally milliseconds away, so a short
 * grace suffices.
 */
const PERSISTENCE_PEEK_HANDOFF_MS = 30000;

/**
 * The same retention while the auth scope is only PRIMED by the peek itself
 * (getPersistedValue's trusted expected identity) and real app auth has not
 * confirmed it yet. Auth hydration is local but can be arbitrarily slow on a
 * loaded profile (service-worker congestion, IndexedDB contention); racing it
 * with a short wall-clock timer silently defeats the one-decode-per-boot
 * handoff exactly on the machines that need it most — the listener then
 * re-reads and re-decodes the full tree while the peek's copy is still alive,
 * doubling peak boot memory. A mismatching or signed-out identity still
 * clears the retained read IMMEDIATELY via setAuthScope; this long backstop
 * only bounds the true leak case (auth never resolving at all), where the
 * page is stuck on its auth spinner anyway.
 */
const PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS = 5 * 60 * 1000;

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

/** One immutable stored range referenced by a manifest. */
interface PersistedRange extends StableRange {
  /** Immutable payload id. A changed/split/merged range always gets a new id. */
  recordId: string;
}

/** The manifest record stored at a root's main key. */
interface PersistedManifest {
  formatVersion: number;
  revision: string;
  updatedAt: number;
  authScope: string | null;
  estimatedBytes: number;
  /** The root's simple hash (empty for compound-only seeds). */
  hash: string;
  /** Ordered ranges describing and locating the complete persisted tree. */
  ranges: PersistedRange[];
}

/** One immutable structured-clone range payload. */
interface PersistedRangeRecord {
  recordId: string;
  /** Exclusive start marker; null for the first range. */
  start: string | null;
  /** Inclusive end marker; equal to the manifest range's post. */
  end: string;
  /** Sparse export-format fragment for exactly (start, end]. */
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
  ranges: PersistedRange[];
  storedUpdatedAt: number;
}

/**
 * Cap on precisely-accumulated changed paths per root between flushes.
 * Matches collectChangedSubtreePaths' default budget: past it the identity
 * diff is the cheaper, equally-correct answer.
 */
const MAX_ACCUMULATED_CHANGED_PATHS = 512;

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

const RANGE_KEY_INFIX = '#range:';

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
  ranges: PersistedRange[];
}

/**
 * Unions two disjoint sparse export fragments without range-deletion
 * semantics. RangeMerge is correct for authoritative server deltas, where an
 * omitted value inside the interval means delete; persisted fragments instead
 * partition one complete snapshot, so omission means "owned by another
 * record". In particular this preserves a prioritized leaf at the exclusive
 * boundary of the following range.
 */
function mergePersistedFragment(base: Node, fragment: Node): Node {
  if (base.isEmpty()) {
    return fragment;
  }
  if (fragment.isEmpty()) {
    return base;
  }
  if (fragment.isLeafNode()) {
    return fragment;
  }
  let result = base;
  fragment.forEachChild(KEY_INDEX, (key, child) => {
    result = result.updateImmediateChild(
      key,
      mergePersistedFragment(result.getImmediateChild(key), child)
    );
  });
  if (!fragment.getPriority().isEmpty()) {
    result = result.updatePriority(fragment.getPriority());
  }
  return result;
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
  /**
   * Changed subtree paths accumulated since the flush baseline
   * (lastFlush_.rootNode), keyed by root. The server names the exact path of
   * every ordinary data push, so steady-state flushes can mark dirty ranges
   * from this list directly instead of re-discovering the same information
   * with a full-width identity diff of two ~60MB trees (the diff's sorted
   * child merges were the single largest CPU slice of a flush).
   *
   * `null` = imprecise: an update arrived whose changed path is unknown or
   * at/above the root (range merges, listen completions, foreign rebases) —
   * the flush falls back to the identity diff, which is exactly today's
   * behavior. Entries reset to [] whenever lastFlush_ gains a fresh baseline.
   */
  private changedSinceFlush_ = new Map<string, string[][] | null>();

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
      manifestHashes: PersistedSeedHashes | null;
      manifestCallbacks: Set<(hashes: PersistedSeedHashes) => void>;
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
  /**
   * True once the APP's auth integration (setPersistenceAuthScope) has
   * confirmed the scope — as opposed to a pre-auth peek merely priming it
   * with a trusted expected identity. Selects the peek-retention budget:
   * a primed-only scope holds the long pre-auth backstop, a confirmed one
   * the short handoff grace (see PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS).
   */
  private authScopeConfirmed_ = false;
  private authGeneration_ = 0;

  isAuthScopeConfigured(): boolean {
    return this.authScopeConfigured_;
  }

  setAuthScope(scope: string | null, confirmedByApp = true): boolean {
    const changed = !this.authScopeConfigured_ || scope !== this.authScope_;
    this.authScopeConfigured_ = true;
    if (confirmedByApp) {
      if (!this.authScopeConfirmed_) {
        this.authScopeConfirmed_ = true;
        if (!changed) {
          // Real auth confirmed the exact scope a pre-auth peek primed: the
          // handoff window is open NOW. Retained reads waiting under the
          // long pre-auth backstop drop to the short post-auth grace —
          // counted from this moment, not from when the read finished.
          this.rearmRetainedReads_();
        }
      }
    } else if (changed) {
      // A prime that CHANGES the scope describes an identity the app has not
      // confirmed yet; its retentions must run under the pre-auth backstop.
      this.authScopeConfirmed_ = false;
    }
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
    this.changedSinceFlush_.clear();
    // Clear retention timers BEFORE dropping the map: a pending cleanupTimer's
    // closure otherwise keeps the entry (and its decoded tree) alive until it
    // fires — minutes, under the pre-auth backstop.
    for (const read of this.activeReads_.values()) {
      if (read.cleanupTimer !== null) {
        clearTimeout(read.cleanupTimer);
      }
    }
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
    private rangeTargetBytes_: number = PERSISTENCE_RANGE_TARGET_BYTES,
    private peekHandoffMs_: number = PERSISTENCE_PEEK_HANDOFF_MS,
    private peekPreAuthHandoffMs_: number = PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS
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
      this.rangeTargetBytes_,
      this.peekHandoffMs_,
      this.peekPreAuthHandoffMs_
    );
    if (this.authScopeConfigured_) {
      rebound.setAuthScope(scope, this.authScopeConfirmed_);
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
      this.changedSinceFlush_.delete(pathString);
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
        this.changedSinceFlush_.delete(pathString);
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
            revision: string | null;
            liveRangeKeys: Set<string>;
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
            // Keep only immutable payloads referenced by the current live
            // manifest. Retired range versions and every legacy sidecar are
            // garbage-collected without materializing their values.
            const drop =
              decision === undefined ||
              decision.expired ||
              !decision.liveRangeKeys.has(key);
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
                  ranges?: unknown;
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
                  : null,
              liveRangeKeys:
                record &&
                record.formatVersion === PERSISTENCE_FORMAT_VERSION &&
                Array.isArray(record.ranges)
                  ? new Set(
                      record.ranges
                        .filter(
                          (range): range is PersistedRange =>
                            range !== null &&
                            typeof range === 'object' &&
                            typeof (range as PersistedRange).recordId ===
                              'string'
                        )
                        .map(range => key + RANGE_KEY_INFIX + range.recordId)
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
                // cannot keep buffering network data indefinitely.
                try {
                  tx.abort();
                } catch (e) {
                  // It may have completed between the timer firing and abort().
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
   * Reads a root's committed manifest and every immutable range it references
   * in one readonly transaction. `onManifest` fires as soon as the requests
   * are queued, overlapping network reconciliation with structured-clone
   * range reads and private Node assembly. Missing/mismatched ranges fail the
   * whole restore; Repo then performs the structural-failure cold relisten.
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
      // Joining an already-progressing read is itself progress. A pre-auth
      // peek may have started the physical read, so replay any already-read
      // manifest to the real listener instead of making it wait for assembly.
      onProgress();
      if (active.manifestHashes !== null) {
        onManifest(active.manifestHashes);
      } else {
        active.manifestCallbacks.add(onManifest);
      }
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
      manifestHashes: PersistedSeedHashes | null;
      manifestCallbacks: Set<(hashes: PersistedSeedHashes) => void>;
    } = {
      promise: Promise.resolve(null),
      progress,
      retainAfterResolve,
      cleanupTimer: null,
      manifestHashes: null,
      manifestCallbacks: new Set([onManifest])
    };
    const promise = this.readRecordOnce_(
      pathString,
      emitProgress,
      expectedAuthScope,
      hashes => {
        entry.manifestHashes = hashes;
        for (const callback of entry.manifestCallbacks) {
          callback(hashes);
        }
        entry.manifestCallbacks.clear();
      }
    );
    entry.promise = promise;
    this.activeReads_.set(pathString, entry);
    const release = (result: ReadResult | null) => {
      entry.progress.clear();
      entry.manifestCallbacks.clear();
      if (this.activeReads_.get(pathString) !== entry) {
        return;
      }
      if (!entry.retainAfterResolve) {
        this.activeReads_.delete(pathString);
        return;
      }
      // Only a completed DECODE earns the long pre-auth budget: it is the
      // one-tree-per-boot handoff auth must not race. A miss/failed read
      // retains nothing worth waiting for — keep the short expiry so a
      // record written meanwhile (another tab) is re-read fresh.
      entry.cleanupTimer = setTimeout(
        () => {
          if (this.activeReads_.get(pathString) === entry) {
            this.activeReads_.delete(pathString);
          }
        },
        result !== null && !this.authScopeConfirmed_
          ? this.peekPreAuthHandoffMs_
          : this.peekHandoffMs_
      );
    };
    void promise.then(release, () => release(null));
    return promise;
  }

  /**
   * Auth just confirmed the scope a pre-auth peek primed: every retained
   * completed read waiting under the long pre-auth backstop switches to the
   * short post-auth grace, counted from now. Entries still resolving (no
   * cleanupTimer yet) pick the right budget in their own release().
   */
  private rearmRetainedReads_(): void {
    for (const [pathString, entry] of this.activeReads_) {
      if (entry.cleanupTimer === null || !entry.retainAfterResolve) {
        continue;
      }
      clearTimeout(entry.cleanupTimer);
      entry.cleanupTimer = setTimeout(() => {
        if (this.activeReads_.get(pathString) === entry) {
          this.activeReads_.delete(pathString);
        }
      }, this.peekHandoffMs_);
    }
  }

  private readRecordOnce_(
    pathString: string,
    onProgress: () => void,
    expectedAuthScope: string | null = this.authScope_,
    onManifest: (hashes: PersistedSeedHashes) => void = () => {}
  ): Promise<ReadResult | null> {
    const key = this.key_(pathString);
    // One readonly transaction is the consistency boundary for manifest +
    // immutable range records. The manifest callback fires after every range
    // request has been synchronously queued, but before those payloads finish
    // cloning, so the network comparison overlaps the complete local restore.
    // The transaction itself only VALIDATES and collects raw structured
    // clones; decode runs after it resolves, in yielded slices (below).
    return this.withStore_<{
      manifest: PersistedManifest;
      rawTrees: Array<unknown | null>;
    } | null>(
      'readonly',
      null,
      (store, done, progress) => {
        const manifestReq = store.get(key);
        manifestReq.onsuccess = () => {
          progress();
          const manifest =
            (manifestReq.result as PersistedManifest | undefined) ?? null;
          if (manifest === null) {
            done(null);
            return;
          }
          const structurallyValid =
            manifest.formatVersion === PERSISTENCE_FORMAT_VERSION &&
            typeof manifest.revision === 'string' &&
            typeof manifest.updatedAt === 'number' &&
            typeof manifest.hash === 'string' &&
            Array.isArray(manifest.ranges) &&
            manifest.ranges.length > 0 &&
            manifest.ranges.every(
              range =>
                range !== null &&
                typeof range === 'object' &&
                typeof range.recordId === 'string' &&
                range.recordId.length > 0 &&
                typeof range.post === 'string' &&
                typeof range.hash === 'string' &&
                typeof range.size === 'number'
            );
          if (!structurallyValid) {
            this.restoreReasons_.set(pathString, 'corrupt');
            done(null);
            return;
          }
          if (manifest.authScope !== expectedAuthScope) {
            this.restoreReasons_.set(pathString, 'auth');
            done(null);
            return;
          }
          if (manifest.updatedAt < Date.now() - PERSISTENCE_MAX_AGE_MS) {
            this.restoreReasons_.set(pathString, 'expired');
            done(null);
            return;
          }

          // The onsuccess callbacks only VALIDATE and collect the raw
          // structured clones — decoding (nodeFromJSON + merge) is deferred
          // to a yielded post-transaction loop below. Chrome coalesces
          // same-transaction request callbacks into one task, so decoding
          // inline produced multi-hundred-ms long tasks (and held every raw
          // clone alive until the last record decoded). The deferred loop
          // bounds task length and releases each clone as it is consumed —
          // both matter on mobile WebKit, where a long-task + peak-memory
          // spike at boot is what gets the page killed.
          let failed = false;
          let remaining = manifest.ranges.length;
          let previousPost: string | null = null;
          const rawTrees: Array<unknown | null> = new Array(
            manifest.ranges.length
          ).fill(null);
          manifest.ranges.forEach((range, index) => {
            const expectedStart = previousPost;
            previousPost = range.post;
            const req = store.get(key + RANGE_KEY_INFIX + range.recordId);
            req.onsuccess = () => {
              progress();
              if (!failed) {
                const record = req.result as PersistedRangeRecord | undefined;
                if (
                  !record ||
                  record.recordId !== range.recordId ||
                  record.start !== expectedStart ||
                  record.end !== range.post ||
                  record.tree === null ||
                  record.tree === undefined
                ) {
                  failed = true;
                  this.restoreReasons_.set(pathString, 'corrupt');
                } else {
                  rawTrees[index] = record.tree;
                }
              }
              remaining--;
              if (remaining === 0) {
                done(failed ? null : { manifest, rawTrees });
              }
            };
          });

          // Every referenced get is now queued in this same snapshot. It is
          // safe to put the range listen on the wire immediately.
          onManifest({
            hash: manifest.hash,
            compoundHash: wireCompoundHashFromRanges(manifest.ranges)
          });
        };
      },
      onProgress
    ).then(async collected => {
      let result: ReadResult | null = null;
      if (collected !== null) {
        const assembled = await this.decodeFragmentsSliced_(
          collected.rawTrees,
          onProgress
        );
        if (this.disposed_) {
          // Disposed mid-decode: the record on disk is fine — do not mark it
          // corrupt (which would delete it below).
          return null;
        }
        if (assembled === null || assembled.isEmpty()) {
          this.restoreReasons_.set(pathString, 'corrupt');
        } else {
          result = {
            record: {
              node: assembled,
              hash: collected.manifest.hash,
              compoundHash: wireCompoundHashFromRanges(
                collected.manifest.ranges
              ),
              updatedAt: collected.manifest.updatedAt,
              revision: collected.manifest.revision
            },
            ranges: collected.manifest.ranges
          };
        }
      }
      if (result === null) {
        const reason = this.restoreReasons_.get(pathString);
        if (reason === 'corrupt' || reason === 'expired') {
          // Best-effort cleanup. Auth/missing misses must not delete another
          // identity's otherwise valid cache record.
          void this.deleteRecord_(pathString);
        }
      }
      return result;
    });
  }

  /**
   * Decodes and merges raw persisted range clones into one Node in yielded
   * slices. Each slice decodes a few records, then yields a macrotask so the
   * main thread can paint/GC between slices; consumed entries are nulled so
   * the structured clones are collectable while later slices run. Returns
   * null when any fragment fails to decode.
   */
  private async decodeFragmentsSliced_(
    rawTrees: Array<unknown | null>,
    progress: () => void
  ): Promise<Node | null> {
    const SLICE_SIZE = 8;
    let assembled: Node = ChildrenNode.EMPTY_NODE;
    for (let i = 0; i < rawTrees.length; i++) {
      if (i > 0 && i % SLICE_SIZE === 0) {
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        if (this.disposed_) {
          return null;
        }
        progress();
      }
      const raw = rawTrees[i];
      rawTrees[i] = null;
      try {
        assembled = mergePersistedFragment(assembled, nodeFromJSON(raw));
      } catch (e) {
        return null;
      }
    }
    return assembled;
  }

  private deleteRecord_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, store => {
      store.delete(key);
      // Immutable ranges and legacy chunk/hash/tree sidecars share '#'.
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
          ranges: state.ranges
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
   * Exact-root optimistic peek. The completed range assembly is retained briefly
   * so the authenticated listener consumes the same immutable Node instead of
   * reconstructing the root twice during boot.
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
   * Listener restore with an idle (no-progress) bound. `onManifest` fires as
   * soon as the stored generation's hashes are known — typically
   * milliseconds — letting the caller send the range listen while immutable
   * range records are still being read and assembled. The callback is suppressed after
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
            // Updates accumulated before this point were named against a
            // pre-restore chain; the restored record starts a new baseline.
            this.changedSinceFlush_.set(pathString, null);
            this.lastFlush_.set(pathString, {
              rootNode: result.record.node,
              revision: result.record.revision,
              ranges: result.ranges,
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

  private accumulateChangedPaths_(
    pathString: string,
    changedPaths: string[][] | undefined
  ): void {
    if (changedPaths === undefined) {
      this.changedSinceFlush_.set(pathString, null);
      return;
    }
    const existing = this.changedSinceFlush_.get(pathString);
    if (existing === null) {
      return; // already imprecise until the next flush baseline
    }
    const list = existing ?? [];
    for (const changedPath of changedPaths) {
      if (list.length >= MAX_ACCUMULATED_CHANGED_PATHS) {
        this.changedSinceFlush_.set(pathString, null);
        return;
      }
      list.push(changedPath);
    }
    this.changedSinceFlush_.set(pathString, list);
  }

  /**
   * `changedPaths` — the root-relative paths of the subtrees this update
   * changed, when the caller knows them precisely: an ordinary server data
   * push names its own path (`[relative]`), a listen certification confirms
   * already-accounted state (`[]`, nothing new). Omitted/undefined marks the
   * accumulated change-set imprecise — a range merge, or any update whose
   * shape the caller cannot name — falling the next flush back to the
   * identity diff.
   */
  serverCacheUpdated(path: Path, node: Node, changedPaths?: string[][]): void {
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
    this.accumulateChangedPaths_(pathString, changedPaths);
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
    this.changedSinceFlush_.delete(pathString);
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
    this.changedSinceFlush_.delete(pathString);
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
    this.changedSinceFlush_.clear();
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
   * boundary posts), re-hashed, and written under new immutable ids. Clean
   * range records carry over verbatim and are never cloned. New records plus
   * the manifest commit in ONE transaction, so every
   * committed generation's hashes exactly describe its stored tree — which
   * is what lets the next boot listen straight off the manifest with zero
   * hashing.
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
        // The stored generation is gone (another identity's manifest, a
        // sweep, or manual storage clearing). lastFlush_ no longer describes
        // storage; left in place, every future identical-node write-through
        // would skip against it and the root would stay unpersisted for the
        // whole session. Resync from storage and rebuild once.
        return this.readRecord_(pathString).then(winner => {
          if (this.disposed_) {
            return;
          }
          // The adopted baseline is another generation's tree; paths named
          // against our own chain do not describe diffs from it.
          this.changedSinceFlush_.set(pathString, null);
          if (winner !== null) {
            this.lastFlush_.set(pathString, {
              rootNode: winner.record.node,
              revision: winner.record.revision,
              ranges: winner.ranges,
              storedUpdatedAt: winner.record.updatedAt
            });
          } else {
            this.lastFlush_.delete(pathString);
          }
          this.flushPending_.add(pathString);
        });
      });
    }

    // Dirty ranges come from the changed paths the server already named
    // (accumulateChangedPath_), consumed against the flush baseline; when
    // the accumulated set is imprecise (null: a range merge, a listen
    // completion, an unknown-path update, overflow) fall back to the
    // identity diff of the two trees — the exact pre-accumulator behavior.
    // Consume-on-read: whatever happens to this flush, the paths below are
    // relative to the CURRENT baseline only once.
    const accumulated = this.changedSinceFlush_.get(pathString);
    this.changedSinceFlush_.set(pathString, []);
    let previousRanges: PersistedRange[] = [];
    let dirty: boolean[] = [];
    let tailDirty = false;
    let changed: string[][] | null = null;
    if (prev && prev.ranges.length > 0) {
      changed =
        accumulated !== null && accumulated !== undefined
          ? accumulated
          : collectChangedSubtreePaths(prev.rootNode, node);
      if (changed !== null) {
        previousRanges = prev.ranges;
        if (changed.length === 0) {
          dirty = new Array(prev.ranges.length).fill(false);
        } else {
          const marked = markDirtyRanges(prev.ranges, changed);
          dirty = marked.dirty;
          tailDirty = marked.tailDirty;
        }
      }
    }

    // First pass: boundaries/sizes only. It never creates canonical strings
    // or export payloads, so a first generation cannot retain another full
    // copy of the root merely to decide its ranges.
    const planner = new CompoundHashBuilder(
      fixedSizeSplitStrategy(this.rangeTargetBytes_),
      true
    );
    let rebuilt: StableRange[];
    try {
      rebuilt = rebuildStableRanges(
        node,
        previousRanges,
        dirty,
        tailDirty,
        planner,
        this.rangeTargetBytes_
      );
    } catch (e) {
      persistenceStats.storageFailures++;
      recordPersistenceEvent(pathString, 'flush-plan-error');
      return this.deleteRecord_(pathString).then(() => {
        this.lastFlush_.delete(pathString);
      });
    }

    interface DirtyRangePlan {
      range: PersistedRange;
      start: string | null;
      end: string;
    }
    const dirtyPlans: DirtyRangePlan[] = [];
    let previousPost: string | null = null;
    let dirtyIndex = 0;
    const ranges: PersistedRange[] = rebuilt.map(range => {
      const carried = range as PersistedRange;
      if (range.hash !== '' && typeof carried.recordId === 'string') {
        previousPost = range.post;
        return carried;
      }
      const persisted: PersistedRange = {
        ...range,
        recordId: revision + '-' + dirtyIndex.toString(36)
      };
      dirtyPlans.push({
        range: persisted,
        start: previousPost,
        end: range.post
      });
      previousPost = range.post;
      dirtyIndex++;
      return persisted;
    });

    const stagedIds: string[] = [];
    const stageBatch = async (plans: DirtyRangePlan[]): Promise<void> => {
      const texts: string[] = [];
      const records: PersistedRangeRecord[] = [];
      for (const plan of plans) {
        const builder = new CompoundHashBuilder(() => false);
        let text: string | undefined;
        let payload: unknown = undefined;
        builder.hashSink = completed => {
          text = completed;
        };
        builder.payloadSink = completed => {
          payload = completed;
        };
        const from =
          plan.start === null
            ? null
            : plan.start === '/'
            ? []
            : plan.start.split('/');
        const to = plan.end === '/' ? [] : plan.end.split('/');
        if (from !== null) {
          builder.seedBoundary(from);
        }
        walkLeafInterval(node, from, to, builder);
        if (
          text === undefined ||
          payload === undefined ||
          builder.posts.length !== 1 ||
          builder.posts[0] !== plan.end
        ) {
          throw new Error(
            'Dirty range did not serialize to its planned boundary'
          );
        }
        texts.push(text);
        records.push({
          recordId: plan.range.recordId,
          start: plan.start,
          end: plan.end,
          tree: payload
        });
      }
      const digests = await digestRangeTexts(texts);
      for (let i = 0; i < plans.length; i++) {
        plans[i].range.hash = digests[i];
      }
      const stored = await this.withStore_<boolean>(
        'readwrite',
        false,
        (store, done, progress) => {
          for (const record of records) {
            const put = store.put(
              record,
              key + RANGE_KEY_INFIX + record.recordId
            );
            put.onsuccess = progress;
          }
          done(true);
        }
      );
      if (!stored) {
        throw new Error('Failed to stage persisted ranges');
      }
      stagedIds.push(...records.map(record => record.recordId));
      // Async activation records can otherwise retain completed IDB request
      // inputs until the whole generation settles. Drop every large reference
      // explicitly and yield a macrotask so WebKit can collect between batches.
      for (const record of records) {
        record.tree = undefined;
      }
      records.length = 0;
      texts.length = 0;
      digests.length = 0;
      await new Promise<void>(resolve => setTimeout(resolve, 0));
    };

    const stageAll = async (): Promise<void> => {
      const batchSize = 4;
      for (let i = 0; i < dirtyPlans.length; i += batchSize) {
        await stageBatch(dirtyPlans.slice(i, i + batchSize));
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
          hash: '',
          ranges
        };
        const liveIds = new Set(ranges.map(range => range.recordId));
        const retiredIds = prev
          ? prev.ranges
              .map(range => range.recordId)
              .filter(recordId => !liveIds.has(recordId))
          : [];

        // The range payloads are immutable staging records. This tiny CAS
        // transaction is the atomic authority switch: until the manifest put
        // commits, a crash leaves the previous generation fully live.
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
              // A first generation may REPLACE a manifest this manager can
              // never restore (another identity's scope, or an unknown
              // format): treating those as CAS winners would strand the
              // adopt-and-retry loser forever — its readRecord_ always
              // resolves null against a foreign manifest, so every retry
              // re-stages the full tree and conflicts again. Same-scope
              // manifests keep strict CAS semantics. Replacement is LIVE
              // scope only: a generation staged under a superseded identity
              // may still publish into an absent key under its own label
              // (reads are scope-checked; see the in-flight relabel test)
              // but must never replace the new identity's fresh manifest.
              const replaceableForeign =
                !prev &&
                current !== undefined &&
                authScope === this.authScope_ &&
                (current.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
                  current.authScope !== authScope);
              if (
                (prev && (!current || current.revision !== prev.revision)) ||
                (!prev && current !== undefined && !replaceableForeign)
              ) {
                done(false);
                return;
              }
              const commit = () => {
                for (const recordId of retiredIds) {
                  const remove = store.delete(key + RANGE_KEY_INFIX + recordId);
                  remove.onsuccess = progress;
                }
                const manifestPut = store.put(manifest, key);
                manifestPut.onsuccess = progress;
                done(true);
              };
              if (stagedIds.length === 0) {
                commit();
                return;
              }
              // Another tab's sweep classifies suffixed records against the
              // manifest that is COMMITTED, so records staged for this still
              // unpublished generation look like orphans there and can be
              // reclaimed between staging and this transaction without
              // moving the manifest revision (in-memory guards only cover
              // this tab). Publishing would durably reference missing
              // payloads. Re-verify every staged id inside the same atomic
              // switch — key-only reads — and treat a loss exactly like a
              // CAS conflict. Ordering is airtight because readwrite
              // transactions on one store serialize: a sweep that ran before
              // this transaction is observed here; one that runs after reads
              // this manifest and keeps its records.
              let missing = false;
              let verified = 0;
              for (const recordId of stagedIds) {
                const stagedKey = key + RANGE_KEY_INFIX + recordId;
                // Key-only where the platform (or fake) provides it; the
                // fallback get only runs in environments without getKey.
                const check =
                  typeof store.getKey === 'function'
                    ? store.getKey(stagedKey)
                    : store.get(stagedKey);
                check.onsuccess = () => {
                  progress();
                  if (missing) {
                    return;
                  }
                  if (check.result === undefined) {
                    missing = true;
                    done(false);
                    return;
                  }
                  if (++verified === stagedIds.length) {
                    commit();
                  }
                };
              }
            };
          }
        ).then(ok => {
          if (!ok || this.disposed_) {
            if (this.disposed_) {
              return;
            }
            // A different tab committed while we staged, or a concurrent
            // sweep reclaimed our still-unreferenced staged records. Remove
            // our immutable ids, adopt the winning manifest/base, then diff
            // the current live Node against it on one coalesced retry.
            return this.withStore_<void>('readwrite', undefined, store => {
              for (const recordId of stagedIds) {
                store.delete(key + RANGE_KEY_INFIX + recordId);
              }
            }).then(() =>
              this.readRecord_(pathString).then(winner => {
                // Same imprecision as the refresh resync: the winner is a
                // foreign baseline.
                this.changedSinceFlush_.set(pathString, null);
                if (winner !== null) {
                  this.lastFlush_.set(pathString, {
                    rootNode: winner.record.node,
                    revision: winner.record.revision,
                    ranges: winner.ranges,
                    storedUpdatedAt: winner.record.updatedAt
                  });
                } else {
                  this.lastFlush_.delete(pathString);
                }
                this.flushPending_.add(pathString);
              })
            );
          }
          persistenceStats.rangesHashed += dirtyPlans.length;
          persistenceStats.rangesReused += ranges.length - dirtyPlans.length;
          persistenceStats.writeThroughs++;
          this.lastFlush_.set(pathString, {
            rootNode: node,
            revision,
            ranges,
            storedUpdatedAt: now
          });
          stampSeedHashes(
            node,
            manifest.hash,
            wireCompoundHashFromRanges(ranges)
          );
          recordPersistenceEvent(
            pathString,
            'stored',
            `${ranges.length} ranges, ${dirtyPlans.length} written`
          );
          if (!prev) {
            void this.gcRangeRecords_(pathString, revision, liveIds);
          }
        });
      })
      .catch(() => {
        persistenceStats.storageFailures++;
        recordPersistenceEvent(pathString, 'flush-range-stage-error');
        // The flush consumed the accumulated changed-paths at its start, but
        // nothing was committed: lastFlush_ still describes the stored
        // baseline, so the paths this flush was covering must flow into the
        // next diff or its ranges would be carried forward stale. Merge them
        // back with whatever accrued since (either side already imprecise
        // stays imprecise).
        const since = this.changedSinceFlush_.get(pathString);
        if (accumulated === null || since === null) {
          this.changedSinceFlush_.set(pathString, null);
        } else if (accumulated !== undefined && accumulated.length > 0) {
          const merged = accumulated.concat(since ?? []);
          this.changedSinceFlush_.set(
            pathString,
            merged.length > MAX_ACCUMULATED_CHANGED_PATHS ? null : merged
          );
        }
        // Staged immutable records are non-authoritative and are reclaimed by
        // the next successful full-generation GC or the deferred sweep.
        if (stagedIds.length > 0) {
          void this.withStore_<void>('readwrite', undefined, store => {
            for (const recordId of stagedIds) {
              store.delete(key + RANGE_KEY_INFIX + recordId);
            }
          });
        }
      });
  }

  private gcRangeRecords_(
    pathString: string,
    revision: string,
    liveIds: Set<string>
  ): Promise<void> {
    const key = this.key_(pathString);
    const prefix = key + RANGE_KEY_INFIX;
    return this.withStore_<void>('readwrite', undefined, (store, done) => {
      const manifestReq = store.get(key);
      manifestReq.onsuccess = () => {
        const manifest = manifestReq.result as PersistedManifest | undefined;
        if (!manifest || manifest.revision !== revision) {
          done(undefined);
          return;
        }
        let range: IDBKeyRange | undefined;
        try {
          range =
            typeof IDBKeyRange !== 'undefined'
              ? IDBKeyRange.bound(prefix, prefix + String.fromCharCode(0xffff))
              : undefined;
        } catch (e) {
          range = undefined;
        }
        const keyCursorStore = store as unknown as {
          openKeyCursor?: (range?: IDBKeyRange) => IDBRequest;
        };
        // A value cursor structured-clones every range payload; on a 65 MB
        // root that would recreate the full-read cost solely to discover keys.
        const req =
          typeof keyCursorStore.openKeyCursor === 'function'
            ? keyCursorStore.openKeyCursor(range)
            : store.openCursor(range);
        req.onsuccess = () => {
          const cursor = req.result as IDBCursor | null;
          if (!cursor) {
            done(undefined);
            return;
          }
          if (typeof cursor.key === 'string' && cursor.key.startsWith(prefix)) {
            const recordId = cursor.key.slice(prefix.length);
            if (!liveIds.has(recordId)) {
              cursor.delete();
            }
          }
          cursor.continue();
        };
      };
    });
  }
}

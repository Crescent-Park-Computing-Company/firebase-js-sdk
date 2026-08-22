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
  StableRangeRebuilder,
  collectChangedSubtreePaths,
  fixedSizeSplitStrategy,
  markDirtyRanges,
  walkLeafInterval
} from './CompoundHash';
import {
  emitPersistenceTrace,
  persistenceTraceSinkInstalled
} from './PersistenceTrace';
import { SeedCompoundHash, stampSeedHashes } from './ServerCacheSeed';
import { ChildrenNode } from './snap/ChildrenNode';
import { KEY_INDEX } from './snap/indexes/KeyIndex';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';
import { Path } from './util/Path';
import { sha1 } from './util/util';
import { yieldMacrotask } from './util/yieldMacrotask';

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
 * Write window for a root with NO flush baseline (first generation after a
 * cold or fallback boot, or after an invalidation). The ordinary window
 * coalesces steady-state churn; a fresh boot has none to coalesce — the
 * complete tree just arrived — and the first stored generation is the only
 * exit from the cold-reload loop (no cache → next boot re-downloads the
 * root). Short-session mobile boots regularly died before the ordinary
 * window even fired, so the first generation starts sooner; the sliced
 * planner and byte-budgeted staging keep it off the critical path. Tests
 * that shrink writeDelayMs below this keep their configured cadence
 * (the effective delay is min of the two).
 * @internal
 */
export const PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS = 3000;

/**
 * Main-thread budget for one slice of flush planning (the stable-range
 * rewalk). Sized to fit inside a frame budget on mobile hardware.
 * @internal
 */
export const FLUSH_PLAN_SLICE_MS = 12;

/**
 * Canonical-text bytes staged per task before yielding. Two default-target
 * ranges (~256 KiB each) per slice keeps serialization work bounded while
 * the unclamped macrotask yield (yieldMacrotask) lets paint/input interleave.
 * @internal
 */
export const FLUSH_STAGE_BATCH_BYTES = 512 * 1024;

/** Thrown out of a sliced flush when the manager was disposed mid-yield. */
class FlushObsoleteError extends Error {
  constructor() {
    super('flush obsolete');
  }
}

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
 *
 * `rootNode` is null when the baseline was adopted from another writer's
 * committed manifest without decoding its tree (see
 * adoptCommittedBaseline_): the revision and ranges are all the CAS needs,
 * but no diff can be computed against an absent tree — the next flush
 * stages a fresh self-contained generation.
 */
interface FlushedState {
  rootNode: Node | null;
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

/**
 * Identity sharing between the previous flush baseline and the tree being
 * written — the flush trace's memory signal. `sharedChildren` counts the new
 * tree's immediate children that ARE the baseline's child objects (===);
 * zero with a present baseline means a wholesale replace (a fallback resend
 * or an ungrafted ingest): until this flush commits, the divorced baseline
 * retains a second complete tree in memory.
 */
/**
 * True when the incoming tree keeps NO immediate-child identity with the
 * flush baseline — the wholesale-replace shape. Identity-only and bounded
 * by the root's child count; never compares content (a structurally-equal
 * rebuilt child still reads as divorced, which only costs staging a fresh
 * generation — never correctness).
 */
function baselineFullyDivorced(prevRoot: Node, node: Node): boolean {
  if (prevRoot === node) {
    return false;
  }
  if (node.isLeafNode() || prevRoot.isLeafNode() || node.isEmpty()) {
    return true;
  }
  let shared = false;
  // forEachChild aborts the traversal on a truthy callback return.
  (node as ChildrenNode).forEachChild(KEY_INDEX, (name, child) => {
    if (prevRoot.getImmediateChild(name) === child) {
      shared = true;
      return true;
    }
  });
  return !shared;
}

function baselineSharing(
  prev: FlushedState | undefined,
  node: Node
): { sharedChildren: number; totalChildren: number } {
  let sharedChildren = 0;
  let totalChildren = 0;
  const prevRoot = prev?.rootNode ?? null;
  if (prevRoot !== null && !node.isLeafNode() && !prevRoot.isLeafNode()) {
    (node as ChildrenNode).forEachChild(KEY_INDEX, (name, child) => {
      totalChildren++;
      if (prevRoot.getImmediateChild(name) === child) {
        sharedChildren++;
      }
    });
  }
  return { sharedChildren, totalChildren };
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

/**
 * Structural validation of a stored manifest: current format, string
 * revision, and a non-empty, well-formed range list. Shared by the full
 * restore read and the manifest-only baseline adoption.
 */
function structurallyValidManifest(
  manifest: PersistedManifest | null | undefined
): manifest is PersistedManifest {
  return (
    manifest !== null &&
    manifest !== undefined &&
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
    )
  );
}

/**
 * Cross-tab single-writer coordination — a writer lease per TRACKED ROOT
 * (Web Lock + heartbeat liveness + steal-on-stale takeover).
 *
 * Persisted generations are already SAFE under concurrent writers —
 * immutable range records plus the manifest revision CAS — but they are not
 * CHEAP under them: two live tabs flushing the same root leapfrog each
 * other's revisions, and every CAS loser abandons its staged generation and
 * re-stages the whole root. Under steady churn that is full-tree
 * serialization and IndexedDB writes in EVERY tab EVERY window,
 * indefinitely — the observed multi-tab boot crash/thrash.
 *
 * THE LOCK COVERS EXACTLY THE RESOURCE IT GATES: write-throughs are
 * per-root, so the lock is per-root (`prefix|root`). A manager requests a
 * root's lock while it tracks the root and returns it when the root leaves
 * tracking (untrack — after the final flush so the last tree still writes
 * under the lease — eviction, dispose). Tabs tracking DISJOINT roots each
 * hold their own locks and never interact; a coarser (manager-wide) lock
 * would let a tab that never even tracks some root starve every tab that
 * does — a follower has no cross-tab channel to hand its tree to the
 * elected writer, so the only correct writer for a root is a tab that
 * TRACKS it.
 *
 * THE LEASE CARRIES ZERO CORRECTNESS DUTIES. It is an economics device
 * (who avoids CAS fights), and every destructive or exclusive storage
 * operation re-validates its justification INSIDE its own readwrite
 * transaction (revision CAS, scope equality, structural invalidity) — a
 * Web Lock is advisory and revocable-by-steal at any await point, so a
 * lease check outside the transaction can never authorize anything.
 * Followers keep tracking in memory and take over through the ordinary
 * stale-baseline CAS path when a lease transfers.
 *
 * LIVENESS is proven by heartbeat, not inferred from lifecycle events: a
 * root's holder stamps that root's shared storage key every
 * LEASE_HEARTBEAT_MS; a queued follower that observes the stamp PRESENT
 * but stale by LEASE_STALE_MS steals that root's lock (Web Locks `steal`).
 * This one mechanism uniformly covers every way a holder can go silent —
 * frozen tabs, back/forward-cached pages, Safari and Firefox background
 * suspension (which fire no freeze event), and even a wedged-but-visible
 * page — where enumerating lifecycle signals cannot. A hidden-but-RUNNING
 * tab keeps heartbeating and keeps persisting. The stolen holder's request
 * promise settles; when its JavaScript resumes it re-queues politely
 * (never steals back unprompted), reconciling any stale baseline through
 * the flush CAS + manifest-only adoption, exactly once.
 *
 * Environments without Web Locks fail open to CAS-only behavior; without
 * shared storage (no localStorage, or storage that throws) heartbeats are
 * disabled and takeover happens only on page death — the CAS remains the
 * correctness backstop in every configuration. Clock skew is a non-issue:
 * all tabs share one machine clock, and the staleness threshold generously
 * exceeds background timer throttling (Chrome clamps background timers to
 * one minute, and a page holding a Web Lock is exempt from intensive
 * throttling).
 */
interface WriterLease {
  state: 'requested' | 'held';
  /** Resolving this hands the held lock back to the browser. */
  release: (() => void) | null;
  /**
   * Aborting this removes a still-QUEUED request from the browser's lock
   * queue (release/dispose before grant, or superseding it with a steal
   * request). Never aborted once held: a held lock is returned via
   * `release`.
   */
  controller: AbortController | null;
  /** When this request was queued (anchors the staleness check). */
  requestedAt: number;
}

/** Heartbeat cadence while holding the writer lease. @internal */
export const LEASE_HEARTBEAT_MS = 20_000;
/**
 * A holder whose heartbeat is older than this is considered suspended and
 * may be stolen from. Must comfortably exceed the worst legitimate
 * heartbeat gap (Chrome background timer clamping is 60s). @internal
 */
export const LEASE_STALE_MS = 120_000;

/** The subset of Storage the heartbeat needs (injectable for tests). */
interface HeartbeatStore {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  /** Optional: stores without it simply skip release-time stamp cleanup. */
  removeItem?(key: string): void;
}

function defaultHeartbeatStore(): HeartbeatStore | null {
  try {
    if (typeof localStorage !== 'undefined' && localStorage !== null) {
      return localStorage;
    }
  } catch (e) {
    // Access itself can throw (storage-disabled documents).
  }
  return null;
}

export interface WebLocksLike {
  request: (
    name: string,
    options: { mode: 'exclusive'; signal?: AbortSignal; steal?: boolean },
    callback: (lock: unknown) => Promise<void>
  ) => Promise<void>;
}

/**
 * Test seam for the Web Locks API. `undefined` = discover the ambient
 * `navigator.locks` (production). Tests MUST pin this (a fake, or `null`
 * for the lock-less/CAS-only environment): the ambient value differs across
 * runtimes — Node has no navigator, real browsers have real Web Locks — and
 * a unit test that inherits it exercises different code paths per runtime
 * (a REAL lock manager would also let one test's undisposed manager block
 * every later test's writes). Injection mirrors the HeartbeatStore seam and
 * keeps the suites free of global stubbing.
 */
let webLocksOverride: WebLocksLike | null | undefined = undefined;

/** @internal */
export function _setWebLocksForTesting(
  locks: WebLocksLike | null | undefined
): void {
  webLocksOverride = locks;
}

function webLocks(): WebLocksLike | null {
  if (webLocksOverride !== undefined) {
    return webLocksOverride;
  }
  if (typeof navigator === 'undefined') {
    return null;
  }
  const locks = (navigator as { locks?: WebLocksLike }).locks;
  return locks && typeof locks.request === 'function' ? locks : null;
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
      /**
       * The decoded root this entry resolved with (set by release; null
       * until then / on miss). Identity key for hasRetainedPeek: stamps may
       * only ride the EXACT retained decode a future listener will join.
       */
      resolvedNode: Node | null;
    }
  >();
  private restoreReasons_ = new Map<string, PersistenceRestoreReason>();
  /** One writer lease per TRACKED root (see the WriterLease notes). */
  private writerLeases_ = new Map<string, WriterLease>();
  /**
   * True while the repo's network is deliberately interrupted (goOffline /
   * repoInterrupt). LIVENESS is not ELIGIBILITY: an offline tab's JS keeps
   * running and heartbeating, but its server cache is frozen — if it kept
   * its leases (or the fail-open gate), an online tab receiving newer
   * server state could never persist it, and storage would hold the
   * disconnected tab's stale tree. While suspended this manager holds no
   * leases, queues none, steals none, and the write gate is CLOSED even
   * where Web Locks don't exist — a stale flush from an offline tab must
   * not overwrite an online writer's fresh generation in the CAS-only
   * environment either. Roots stay tracked; trees stay in memory; resume
   * re-acquires and the armed write windows flush whatever was pending.
   * (Deliberate-offline only: an involuntary network drop hits every tab
   * on the machine alike — no online follower exists to starve — and the
   * connection self-reconnects, so leases follow repoInterrupt/repoResume,
   * not transient socket state.)
   */
  private networkSuspended_ = false;
  /** One timer for all leases: held → heartbeat, requested → steal check. */
  private leaseTimer_: ReturnType<typeof setInterval> | null = null;
  private heartbeatStore_: HeartbeatStore | null;
  /**
   * Identifies THIS manager's heartbeat stamps (`<ms>|<token>`), so a
   * clean release can remove its own stamp without ever deleting a
   * successor's. Without cleanup, a departed holder's stamp lingers: a
   * later holder whose storage cannot WRITE never overwrites it, and a
   * follower that can READ sees a PRESENT-but-stale heartbeat — and
   * steals from a perfectly healthy writer, contradicting the documented
   * page-death fallback for storage-denied holders.
   */
  private heartbeatToken_ =
    Date.now().toString(36) + Math.random().toString(36).slice(2);
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

  /**
   * The current identity-scope generation — bumped by every setAuthScope
   * that changes the scope. Callers whose continuation spans an await after
   * peek() resolves capture this before the wait and compare after, so a
   * scope switch mid-continuation invalidates the result exactly like
   * peek()'s own resolution-time check. @internal
   */
  authGeneration(): number {
    return this.authGeneration_;
  }

  /**
   * True while THE read that decoded `node` is still RETAINED at this root
   * for a future listener join (see readRecord_'s retainAfterResolve) — the
   * only window in which materialization stamps have a consumer. Identity-
   * bound on purpose: a path-only check would also pass for a REPLACEMENT
   * read (the original consumed by a listener mid-walk, a second peek
   * retained since), and stamps would then ride the consumed read's live
   * nodes with no replay ever taking them — a session-long pinned copy of
   * each subtree. False once the read was consumed, expired, superseded,
   * or the manager disposed. @internal
   */
  hasRetainedPeek(pathString: string, node: Node): boolean {
    const entry = this.activeReads_.get(pathString);
    return entry?.retainAfterResolve === true && entry.resolvedNode === node;
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

  /**
   * Lifecycle flush (F11): fire every root's pending write NOW when the page
   * hides. The write window is a debounce for foreground UX; a hiding page
   * has no UX to protect and may never come back — iOS Safari kills
   * background tabs under memory pressure with no beforeunload. An
   * unflushed generation makes the NEXT boot restore a staler tree, whose
   * listen then resends a bigger server delta, which is the giant-message
   * crash amplifier. Best-effort by design: an IndexedDB commit that loses
   * the race against teardown simply doesn't commit (the manifest CAS keeps
   * storage consistent), which is exactly today's behavior without the
   * attempt.
   */
  private lifecycleFlush_ = (): void => {
    if (this.disposed_ || !this.authScopeConfigured_) {
      return;
    }
    for (const pathString of [...this.latest_.keys()]) {
      // flush_ itself skips a root whose newest tree is already stored.
      void this.flushNow(pathString);
    }
  };

  private onVisibilityChange_ = (): void => {
    if (
      typeof document !== 'undefined' &&
      document.visibilityState === 'hidden'
    ) {
      this.lifecycleFlush_();
    }
  };

  private installLifecycleFlush_(): void {
    if (typeof window === 'undefined' || typeof document === 'undefined') {
      return;
    }
    window.addEventListener('pagehide', this.lifecycleFlush_);
    document.addEventListener('visibilitychange', this.onVisibilityChange_);
  }

  private removeLifecycleFlush_(): void {
    if (typeof window === 'undefined' || typeof document === 'undefined') {
      return;
    }
    window.removeEventListener('pagehide', this.lifecycleFlush_);
    document.removeEventListener('visibilitychange', this.onVisibilityChange_);
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
    private peekPreAuthHandoffMs_: number = PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS,
    private leaseHeartbeatMs_: number = LEASE_HEARTBEAT_MS,
    private leaseStaleMs_: number = LEASE_STALE_MS,
    heartbeatStore: HeartbeatStore | null = defaultHeartbeatStore()
  ) {
    this.heartbeatStore_ = heartbeatStore;
    this.installLifecycleFlush_();
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
      this.peekPreAuthHandoffMs_,
      this.leaseHeartbeatMs_,
      this.leaseStaleMs_,
      this.heartbeatStore_
    );
    rebound.networkSuspended_ = this.networkSuspended_;
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
    this.ensureWriterLease_(pathString);
  }

  /**
   * Follows the repo's DELIBERATE network state (repoInterrupt/repoResume,
   * i.e. goOffline/goOnline — see networkSuspended_). Suspending returns
   * every lease so an online tab becomes each root's writer; roots stay
   * tracked and trees stay in memory. Resuming re-queues politely (never
   * steals) and re-arms the write windows, so data seen before or during
   * the offline stretch persists once this tab is eligible again — in the
   * lock-less environment the re-armed window is the whole story, since
   * eligibility there is only the gate.
   */
  setNetworkSuspended(suspended: boolean): void {
    if (this.networkSuspended_ === suspended || this.disposed_) {
      return;
    }
    this.networkSuspended_ = suspended;
    if (suspended) {
      this.releaseAllWriterLeases_();
      return;
    }
    for (const pathString of this.trackedRoots_) {
      this.ensureWriterLease_(pathString);
      this.armWriteWindowIfPending_(pathString);
    }
  }

  /**
   * True when this manager may write the root: it holds the root's writer
   * lease, or leases are unenforceable here (no Web Locks, or the root has
   * no lease entry — the manifest CAS remains the correctness backstop).
   */
  private holdsWriterLease_(pathString: string): boolean {
    if (this.networkSuspended_) {
      // Ineligible, not merely lease-less: with no lease entry the gate
      // would fail OPEN, and an offline tab's adopt-then-restage would
      // overwrite an online writer's fresh generation with stale data.
      return false;
    }
    const lease = this.writerLeases_.get(pathString);
    return lease === undefined ? true : lease.state === 'held';
  }

  /** The root's shared heartbeat key. */
  private heartbeatKey_(pathString: string): string {
    return 'firebase-database-persistence-writer|' + this.key_(pathString);
  }

  private writeHeartbeat_(pathString: string): void {
    try {
      this.heartbeatStore_?.setItem(
        this.heartbeatKey_(pathString),
        Date.now() + '|' + this.heartbeatToken_
      );
    } catch (e) {
      // Storage that exists but THROWS (storage-disabled documents, quota)
      // means this manager cannot participate in the heartbeat protocol at
      // all: keep trying and every holder stamp fails silently while
      // followers keep reading whatever is there. Disable the channel for
      // this manager's lifetime — takeover degrades to page death, which is
      // the documented no-shared-storage mode.
      this.heartbeatStore_ = null;
    }
  }

  private readHeartbeat_(pathString: string): number {
    try {
      const raw = this.heartbeatStore_?.getItem(this.heartbeatKey_(pathString));
      // `<ms>|<token>` (and bare `<ms>` from older stamps) both parse.
      const value =
        raw === null || raw === undefined
          ? NaN
          : Number(String(raw).split('|')[0]);
      return isNaN(value) ? 0 : value;
    } catch (e) {
      // See writeHeartbeat_: a throwing store is a dead channel, and a
      // reader that cannot see heartbeats must never steal (the tick's
      // null-store guard makes this permanent, not just this tick).
      this.heartbeatStore_ = null;
      return 0;
    }
  }

  /**
   * One tick, role by lease state: a holder proves liveness (heartbeat); a
   * queued follower checks the holder's liveness and STEALS the lock when
   * the heartbeat is PRESENT but stale — the holder stamped once (every
   * holder stamps at grant) and then went silent: frozen, cached,
   * suspended, or wedged, and would otherwise starve every live tab's
   * writes for as long as it existed. An ABSENT heartbeat never justifies a
   * steal: it means the liveness protocol is not operating for this lock —
   * the holder's storage throws, the stamp was cleared, or nothing was
   * ever granted — and stealing on silence alone would take the lock from
   * a perfectly healthy writer over and over (each stolen holder re-queues
   * and, reading the same absence, steals right back). Without a readable
   * heartbeat, takeover degrades to page death — the documented
   * no-shared-storage mode. The request-time anchor additionally prevents
   * stealing within the staleness budget of first joining the queue.
   */
  private onLeaseTick_(): void {
    if (this.disposed_) {
      return;
    }
    for (const [pathString, lease] of this.writerLeases_) {
      if (lease.state === 'held') {
        this.writeHeartbeat_(pathString);
        continue;
      }
      if (this.heartbeatStore_ === null) {
        return;
      }
      const heartbeat = this.readHeartbeat_(pathString);
      if (heartbeat <= 0) {
        continue;
      }
      const freshest = Math.max(heartbeat, lease.requestedAt);
      if (Date.now() - freshest > this.leaseStaleMs_) {
        this.requestWriterLease_(pathString, true);
      }
    }
  }

  /** Requests the root's writer lease once (idempotent per root). */
  private ensureWriterLease_(pathString: string): void {
    if (
      this.writerLeases_.has(pathString) ||
      this.disposed_ ||
      this.networkSuspended_
    ) {
      return;
    }
    const locks = webLocks();
    if (locks === null) {
      return;
    }
    this.requestWriterLease_(pathString, false);
    if (this.leaseTimer_ === null && this.writerLeases_.size > 0) {
      this.leaseTimer_ = setInterval(() => {
        this.onLeaseTick_();
      }, this.leaseHeartbeatMs_);
      (this.leaseTimer_ as { unref?: () => void }).unref?.();
    }
  }

  /**
   * Puts a lease request for the root in the browser's queue, superseding
   * any current one (`steal` preempts a stale holder; see onLeaseTick_).
   */
  private requestWriterLease_(pathString: string, steal: boolean): void {
    const locks = webLocks();
    if (locks === null) {
      return;
    }
    const previous = this.writerLeases_.get(pathString);
    const lease: WriterLease = {
      state: 'requested',
      release: null,
      // The Web Locks spec FORBIDS combining `signal` with `steal`
      // (NotSupportedError — verified in Chrome: the request rejects
      // immediately and no steal happens). A steal needs no abort path
      // anyway: it is granted almost at once, and a steal that lands after
      // this lease was superseded/disposed is handed straight back by the
      // grant callback's identity check.
      controller:
        !steal && typeof AbortController !== 'undefined'
          ? new AbortController()
          : null,
      requestedAt: Date.now()
    };
    this.writerLeases_.set(pathString, lease);
    // Replace-then-abort: the superseded request's rejection sees a
    // different current lease and is a no-op.
    if (previous !== undefined && previous.state === 'requested') {
      previous.controller?.abort();
    }
    const options: {
      mode: 'exclusive';
      signal?: AbortSignal;
      steal?: boolean;
    } = { mode: 'exclusive' };
    if (lease.controller !== null) {
      options.signal = lease.controller.signal;
    }
    if (steal) {
      options.steal = true;
    }
    const onSettled = (failed: boolean) => {
      if (this.writerLeases_.get(pathString) !== lease) {
        return; // superseded or released: nothing to do
      }
      if (lease.state === 'held') {
        // A held lock's request promise only settles early when another
        // tab STOLE it (a stale-heartbeat takeover while this page was
        // suspended, or a lock-manager failure treated the same way). Stop
        // writing at once and re-queue politely — never steal back
        // unprompted; any stale baseline reconciles through the flush CAS.
        //
        // Settle the STOLEN callback first: the UA keeps the holder
        // callback pending until the promise it returned settles, and
        // re-queueing replaces the map entry, so no later release or
        // dispose could ever reach this resolver again. Left unsettled,
        // every steal leaks one pending callback — whose closure retains
        // this manager (and, once disposed, its baselines) indefinitely.
        lease.release?.();
        lease.release = null;
        this.requestWriterLease_(pathString, false);
        return;
      }
      if (failed) {
        // Queued-request failure (not a supersede — those hit the identity
        // guard above): fail open rather than never persisting, and re-arm
        // the write window a skipped flush may have consumed.
        this.writerLeases_.delete(pathString);
        this.stopLeaseTimerIfIdle_();
        this.armWriteWindowIfPending_(pathString);
      }
    };
    try {
      void locks
        .request(this.writerLeaseName_(pathString), options, () => {
          if (this.writerLeases_.get(pathString) !== lease || this.disposed_) {
            // Superseded/released/disposed while queued: hand the lock
            // straight back so the next tab's request is granted.
            return Promise.resolve();
          }
          lease.state = 'held';
          this.writeHeartbeat_(pathString);
          // Writes for this root were skipped while another tab held its
          // lease; whatever is pending in memory enters the ordinary write
          // window now. A stale baseline (the old holder committed)
          // resolves through the flush CAS + adoptCommittedBaseline_,
          // exactly once.
          this.armWriteWindowIfPending_(pathString);
          return new Promise<void>(resolve => {
            lease.release = resolve;
          });
        })
        .then(
          () => onSettled(false),
          () => onSettled(true)
        );
    } catch (e) {
      // A synchronously-throwing request() must not break the listen path
      // that called track().
      onSettled(true);
    }
  }

  private writerLeaseName_(pathString: string): string {
    return 'firebase-database-persistence-write|' + this.key_(pathString);
  }

  /**
   * Removes THIS manager's own heartbeat stamp (token-checked, so a
   * successor's stamp is never deleted). The get→remove pair is not
   * atomic; the benign worst case is deleting a successor stamp written
   * in between — absence never justifies a steal, and the successor
   * re-stamps on its next tick. A crashed holder never runs this, so its
   * stamp can linger: a follower may then steal once from a write-denied
   * successor — accepted residual; the stealer stamps and it stabilizes.
   */
  private clearOwnHeartbeat_(pathString: string): void {
    const store = this.heartbeatStore_;
    if (store === null || typeof store.removeItem !== 'function') {
      return;
    }
    try {
      const key = this.heartbeatKey_(pathString);
      const raw = store.getItem(key);
      if (typeof raw === 'string' && raw.endsWith('|' + this.heartbeatToken_)) {
        store.removeItem(key);
      }
    } catch (e) {
      this.heartbeatStore_ = null;
    }
  }

  /** Returns the root's writer lease to the browser (idempotent). */
  private releaseWriterLease_(pathString: string): void {
    const lease = this.writerLeases_.get(pathString);
    if (lease === undefined) {
      return;
    }
    this.writerLeases_.delete(pathString);
    this.stopLeaseTimerIfIdle_();
    if (lease.state === 'held') {
      // Clean handoff: take the stamp with us, so a successor that cannot
      // write storage is judged by ABSENCE (page-death handoff), not by
      // our lingering, eventually-stale stamp (see heartbeatToken_).
      this.clearOwnHeartbeat_(pathString);
      lease.release?.();
    } else {
      lease.controller?.abort();
    }
  }

  /**
   * Cleanup-completion rule shared by untrack paths: return the root's
   * lease unless the root was re-tracked meanwhile — the new listen owns
   * it now.
   */
  private releaseWriterLeaseIfUntracked_(pathString: string): void {
    if (!this.trackedRoots_.has(pathString)) {
      this.releaseWriterLease_(pathString);
    }
  }

  /** Returns every lease (dispose). */
  private releaseAllWriterLeases_(): void {
    for (const pathString of [...this.writerLeases_.keys()]) {
      this.releaseWriterLease_(pathString);
    }
  }

  /**
   * A tab tracking nothing must neither heartbeat nor evaluate steals: the
   * tick stops with the last lease and restarts with the next track().
   */
  private stopLeaseTimerIfIdle_(): void {
    if (this.writerLeases_.size === 0 && this.leaseTimer_ !== null) {
      clearInterval(this.leaseTimer_);
      this.leaseTimer_ = null;
    }
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
      // Housekeeping delete (the covering ancestor's record is the one
      // future sessions should restore): guarded to the one generation
      // this manager itself verified or wrote — a revision-NAMED delete is
      // safe without lock ownership by construction, because it can never
      // remove a successor generation some other writer committed. An
      // ADOPTED baseline (rootNode null) carries another writer's revision
      // for content this manager never saw — it authorizes nothing, and
      // skipping is safe (a leftover record is at worst a slightly stale
      // shadow the hash protocol revalidates).
      const prev = this.lastFlush_.get(pathString);
      const ownedRevision =
        prev !== undefined && prev.rootNode !== null ? prev.revision : null;
      this.latest_.delete(pathString);
      this.lastFlush_.delete(pathString);
      this.changedSinceFlush_.delete(pathString);
      if (ownedRevision !== null) {
        void this.enqueue_(pathString, () =>
          this.deleteRecordIfRevision_(pathString, ownedRevision)
        );
      }
      // The delete authorizes itself (revision-named), so the lease can
      // return right away; the re-track guard keeps a fresh listen's lease.
      this.releaseWriterLeaseIfUntracked_(pathString);
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
      // AFTER the final flush so the last tree still writes under this
      // tab's lease; a re-tracked root keeps it (the new listen owns it).
      this.releaseWriterLeaseIfUntracked_(pathString);
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
      resolvedNode: Node | null;
    } = {
      promise: Promise.resolve(null),
      progress,
      retainAfterResolve,
      cleanupTimer: null,
      manifestHashes: null,
      manifestCallbacks: new Set([onManifest]),
      resolvedNode: null
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
      entry.resolvedNode = result === null ? null : result.record.node;
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
    // The revision of the generation a corrupt/expired verdict was reached
    // ON — the cleanup below deletes only THAT generation (a revision-named
    // delete cannot remove a successor another writer commits between this
    // read and the cleanup transaction). Null when the stored manifest is
    // too malformed to even carry a string revision (nothing current can be
    // named; see the cleanup site).
    let cleanupRevision: string | null = null;
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
          // ABSENT (undefined) is a plain miss; a stored literal `null` is
          // NOT — it is garbage that must flow to the corrupt branch so the
          // in-transaction cleanup reclaims it and its sidecars (folding it
          // into the miss would leave it cached forever).
          if (manifestReq.result === undefined) {
            done(null);
            return;
          }
          const manifest = manifestReq.result as PersistedManifest | null;
          if (!structurallyValidManifest(manifest)) {
            const rawRevision =
              manifest === null
                ? undefined
                : (manifest as { revision?: unknown }).revision;
            if (typeof rawRevision === 'string') {
              cleanupRevision = rawRevision;
            }
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
            cleanupRevision = manifest.revision;
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
                  cleanupRevision = manifest.revision;
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
          cleanupRevision = collected.manifest.revision;
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
          // Best-effort cleanup, NAMED to the generation the verdict was
          // reached on: a successor committed meanwhile (this manager may
          // be a follower queued behind another tab's lease) must survive.
          // Auth/missing misses must not delete another identity's
          // otherwise valid cache record. A manifest too malformed to carry
          // a revision cannot be named — its cleanup re-reaches the verdict
          // INSIDE the delete transaction instead (deleteRecordIfInvalid_):
          // "no CAS writer produced this" was established by a readonly
          // read and does not hold across the transaction boundary — a
          // concurrent writer may have replaced the garbage with a valid
          // generation by the time the delete runs.
          void (cleanupRevision !== null
            ? this.deleteRecordIfRevision_(pathString, cleanupRevision)
            : this.deleteRecordIfInvalid_(pathString));
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

  /**
   * Cleanup for a manifest judged structurally invalid: the verdict is
   * re-reached INSIDE the readwrite transaction, so a valid generation a
   * concurrent writer committed after the (readonly) judgement is never
   * touched. Still-invalid garbage — whatever garbage it is by now — goes.
   */
  private deleteRecordIfInvalid_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, (store, done) => {
      const req = store.get(key);
      req.onsuccess = () => {
        const manifest = req.result as PersistedManifest | undefined;
        if (manifest === undefined || structurallyValidManifest(manifest)) {
          done(undefined);
          return;
        }
        this.deleteRecordInStore_(store, key);
        done(undefined);
      };
    });
  }

  /**
   * Housekeeping variant of deleteRecord_: deletes the root's record only
   * while the committed manifest still carries `expectedRevision` — the one
   * generation this manager itself verified or wrote. An unconditional
   * housekeeping delete could erase a FRESH generation another tab
   * committed for this root after this manager last looked (that tab keeps
   * flushing under its own lease and would skip identical rewrites against
   * a lastFlush_ that no longer describes storage). Check and delete run in
   * ONE readwrite transaction, so a concurrent commit cannot interleave
   * between them. Skipping is always safe: a record left behind is at
   * worst a slightly stale shadow, and every restored record is
   * revalidated against the server by the hash protocol anyway.
   */
  private deleteRecordIfRevision_(
    pathString: string,
    expectedRevision: string
  ): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, store => {
      const req = store.get(key);
      req.onsuccess = () => {
        const manifest = req.result as PersistedManifest | undefined;
        if (!manifest || manifest.revision !== expectedRevision) {
          return;
        }
        this.deleteRecordInStore_(store, key);
      };
    });
  }

  /** Deletes a root's manifest and every '#'-suffixed sidecar in `store`. */
  private deleteRecordInStore_(store: IDBObjectStore, key: string): void {
    store.delete(key);
    // Immutable ranges and legacy chunk/hash/tree sidecars share '#'.
    // suffix namespace. Range-delete where the platform has IDBKeyRange;
    // cursor-walk otherwise (Node, test fakes) — key-only, no values.
    if (typeof IDBKeyRange !== 'undefined') {
      try {
        store.delete(
          IDBKeyRange.bound(key + '#', key + '#' + String.fromCharCode(0xffff))
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
        state.rootNode !== null &&
        pathString !== root &&
        (root === '/' || pathString.startsWith(root + '/')) &&
        (bestRoot === null || root.length > bestRoot.length)
      ) {
        const rootNode = state.rootNode;
        bestRoot = root;
        source = Promise.resolve({
          record: {
            node: rootNode,
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

  /**
   * Re-enters the ordinary write window when the root still has work: it is
   * tracked and holds a pending tree in latest_ (flush_ reads latest_ when
   * it runs, so whatever landed meanwhile is covered). The one definition
   * used by every deferred-retry path — a lease grant after skipped writes,
   * a failed-open lock acquisition, and the stale-baseline adoption.
   */
  private armWriteWindowIfPending_(pathString: string): void {
    if (
      !this.disposed_ &&
      this.trackedRoots_.has(pathString) &&
      this.latest_.has(pathString)
    ) {
      this.armWriteWindow_(pathString);
    }
  }

  /**
   * Arms the non-restarting single-flight write window for a root. Two
   * regimes: a root with a flush baseline coalesces under the ordinary
   * window; a root with none (first generation — see
   * PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS) flushes on the shorter of
   * the two delays so the cache exists before short mobile sessions end.
   */
  private armWriteWindow_(pathString: string): void {
    if (!this.writeTimers_.has(pathString)) {
      const delay = this.lastFlush_.has(pathString)
        ? this.writeDelayMs_
        : Math.min(
            this.writeDelayMs_,
            PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS
          );
      this.writeTimers_.set(
        pathString,
        setTimeout(() => {
          this.writeTimers_.delete(pathString);
          this.scheduleFlush_(pathString);
        }, delay)
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
    // Divorce release: after a wholesale replace (a fallback resend, a giant
    // sliced overwrite, a range merge folding the whole root — regardless of
    // reported precision, since a sliced root push reports the precise
    // root path) the incoming tree shares NO immediate-child identity with
    // the flush baseline. Keeping `prev.rootNode` then retains a second
    // complete tree in memory for a diff that would collapse to all-dirty
    // anyway (visit-budget bail) — and a tab that never wins the writer
    // lease NEVER flushes, so without this release the divorced baseline
    // stays pinned for the tab's whole lifetime. Drop the tree but keep the
    // revision/ranges (rootNode: null — the adopted-baseline shape): the
    // CAS still works, and the next flush stages a fresh self-contained
    // generation exactly as it does after a manifest-only adoption. The
    // scan aborts on the first shared child, so an ordinary incremental
    // update (siblings keep identity by construction) costs a few lookups.
    if (
      prev !== undefined &&
      prev.rootNode !== null &&
      baselineFullyDivorced(prev.rootNode, node)
    ) {
      this.lastFlush_.set(pathString, {
        rootNode: null,
        revision: prev.revision,
        ranges: prev.ranges,
        storedUpdatedAt: prev.storedUpdatedAt
      });
      this.changedSinceFlush_.set(pathString, null);
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
    // The record being invalidated is the one this manager just RESTORED —
    // its revision sits in lastFlush_ (set by restoreForListen). Name the
    // delete to it so a successor generation another writer committed
    // meanwhile survives. Unnamable (no verified baseline): skip — the next
    // restore of a genuinely bad record fails again and readRecordOnce_'s
    // own named cleanup removes it.
    const prev = this.lastFlush_.get(pathString);
    const restoredRevision =
      prev !== undefined && prev.rootNode !== null ? prev.revision : null;
    this.latest_.delete(pathString);
    this.lastFlush_.delete(pathString);
    this.changedSinceFlush_.delete(pathString);
    recordPersistenceEvent(pathString, 'invalidate', 'corrupt-or-incompatible');
    if (restoredRevision !== null) {
      void this.enqueue_(pathString, () =>
        this.deleteRecordIfRevision_(pathString, restoredRevision)
      );
    }
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
    // The root left tracking: return its lease so a tab that still tracks
    // it can write. Order relative to the queued purge is free — the purge
    // authorizes itself inside its own transaction, never via the lease.
    this.releaseWriterLease_(pathString);
    // The purge is IMMEDIATE and atomic — it must never wait for the
    // writer lease. The lease is held for the holder tab's lifetime, and a
    // holder that does not listen to this root never receives the
    // revocation itself: a purge deferred to lease grant would leave the
    // revoked bytes cached for as long as that tab lives, violating the
    // invariant above. Deleting without the lease is made safe by SCOPE,
    // checked in the same readwrite transaction: only the writer can have
    // committed a newer generation here, and a same-scope writer tracking
    // this root receives the same revocation and evicts too (clearing its
    // own lastFlush_, so no stale identical-rewrite short-circuit
    // survives); a writer NOT tracking this root never writes it at all. A
    // manifest under ANOTHER identity's scope is left alone — this user's
    // revoked bytes are not in it, and the other identity's access is its
    // own. (Residual, accepted: a same-scope writer that legitimately
    // RETAINS access through different query-level rules can have a fresh
    // generation purged and skip identical rewrites against its stale
    // lastFlush_ until the manifest-refresh path self-heals it — bounded
    // cache staleness, never corruption.) Through the root's queue, so a
    // flush of this manager already in flight finishes first.
    // The scope whose access was revoked is CAPTURED NOW, not read later:
    // the purge runs behind any in-flight per-root work, and an account
    // switch (setAuthScope) can land in that gap. Compared against the
    // manager's LIVE scope, the old identity's revoked record would read
    // as "another identity's" and be preserved, while a fresh record the
    // NEW identity just committed would match and be deleted — exactly
    // backwards. The reference value for in-transaction validation must be
    // immutable, like deleteRecordIfRevision_'s expectedRevision.
    const revokedScope = this.authScope_;
    void this.enqueue_(pathString, () =>
      this.purgeEvictedRecord_(pathString, revokedScope)
    );
  }

  /**
   * Eviction's delete: manifest + sidecars in one transaction, gated on the
   * stored manifest belonging to the REVOKED scope (captured at evict();
   * see the comment there). `null` is a REAL scope — the anonymous
   * identity — not malformation: an anonymous user's valid record must
   * survive a signed-in tab's eviction exactly like any other identity's.
   * The unconditional purge is reserved for values no live writer produced
   * — a structurally invalid manifest, a malformed scope field (neither
   * string nor null), or a stored value that is not an object at all
   * (null, primitives): eviction is exactly the moment to drop those WITH
   * their sidecars, which may still carry revoked bytes. Only a truly
   * ABSENT record (undefined) is a no-op. Field reads happen only after
   * structural validation — a stored literal `null` passes an
   * undefined-check and then throws on property access, aborting the
   * transaction and silently RETAINING the revoked record.
   */
  private purgeEvictedRecord_(
    pathString: string,
    revokedScope: string | null
  ): Promise<void> {
    const key = this.key_(pathString);
    return this.withStore_<void>('readwrite', undefined, (store, done) => {
      const req = store.get(key);
      req.onsuccess = () => {
        const stored = req.result as PersistedManifest | null | undefined;
        if (stored === undefined) {
          done(undefined);
          return;
        }
        if (structurallyValidManifest(stored)) {
          const scope = (stored as { authScope?: unknown }).authScope;
          if (
            (typeof scope === 'string' || scope === null) &&
            scope !== revokedScope
          ) {
            // Another identity's valid record: the revoked bytes are not
            // in it, and the other identity's access is its own.
            done(undefined);
            return;
          }
        }
        this.deleteRecordInStore_(store, key);
        done(undefined);
      };
    });
  }

  dispose(): void {
    this.disposed_ = true;
    this.removeLifecycleFlush_();
    this.releaseAllWriterLeases_();
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
   * Adopts the currently COMMITTED generation as the next flush baseline
   * WITHOUT reading or decoding its range payloads — a manifest-only read.
   *
   * Used when this manager discovers its baseline is stale (the flush CAS
   * lost to another writer, or the stored generation vanished): the
   * winner's revision + ranges are all the next CAS needs, while its tree
   * stays undecoded (rootNode: null). The follow-up flush cannot diff
   * against an absent tree, so it stages a fresh self-contained generation
   * — the same write the old adopt-and-diff produced anyway (a freshly
   * decoded tree shares no identity with the live one, so its identity
   * diff marked every range dirty) minus the full IndexedDB read and Node
   * decode of the entire root that made every cross-tab conflict as
   * expensive as a cold restore.
   *
   * The retry enters the ordinary NON-RESTARTING write window instead of
   * re-flushing immediately: under sustained cross-tab churn an immediate
   * retry conflicts again back-to-back — full-tree work with no pause
   * between attempts (the multi-tab thrash the write leases exist to
   * prevent, kept bounded here for lease-less environments too).
   */
  private adoptCommittedBaseline_(pathString: string): Promise<void> {
    const key = this.key_(pathString);
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
      if (this.disposed_) {
        return;
      }
      // The adopted baseline is another generation's tree; paths named
      // against our own chain do not describe diffs from it.
      this.changedSinceFlush_.set(pathString, null);
      if (
        structurallyValidManifest(manifest) &&
        manifest.authScope === this.authScope_
      ) {
        this.lastFlush_.set(pathString, {
          rootNode: null,
          revision: manifest.revision,
          ranges: manifest.ranges,
          storedUpdatedAt: manifest.updatedAt
        });
      } else {
        // Missing, foreign-scope, or unreadable: the next flush stages
        // under the absent / replaceable-foreign CAS arm instead.
        this.lastFlush_.delete(pathString);
      }
      // The write window is the ONLY retry path. A window that elapsed
      // while the losing flush was in flight marked flushPending_, and the
      // queue drain would re-flush IMMEDIATELY on settle — full-tree
      // staging back-to-back under sustained lease-less churn, bypassing
      // the debounce this adoption exists to provide. The armed window
      // supersedes it: flush_ reads latest_ when it runs, so the update
      // that marked the queue pending is still fully covered, just
      // deferred. For an untracked root (the final flush from untrack lost
      // the CAS) there is deliberately no retry at all: the winner's
      // generation is a coherent snapshot seconds-fresh at most, and the
      // hash protocol revalidates it on the next boot — not worth keeping
      // the tree and lease alive past untrack (this matches the pre-lease
      // behavior, whose drain retry always found latest_ already released).
      this.flushPending_.delete(pathString);
      this.armWriteWindowIfPending_(pathString);
    });
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
    if (!this.holdsWriterLease_(pathString)) {
      // Another tab is this root's writer. latest_ keeps the newest tree in
      // memory; if the lease ever transfers here, the grant callback
      // re-enters the ordinary write window for this root.
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
      // An empty tree is a GENERATION, and deleting the record is its
      // commit — so it obeys the exact CAS arms a manifest commit does,
      // inside one readwrite transaction. The lease check above ran before
      // async work: Web Locks `steal` can revoke it while this flush is
      // suspended, and an unconditional delete on resume would erase the
      // manifest and ranges the NEW holder committed meanwhile. With a
      // baseline, delete only the baseline's revision (adopted counts —
      // this is commit CAS, not an ownership guard); with none, only an
      // absent record or a replaceable-foreign manifest (same live scope
      // staging over another identity/format — see the commit arms) may be
      // removed. Anything else is a CAS conflict: adopt the winner
      // manifest-only and let the write window retry — where the lease
      // gate runs again, so a stolen holder never retries as a writer.
      return this.withStore_<boolean>(
        'readwrite',
        false,
        (store, done, progress) => {
          // Eligibility is re-checked INSIDE the commit transaction: the
          // gate at flush entry ran before async staging, and the lease can
          // be released (goOffline) or lost (steal) while this flush was
          // suspended in between. Losing it reads as a CAS conflict — the
          // adopt + write-window retry re-runs the entry gate. Re-running
          // the LIVE gate (not a captured token) deliberately still allows
          // a suspend→resume→re-granted holder to commit: it is the
          // eligible writer again, and nothing newer can exist locally.
          if (!this.holdsWriterLease_(pathString)) {
            done(false);
            return;
          }
          const req = store.get(key);
          req.onsuccess = () => {
            progress();
            // A stored literal `null` is garbage no CAS writer produced;
            // normalized to ABSENT so the empty commit succeeds instead of
            // throwing on the field reads below (an exception here aborts
            // the transaction, and every window retry would abort the same
            // way — the root could never flush again). Restore-side
            // cleanup (deleteRecordIfInvalid_) reclaims the value and its
            // sidecars.
            const current = (req.result ?? undefined) as
              | PersistedManifest
              | undefined;
            if (current === undefined) {
              done(true);
              return;
            }
            const replaceableForeign =
              !prev &&
              authScope === this.authScope_ &&
              (current.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
                current.authScope !== authScope);
            if (
              (prev && current.revision === prev.revision) ||
              replaceableForeign
            ) {
              this.deleteRecordInStore_(store, key);
              done(true);
              return;
            }
            done(false);
          };
        }
      ).then(ok => {
        if (this.disposed_) {
          return;
        }
        if (ok) {
          this.lastFlush_.delete(pathString);
          if (persistenceTraceSinkInstalled()) {
            emitPersistenceTrace({
              type: 'flush',
              path: pathString,
              mode: 'empty',
              ranges: 0,
              rangesHashed: 0,
              rangesReused: 0,
              ...baselineSharing(prev, node)
            });
          }
          return;
        }
        return this.adoptCommittedBaseline_(pathString);
      });
    }
    if (prev && prev.rootNode === node) {
      // Content-identical: refresh only the manifest timestamp. The revision
      // guard prevents a stale tab from refreshing a superseded generation.
      // 'ineligible' (the lease was released or lost between the entry gate
      // and this transaction — see the empty-path comment) is a plain
      // no-op, NOT a conflict: the stored generation may be perfectly
      // current, and an ineligible tab must neither extend its perceived
      // freshness nor discard its own decoded baseline over it.
      return this.withStore_<'refreshed' | 'conflict' | 'ineligible'>(
        'readwrite',
        'conflict',
        (store, done, progress) => {
          if (!this.holdsWriterLease_(pathString)) {
            done('ineligible');
            return;
          }
          const req = store.get(key);
          req.onsuccess = () => {
            progress();
            const current = req.result as PersistedManifest | undefined;
            if (current && current.revision === prev.revision) {
              const put = store.put({ ...current, updatedAt: now }, key);
              put.onsuccess = progress;
              done('refreshed');
            } else {
              done('conflict');
            }
          };
        }
      ).then(outcome => {
        if (this.disposed_ || outcome === 'ineligible') {
          return;
        }
        if (outcome === 'refreshed') {
          this.lastFlush_.set(pathString, { ...prev, storedUpdatedAt: now });
          return;
        }
        // The stored generation is gone (another identity's manifest, a
        // sweep, or manual storage clearing). lastFlush_ no longer describes
        // storage; left in place, every future identical-node write-through
        // would skip against it and the root would stay unpersisted for the
        // whole session. Resync from the committed manifest — never a full
        // range read/decode — and rebuild once, through the write window.
        return this.adoptCommittedBaseline_(pathString);
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
    // An adopted baseline (rootNode null — another writer's committed
    // manifest) has UNKNOWN content: neither the identity diff nor paths
    // accumulated against our own chain describe differences from it, and
    // carrying any of its ranges over unverified would splice two server
    // snapshots into one stored tree. Stage a fresh full generation; its
    // revision still CASes against the adopted manifest.
    if (prev && prev.ranges.length > 0 && prev.rootNode !== null) {
      const changed =
        accumulated !== null && accumulated !== undefined
          ? accumulated
          : collectChangedSubtreePaths(prev.rootNode, node);
      previousRanges = prev.ranges;
      if (changed.length === 0) {
        dirty = new Array(prev.ranges.length).fill(false);
      } else {
        const marked = markDirtyRanges(prev.ranges, changed);
        dirty = marked.dirty;
        tailDirty = marked.tailDirty;
      }
    }

    // First pass: boundaries/sizes only. It never creates canonical strings
    // or export payloads, so a first generation cannot retain another full
    // copy of the root merely to decide its ranges. Drained in bounded
    // main-thread slices: a whole-root plan (first generation after a cold
    // boot — every range dirty) is a full leaf walk, and running it
    // synchronously was a multi-second stall exactly on the boots that must
    // complete their first flush to escape the cold-reload loop.
    const planner = new CompoundHashBuilder(
      fixedSizeSplitStrategy(this.rangeTargetBytes_),
      true
    );
    const planSliced = async (): Promise<StableRange[]> => {
      const rebuilder = new StableRangeRebuilder(
        node,
        previousRanges,
        dirty,
        tailDirty,
        planner,
        this.rangeTargetBytes_
      );
      while (!rebuilder.drainUntil(Date.now() + FLUSH_PLAN_SLICE_MS)) {
        await yieldMacrotask();
        if (this.disposed_) {
          throw new FlushObsoleteError();
        }
      }
      return rebuilder.result();
    };
    return planSliced().then(
      rebuilt => this.finishFlush_(pathString, entry, prev, accumulated, rebuilt),
      e => {
        if (e instanceof FlushObsoleteError) {
          return;
        }
        persistenceStats.storageFailures++;
        recordPersistenceEvent(pathString, 'flush-plan-error');
        // Protective cleanup of THIS manager's own possibly-implicated
        // generation — housekeeping, so it follows the ownership rule (see
        // deleteRecordIfRevision_ / the covered-untrack delete): only a
        // revision this manager itself verified or wrote. An ADOPTED baseline
        // (rootNode null) is another writer's generation — a local planning
        // failure says nothing about it — and with no baseline at all there
        // is nothing of ours to protect against. A lease lost to a steal
        // while this flush was suspended is covered the same way: the
        // revision-named delete cannot touch the new holder's generation.
        const owned =
          prev !== undefined && prev.rootNode !== null ? prev.revision : null;
        this.lastFlush_.delete(pathString);
        return owned !== null
          ? this.deleteRecordIfRevision_(pathString, owned)
          : Promise.resolve();
      }
    );
  }

  /**
   * Second half of a flush: stages the planned dirty ranges and commits the
   * generation. Split from flush_ so the sliced planner can yield between
   * slices without holding the whole body in one closure. `entry` is the
   * latest_ record the flush entered with (its node/revision/authScope are
   * the generation being written); `rebuilt` is the planned range list —
   * clean ranges carried with their recordIds, dirty ranges with empty
   * hashes to be serialized, digested, and staged here.
   */
  private finishFlush_(
    pathString: string,
    entry: { node: Node; revision: string; authScope: string | null },
    prev: FlushedState | undefined,
    accumulated: string[][] | null | undefined,
    rebuilt: StableRange[]
  ): Promise<void> {
    const { node, revision, authScope } = entry;
    const now = Date.now();
    const key = this.key_(pathString);

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
      // explicitly and yield a macrotask so WebKit can collect between
      // batches (yieldMacrotask: MessageChannel, exempt from the nested
      // setTimeout clamp that stretched many-batch generations by seconds).
      for (const record of records) {
        record.tree = undefined;
      }
      records.length = 0;
      texts.length = 0;
      digests.length = 0;
      await yieldMacrotask();
      if (this.disposed_) {
        throw new FlushObsoleteError();
      }
    };

    const stageAll = async (): Promise<void> => {
      // Batches are cut by planned canonical-text bytes, not range count:
      // ranges vary from a few bytes to ~2x the split target, and a fixed
      // count made slice cost swing with them. A single oversized range
      // still ships alone (the batch admits the first plan unconditionally).
      let batch: DirtyRangePlan[] = [];
      let batchBytes = 0;
      for (const plan of dirtyPlans) {
        if (batch.length > 0 && batchBytes + plan.range.size > FLUSH_STAGE_BATCH_BYTES) {
          await stageBatch(batch);
          batch = [];
          batchBytes = 0;
        }
        batch.push(plan);
        batchBytes += plan.range.size;
      }
      if (batch.length > 0) {
        await stageBatch(batch);
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
          // Sum of canonical-text range sizes, already computed by the
          // planner (the old estimateSerializedNodeSize call here was a
          // second full-tree walk solely for this field). NOTE: a different
          // measure than that estimate (canonical text vs JSON-ish size) —
          // same order of magnitude, and the LRU sweep that consumes
          // estimatedBytes only needs a consistent-scale byte proxy. Old
          // manifests keep their estimate until content next changes; the
          // mixed sum drifts the sweep budget by at most that scale gap.
          estimatedBytes: ranges.reduce((sum, range) => sum + range.size, 0),
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
            // Same in-transaction eligibility re-check as the empty path:
            // the entry gate ran before staging, and the lease can be
            // released (goOffline) or lost (steal) while the staging work
            // was in flight. Failing reads as a CAS conflict — staged ids
            // are reclaimed and the write window (which re-runs the entry
            // gate) owns any retry.
            if (!this.holdsWriterLease_(pathString)) {
              done(false);
              return;
            }
            const currentReq = store.get(key);
            currentReq.onsuccess = () => {
              progress();
              // Stored `null` normalizes to ABSENT (see the empty-path
              // comment): the first-generation commit then OVERWRITES the
              // garbage instead of throwing on the replaceableForeign
              // field reads and aborting every commit of this root forever.
              const current = (currentReq.result ?? undefined) as
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
            // our immutable ids and adopt the winning manifest as the CAS
            // baseline (manifest-only — no range read, no decode); the next
            // window stages one fresh self-contained generation against it.
            return this.withStore_<void>('readwrite', undefined, store => {
              for (const recordId of stagedIds) {
                store.delete(key + RANGE_KEY_INFIX + recordId);
              }
            }).then(() => this.adoptCommittedBaseline_(pathString));
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
          if (persistenceTraceSinkInstalled()) {
            emitPersistenceTrace({
              type: 'flush',
              path: pathString,
              mode: 'commit',
              ranges: ranges.length,
              rangesHashed: dirtyPlans.length,
              rangesReused: ranges.length - dirtyPlans.length,
              ...baselineSharing(prev, node)
            });
          }
          if (!prev) {
            void this.gcRangeRecords_(pathString, revision, liveIds);
          }
        });
      })
      .catch((e: unknown) => {
        if (e instanceof FlushObsoleteError) {
          // Disposed mid-stage: staged ids are reclaimed by the next
          // successful GC/sweep; nothing to merge back — the manager is gone.
          return;
        }
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

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
import { SeedCompoundHash } from './ServerCacheSeed';
import { Node } from './snap/Node';
import { Path } from './util/Path';
/**
 * Records older than this are dropped (staleness makes a full download
 * likely anyway; bounded retention caps disk use).
 */
export declare const PERSISTENCE_MAX_AGE_MS: number;
export declare const PERSISTENCE_MAX_CACHE_BYTES: number;
/**
 * Default width of the flush coalescing window (see the write policy in the
 * file header). Configurable per manager (writeDelayMs). The window is
 * non-restarting: a root that churns continuously still flushes every
 * window, and never more often than one in-flight flush allows.
 * @internal
 */
export declare const PERSISTENCE_WRITE_DEBOUNCE_MS = 15000;
/**
 * Constant canonical-text target for one persisted/hash range. Boundaries are
 * stable across generations and only dirty runs reconsult this target. The
 * constructor accepts an override so 128/256/512 KiB can be benchmarked
 * without changing protocol code.
 * @internal
 */
export declare const PERSISTENCE_RANGE_TARGET_BYTES: number;
/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed manifest or tree read) resets this budget. The same
 * bound applies to each IndexedDB open/transaction, so a request that fires
 * neither success nor error can never hold the live listen forever.
 */
export declare const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the tree record stays put) — so a tree that never
 * changes but is used daily never ages into the expiry cutoff.
 * @internal
 */
export declare const PERSISTENCE_REFRESH_AGE_MS: number;
/**
 * How long after startup the expiry sweep runs. Deferred so the sweep's
 * store-wide transaction can never delay the boot restores, which IndexedDB
 * would otherwise queue behind it.
 * @internal
 */
export declare const PERSISTENCE_SWEEP_DELAY_MS = 15000;
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
export type PersistenceRestoreReason = 'missing' | 'expired' | 'auth' | 'corrupt' | 'timeout';
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
/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
export declare const persistenceStats: {
    restoredRoots: string[];
    restoreMisses: string[];
    writeThroughs: number;
    rangesHashed: number;
    rangesReused: number;
    evictions: number;
    storageFailures: number;
    events: Array<{
        at: number;
        path: string;
        event: string;
        detail?: string;
    }>;
};
export declare class PersistenceManager {
    private prefix_;
    private idbFactory_;
    private schemaKnownCurrent_;
    private operationTimeoutMs_;
    private cacheMaxBytes_;
    private writeDelayMs_;
    private rangeTargetBytes_;
    private db_;
    /** Roots explicitly selected by the application (keepSynced semantics). */
    private persistentRoots_;
    /** Active selected roots currently flowing through persistence. */
    private trackedRoots_;
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
    private changedSinceFlush_;
    private latest_;
    /**
     * What IndexedDB currently holds per root (see FlushedState) — the basis
     * for identity-diff dirty marking and no-op flushes.
     */
    private lastFlush_;
    /**
     * Distinguishes this manager's write tokens from every other tab's and
     * session's — numeric counters restart at zero on reload, which would let
     * a new data write pair up with a write token from another manager
     * instance.
     */
    private instanceId_;
    private writeCounter_;
    /**
     * The single-flight coalescing window per root: `timer` is the pending
     * (non-restarting) window; `rearm` marks a change that landed while the
     * root's queue was busy flushing — exactly one follow-up window is armed
     * when the queue drains, however many changes landed meanwhile.
     */
    private writeTimers_;
    private flushPending_;
    /** In-flight storage operations per root (see enqueue_). */
    private queues_;
    /**
     * One physical IndexedDB decode per root. The pre-auth peek and the
     * authenticated listener often overlap; without coalescing they each read
     * the tree record and rebuilt the same large Node tree concurrently.
     */
    private activeReads_;
    private restoreReasons_;
    private activeRestoreCount_;
    private restoreQueue_;
    private writesDeferredUntilRestores_;
    private sweepTimer_;
    private sweepInFlight_;
    private disposed_;
    private authScope_;
    private authScopeConfigured_;
    private authGeneration_;
    setAuthScope(scope: string | null): boolean;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, schemaKnownCurrent_?: boolean, operationTimeoutMs_?: number, cacheMaxBytes_?: number, writeDelayMs_?: number, rangeTargetBytes_?: number);
    rebindTo(prefix: string): PersistenceManager;
    setPersistentPath(pathString: string, enabled: boolean): void;
    isPersistentPath(pathString: string): boolean;
    /**
     * Marks a root as persistence-managed; write-throughs only run for
     * tracked roots (and their descendants' updates).
     */
    track(pathString: string): void;
    /**
     * The root's last listen stopped. When a live tracked ancestor covers the
     * root, its record — which contains this subtree and keeps flushing — is
     * the one future sessions should restore, so the child's own record is
     * deleted rather than left to shadow it. Otherwise any pending
     * write-through is flushed so IndexedDB holds the final tree for the next
     * session. Either way the in-memory copies are released — only live
     * listens need them.
     */
    untrack(pathString: string): void;
    /**
     * The nearest (deepest) tracked root at-or-above `pathString`, or null.
     */
    trackedRootFor(pathString: string): string | null;
    private open_;
    /**
     * Test seam: runs the deferred expiry sweep immediately.
     * @internal
     */
    sweepNow(): Promise<void>;
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
    private sweepExpired_;
    private openAtVersion_;
    private key_;
    /**
     * Runs `body` against the object store in a transaction of the given mode
     * and resolves with what `body` chose to deliver (via its `done` callback)
     * once the transaction completes. Every failure path — no database, a
     * throwing store call, an aborted transaction — resolves `fallback` and
     * counts one storageFailure (except when IndexedDB is absent altogether,
     * which is a supported cold-load configuration, not a failure).
     */
    private withStore_;
    /**
     * Reads a root's committed manifest and every immutable range it references
     * in one readonly transaction. `onManifest` fires as soon as the requests
     * are queued, overlapping network reconciliation with structured-clone
     * range reads and private Node assembly. Missing/mismatched ranges fail the
     * whole restore; Repo then performs the structural-failure cold relisten.
     */
    private readRecord_;
    private readRecordOnce_;
    /**
     * Decodes and merges raw persisted range clones into one Node in yielded
     * slices. Each slice decodes a few records, then yields a macrotask so the
     * main thread can paint/GC between slices; consumed entries are nulled so
     * the structured clones are collectable while later slices run. Returns
     * null when any fragment fails to decode.
     */
    private decodeFragmentsSliced_;
    private deleteRecord_;
    private withRestoreSlot_;
    /**
     * Projects an exact-path peek from a covering root that is already restored
     * or actively restoring in this manager. This never starts a large ancestor
     * read just to answer a tiny token lookup; it only reuses work the app is
     * already paying for, preserving the exact-root fast path on direct boots.
     */
    private peekFromCoveringRead_;
    /**
     * Exact-root optimistic peek. The completed range assembly is retained briefly
     * so the authenticated listener consumes the same immutable Node instead of
     * reconstructing the root twice during boot.
     */
    peek(pathString: string, expectedAuthScope?: string | null): Promise<PersistedRecord | null>;
    /**
     * Listener restore with an idle (no-progress) bound. `onManifest` fires as
     * soon as the stored generation's hashes are known — typically
     * milliseconds — letting the caller send the range listen while immutable
     * range records are still being read and assembled. The callback is suppressed after
     * a timeout/miss resolution, and never fires once the returned promise has
     * settled null.
     */
    restoreForListen(pathString: string, onManifest?: (hashes: PersistedSeedHashes) => void): Promise<PersistenceRestoreResult>;
    /**
     * Bounds a read by an IDLE (no-progress) timeout. The factory form lets
     * chunked restores reset the timer after every completed chunk; callers
     * that pass an already-started Promise retain the old total-time bound.
     */
    private raceRestoreTimeout_;
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
    private flushWritesDeferredUntilRestores_;
    /** Arms the non-restarting single-flight write window for a root. */
    private armWriteWindow_;
    private accumulateChangedPaths_;
    /**
     * `changedPaths` — the root-relative paths of the subtrees this update
     * changed, when the caller knows them precisely: an ordinary server data
     * push names its own path (`[relative]`), a listen certification confirms
     * already-accounted state (`[]`, nothing new). Omitted/undefined marks the
     * accumulated change-set imprecise — a range merge, or any update whose
     * shape the caller cannot name — falling the next flush back to the
     * identity diff.
     */
    serverCacheUpdated(path: Path, node: Node, changedPaths?: string[][]): void;
    /**
     * Enqueues a flush unless the root's queue is still working — then one
     * flush is marked pending and enqueued when the queue drains. Without the
     * mark, a root whose flush takes longer than the window would queue
     * flushes faster than they complete, unboundedly. This is the
     * single-flight guarantee: at most one flush in flight per root, effective
     * cadence max(writeDelayMs, flush duration).
     */
    private scheduleFlush_;
    /** Drop an unusable persisted record but keep the live root tracked. */
    invalidate(path: Path): void;
    /**
     * The viewer lost access to a root: a cached copy must not outlive the
     * access that produced it, and the root leaves write-through tracking
     * entirely — SyncTree never calls stopListening for server-revoked
     * listens, so nothing else would ever untrack it.
     */
    evict(path: Path): void;
    dispose(): void;
    /**
     * Test seam: forces a pending flush window to fire now.
     */
    flushNow(pathString: string): Promise<void>;
    /**
     * Chains an operation onto the root's queue. One writer per root at a
     * time: a flush's manifest, chunks, and integrated hashes stay revision-coupled
     * before the next flush or delete for that root starts, which is the
     * whole storage consistency argument — no cross-operation races to
     * reason about.
     */
    private enqueue_;
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
    private flush_;
    private gcRangeRecords_;
}

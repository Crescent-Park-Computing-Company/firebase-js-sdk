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
 * Serialized-text target for one persisted segment. Boundaries carry over
 * across generations; only dirty runs reconsult this target.
 * @internal
 */
export declare const PERSISTENCE_SEGMENT_TARGET_BYTES: number;
/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed read) resets this budget. The same bound applies to
 * each IndexedDB open/transaction, so a request that fires neither success
 * nor error can never hold the live listen forever.
 */
export declare const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the segment records stay put) — so a tree that never
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
 * Minimum age (from the timestamp embedded in the id) before the sweep may
 * reclaim a segment record no manifest references. The guard keeps a sweep
 * in one tab from deleting records another tab has staged for a generation
 * whose manifest hasn't committed yet — staging and commit are seconds
 * apart, never an hour.
 * @internal
 */
export declare const PERSISTENCE_ORPHAN_MIN_AGE_MS: number;
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
export type PersistenceRestoreReason = 'missing' | 'expired' | 'auth' | 'corrupt' | 'timeout';
export interface PersistenceRestoreResult {
    record: PersistedRecord | null;
    reason?: PersistenceRestoreReason;
}
/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
export declare const persistenceStats: {
    restoredRoots: string[];
    restoreMisses: string[];
    writeThroughs: number;
    segmentsWritten: number;
    segmentsReused: number;
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
    private segmentTargetBytes_;
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
    private latest_;
    /**
     * What IndexedDB currently holds per root (see FlushedState) — the basis
     * for identity-diff dirty marking and no-op flushes.
     */
    private lastFlush_;
    /**
     * Distinguishes this manager's revisions and record ids from every other
     * tab's and session's — numeric counters restart at zero on reload.
     */
    private instanceId_;
    private writeCounter_;
    private segmentCounter_;
    /**
     * The single-flight coalescing window per root: `writeTimers_` holds the
     * pending (non-restarting) window; `flushPending_` marks a change that
     * landed while the root's queue was busy flushing — exactly one follow-up
     * flush runs when the queue drains, however many changes landed meanwhile.
     */
    private writeTimers_;
    private flushPending_;
    /** In-flight storage operations per root (see enqueue_). */
    private queues_;
    /**
     * One physical IndexedDB decode per root. The pre-auth peek and the
     * authenticated listener often overlap; without coalescing they each read
     * the segment records and rebuilt the same large Node tree concurrently.
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
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, schemaKnownCurrent_?: boolean, operationTimeoutMs_?: number, cacheMaxBytes_?: number, writeDelayMs_?: number, segmentTargetBytes_?: number);
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
    private openAtVersion_;
    private key_;
    /**
     * A fresh, never-reused segment record id. The leading base36 wall clock
     * is what the sweep's orphan age guard reads (see sweepExpired_).
     */
    private newSegmentId_;
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
     * Coalesced physical read: one manifest+segments decode per root however
     * many callers (pre-auth peek, authenticated listener) overlap on it.
     */
    private readRecord_;
    /**
     * One physical restore. A single readonly transaction reads the manifest
     * and queues every referenced segment get; each segment's JSON text is
     * parsed and its children built as results arrive (bounded work per
     * event-loop turn by IndexedDB's own request cadence), and the split tree
     * is assembled once the transaction completes.
     */
    private readRecordOnce_;
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
     * Exact-root optimistic peek. The completed assembly is retained briefly
     * so the authenticated listener consumes the same immutable Node instead
     * of reconstructing the root twice during boot.
     */
    peek(pathString: string, expectedAuthScope?: string | null): Promise<PersistedRecord | null>;
    /**
     * Listener restore with an idle (no-progress) bound. Resolves with the
     * assembled tree; the caller applies it and derives the listen hashes
     * from the node itself (see repoStartServerListen).
     */
    restoreForListen(pathString: string): Promise<PersistenceRestoreResult>;
    /**
     * Bounds a read by an IDLE (no-progress) timeout. The factory form lets
     * segment restores reset the timer after every completed request; callers
     * that pass an already-started Promise retain the old total-time bound.
     */
    private raceRestoreTimeout_;
    private flushWritesDeferredUntilRestores_;
    /** Arms the non-restarting single-flight write window for a root. */
    private armWriteWindow_;
    serverCacheUpdated(path: Path, node: Node): void;
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
     * time: a flush's segments and manifest commit before the next flush or
     * delete for that root starts — no cross-operation races to reason about
     * WITHIN a tab. (Cross-tab writers are last-writer-wins by design; see
     * the file header.)
     */
    private enqueue_;
    /**
     * One generation: identity-diff against the last known stored tree maps
     * the change set to dirty segments; only those are re-serialized (direct
     * Node -> JSON text, no export objects) and written under fresh ids in
     * bounded batches. Clean segment records carry over verbatim and are
     * never read or cloned. The commit is one small last-writer-wins
     * transaction: manifest put + retired-id deletes. No hashing anywhere —
     * the next boot derives listen hashes from whatever tree it assembles.
     */
    private flush_;
    sweepNow(): Promise<void>;
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
    private sweepExpired_;
}

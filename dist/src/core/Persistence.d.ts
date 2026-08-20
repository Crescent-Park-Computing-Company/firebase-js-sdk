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
export declare const PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS = 3000;
/**
 * Main-thread budget for one slice of flush planning (the stable-range
 * rewalk). Sized to fit inside a frame budget on mobile hardware.
 * @internal
 */
export declare const FLUSH_PLAN_SLICE_MS = 12;
/**
 * Canonical-text bytes staged per task before yielding. Two default-target
 * ranges (~256 KiB each) per slice keeps serialization work bounded while
 * the unclamped macrotask yield (yieldMacrotask) lets paint/input interleave.
 * @internal
 */
export declare const FLUSH_STAGE_BATCH_BYTES: number;
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
/** Heartbeat cadence while holding the writer lease. @internal */
export declare const LEASE_HEARTBEAT_MS = 20000;
/**
 * A holder whose heartbeat is older than this is considered suspended and
 * may be stolen from. Must comfortably exceed the worst legitimate
 * heartbeat gap (Chrome background timer clamping is 60s). @internal
 */
export declare const LEASE_STALE_MS = 120000;
/** The subset of Storage the heartbeat needs (injectable for tests). */
interface HeartbeatStore {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
    /** Optional: stores without it simply skip release-time stamp cleanup. */
    removeItem?(key: string): void;
}
export interface WebLocksLike {
    request: (name: string, options: {
        mode: 'exclusive';
        signal?: AbortSignal;
        steal?: boolean;
    }, callback: (lock: unknown) => Promise<void>) => Promise<void>;
}
/** @internal */
export declare function _setWebLocksForTesting(locks: WebLocksLike | null | undefined): void;
export declare class PersistenceManager {
    private prefix_;
    private idbFactory_;
    private schemaKnownCurrent_;
    private operationTimeoutMs_;
    private cacheMaxBytes_;
    private writeDelayMs_;
    private rangeTargetBytes_;
    private peekHandoffMs_;
    private peekPreAuthHandoffMs_;
    private leaseHeartbeatMs_;
    private leaseStaleMs_;
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
    /** One writer lease per TRACKED root (see the WriterLease notes). */
    private writerLeases_;
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
    private networkSuspended_;
    /** One timer for all leases: held → heartbeat, requested → steal check. */
    private leaseTimer_;
    private heartbeatStore_;
    /**
     * Identifies THIS manager's heartbeat stamps (`<ms>|<token>`), so a
     * clean release can remove its own stamp without ever deleting a
     * successor's. Without cleanup, a departed holder's stamp lingers: a
     * later holder whose storage cannot WRITE never overwrites it, and a
     * follower that can READ sees a PRESENT-but-stale heartbeat — and
     * steals from a perfectly healthy writer, contradicting the documented
     * page-death fallback for storage-denied holders.
     */
    private heartbeatToken_;
    private activeRestoreCount_;
    private restoreQueue_;
    private writesDeferredUntilRestores_;
    private sweepTimer_;
    private sweepInFlight_;
    private disposed_;
    private authScope_;
    private authScopeConfigured_;
    /**
     * True once the APP's auth integration (setPersistenceAuthScope) has
     * confirmed the scope — as opposed to a pre-auth peek merely priming it
     * with a trusted expected identity. Selects the peek-retention budget:
     * a primed-only scope holds the long pre-auth backstop, a confirmed one
     * the short handoff grace (see PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS).
     */
    private authScopeConfirmed_;
    private authGeneration_;
    isAuthScopeConfigured(): boolean;
    /**
     * The current identity-scope generation — bumped by every setAuthScope
     * that changes the scope. Callers whose continuation spans an await after
     * peek() resolves capture this before the wait and compare after, so a
     * scope switch mid-continuation invalidates the result exactly like
     * peek()'s own resolution-time check. @internal
     */
    authGeneration(): number;
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
    hasRetainedPeek(pathString: string, node: Node): boolean;
    setAuthScope(scope: string | null, confirmedByApp?: boolean): boolean;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, schemaKnownCurrent_?: boolean, operationTimeoutMs_?: number, cacheMaxBytes_?: number, writeDelayMs_?: number, rangeTargetBytes_?: number, peekHandoffMs_?: number, peekPreAuthHandoffMs_?: number, leaseHeartbeatMs_?: number, leaseStaleMs_?: number, heartbeatStore?: HeartbeatStore | null);
    rebindTo(prefix: string): PersistenceManager;
    setPersistentPath(pathString: string, enabled: boolean): void;
    isPersistentPath(pathString: string): boolean;
    /**
     * Marks a root as persistence-managed; write-throughs only run for
     * tracked roots (and their descendants' updates).
     */
    track(pathString: string): void;
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
    setNetworkSuspended(suspended: boolean): void;
    /**
     * True when this manager may write the root: it holds the root's writer
     * lease, or leases are unenforceable here (no Web Locks, or the root has
     * no lease entry — the manifest CAS remains the correctness backstop).
     */
    private holdsWriterLease_;
    /** The root's shared heartbeat key. */
    private heartbeatKey_;
    private writeHeartbeat_;
    private readHeartbeat_;
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
    private onLeaseTick_;
    /** Requests the root's writer lease once (idempotent per root). */
    private ensureWriterLease_;
    /**
     * Puts a lease request for the root in the browser's queue, superseding
     * any current one (`steal` preempts a stale holder; see onLeaseTick_).
     */
    private requestWriterLease_;
    private writerLeaseName_;
    /**
     * Removes THIS manager's own heartbeat stamp (token-checked, so a
     * successor's stamp is never deleted). The get→remove pair is not
     * atomic; the benign worst case is deleting a successor stamp written
     * in between — absence never justifies a steal, and the successor
     * re-stamps on its next tick. A crashed holder never runs this, so its
     * stamp can linger: a follower may then steal once from a write-denied
     * successor — accepted residual; the stealer stamps and it stabilizes.
     */
    private clearOwnHeartbeat_;
    /** Returns the root's writer lease to the browser (idempotent). */
    private releaseWriterLease_;
    /**
     * Cleanup-completion rule shared by untrack paths: return the root's
     * lease unless the root was re-tracked meanwhile — the new listen owns
     * it now.
     */
    private releaseWriterLeaseIfUntracked_;
    /** Returns every lease (dispose). */
    private releaseAllWriterLeases_;
    /**
     * A tab tracking nothing must neither heartbeat nor evaluate steals: the
     * tick stops with the last lease and restarts with the next track().
     */
    private stopLeaseTimerIfIdle_;
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
    /**
     * Auth just confirmed the scope a pre-auth peek primed: every retained
     * completed read waiting under the long pre-auth backstop switches to the
     * short post-auth grace, counted from now. Entries still resolving (no
     * cleanupTimer yet) pick the right budget in their own release().
     */
    private rearmRetainedReads_;
    private readRecordOnce_;
    /**
     * Decodes and merges raw persisted range clones into one Node in yielded
     * slices. Each slice decodes a few records, then yields a macrotask so the
     * main thread can paint/GC between slices; consumed entries are nulled so
     * the structured clones are collectable while later slices run. Returns
     * null when any fragment fails to decode.
     */
    private decodeFragmentsSliced_;
    /**
     * Cleanup for a manifest judged structurally invalid: the verdict is
     * re-reached INSIDE the readwrite transaction, so a valid generation a
     * concurrent writer committed after the (readonly) judgement is never
     * touched. Still-invalid garbage — whatever garbage it is by now — goes.
     */
    private deleteRecordIfInvalid_;
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
    private deleteRecordIfRevision_;
    /** Deletes a root's manifest and every '#'-suffixed sidecar in `store`. */
    private deleteRecordInStore_;
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
    /**
     * Re-enters the ordinary write window when the root still has work: it is
     * tracked and holds a pending tree in latest_ (flush_ reads latest_ when
     * it runs, so whatever landed meanwhile is covered). The one definition
     * used by every deferred-retry path — a lease grant after skipped writes,
     * a failed-open lock acquisition, and the stale-baseline adoption.
     */
    private armWriteWindowIfPending_;
    /**
     * Arms the non-restarting single-flight write window for a root. Two
     * regimes: a root with a flush baseline coalesces under the ordinary
     * window; a root with none (first generation — see
     * PERSISTENCE_FIRST_GENERATION_WRITE_DELAY_MS) flushes on the shorter of
     * the two delays so the cache exists before short mobile sessions end.
     */
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
    private purgeEvictedRecord_;
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
    private adoptCommittedBaseline_;
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
    /**
     * Second half of a flush: stages the planned dirty ranges and commits the
     * generation. Split from flush_ so the sliced planner can yield between
     * slices without holding the whole body in one closure. `entry` is the
     * latest_ record the flush entered with (its node/revision/authScope are
     * the generation being written); `rebuilt` is the planned range list —
     * clean ranges carried with their recordIds, dirty ranges with empty
     * hashes to be serialized, digested, and staged here.
     */
    private finishFlush_;
    private gcRangeRecords_;
}
export {};

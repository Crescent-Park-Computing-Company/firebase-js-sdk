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
 * How long after the last server update a root's write-through runs. The
 * flush re-serializes the chunks the update dirtied, so it is deliberately
 * coarse.
 * @internal
 */
export declare const PERSISTENCE_WRITE_DEBOUNCE_MS = 10000;
/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Every completed metadata/chunk read resets this budget: a large Safari
 * restore that is steadily advancing must not be abandoned into a much slower
 * full network load merely because its total wall time exceeded the budget.
 * The same bound applies to each IndexedDB open/transaction, so a request that
 * fires neither success nor error can never hold the live listen forever.
 */
export declare const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/**
 * Target serialized size of one chunk record. Peak transient memory of a
 * flush or restore is a few multiples of THIS (one chunk's exported JSON
 * plus its structured clone), not of the whole root. A single leaf larger
 * than the target still becomes one oversized chunk — leaves cannot split.
 * @internal
 */
export declare const PERSISTENCE_CHUNK_TARGET_BYTES: number;
/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the chunks and integrated hashes stay put) — so a tree that
 * never changes but is used daily never ages into the expiry cutoff.
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
/** @internal */
export declare function onPersistenceEvent(listener: (event: {
    at: number;
    path: string;
    event: string;
    detail?: string;
}) => void): () => void;
export declare const persistenceStats: {
    restoredRoots: string[];
    restoreMisses: string[];
    writeThroughs: number;
    chunksWritten: number;
    chunksSkipped: number;
    hashRecomputes: number;
    evictions: number;
    storageFailures: number;
    events: Array<{
        at: number;
        path: string;
        event: string;
        detail?: string;
    }>;
};
/**
 * One PersistenceManager per Repo. `prefix` namespaces records so multiple
 * databases/apps sharing the page don't collide. `idbFactory` exists for
 * tests (Node has no IndexedDB); production uses the global.
 */
export declare class PersistenceManager {
    private prefix_;
    private idbFactory_;
    private schemaKnownCurrent_;
    private operationTimeoutMs_;
    private cacheMaxBytes_;
    private db_;
    /** Roots explicitly selected by the application (keepSynced semantics). */
    private persistentRoots_;
    /** Active selected roots currently flowing through persistence. */
    private trackedRoots_;
    /**
     * Latest server tree per root. Revisions come from a single manager-wide
     * counter, so no revision is ever reissued — an in-flight hash recompute
     * can never collide with a tree that arrived after its root was evicted
     * and re-tracked.
     */
    private latest_;
    /**
     * What IndexedDB currently holds per root (see FlushedState) — the basis
     * for skipping clean chunks and no-op flushes.
     */
    private lastFlush_;
    /**
     * Distinguishes this manager's write tokens from every other tab's and
     * session's — numeric counters restart at zero on reload, which let a new
     * data write pair up with a write token from another manager instance.
     */
    private instanceId_;
    private writeCounter_;
    private writeTimers_;
    /**
     * Roots whose throttle fired while their queue was busy: exactly one
     * flush is re-enqueued when the queue drains, however many intervals
     * elapsed meanwhile — the queue can never grow faster than it drains.
     */
    private flushPending_;
    /** In-flight storage operations per root (see enqueue_). */
    private queues_;
    /**
     * One physical IndexedDB decode per root. The pre-auth peek and the
     * authenticated listener often overlap; without coalescing they each read
     * every chunk and rebuilt the same large Node tree concurrently.
     */
    private activeReads_;
    private activeRestoreCount_;
    private restoreQueue_;
    private sweepTimer_;
    private disposed_;
    private authScope_;
    setAuthScope(scope: string | null): void;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, schemaKnownCurrent_?: boolean, operationTimeoutMs_?: number, cacheMaxBytes_?: number);
    /**
     * A replacement manager for a different key prefix — used when emulator
     * configuration changes the RepoInfo after persistence was enabled but
     * before the repo started (no queues or tracked roots exist yet).
     */
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
     * Expiry is decided by each root's manifest (or legacy record): the
     * '#'-suffixed chunk and legacy-hash records carry no authority of their own and
     * are dropped exactly when their manifest is dropped, is missing (orphans
     * from an interrupted write), or no longer lists them. Scoped to this
     * manager's key range and reading keys before values, where the platform
     * allows, so foreign records are never materialized. Best-effort: any
     * failure leaves the records for the next session's sweep.
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
     * Reads a root's stored state in one readonly transaction: the manifest
     * the manifest first, then — for chunked records — each chunk in
     * sequence, folded into the assembled tree as it arrives so only one
     * chunk's parsed JSON is ever held at a time. The hash record joins only
     * when its revision matches the manifest's; a chunk whose revision doesn't
     * match the manifest's expectation (an interrupted or foreign write)
     * resolves null, and the leftovers are deleted best-effort. Expired
     * records also resolve null (and are deleted best-effort).
     */
    private readRecord_;
    private readRecordOnce_;
    /**
     * Deletes everything stored for a root: manifest, legacy hash record, and every
     * chunk the manifest lists (plus, where the platform provides key ranges,
     * any orphaned chunk tail beyond it).
     */
    private deleteRecord_;
    /**
     * Restores the persisted record for a root. Resolves null on miss, expiry,
     * storage failure, or timeout — the caller then attaches unseeded. A hit
     * also primes the flush-skip state: the store is KNOWN to hold exactly
     * this tree, so when the server certifies it unchanged (the common warm
     * boot), the follow-up write-through skips without serializing anything.
     */
    private withRestoreSlot_;
    /**
     * Exact-root optimistic peek. The completed decode is retained briefly so
     * the authenticated listener can consume the same immutable Node instead of
     * decoding a large IndexedDB record twice during boot.
     */
    peek(pathString: string, expectedAuthScope?: string | null): Promise<PersistedRecord | null>;
    /**
     * Listener restore with an idle (no-progress) bound. Healthy chunked reads
     * can take arbitrarily long in total as long as each chunk advances; a stuck
     * IndexedDB request returns null so Repo cancels the seeded listen and
     * restarts once against the live in-memory cache.
     */
    restoreForListen(pathString: string): Promise<PersistedRecord | null>;
    /**
     * Bounds a read by an IDLE (no-progress) timeout. The factory form lets
     * chunked restores reset the timer after every completed chunk; callers
     * that pass an already-started Promise retain the old total-time bound.
     */
    private raceRestoreTimeout_;
    /**
     * Write-through: the server confirmed `node` as the state of the tracked
     * root `path`. Throttled per root; hashes recompute afterwards in idle
     * slices against the same revision. A tree the store is known to already
     * hold — the warm boot's listen-'ok' certifying the restored tree
     * unchanged — is skipped outright unless its stored timestamp needs a
     * refresh (see PERSISTENCE_REFRESH_AGE_MS).
     */
    serverCacheUpdated(path: Path, node: Node): void;
    /**
     * Enqueues a flush unless the root's queue is still working — then one
     * flush is marked pending and enqueued when the queue drains. Without the
     * mark, a root whose flush takes longer than the throttle interval would
     * queue flushes faster than they complete, unboundedly.
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
     * Test seam: forces a pending throttled flush to run now.
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
    private flush_;
}

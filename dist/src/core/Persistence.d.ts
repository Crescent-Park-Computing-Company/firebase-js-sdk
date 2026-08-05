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
/**
 * How long after the last server update a root's write-through runs. The
 * flush serializes the whole root (val(true) + the structured clone into
 * IndexedDB), so it is deliberately coarse for very large roots.
 * @internal
 */
export declare const PERSISTENCE_WRITE_DEBOUNCE_MS = 10000;
/**
 * A restore that hasn't settled by this budget attaches the listen unseeded
 * — persistence may add at most this much latency to a root's FIRST listen,
 * and only when IndexedDB is pathologically slow.
 */
export declare const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
export interface PersistedRecord {
    json: unknown;
    hash?: string;
    compoundHash?: SeedCompoundHash;
    updatedAt: number;
    /**
     * Write token unique ACROSS manager instances (tabs, reloads) — the join
     * key coupling a hash record to the exact data write it describes.
     */
    revision: string;
}
/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
export declare const persistenceStats: {
    restoredRoots: string[];
    restoreMisses: string[];
    writeThroughs: number;
    hashRecomputes: number;
    evictions: number;
    storageFailures: number;
};
/**
 * One PersistenceManager per Repo. `prefix` namespaces records so multiple
 * databases/apps sharing the page don't collide. `idbFactory` exists for
 * tests (Node has no IndexedDB); production uses the global.
 */
export declare class PersistenceManager {
    private prefix_;
    private idbFactory_;
    private maxRootBytes_;
    private db_;
    /**
     * Roots that flow through persistence (complete default listens).
     */
    private trackedRoots_;
    /**
     * Latest server tree per root. Revisions come from a single manager-wide
     * counter, so no revision is ever reissued — an in-flight hash recompute
     * can never collide with a tree that arrived after its root was evicted
     * and re-tracked.
     */
    private latest_;
    /**
     * Distinguishes this manager's write tokens from every other tab's and
     * session's — numeric counters restart at zero on reload, which let a new
     * data write pair up with a surviving old hash sidecar.
     */
    private instanceId_;
    private writeCounter_;
    private writeTimers_;
    /** In-flight storage operations per root (see enqueue_). */
    private queues_;
    private disposed_;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, maxRootBytes_?: number);
    /**
     * A replacement manager for a different key prefix — used when emulator
     * configuration changes the RepoInfo after persistence was enabled but
     * before the repo started (no queues or tracked roots exist yet).
     */
    rebindTo(prefix: string): PersistenceManager;
    /**
     * Marks a root as persistence-managed; write-throughs only run for
     * tracked roots (and their descendants' updates).
     */
    track(pathString: string): void;
    /**
     * The root's last listen stopped: flush any pending write-through so
     * IndexedDB holds the final tree for the next session, then release the
     * in-memory copy — only live listens need it.
     */
    untrack(pathString: string): void;
    /**
     * The nearest tracked root at-or-above `pathString`, or null.
     */
    trackedRootFor(pathString: string): string | null;
    private open_;
    /**
     * Deletes this manager's expired records (see PERSISTENCE_MAX_AGE_MS) by
     * cursor walk. Both record kinds carry `updatedAt`, so data and hash
     * records expire together. Best-effort: any failure leaves the records
     * for the next session's sweep.
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
     * Reads a root's data record and joins its hash record when the revisions
     * match, in one readonly transaction. Expired records resolve null (and
     * are deleted best-effort).
     */
    private readRecord_;
    private deleteRecord_;
    /**
     * Restores the persisted record for a root. Resolves null on miss, expiry,
     * storage failure, or timeout — the caller then attaches unseeded.
     */
    restore(pathString: string): Promise<PersistedRecord | null>;
    /**
     * The boot-peek read (see getPersistedValue): resolves the record of the
     * DEEPEST persisted ancestor of `pathString` (or of the path itself),
     * fetching the whole ancestor chain in one readonly transaction. Expired
     * ancestors are skipped (and deleted best-effort). Does not touch the
     * restore counters — a peek is not a listen restore.
     */
    restoreNearest(pathString: string): Promise<{
        root: string;
        record: PersistedRecord;
    } | null>;
    /**
     * Bounds a read by PERSISTENCE_RESTORE_TIMEOUT_MS, clearing the timer as
     * soon as the read settles first (the common case — otherwise every
     * restore would pin its Repo in memory for the full budget).
     */
    private raceRestoreTimeout_;
    /**
     * Write-through: the server confirmed `node` as the state of the tracked
     * root `path`. Debounced per root; hashes recompute afterwards in idle
     * slices against the same revision.
     */
    serverCacheUpdated(path: Path, node: Node): void;
    /**
     * The viewer lost access to a root: a cached copy must not outlive the
     * access that produced it.
     */
    evict(path: Path): void;
    dispose(): void;
    /**
     * Test seam: forces a pending debounced flush to run now.
     */
    flushNow(pathString: string): Promise<void>;
    /**
     * Chains an operation onto the root's queue. One writer per root at a
     * time: a flush's data and hash records land as a couple before the next
     * flush or delete for that root starts, which is the whole storage
     * consistency argument — no cross-operation races to reason about.
     */
    private enqueue_;
    private flush_;
}

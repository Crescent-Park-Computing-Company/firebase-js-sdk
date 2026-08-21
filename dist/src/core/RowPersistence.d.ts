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
import { KernelCompoundHash } from './RowHashKernel';
import { Node } from './snap/Node';
import { Path } from './util/Path';
export declare const ROW_PERSISTENCE_WRITE_DEBOUNCE_MS = 15000;
export declare const ROW_PERSISTENCE_FIRST_GEN_DELAY_MS = 3000;
/** Peek retention while the app has confirmed the primed scope. */
export declare const ROW_PERSISTENCE_PEEK_HANDOFF_MS = 30000;
/** Peek retention while the scope is primed but unconfirmed (slow auth). */
export declare const ROW_PERSISTENCE_PEEK_PREAUTH_MS: number;
export declare const ROW_PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/** Cached roots older than this are swept (30 days, Android parity). */
export declare const ROW_PERSISTENCE_MAX_AGE_MS: number;
/** Sweep delay after the first restore — far off every boot-critical path. */
export declare const ROW_PERSISTENCE_SWEEP_DELAY_MS: number;
/** Retained for constructor compatibility; whole generations are atomic. */
export declare const ROW_PERSISTENCE_STAGE_TXN_BYTES: number;
/** Worker hash wall-clock ceiling before the main-thread fallback runs. */
export declare const ROW_PERSISTENCE_WORKER_HASH_TIMEOUT_MS = 20000;
export interface RowRestoreResult {
    node: Node | null;
    reason?: 'corrupt' | 'timeout';
}
export interface RowListenHashes {
    hash: string;
    compoundHash: KernelCompoundHash;
}
/** Injectable Web Locks surface (null in Node / unsupported browsers). */
export interface WebLocksLike {
    request(name: string, options: {
        mode: 'exclusive';
    }, callback: (lock: unknown | null) => Promise<unknown>): Promise<unknown>;
}
export declare class RowPersistenceManager {
    private prefix_;
    private idbFactory_;
    private webLocks_;
    private writeDelayMs_;
    private firstGenDelayMs_;
    private splitThresholdBytes_;
    private peekHandoffMs_;
    private peekPreAuthMs_;
    private restoreTimeoutMs_;
    private stageTxnBytes_;
    private workerHashTimeoutMs_;
    private maxAgeMs_;
    private db_;
    private persistentRoots_;
    private tracked_;
    private peeks_;
    private authScope_;
    private authScopeConfigured_;
    private authScopeConfirmed_;
    private authGeneration_;
    private networkSuspended_;
    private disposed_;
    private sweepTimer_;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, webLocks_?: WebLocksLike | null, writeDelayMs_?: number, firstGenDelayMs_?: number, splitThresholdBytes_?: number, peekHandoffMs_?: number, peekPreAuthMs_?: number, restoreTimeoutMs_?: number, stageTxnBytes_?: number, workerHashTimeoutMs_?: number, maxAgeMs_?: number);
    isAuthScopeConfigured(): boolean;
    authGeneration(): number;
    /**
     * Configures the identity scope. `confirmedByApp=false` is a pre-auth
     * peek priming a trusted expected identity; `true` is the app's real auth
     * integration. Returns whether the scope CHANGED (callers cancel pending
     * seed restores on a confirmed change).
     */
    setAuthScope(scope: string | null, confirmedByApp?: boolean): boolean;
    private scopeKey_;
    setPersistentPath(pathString: string, enabled: boolean): void;
    isPersistentPath(pathString: string): boolean;
    /**
     * The tracked root at or above `pathString`, or null. Roots are the paths
     * listeners selected with {persistent: true}; a server update anywhere
     * under one re-persists through that root.
     */
    trackedRootFor(pathString: string): string | null;
    /**
     * EVERY tracked root a server update at `pathString` touches — roots at
     * or above the path (the change is inside their subtree) AND roots below
     * it (an overwrite at an ancestor rewrites their whole tree). Overlapping
     * persistent registrations are legal (ancestor + descendant listeners),
     * and each stored root must stay current or its next boot hash would
     * claim bytes it does not hold.
     */
    trackedRootsFor(pathString: string): string[];
    track(pathString: string): void;
    untrack(pathString: string): void;
    private resetTrackedRoot_;
    private releaseRoot_;
    /**
     * Queues a BLOCKING exclusive lock request for (scope, root). The UA
     * grants it when the current holder releases — an untrack's final flush,
     * a scope switch, or tab death (release is UA-guaranteed) — so writer
     * succession is automatic with zero steal/heartbeat machinery. Until the
     * grant this tab is a follower: it keeps dirty sets in memory and does
     * not touch storage. On grant it refreshes the row index (rows on disk
     * may lag its memory) and flushes whatever is pending.
     */
    private acquireWriterLock_;
    private isWriter_;
    private open_;
    private requestDone_;
    private txnDone_;
    /**
     * Reads the cached tree for `pathString` without starting the repo — the
     * pre-auth boot peek. One physical read per root: an overlapping
     * authenticated restore consumes the same decode (restoreForListen). The
     * resolved node is RETAINED under the pre-auth backstop (or the short
     * grace once the scope is confirmed) so the later listener join reuses
     * this decode instead of reading twice.
     */
    peek(pathString: string, expectedAuthScope: string | null): Promise<{
        node: Node;
    } | null>;
    /**
     * True while THE retained read that decoded `node` is still awaiting its
     * listener join — the only window in which materialization stamps have a
     * consumer (identity-bound; see v1 hasRetainedPeek).
     */
    hasRetainedPeek(pathString: string, node: Node): boolean;
    private startRead_;
    private dropPeek_;
    private rearmRetainedPeeks_;
    private clearAllPeeks_;
    /** One physical root read: meta check, row getAll, sliced assemble. */
    private readRoot_;
    /** The root path string used inside row keys ('/a/b' canonical form). */
    private rootKey_;
    /**
     * The authenticated listener's restore: consumes the retained peek's
     * decode when one exists (the one-decode-per-boot handoff), otherwise
     * performs its own read. Resolves within `restoreTimeoutMs_` or reports
     * a timeout miss (the listen then goes cold — liveness over cache).
     */
    restoreForListen(pathString: string): Promise<RowRestoreResult>;
    /**
     * Computes the wire listen hashes for `pathString` from its stored rows.
     *
     * Worker-first: a Blob-URL worker opens its own readonly IDB connection,
     * streams the rows through the parity-tested kernel, and posts back only
     * {posts, hashes} — zero main-thread hashing cost (see HashWorker). Any
     * worker failure (unavailable, spawn error, IDB error, kernel overlap,
     * timeout) falls back to the main-thread kernel in bounded yielded
     * slices. Returns null when there are no rows or both paths fail — the
     * listen then sends a plain full listen.
     */
    computeListenHashes(pathString: string): Promise<RowListenHashes | null>;
    private computeListenHashesOnMainThread_;
    /**
     * Write-through entry: the server updated `path`; `node` is the complete
     * server cache at the TRACKED ROOT containing it. `changedPaths` names
     * what changed relative to the root (undefined = unknown = whole root).
     * Mirrors the v1 serverCacheUpdated signature so Repo call sites carry
     * over unchanged.
     */
    serverCacheUpdated(path: Path, node: Node, changedPaths?: string[][]): void;
    /**
     * Flushes any pending dirt for `pathString` immediately (skipping the
     * debounce window). Used before boot-hashing a grafted base and before
     * reconnect hashing, so the rows describe exactly the live cache.
     * Resolves when the flush (if any) completed.
     */
    flushNow(pathString: string): Promise<void>;
    /** The currently tracked persistent root paths. */
    trackedPaths(): string[];
    /**
     * True while the root's rows lag its live server cache: dirt is pending
     * or a flush is in flight. The listen-hash rule builds on this — a
     * compound hash is only claimed when the rows equal the live cache, so a
     * claim can never describe bytes older than what the client holds.
     */
    hasPendingDirt(pathString: string): boolean;
    private armWindow_;
    /** Single-flight flush of everything dirty at the root. */
    private flushNow_;
    private flushNowImpl_;
    /**
     * First generation / unknown-change rewrite of the whole root. Byte-
     * budgeted staging with meta LAST: crash mid-stage reads as "no cache"
     * on the next boot, never a torn generation claiming completeness.
     */
    private flushWholeRoot_;
    /**
     * Incremental flush: each dirty path normalizes to its containing row's
     * boundary (disjoint-rows invariant), covered duplicates drop, and each
     * boundary's subtree is deleted+rewritten — ONE readwrite transaction,
     * no hashing, work proportional to the change.
     */
    private flushIncremental_;
    /**
     * One deferred sweep per manager lifetime: deletes roots whose meta is
     * older than maxAge (any scope — an account that never logs in again
     * must not hold storage forever) and orphan rows whose meta is absent
     * (a torn first generation). Scheduled off the boot path; failures are
     * ignored (the next session sweeps again).
     */
    private scheduleSweep_;
    private sweep_;
    /** Drops the stored cache for the root containing `path` (corrupt). */
    invalidate(path: Path): void;
    /** Removes the stored cache when a persistent listener is torn down. */
    evict(path: Path): void;
    private deleteRoot_;
    /**
     * Deliberate offline (goOffline/repoInterrupt). Liveness is not
     * eligibility: a suspended tab keeps running but its server cache is
     * frozen, so it RELEASES its writer locks — the UA then grants them to a
     * queued online tab, which persists the newest server state. Resume
     * re-queues; writership returns whenever the interim holder unsubscribes
     * or dies. While suspended the write gate stays closed even without Web
     * Locks, so a frozen tree never overwrites an online writer's rows.
     */
    setNetworkSuspended(suspended: boolean): void;
    rebindTo(prefix: string): RowPersistenceManager;
    dispose(): void;
}

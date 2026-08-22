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
export declare const ROW_PERSISTENCE_WRITE_DEBOUNCE_MS = 30000;
export declare const ROW_PERSISTENCE_FIRST_GEN_DELAY_MS = 3000;
/** Peek retention while the app has confirmed the primed scope. */
export declare const ROW_PERSISTENCE_PEEK_HANDOFF_MS = 30000;
/** Peek retention while the scope is primed but unconfirmed (slow auth). */
export declare const ROW_PERSISTENCE_PEEK_PREAUTH_MS: number;
export declare const ROW_PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/** Cached roots older than this are swept (30 days, Android parity). */
export declare const ROW_PERSISTENCE_MAX_AGE_MS: number;
/** Non-live generations older than this are garbage (crashed/raced writes). */
export declare const ROW_PERSISTENCE_ORPHAN_GEN_AGE_MS: number;
/** Serialized bytes per chunk — the slice unit for parse and write. */
export declare const ROW_PERSISTENCE_CHUNK_BYTES: number;
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
        ifAvailable?: boolean;
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
    private chunkBytes_;
    private workerHashTimeoutMs_;
    private maxAgeMs_;
    private orphanGenAgeMs_;
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
    private sweepDone_;
    constructor(prefix_: string, idbFactory_?: IDBFactory | null, webLocks_?: WebLocksLike | null, writeDelayMs_?: number, firstGenDelayMs_?: number, splitThresholdBytes_?: number, peekHandoffMs_?: number, peekPreAuthMs_?: number, restoreTimeoutMs_?: number, chunkBytes_?: number, workerHashTimeoutMs_?: number, maxAgeMs_?: number, orphanGenAgeMs_?: number);
    isAuthScopeConfigured(): boolean;
    authGeneration(): number;
    /**
     * Configures the identity scope. `confirmedByApp=false` is a pre-auth
     * peek priming a trusted expected identity; `true` is the app's real
     * auth integration. Returns whether the scope CHANGED.
     */
    setAuthScope(scope: string | null, confirmedByApp?: boolean): boolean;
    private scopeKey_;
    setPersistentPath(pathString: string, enabled: boolean): void;
    isPersistentPath(pathString: string): boolean;
    /** Every path currently selected with {persistent:true}. */
    persistentPaths(): string[];
    /** The tracked root at or above `pathString`, or null. */
    trackedRootFor(pathString: string): string | null;
    /**
     * EVERY tracked root a server update at `pathString` touches — roots at
     * or above the path AND roots below it (an ancestor overwrite rewrites
     * their subtree). Each stored root must stay current or its next boot
     * hash would claim bytes it does not hold.
     */
    trackedRootsFor(pathString: string): string[];
    trackedPaths(): string[];
    track(pathString: string): void;
    untrack(pathString: string): void;
    private resetTrackedRoot_;
    private releaseRoot_;
    /**
     * Two-phase writer acquisition — an OPTIMIZATION ONLY (avoids duplicate
     * snapshot writes from N tabs); correctness never depends on it, because
     * generations are complete-or-invisible regardless of who writes.
     * 1. An `ifAvailable` probe decides immediately (lockDecided resolves).
     * 2. When the probe lost, a blocking queued request waits for succession
     *    (the UA grants it when the holder releases or its tab dies).
     */
    private acquireWriterLock_;
    private isWriter_;
    private open_;
    private requestDone_;
    private txnDone_;
    /** meta key for a root: scope·root (both escaped) + terminator. */
    private metaKey_;
    /** chunk key: metaKey · gen · index (zero-padded for range order). */
    private chunkKey_;
    private chunkRange_;
    /**
     * Reads the cached tree for `pathString` without starting the repo — the
     * pre-auth boot peek. One physical read per root; the resolved node is
     * RETAINED under the pre-auth backstop (or the short confirmed grace) so
     * the authenticated listener consumes this same decode.
     */
    peek(pathString: string, expectedAuthScope: string | null): Promise<{
        node: Node;
    } | null>;
    /**
     * True while THE retained read that decoded `node` is still awaiting its
     * listener join — the only window in which materialization stamps have a
     * consumer (identity-bound).
     */
    hasRetainedPeek(pathString: string, node: Node): boolean;
    private startRead_;
    private dropPeek_;
    private rearmRetainedPeeks_;
    private clearAllPeeks_;
    /** One physical root read: meta → live gen's chunks → sliced assemble. */
    private readRoot_;
    /**
     * The authenticated listener's restore: consumes the retained peek's
     * decode when one exists, otherwise performs its own read. Resolves
     * within `restoreTimeoutMs_` or reports a timeout miss.
     */
    restoreForListen(pathString: string): Promise<RowRestoreResult>;
    /**
     * Computes the wire listen hashes from the stored chunks of the exact
     * generation this manager last restored or committed. Immutability makes
     * the snapshot binding structural: the chunks either ARE that generation
     * byte for byte, or some are missing (foreign swap GC'd it — chunkCount
     * mismatch) and the claim declines. Worker-first; sliced main-thread
     * fallback. Null ⇒ the listen goes out plain (full resend, uncertified).
     */
    computeListenHashes(pathString: string): Promise<RowListenHashes | null>;
    private computeListenHashesOnMainThread_;
    /**
     * Write-through entry: the server updated `path`; `node` is the complete
     * server cache at the tracked root. `changedPaths` is accepted for call
     * compatibility; the snapshot model only needs "did anything change",
     * which the node identity answers exactly ([] = a certification restating
     * known state = nothing new).
     */
    serverCacheUpdated(path: Path, node: Node, changedPaths?: string[][]): void;
    /**
     * Flushes pending dirt immediately (skipping the debounce window).
     * Resolves when the flush (if any) completed.
     */
    flushNow(pathString: string): Promise<void>;
    /** True while the committed snapshot lags the live cache. */
    hasPendingDirt(pathString: string): boolean;
    private armWindow_;
    private flushNow_;
    /**
     * One snapshot flush: serialize → write chunks under a fresh gen (any
     * number of transactions; unreferenced chunks are invisible) → swap the
     * meta pointer and delete the previous generation's chunks in ONE final
     * transaction. Everything before the swap is free to fail or race.
     */
    private flushImpl_;
    /** Drops the stored cache for the root containing `path` (corrupt). */
    invalidate(path: Path): void;
    /** Removes the stored cache when a persistent listener is torn down. */
    evict(path: Path): void;
    /**
     * Deletes a root's stored cache. The namespace is captured synchronously
     * (an auth switch during the awaits must not redirect the delete), and
     * local state is invalidated first so no queued flush resurrects it.
     * The one delete transaction removes meta + every chunk of every gen —
     * a concurrent writer's in-flight gen simply becomes orphan chunks that
     * its own swap either re-references (it wins) or the sweep ages out.
     */
    private deleteRoot_;
    /**
     * One deferred sweep per manager lifetime, off the boot path. In ONE
     * readwrite transaction (serializable against writers): delete metas
     * older than maxAge, chunks of non-live generations older than the
     * orphan age (crashed/raced writes — their key embeds no timestamp, so
     * age rides the gen id's time prefix), and chunks with no meta at all.
     */
    private scheduleSweep_;
    private sweep_;
    /**
     * Deliberate offline (goOffline/repoInterrupt): a suspended tab's server
     * cache is frozen, so it releases writership (the UA grants the lock to
     * an online tab) and stops flushing. Resume re-queues.
     */
    setNetworkSuspended(suspended: boolean): void;
    rebindTo(prefix: string): RowPersistenceManager;
    dispose(): void;
}

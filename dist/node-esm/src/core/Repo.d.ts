/**
 * @license
 * Copyright 2017 Google LLC
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
import { ValueEventRegistration } from '../api/Reference_impl';
import { AppCheckTokenProvider } from './AppCheckTokenProvider';
import { AuthTokenProvider } from './AuthTokenProvider';
import { PersistenceManager } from './Persistence';
import { PersistentConnection } from './PersistentConnection';
import { RepoInfo } from './RepoInfo';
import { ServerActions } from './ServerActions';
import { ListenHashFn } from './ServerCacheSeed';
import { Node } from './snap/Node';
import { SnapshotHolder } from './SnapshotHolder';
import { SparseSnapshotTree } from './SparseSnapshotTree';
import { StatsCollection } from './stats/StatsCollection';
import { StatsListener } from './stats/StatsListener';
import { StatsReporter } from './stats/StatsReporter';
import { SyncTree } from './SyncTree';
import { Indexable } from './util/misc';
import { Path } from './util/Path';
import { Tree } from './util/Tree';
import { Event } from './view/Event';
import { EventQueue } from './view/EventQueue';
import { EventRegistration, QueryContext } from './view/EventRegistration';
declare const enum TransactionStatus {
    RUN = 0,
    SENT = 1,
    COMPLETED = 2,
    SENT_NEEDS_ABORT = 3,
    NEEDS_ABORT = 4
}
interface Transaction {
    path: Path;
    update: (a: unknown) => unknown;
    onComplete: (error: Error | null, committed: boolean, node: Node | null) => void;
    status: TransactionStatus;
    order: number;
    applyLocally: boolean;
    retryCount: number;
    unwatcher: () => void;
    abortReason: string | null;
    currentWriteId: number;
    currentInputSnapshot: Node | null;
    currentOutputSnapshotRaw: Node | null;
    currentOutputSnapshotResolved: Node | null;
}
/**
 * A connection to a single data repository.
 */
interface PendingSeedRestore {
    cancelled: boolean;
}
export declare class Repo {
    repoInfo_: RepoInfo;
    forceRestClient_: boolean;
    authTokenProvider_: AuthTokenProvider;
    appCheckProvider_: AppCheckTokenProvider;
    /** Key for uniquely identifying this repo, used in RepoManager */
    readonly key: string;
    dataUpdateCount: number;
    infoSyncTree_: SyncTree;
    serverSyncTree_: SyncTree;
    stats_: StatsCollection;
    statsListener_: StatsListener | null;
    eventQueue_: EventQueue;
    nextWriteId_: number;
    server_: ServerActions;
    statsReporter_: StatsReporter;
    infoData_: SnapshotHolder;
    interceptServerDataCallback_: ((a: string, b: unknown) => void) | null;
    /** A list of data pieces and paths to be set when this client disconnects. */
    onDisconnect_: SparseSnapshotTree;
    /** Stores queues of outstanding transactions for Firebase locations. */
    transactionQueueTree_: Tree<Transaction[]>;
    persistentConnection_: PersistentConnection | null;
    /**
     * Server-cache persistence (see core/Persistence.ts); null unless the app
     * enabled it before this Repo's first listen.
     */
    persistence_: PersistenceManager | null;
    /**
     * Listens held back while their persisted root restores, keyed by path.
     * stopListening flips the token so a listen whose last registration was
     * removed mid-restore is never sent (see repoStartServerListen).
     */
    pendingSeedRestores_: Map<string, PendingSeedRestore>;
    /**
     * Listen-complete state per default complete listen, keyed by path: whether
     * the current listen has received its initial server response, and waiters
     * to resolve when it does (see whenListenComplete in api/Database.ts).
     */
    listenCompletions_: Map<string, {
        complete: boolean;
        waiters: Array<() => void>;
    }>;
    constructor(repoInfo_: RepoInfo, forceRestClient_: boolean, authTokenProvider_: AuthTokenProvider, appCheckProvider_: AppCheckTokenProvider);
    /**
     * @returns The URL corresponding to the root of this Firebase.
     */
    toString(): string;
}
export declare function repoStart(repo: Repo, appId: string, authOverride?: object): void;
/**
 * @returns The time in milliseconds, taking the server offset into account if we have one.
 */
export declare function repoServerTime(repo: Repo): number;
/**
 * Generate ServerValues using some variables from the repo object.
 */
export declare function repoGenerateServerValues(repo: Repo): Indexable;
/**
 * Sends a listen for the server sync tree, restoring the persisted server
 * cache first where applicable: a complete default listen on a persisted
 * root is held until the stored tree restores (bounded inside restore()),
 * the restored tree is applied as server data — raising cached events
 * immediately, mobile-persistence semantics — and the listen then goes out
 * carrying the restored tree's hashes. Roots that were never persisted
 * resolve null instantly and attach exactly as before.
 */
export declare function repoStartServerListen(repo: Repo, query: QueryContext, tag: number | null, currentHashFn: ListenHashFn, onComplete: (status: string, data?: unknown) => Event[]): void;
/**
 * Stops a server listen. With persistence, a complete default listen may
 * still be waiting on its restore — cancel it so it never attaches — and its
 * root leaves write-through tracking (flushing the final tree to IndexedDB),
 * releasing the in-memory copy that only live listens need.
 */
export declare function repoStopServerListen(repo: Repo, query: QueryContext, tag: number | null): void;
/**
 * Resolves when the current default complete listen at `pathString` has
 * received its initial response from the server — the moment a restored
 * cache is certified (unchanged tree) or replaced (changed tree). Resolves
 * immediately if that already happened, or if no such listen exists; also
 * resolves if the listen stops first, so callers never hang.
 */
export declare function repoWhenListenComplete(repo: Repo, pathString: string): Promise<void>;
/**
 * Settles every outstanding whenListenComplete waiter and clears the
 * completion registry. Called when the repo is deleted (deleteApp): its
 * listens can never respond again, and an unsettleable waiter would hang
 * its caller and retain the Repo forever.
 */
export declare function repoCancelPendingSeedRestores(repo: Repo): void;
export declare function repoSettleListenCompletions(repo: Repo): void;
export declare function repoDispose(repo: Repo): void;
export declare function repoInterceptServerData(repo: Repo, callback: ((a: string, b: unknown) => unknown) | null): void;
/**
 * The purpose of `getValue` is to return the latest known value
 * satisfying `query`.
 *
 * This method will first check for in-memory cached values
 * belonging to active listeners. If they are found, such values
 * are considered to be the most up-to-date.
 *
 * If the client is not connected, this method will wait until the
 *  repo has established a connection and then request the value for `query`.
 * If the client is not able to retrieve the query result for another reason,
 * it reports an error.
 *
 * @param query - The query to surface a value for.
 */
export declare function repoGetValue(repo: Repo, query: QueryContext, eventRegistration: ValueEventRegistration): Promise<Node>;
export declare function repoSetWithPriority(repo: Repo, path: Path, newVal: unknown, newPriority: number | string | null, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoUpdate(repo: Repo, path: Path, childrenToMerge: {
    [k: string]: unknown;
}, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoOnDisconnectCancel(repo: Repo, path: Path, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoOnDisconnectSet(repo: Repo, path: Path, value: unknown, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoOnDisconnectSetWithPriority(repo: Repo, path: Path, value: unknown, priority: unknown, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoOnDisconnectUpdate(repo: Repo, path: Path, childrenToMerge: {
    [k: string]: unknown;
}, onComplete: ((status: Error | null, errorReason?: string) => void) | null): void;
export declare function repoAddEventCallbackForQuery(repo: Repo, query: QueryContext, eventRegistration: EventRegistration): void;
export declare function repoRemoveEventCallbackForQuery(repo: Repo, query: QueryContext, eventRegistration: EventRegistration): void;
export declare function repoInterrupt(repo: Repo): void;
export declare function repoResume(repo: Repo): void;
export declare function repoStats(repo: Repo, showDelta?: boolean): void;
export declare function repoStatsIncrementCounter(repo: Repo, metric: string): void;
export declare function repoCallOnCompleteCallback(repo: Repo, callback: ((status: Error | null, errorReason?: string) => void) | null, status: string, errorReason?: string | null): void;
/**
 * Creates a new transaction, adds it to the transactions we're tracking, and
 * sends it to the server if possible.
 *
 * @param path - Path at which to do transaction.
 * @param transactionUpdate - Update callback.
 * @param onComplete - Completion callback.
 * @param unwatcher - Function that will be called when the transaction no longer
 * need data updates for `path`.
 * @param applyLocally - Whether or not to make intermediate results visible
 */
export declare function repoStartTransaction(repo: Repo, path: Path, transactionUpdate: (a: unknown) => unknown, onComplete: ((error: Error, committed: boolean, node: Node) => void) | null, unwatcher: () => void, applyLocally: boolean): void;
export {};

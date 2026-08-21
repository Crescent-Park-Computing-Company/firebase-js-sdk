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

/**
 * Row-model persistence manager (persistence v2).
 *
 * Replaces the stable-range/manifest design with the Android storage model:
 * path-keyed rows of export JSON (RowStore), no stored hashes, compound
 * hashes computed at listen time from the rows (RowHashKernel). A stale or
 * torn cache is plain staleness the server heals through range merges —
 * never a protocol-corruption state — so this manager carries none of the
 * v1 invariant machinery: no manifests, no revision CAS, no staged
 * verification, no lease heartbeat/steal, no boot buffers.
 *
 * Multi-tab: one Web Locks lock per (scope, root). The holder writes;
 * followers keep their dirty sets in memory and do not touch storage. The
 * lock releases automatically on tab death (UA-guaranteed). Where Web Locks
 * are unavailable every tab writes — rows are last-writer-wins server data,
 * so concurrent writers cost duplicate work, not correctness.
 *
 * Auth: scope-keyed rows; peek retention preserves the v1 (PR #4)
 * semantics — a pre-auth peek is retained under a long backstop until the
 * app confirms the scope, then drops to the short handoff grace; a scope
 * change invalidates everything immediately.
 */


import {
  hashRowsInWorker,
  workerHashAvailable
} from './HashWorker';
import { createRowHashKernel, KernelCompoundHash } from './RowHashKernel';
import {
  ROW_KEY_SEPARATOR,
  ROW_SPLIT_THRESHOLD_BYTES,
  RowIndex,
  assembleRowsSliced,
  decodeRowKeyRelativePath,
  encodeRowKey,
  rowKeyRange,
  splitNodeIntoRows
} from './RowStore';
import { Node } from './snap/Node';
import { Path } from './util/Path';
import { warn , sha1 } from './util/util';
import { yieldMacrotask } from './util/yieldMacrotask';

const DB_NAME = 'firebase-database-persistence';
/** v10 replaces the v1 range/manifest store with the row stores. */
const DB_VERSION = 10;
const ROWS_STORE = 'rows';
const META_STORE = 'meta';
const LEGACY_STORE = 'firebase-server-cache';

export const ROW_PERSISTENCE_WRITE_DEBOUNCE_MS = 15000;
export const ROW_PERSISTENCE_FIRST_GEN_DELAY_MS = 3000;
/** Peek retention while the app has confirmed the primed scope. */
export const ROW_PERSISTENCE_PEEK_HANDOFF_MS = 30000;
/** Peek retention while the scope is primed but unconfirmed (slow auth). */
export const ROW_PERSISTENCE_PEEK_PREAUTH_MS = 5 * 60 * 1000;
export const ROW_PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/** Cached roots older than this are swept (30 days, Android parity). */
export const ROW_PERSISTENCE_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
/** Sweep delay after the first restore — far off every boot-critical path. */
export const ROW_PERSISTENCE_SWEEP_DELAY_MS = 60 * 1000;
/** Byte budget per first-generation staging transaction. */
export const ROW_PERSISTENCE_STAGE_TXN_BYTES = 4 * 1024 * 1024;
/** Parsed-bytes budget per restore assembly slice. */
const RESTORE_SLICE_BYTES = 256 * 1024;
/** Worker hash wall-clock ceiling before the main-thread fallback runs. */
export const ROW_PERSISTENCE_WORKER_HASH_TIMEOUT_MS = 20000;

/** The auth-scope key used in row keys for the signed-out state. */
const PUBLIC_SCOPE = '.public';

interface RootMeta {
  updatedAt: number;
  formatVersion: number;
}
const META_FORMAT_VERSION = 1;

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
  request(
    name: string,
    options: { mode: 'exclusive' },
    callback: (lock: unknown | null) => Promise<unknown>
  ): Promise<unknown>;
}

function defaultWebLocks(): WebLocksLike | null {
  if (
    typeof navigator !== 'undefined' &&
    typeof navigator.locks !== 'undefined'
  ) {
    return navigator.locks as unknown as WebLocksLike;
  }
  return null;
}

interface RetainedPeek {
  promise: Promise<{ node: Node; rowPaths: string[][] } | null>;
  /** Set when the read resolves; identity key for the handoff stamps. */
  resolvedNode: Node | null;
  retained: boolean;
  timer: ReturnType<typeof setTimeout> | null;
}

interface TrackedRoot {
  /** Newest complete server-cache tree awaiting flush (null = clean). */
  latest: Node | null;
  latestScope: string | null;
  /** Dirty subtree paths since the last flush; null = whole root. */
  dirty: string[][] | null | undefined; // undefined = nothing dirty
  /** Row-path index for boundary normalization; null until known. */
  rowIndex: RowIndex | null;
  /** Whether a committed generation exists (meta present). */
  hasGeneration: boolean;
  windowTimer: ReturnType<typeof setTimeout> | null;
  /** Single-flight: a flush is running; re-arm afterward if set. */
  flushing: boolean;
  rearm: boolean;
  /** Held Web Lock release callback (null = not the writer). */
  releaseLock: (() => void) | null;
}

export class RowPersistenceManager {
  private db_: Promise<IDBDatabase | null> | null = null;
  private persistentRoots_ = new Map<string, number>();
  private tracked_ = new Map<string, TrackedRoot>();
  private peeks_ = new Map<string, RetainedPeek>();
  private authScope_: string | null = null;
  private authScopeConfigured_ = false;
  private authScopeConfirmed_ = false;
  private authGeneration_ = 0;
  private networkSuspended_ = false;
  private disposed_ = false;
  private sweepTimer_: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private prefix_: string,
    private idbFactory_: IDBFactory | null = typeof indexedDB !== 'undefined'
      ? indexedDB
      : null,
    private webLocks_: WebLocksLike | null = defaultWebLocks(),
    private writeDelayMs_: number = ROW_PERSISTENCE_WRITE_DEBOUNCE_MS,
    private firstGenDelayMs_: number = ROW_PERSISTENCE_FIRST_GEN_DELAY_MS,
    private splitThresholdBytes_: number = ROW_SPLIT_THRESHOLD_BYTES,
    private peekHandoffMs_: number = ROW_PERSISTENCE_PEEK_HANDOFF_MS,
    private peekPreAuthMs_: number = ROW_PERSISTENCE_PEEK_PREAUTH_MS,
    private restoreTimeoutMs_: number = ROW_PERSISTENCE_RESTORE_TIMEOUT_MS,
    private stageTxnBytes_: number = ROW_PERSISTENCE_STAGE_TXN_BYTES,
    private workerHashTimeoutMs_: number = ROW_PERSISTENCE_WORKER_HASH_TIMEOUT_MS,
    private maxAgeMs_: number = ROW_PERSISTENCE_MAX_AGE_MS
  ) {}

  // ─────────────────────────── auth scope ────────────────────────────────

  isAuthScopeConfigured(): boolean {
    return this.authScopeConfigured_;
  }

  authGeneration(): number {
    return this.authGeneration_;
  }

  /**
   * Configures the identity scope. `confirmedByApp=false` is a pre-auth
   * peek priming a trusted expected identity; `true` is the app's real auth
   * integration. Returns whether the scope CHANGED (callers cancel pending
   * seed restores on a confirmed change).
   */
  setAuthScope(scope: string | null, confirmedByApp = true): boolean {
    const changed = !this.authScopeConfigured_ || scope !== this.authScope_;
    this.authScopeConfigured_ = true;
    if (confirmedByApp) {
      if (!this.authScopeConfirmed_) {
        this.authScopeConfirmed_ = true;
        if (!changed) {
          // Real auth confirmed the primed scope: retained peeks drop from
          // the pre-auth backstop to the short handoff grace, counted now.
          this.rearmRetainedPeeks_(this.peekHandoffMs_);
        }
      }
    } else if (changed) {
      this.authScopeConfirmed_ = false;
    }
    if (!changed) {
      return false;
    }
    this.authGeneration_++;
    this.authScope_ = scope;
    // A different identity invalidates every in-memory holding: retained
    // peeks (another account's tree must never reach a listener), pending
    // trees, dirty sets, and writer locks (their names embed the scope).
    this.clearAllPeeks_();
    for (const [pathString, root] of this.tracked_) {
      this.resetTrackedRoot_(root);
      void this.acquireWriterLock_(pathString, root);
    }
    return true;
  }

  private scopeKey_(): string {
    return this.authScope_ === null ? PUBLIC_SCOPE : this.authScope_;
  }

  // ───────────────────────── root selection ──────────────────────────────

  setPersistentPath(pathString: string, enabled: boolean): void {
    const count = this.persistentRoots_.get(pathString) ?? 0;
    if (enabled) {
      this.persistentRoots_.set(pathString, count + 1);
    } else if (count <= 1) {
      this.persistentRoots_.delete(pathString);
    } else {
      this.persistentRoots_.set(pathString, count - 1);
    }
  }

  isPersistentPath(pathString: string): boolean {
    return this.persistentRoots_.has(pathString);
  }

  /**
   * The tracked root at or above `pathString`, or null. Roots are the paths
   * listeners selected with {persistent: true}; a server update anywhere
   * under one re-persists through that root.
   */
  trackedRootFor(pathString: string): string | null {
    for (const root of this.tracked_.keys()) {
      if (pathString === root || pathString.startsWith(root === '/' ? '/' : root + '/')) {
        return root;
      }
    }
    return null;
  }

  track(pathString: string): void {
    if (this.disposed_ || this.tracked_.has(pathString)) {
      return;
    }
    const root: TrackedRoot = {
      latest: null,
      latestScope: null,
      dirty: undefined,
      rowIndex: null,
      hasGeneration: false,
      windowTimer: null,
      flushing: false,
      rearm: false,
      releaseLock: null
    };
    this.tracked_.set(pathString, root);
    void this.acquireWriterLock_(pathString, root);
  }

  untrack(pathString: string): void {
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return;
    }
    // Flush what is pending before releasing writership, so the final tree
    // of a closed listener survives to the next boot.
    const finalFlush =
      root.dirty !== undefined && root.latest !== null && !this.networkSuspended_
        ? this.flushNow_(pathString, root)
        : Promise.resolve();
    void finalFlush.then(() => {
      // Re-track since? The new registration owns the entry now.
      if (this.tracked_.get(pathString) !== root) {
        return;
      }
      this.releaseRoot_(root);
      this.tracked_.delete(pathString);
    });
  }

  private resetTrackedRoot_(root: TrackedRoot): void {
    if (root.windowTimer !== null) {
      clearTimeout(root.windowTimer);
      root.windowTimer = null;
    }
    root.latest = null;
    root.latestScope = null;
    root.dirty = undefined;
    root.rowIndex = null;
    root.hasGeneration = false;
    root.rearm = false;
    if (root.releaseLock !== null) {
      root.releaseLock();
      root.releaseLock = null;
    }
  }

  private releaseRoot_(root: TrackedRoot): void {
    this.resetTrackedRoot_(root);
  }

  // ─────────────────────────── writer lock ───────────────────────────────

  /**
   * Queues a BLOCKING exclusive lock request for (scope, root). The UA
   * grants it when the current holder releases — an untrack's final flush,
   * a scope switch, or tab death (release is UA-guaranteed) — so writer
   * succession is automatic with zero steal/heartbeat machinery. Until the
   * grant this tab is a follower: it keeps dirty sets in memory and does
   * not touch storage. On grant it refreshes the row index (rows on disk
   * may lag its memory) and flushes whatever is pending.
   */
  private async acquireWriterLock_(
    pathString: string,
    root: TrackedRoot
  ): Promise<void> {
    if (this.webLocks_ === null) {
      // Fail open: no cross-tab exclusion, every tab writes (LWW rows).
      return;
    }
    const name =
      'firebase-db-rows|' + this.prefix_ + '|' + this.scopeKey_() + '|' + pathString;
    const generation = this.authGeneration_;
    try {
      await this.webLocks_.request(name, { mode: 'exclusive' }, () => {
        if (
          this.disposed_ ||
          this.networkSuspended_ ||
          this.authGeneration_ !== generation ||
          this.tracked_.get(pathString) !== root
        ) {
          // Granted after this tracking ended (or while deliberately
          // offline): release immediately so the next queued tab takes
          // over. Resume queues a fresh request.
          return Promise.resolve();
        }
        return new Promise<void>(resolve => {
          root.releaseLock = resolve;
          root.rowIndex = null;
          if (root.dirty !== undefined && root.windowTimer === null) {
            this.armWindow_(pathString, root);
          }
        });
      });
    } catch (e) {
      // Lock API failure: stay a follower (rows go stale until next boot).
    }
  }

  private isWriter_(root: TrackedRoot): boolean {
    // No Web Locks (Node, exotic embedders): every tab writes — a duplicate
    // LWW write is cheaper than having no writer at all.
    return this.webLocks_ === null || root.releaseLock !== null;
  }

  // ──────────────────────────── IDB layer ────────────────────────────────

  private open_(): Promise<IDBDatabase | null> {
    if (this.db_ !== null) {
      return this.db_;
    }
    this.db_ = new Promise(resolve => {
      if (this.idbFactory_ === null) {
        resolve(null);
        return;
      }
      let request: IDBOpenDBRequest;
      try {
        request = this.idbFactory_.open(DB_NAME, DB_VERSION);
      } catch (e) {
        resolve(null);
        return;
      }
      request.onupgradeneeded = () => {
        const db = request.result;
        if (db.objectStoreNames.contains(LEGACY_STORE)) {
          db.deleteObjectStore(LEGACY_STORE);
        }
        if (!db.objectStoreNames.contains(ROWS_STORE)) {
          db.createObjectStore(ROWS_STORE);
        }
        if (!db.objectStoreNames.contains(META_STORE)) {
          db.createObjectStore(META_STORE);
        }
      };
      request.onsuccess = () => {
        const db = request.result;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      request.onerror = () => resolve(null);
      request.onblocked = () => {
        // Another tab holds an old version open; fail open to live RTDB.
        resolve(null);
      };
    });
    return this.db_;
  }

  private requestDone_<T>(request: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error('idb error'));
    });
  }

  private txnDone_(txn: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
      txn.oncomplete = () => resolve();
      txn.onerror = () => reject(txn.error ?? new Error('idb txn error'));
      txn.onabort = () => reject(txn.error ?? new Error('idb txn abort'));
    });
  }

  // ─────────────────────────── peek (boot) ───────────────────────────────

  /**
   * Reads the cached tree for `pathString` without starting the repo — the
   * pre-auth boot peek. One physical read per root: an overlapping
   * authenticated restore consumes the same decode (restoreForListen). The
   * resolved node is RETAINED under the pre-auth backstop (or the short
   * grace once the scope is confirmed) so the later listener join reuses
   * this decode instead of reading twice.
   */
  peek(
    pathString: string,
    expectedAuthScope: string | null
  ): Promise<{ node: Node } | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    if (
      expectedAuthScope !== null &&
      this.authScopeConfigured_ &&
      this.authScope_ !== expectedAuthScope
    ) {
      return Promise.resolve(null);
    }
    const generation = this.authGeneration_;
    const entry = this.startRead_(pathString, /* retain= */ true);
    return entry.promise.then(result => {
      if (result === null || this.authGeneration_ !== generation) {
        return null;
      }
      return { node: result.node };
    });
  }

  /**
   * True while THE retained read that decoded `node` is still awaiting its
   * listener join — the only window in which materialization stamps have a
   * consumer (identity-bound; see v1 hasRetainedPeek).
   */
  hasRetainedPeek(pathString: string, node: Node): boolean {
    const entry = this.peeks_.get(pathString);
    return entry?.retained === true && entry.resolvedNode === node;
  }

  private startRead_(pathString: string, retain: boolean): RetainedPeek {
    const existing = this.peeks_.get(pathString);
    if (existing !== undefined) {
      return existing;
    }
    const entry: RetainedPeek = {
      promise: this.readRoot_(pathString),
      resolvedNode: null,
      retained: retain,
      timer: null
    };
    this.peeks_.set(pathString, entry);
    void entry.promise.then(result => {
      if (this.peeks_.get(pathString) !== entry) {
        return;
      }
      if (result === null) {
        this.peeks_.delete(pathString);
        return;
      }
      entry.resolvedNode = result.node;
      if (entry.retained) {
        const budget = this.authScopeConfirmed_
          ? this.peekHandoffMs_
          : this.peekPreAuthMs_;
        entry.timer = setTimeout(() => {
          this.dropPeek_(pathString, entry);
        }, budget);
      }
    });
    return entry;
  }

  private dropPeek_(pathString: string, entry: RetainedPeek): void {
    if (this.peeks_.get(pathString) !== entry) {
      return;
    }
    if (entry.timer !== null) {
      clearTimeout(entry.timer);
    }
    this.peeks_.delete(pathString);
  }

  private rearmRetainedPeeks_(budget: number): void {
    for (const [pathString, entry] of this.peeks_) {
      if (entry.retained && entry.resolvedNode !== null) {
        if (entry.timer !== null) {
          clearTimeout(entry.timer);
        }
        entry.timer = setTimeout(() => {
          this.dropPeek_(pathString, entry);
        }, budget);
      }
    }
  }

  private clearAllPeeks_(): void {
    for (const entry of this.peeks_.values()) {
      if (entry.timer !== null) {
        clearTimeout(entry.timer);
      }
    }
    this.peeks_.clear();
  }

  /** One physical root read: meta check, row getAll, sliced assemble. */
  private async readRoot_(
    pathString: string
  ): Promise<{ node: Node; rowPaths: string[][] } | null> {
    const db = await this.open_();
    if (db === null) {
      return null;
    }
    const scope = this.scopeKey_();
    try {
      const txn = db.transaction([ROWS_STORE, META_STORE], 'readonly');
      const metaKey = encodeRowKey(scope, this.rootKey_(pathString), []);
      const metaReq = txn.objectStore(META_STORE).get(metaKey);
      const range = rowKeyRange(scope, this.rootKey_(pathString), []);
      const keysReq = txn.objectStore(ROWS_STORE).getAllKeys(range);
      const valuesReq = txn.objectStore(ROWS_STORE).getAll(range);
      const [meta, keys, values] = await Promise.all([
        this.requestDone_(metaReq),
        this.requestDone_(keysReq),
        this.requestDone_(valuesReq)
      ]);
      if (
        meta === undefined ||
        (meta as RootMeta).formatVersion !== META_FORMAT_VERSION
      ) {
        return null;
      }
      const rows: Array<[string[], string]> = [];
      const rowPaths: string[][] = [];
      for (let i = 0; i < keys.length; i++) {
        const relative = decodeRowKeyRelativePath(
          keys[i] as string,
          scope,
          this.rootKey_(pathString)
        );
        rows.push([relative, values[i] as string]);
        rowPaths.push(relative);
      }
      const node = await assembleRowsSliced(
        rows,
        yieldMacrotask,
        RESTORE_SLICE_BYTES
      );
      if (node.isEmpty() && rows.length > 0) {
        return null;
      }
      return { node, rowPaths };
    } catch (e) {
      warn('persistence read failed: ' + (e as Error | null)?.message);
      return null;
    }
  }

  /** The root path string used inside row keys ('/a/b' canonical form). */
  private rootKey_(pathString: string): string {
    return pathString;
  }

  // ───────────────────────── restore (listen) ────────────────────────────

  /**
   * The authenticated listener's restore: consumes the retained peek's
   * decode when one exists (the one-decode-per-boot handoff), otherwise
   * performs its own read. Resolves within `restoreTimeoutMs_` or reports
   * a timeout miss (the listen then goes cold — liveness over cache).
   */
  restoreForListen(pathString: string): Promise<RowRestoreResult> {
    if (this.disposed_ || !this.authScopeConfigured_) {
      return Promise.resolve({ node: null });
    }
    const generation = this.authGeneration_;
    const entry = this.startRead_(pathString, /* retain= */ false);
    let timedOut = false;
    const timeout = new Promise<null>(resolve => {
      setTimeout(() => {
        timedOut = true;
        resolve(null);
      }, this.restoreTimeoutMs_);
    });
    return Promise.race([entry.promise, timeout]).then(result => {
      // The listener consumed (or abandoned) the read; retention ends.
      this.dropPeek_(pathString, entry);
      this.scheduleSweep_();
      if (this.authGeneration_ !== generation) {
        return { node: null };
      }
      if (result === null) {
        return timedOut ? { node: null, reason: 'timeout' as const } : { node: null };
      }
      const root = this.tracked_.get(pathString);
      if (root !== null && root !== undefined) {
        root.rowIndex = RowIndex.fromRelativePaths(result.rowPaths);
        root.hasGeneration = true;
      }
      return { node: result.node };
    });
  }

  // ───────────────────────── listen hashes ───────────────────────────────

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
  async computeListenHashes(
    pathString: string
  ): Promise<RowListenHashes | null> {
    const scope = this.scopeKey_();
    const rootKey = this.rootKey_(pathString);
    if (this.idbFactory_ !== null && workerHashAvailable()) {
      const prefix = encodeRowKey(scope, rootKey, []);
      try {
        const compoundHash = await hashRowsInWorker(
          {
            dbName: DB_NAME,
            storeName: ROWS_STORE,
            lowerKey: prefix,
            upperKey: prefix + '\uffff',
            prefixLength: prefix.length,
            separator: ROW_KEY_SEPARATOR
          },
          this.workerHashTimeoutMs_
        );
        if (compoundHash.posts.length === 0 && compoundHash.hashes[0] === '') {
          // Zero rows: no cache to certify.
          return null;
        }
        // h:'' + ch — the simple hash never matches, so the server always
        // evaluates the compound hash (range merges), the wire shape the
        // fork's e2e certification covers.
        return { hash: '', compoundHash };
      } catch (e) {
        // Fall through to the main-thread sliced kernel.
      }
    }
    return this.computeListenHashesOnMainThread_(pathString);
  }

  private async computeListenHashesOnMainThread_(
    pathString: string
  ): Promise<RowListenHashes | null> {
    const db = await this.open_();
    if (db === null) {
      return null;
    }
    const scope = this.scopeKey_();
    try {
      const txn = db.transaction(ROWS_STORE, 'readonly');
      const range = rowKeyRange(scope, this.rootKey_(pathString), []);
      const keysReq = txn.objectStore(ROWS_STORE).getAllKeys(range);
      const valuesReq = txn.objectStore(ROWS_STORE).getAll(range);
      const [keys, values] = await Promise.all([
        this.requestDone_(keysReq),
        this.requestDone_(valuesReq)
      ]);
      if (keys.length === 0) {
        return null;
      }
      const rows = [];
      for (let i = 0; i < keys.length; i++) {
        rows.push({
          path: decodeRowKeyRelativePath(
            keys[i] as string,
            scope,
            this.rootKey_(pathString)
          ),
          json: values[i] as string
        });
      }
      const kernel = createRowHashKernel(
        text => Promise.resolve(sha1(text)),
        yieldMacrotask
      );
      const compoundHash = await kernel.hashRows(rows);
      return { hash: '', compoundHash };
    } catch (e) {
      return null;
    }
  }

  // ─────────────────────────── write path ────────────────────────────────

  /**
   * Write-through entry: the server updated `path`; `node` is the complete
   * server cache at the TRACKED ROOT containing it. `changedPaths` names
   * what changed relative to the root (undefined = unknown = whole root).
   * Mirrors the v1 serverCacheUpdated signature so Repo call sites carry
   * over unchanged.
   */
  serverCacheUpdated(path: Path, node: Node, changedPaths?: string[][]): void {
    if (this.disposed_ || !this.authScopeConfigured_) {
      return;
    }
    const pathString = path.toString();
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return;
    }
    root.latest = node;
    root.latestScope = this.authScope_;
    if (changedPaths === undefined) {
      root.dirty = null; // whole root
    } else if (root.dirty === undefined) {
      root.dirty = changedPaths.slice();
    } else if (root.dirty !== null) {
      root.dirty = root.dirty.concat(changedPaths);
    }
    if (this.networkSuspended_) {
      return;
    }
    this.armWindow_(pathString, root);
  }

  /**
   * Flushes any pending dirt for `pathString` immediately (skipping the
   * debounce window). Used before boot-hashing a grafted base and before
   * reconnect hashing, so the rows describe exactly the live cache.
   * Resolves when the flush (if any) completed.
   */
  flushNow(pathString: string): Promise<void> {
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return Promise.resolve();
    }
    if (root.windowTimer !== null) {
      clearTimeout(root.windowTimer);
      root.windowTimer = null;
    }
    return this.flushNow_(pathString, root);
  }

  /** The currently tracked persistent root paths. */
  trackedPaths(): string[] {
    return [...this.tracked_.keys()];
  }

  /**
   * True while the root's rows lag its live server cache: dirt is pending
   * or a flush is in flight. The listen-hash rule builds on this — a
   * compound hash is only claimed when the rows equal the live cache, so a
   * claim can never describe bytes older than what the client holds.
   */
  hasPendingDirt(pathString: string): boolean {
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return false;
    }
    return root.dirty !== undefined || root.flushing;
  }

  private armWindow_(pathString: string, root: TrackedRoot): void {
    if (root.windowTimer !== null) {
      return; // non-restarting window
    }
    const delay = root.hasGeneration
      ? this.writeDelayMs_
      : Math.min(this.writeDelayMs_, this.firstGenDelayMs_);
    root.windowTimer = setTimeout(() => {
      root.windowTimer = null;
      void this.flushNow_(pathString, root);
    }, delay);
  }

  /** Single-flight flush of everything dirty at the root. */
  private async flushNow_(
    pathString: string,
    root: TrackedRoot
  ): Promise<void> {
    if (root.flushing) {
      root.rearm = true;
      return;
    }
    if (
      root.latest === null ||
      root.dirty === undefined ||
      !this.isWriter_(root) ||
      this.networkSuspended_ ||
      root.latestScope !== this.authScope_
    ) {
      return;
    }
    root.flushing = true;
    const node = root.latest;
    const dirty = root.dirty;
    root.dirty = undefined;
    const generation = this.authGeneration_;
    try {
      if (dirty === null || root.rowIndex === null || !root.hasGeneration) {
        await this.flushWholeRoot_(pathString, root, node, generation);
      } else {
        await this.flushIncremental_(pathString, root, node, dirty, generation);
      }
    } catch (e) {
      warn('persistence flush failed: ' + (e as Error | null)?.message);
      // Leave hasGeneration as-is; the next window retries from latest.
      if (root.dirty === undefined) {
        root.dirty = null;
      }
    } finally {
      root.flushing = false;
      if (root.rearm) {
        root.rearm = false;
        if (root.dirty === undefined) {
          root.dirty = null;
        }
        this.armWindow_(pathString, root);
      }
    }
  }

  /**
   * First generation / unknown-change rewrite of the whole root. Byte-
   * budgeted staging with meta LAST: crash mid-stage reads as "no cache"
   * on the next boot, never a torn generation claiming completeness.
   */
  private async flushWholeRoot_(
    pathString: string,
    root: TrackedRoot,
    node: Node,
    generation: number
  ): Promise<void> {
    const db = await this.open_();
    if (db === null || this.authGeneration_ !== generation) {
      return;
    }
    const scope = this.scopeKey_();
    const rootKey = this.rootKey_(pathString);
    const metaKey = encodeRowKey(scope, rootKey, []);
    const range = rowKeyRange(scope, rootKey, []);
    // 1) Invalidate: delete meta + existing rows. From here until the final
    //    meta put, the cache reads as absent.
    {
      const txn = db.transaction([ROWS_STORE, META_STORE], 'readwrite');
      txn.objectStore(META_STORE).delete(metaKey);
      txn.objectStore(ROWS_STORE).delete(range);
      await this.txnDone_(txn);
    }
    // 2) Stage rows in byte-budgeted transactions (macrotask yields between
    //    them keep the main thread responsive on a large first generation).
    const rows = splitNodeIntoRows([], node, this.splitThresholdBytes_);
    let index = 0;
    while (index < rows.length) {
      if (this.disposed_ || this.authGeneration_ !== generation) {
        return;
      }
      const txn = db.transaction(ROWS_STORE, 'readwrite');
      const store = txn.objectStore(ROWS_STORE);
      let batchBytes = 0;
      while (index < rows.length && batchBytes < this.stageTxnBytes_) {
        const [segs, json] = rows[index];
        store.put(json, encodeRowKey(scope, rootKey, segs));
        batchBytes += json.length;
        index++;
      }
      await this.txnDone_(txn);
      if (index < rows.length) {
        await yieldMacrotask();
      }
    }
    // 3) Commit: meta present = generation complete.
    {
      const txn = db.transaction(META_STORE, 'readwrite');
      const meta: RootMeta = {
        updatedAt: Date.now(),
        formatVersion: META_FORMAT_VERSION
      };
      txn.objectStore(META_STORE).put(meta, metaKey);
      await this.txnDone_(txn);
    }
    root.rowIndex = RowIndex.fromRelativePaths(rows.map(r => r[0]));
    root.hasGeneration = true;
  }

  /**
   * Incremental flush: each dirty path normalizes to its containing row's
   * boundary (disjoint-rows invariant), covered duplicates drop, and each
   * boundary's subtree is deleted+rewritten — ONE readwrite transaction,
   * no hashing, work proportional to the change.
   */
  private async flushIncremental_(
    pathString: string,
    root: TrackedRoot,
    node: Node,
    dirty: string[][],
    generation: number
  ): Promise<void> {
    const db = await this.open_();
    if (db === null || this.authGeneration_ !== generation) {
      return;
    }
    const rowIndex = root.rowIndex!;
    // Normalize to row boundaries, then drop paths covered by another.
    const boundaries: string[][] = [];
    for (let i = 0; i < dirty.length; i++) {
      boundaries.push(rowIndex.rowBoundaryFor(dirty[i]) ?? dirty[i]);
    }
    boundaries.sort((a, b) => a.length - b.length);
    const chosen: string[][] = [];
    outer: for (let i = 0; i < boundaries.length; i++) {
      for (let j = 0; j < chosen.length; j++) {
        const c = chosen[j];
        if (
          c.length <= boundaries[i].length &&
          c.every((seg, k) => boundaries[i][k] === seg)
        ) {
          continue outer;
        }
      }
      chosen.push(boundaries[i]);
    }
    const scope = this.scopeKey_();
    const rootKey = this.rootKey_(pathString);
    const txn = db.transaction([ROWS_STORE, META_STORE], 'readwrite');
    const store = txn.objectStore(ROWS_STORE);
    for (let i = 0; i < chosen.length; i++) {
      const segs = chosen[i];
      store.delete(rowKeyRange(scope, rootKey, segs));
      const subtree = node.getChild(new Path(segs.join('/')));
      const newRows = splitNodeIntoRows(segs, subtree, this.splitThresholdBytes_);
      for (let j = 0; j < newRows.length; j++) {
        store.put(newRows[j][1], encodeRowKey(scope, rootKey, newRows[j][0]));
      }
      rowIndex.replaceSubtree(segs, newRows.map(r => r[0]));
    }
    const meta: RootMeta = {
      updatedAt: Date.now(),
      formatVersion: META_FORMAT_VERSION
    };
    txn.objectStore(META_STORE).put(meta, encodeRowKey(scope, rootKey, []));
    await this.txnDone_(txn);
  }

  /**
   * One deferred sweep per manager lifetime: deletes roots whose meta is
   * older than maxAge (any scope — an account that never logs in again
   * must not hold storage forever) and orphan rows whose meta is absent
   * (a torn first generation). Scheduled off the boot path; failures are
   * ignored (the next session sweeps again).
   */
  private scheduleSweep_(): void {
    if (this.sweepTimer_ !== null || this.disposed_) {
      return;
    }
    this.sweepTimer_ = setTimeout(() => {
      void this.sweep_();
    }, ROW_PERSISTENCE_SWEEP_DELAY_MS);
  }

  private async sweep_(): Promise<void> {
    const db = await this.open_();
    if (db === null || this.disposed_) {
      return;
    }
    try {
      const readTxn = db.transaction([ROWS_STORE, META_STORE], 'readonly');
      const metaKeysReq = readTxn.objectStore(META_STORE).getAllKeys();
      const metaValuesReq = readTxn.objectStore(META_STORE).getAll();
      const rowKeysReq = readTxn.objectStore(ROWS_STORE).getAllKeys();
      const [metaKeys, metaValues, rowKeys] = await Promise.all([
        this.requestDone_(metaKeysReq),
        this.requestDone_(metaValuesReq),
        this.requestDone_(rowKeysReq)
      ]);
      const now = Date.now();
      const liveMeta = new Set<string>();
      const expired: string[] = [];
      for (let i = 0; i < metaKeys.length; i++) {
        const meta = metaValues[i] as RootMeta | null;
        if (
          meta === null ||
          typeof meta !== 'object' ||
          meta.formatVersion !== META_FORMAT_VERSION ||
          now - meta.updatedAt > this.maxAgeMs_
        ) {
          expired.push(metaKeys[i] as string);
        } else {
          liveMeta.add(metaKeys[i] as string);
        }
      }
      // A row belongs to the meta whose key is its scope·root prefix; the
      // meta key is the shortest prefix ending in ROW_KEY_SEPARATOR twice
      // (scope + root). Orphans (no live meta prefix) are torn/expired.
      const doomedRows: string[] = [];
      for (let i = 0; i < rowKeys.length; i++) {
        const key = rowKeys[i] as string;
        const second = key.indexOf(
          ROW_KEY_SEPARATOR,
          key.indexOf(ROW_KEY_SEPARATOR) + 1
        );
        const metaKey = key.slice(0, second + 1);
        if (!liveMeta.has(metaKey)) {
          doomedRows.push(key);
        }
      }
      if (expired.length === 0 && doomedRows.length === 0) {
        return;
      }
      const writeTxn = db.transaction([ROWS_STORE, META_STORE], 'readwrite');
      const metaStore = writeTxn.objectStore(META_STORE);
      const rowStore = writeTxn.objectStore(ROWS_STORE);
      for (let i = 0; i < expired.length; i++) {
        metaStore.delete(expired[i]);
      }
      for (let i = 0; i < doomedRows.length; i++) {
        rowStore.delete(doomedRows[i]);
      }
      await this.txnDone_(writeTxn);
    } catch (e) {
      // Best-effort; sweep again next session.
    }
  }

  // ───────────────────── invalidate / evict / sweep ──────────────────────

  /** Drops the stored cache for the root containing `path` (corrupt). */
  invalidate(path: Path): void {
    const rootString = this.trackedRootFor(path.toString());
    void this.deleteRoot_(rootString ?? path.toString());
    const root = rootString !== null ? this.tracked_.get(rootString) : undefined;
    if (root !== undefined) {
      root.rowIndex = null;
      root.hasGeneration = false;
    }
  }

  /** Removes the stored cache when a persistent listener is torn down. */
  evict(path: Path): void {
    void this.deleteRoot_(path.toString());
  }

  private async deleteRoot_(pathString: string): Promise<void> {
    const db = await this.open_();
    if (db === null) {
      return;
    }
    const scope = this.scopeKey_();
    try {
      const txn = db.transaction([ROWS_STORE, META_STORE], 'readwrite');
      txn
        .objectStore(META_STORE)
        .delete(encodeRowKey(scope, this.rootKey_(pathString), []));
      txn
        .objectStore(ROWS_STORE)
        .delete(rowKeyRange(scope, this.rootKey_(pathString), []));
      await this.txnDone_(txn);
    } catch (e) {
      // Fail open.
    }
  }

  // ───────────────────────────── lifecycle ───────────────────────────────

  /**
   * Deliberate offline (goOffline/repoInterrupt). Liveness is not
   * eligibility: a suspended tab keeps running but its server cache is
   * frozen, so it RELEASES its writer locks — the UA then grants them to a
   * queued online tab, which persists the newest server state. Resume
   * re-queues; writership returns whenever the interim holder unsubscribes
   * or dies. While suspended the write gate stays closed even without Web
   * Locks, so a frozen tree never overwrites an online writer's rows.
   */
  setNetworkSuspended(suspended: boolean): void {
    if (this.networkSuspended_ === suspended) {
      return;
    }
    this.networkSuspended_ = suspended;
    for (const [pathString, root] of this.tracked_) {
      if (suspended) {
        if (root.windowTimer !== null) {
          clearTimeout(root.windowTimer);
          root.windowTimer = null;
        }
        if (root.releaseLock !== null) {
          root.releaseLock();
          root.releaseLock = null;
        }
      } else {
        void this.acquireWriterLock_(pathString, root);
        if (root.dirty !== undefined) {
          this.armWindow_(pathString, root);
        }
      }
    }
  }

  rebindTo(prefix: string): RowPersistenceManager {
    const rebound = new RowPersistenceManager(
      prefix,
      this.idbFactory_,
      this.webLocks_,
      this.writeDelayMs_,
      this.firstGenDelayMs_,
      this.splitThresholdBytes_,
      this.peekHandoffMs_,
      this.peekPreAuthMs_,
      this.restoreTimeoutMs_,
      this.stageTxnBytes_,
      this.workerHashTimeoutMs_,
      this.maxAgeMs_
    );
    if (this.authScopeConfigured_) {
      rebound.setAuthScope(this.authScope_, this.authScopeConfirmed_);
    }
    for (const [pathString, count] of this.persistentRoots_) {
      rebound.persistentRoots_.set(pathString, count);
    }
    this.dispose();
    return rebound;
  }

  dispose(): void {
    if (this.disposed_) {
      return;
    }
    this.disposed_ = true;
    this.clearAllPeeks_();
    for (const root of this.tracked_.values()) {
      this.releaseRoot_(root);
    }
    this.tracked_.clear();
    if (this.sweepTimer_ !== null) {
      clearTimeout(this.sweepTimer_);
      this.sweepTimer_ = null;
    }
    void this.db_?.then(db => db?.close());
    this.db_ = null;
  }
}

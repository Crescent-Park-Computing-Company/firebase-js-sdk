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
 * Generation-swap snapshot persistence (v3).
 *
 * One LOGICAL snapshot per root, stored as immutable ~2MiB chunks under a
 * generation id, made live by a single-key meta pointer swap:
 *
 *   chunks: scope·root·gen·index  →  JSON of [pathSegments, exportJson][]
 *   meta:   scope·root            →  { gen, chunkCount, updatedAt, ... }
 *
 * The design premise: incremental writes against a mutable store
 * manufacture partial states, and every partial state needs a defense
 * (v1's manifests/CAS, v2's staging markers/nonces/serializable sweeps —
 * where every review finding across two audit loops lived). Here there are
 * no partial states to defend:
 *
 * - Chunks are IMMUTABLE once written and INVISIBLE until the meta pointer
 *   references them. A crash mid-write leaves garbage chunks (aged out by
 *   the sweep), never a torn generation. Two tabs racing write two
 *   complete generations; the last pointer swap wins.
 * - Hash-snapshot binding is free: the worker hashes the chunks of the
 *   exact generation this manager restored or committed (root.gen).
 *   Immutability IS the binding — no same-snapshot re-reads, no ownership
 *   nonces, no claims-require-locks rule. A foreign swap that removed the
 *   generation → chunks missing → the claim declines → plain listen.
 * - Every flush is a full snapshot. Mitigations: a one-pointer-compare
 *   skip when the tree didn't change, a WeakMap chunk-payload cache so
 *   serialization cost tracks the CHANGED subtrees (Nodes are immutable
 *   and structurally shared), and a 30s debounce. Web Locks remains a
 *   pure write-dedup optimization — never a correctness mechanism.
 *
 * Auth: rows are namespaced by repo prefix + identity scope exactly like
 * v2 ('public' | 'auth:<uid>' folded with prefix_ into the key's first
 * component); peek retention keeps the pre-auth backstop / confirmed
 * grace semantics.
 */

import { hashRowsInWorker, workerHashAvailable } from './HashWorker';
import { createRowHashKernel, KernelCompoundHash } from './RowHashKernel';
import {
  ROW_KEY_SEPARATOR,
  ROW_SPLIT_THRESHOLD_BYTES,
  assembleRowsSliced,
  encodeRowSegment,
  splitNodeIntoRows
} from './RowStore';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { Node } from './snap/Node';
import { Path } from './util/Path';
import { sha1, warn } from './util/util';
import { yieldMacrotask } from './util/yieldMacrotask';

const DB_NAME = 'firebase-database-persistence';
/** v11 replaces the v2 row/meta stores with the chunk/meta stores. */
const DB_VERSION = 11;
const CHUNKS_STORE = 'chunks';
const META_STORE = 'meta';
const LEGACY_STORES = ['firebase-server-cache', 'rows'];

export const ROW_PERSISTENCE_WRITE_DEBOUNCE_MS = 30000;
export const ROW_PERSISTENCE_FIRST_GEN_DELAY_MS = 3000;
/** Peek retention while the app has confirmed the primed scope. */
export const ROW_PERSISTENCE_PEEK_HANDOFF_MS = 30000;
/** Peek retention while the scope is primed but unconfirmed (slow auth). */
export const ROW_PERSISTENCE_PEEK_PREAUTH_MS = 5 * 60 * 1000;
export const ROW_PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/** Cached roots older than this are swept (30 days, Android parity). */
export const ROW_PERSISTENCE_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
/** Non-live generations older than this are garbage (crashed/raced writes). */
export const ROW_PERSISTENCE_ORPHAN_GEN_AGE_MS = 10 * 60 * 1000;
/** Serialized bytes per chunk — the slice unit for parse and write. */
export const ROW_PERSISTENCE_CHUNK_BYTES = 2 * 1024 * 1024;
export const ROW_PERSISTENCE_WORKER_HASH_TIMEOUT_MS = 20000;
/** Parsed-bytes budget per restore assembly slice. */
const RESTORE_SLICE_BYTES = 256 * 1024;

interface RootMeta {
  /** The LIVE generation id; its chunks are scope·root·gen·0..chunkCount-1. */
  gen: string;
  chunkCount: number;
  updatedAt: number;
  formatVersion: number;
}
const META_FORMAT_VERSION = 3;

function newGen(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

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
    options: { mode: 'exclusive'; ifAvailable?: boolean },
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
  promise: Promise<{ node: Node; gen: string } | null>;
  /** Set when the read resolves; identity key for the handoff stamps. */
  resolvedNode: Node | null;
  retained: boolean;
  timer: ReturnType<typeof setTimeout> | null;
}

interface TrackedRoot {
  /** Newest complete server-cache tree awaiting flush (null = clean). */
  latest: Node | null;
  latestScope: string | null;
  /** True when `latest` differs from the last committed snapshot. */
  dirty: boolean;
  /** The root Node of the last committed/restored generation. */
  committedNode: Node | null;
  /** The generation this manager last restored or committed (the claim). */
  gen: string | null;
  windowTimer: ReturnType<typeof setTimeout> | null;
  activeFlush: Promise<void> | null;
  rearm: boolean;
  /** Held Web Lock release callback (null = not the writer). */
  releaseLock: (() => void) | null;
  /** Resolves once the initial lock acquisition DECIDED (see acquire). */
  lockDecided: Promise<void> | null;
  /** Set while an untrack drain is in flight; track() clears it to cancel. */
  untrackPending: boolean;
}

/**
 * Chunk payloads per top-level child, keyed by the child Node's identity.
 * Nodes are immutable and structurally shared across server updates, so a
 * cached serialization stays valid as long as the child object lives — a
 * snapshot flush re-serializes only the top-level children that actually
 * changed. Module-level WeakMap: entries die with their nodes.
 */
const childPayloadCache = new WeakMap<Node, string>();

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
  private sweepDone_ = false;

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
    private chunkBytes_: number = ROW_PERSISTENCE_CHUNK_BYTES,
    private workerHashTimeoutMs_: number = ROW_PERSISTENCE_WORKER_HASH_TIMEOUT_MS,
    private maxAgeMs_: number = ROW_PERSISTENCE_MAX_AGE_MS,
    private orphanGenAgeMs_: number = ROW_PERSISTENCE_ORPHAN_GEN_AGE_MS
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
   * peek priming a trusted expected identity; `true` is the app's real
   * auth integration. Returns whether the scope CHANGED.
   */
  setAuthScope(scope: string | null, confirmedByApp = true): boolean {
    const changed = !this.authScopeConfigured_ || scope !== this.authScope_;
    this.authScopeConfigured_ = true;
    if (confirmedByApp) {
      if (!this.authScopeConfirmed_) {
        this.authScopeConfirmed_ = true;
        if (!changed) {
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
    // trees, and writer locks (their names embed the scope).
    this.clearAllPeeks_();
    for (const [pathString, root] of this.tracked_) {
      this.resetTrackedRoot_(root);
      void this.acquireWriterLock_(pathString, root);
    }
    return true;
  }

  private scopeKey_(): string {
    // Repo instance + identity, structurally tagged, folded into the key's
    // first component (encodeRowSegment escapes the separators).
    const identity =
      this.authScope_ === null ? 'public' : 'auth:' + this.authScope_;
    return this.prefix_ + '|' + identity;
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

  /** Every path currently selected with {persistent:true}. */
  persistentPaths(): string[] {
    return [...this.persistentRoots_.keys()];
  }

  /** The tracked root at or above `pathString`, or null. */
  trackedRootFor(pathString: string): string | null {
    for (const root of this.tracked_.keys()) {
      if (
        pathString === root ||
        pathString.startsWith(root === '/' ? '/' : root + '/')
      ) {
        return root;
      }
    }
    return null;
  }

  /**
   * EVERY tracked root a server update at `pathString` touches — roots at
   * or above the path AND roots below it (an ancestor overwrite rewrites
   * their subtree). Each stored root must stay current or its next boot
   * hash would claim bytes it does not hold.
   */
  trackedRootsFor(pathString: string): string[] {
    const roots: string[] = [];
    for (const root of this.tracked_.keys()) {
      const rootPrefix = root === '/' ? '/' : root + '/';
      const pathPrefix = pathString === '/' ? '/' : pathString + '/';
      if (
        pathString === root ||
        pathString.startsWith(rootPrefix) ||
        root.startsWith(pathPrefix)
      ) {
        roots.push(root);
      }
    }
    return roots;
  }

  trackedPaths(): string[] {
    return [...this.tracked_.keys()];
  }

  track(pathString: string): void {
    if (this.disposed_) {
      return;
    }
    const existing = this.tracked_.get(pathString);
    if (existing !== undefined) {
      // Cancel an in-flight untrack teardown: the path was re-selected
      // while its drain ran. The entry, lock, and pending dirt stay.
      existing.untrackPending = false;
      return;
    }
    const root: TrackedRoot = {
      latest: null,
      latestScope: null,
      dirty: false,
      committedNode: null,
      gen: null,
      windowTimer: null,
      activeFlush: null,
      rearm: false,
      releaseLock: null,
      lockDecided: null,
      untrackPending: false
    };
    this.tracked_.set(pathString, root);
    void this.acquireWriterLock_(pathString, root);
  }

  untrack(pathString: string): void {
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return;
    }
    root.untrackPending = true;
    // Drain until clean before releasing writership — the final tree of a
    // closed listener must survive to the next boot.
    const drain = async (): Promise<void> => {
      for (let i = 0; i < 10; i++) {
        if (root.activeFlush !== null) {
          await root.activeFlush;
        }
        if (
          !root.dirty ||
          root.latest === null ||
          this.networkSuspended_ ||
          !this.isWriter_(root)
        ) {
          return;
        }
        await this.flushNow_(pathString, root);
      }
    };
    void drain().then(() => {
      if (this.tracked_.get(pathString) !== root || !root.untrackPending) {
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
    root.dirty = false;
    root.committedNode = null;
    root.gen = null;
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
   * Two-phase writer acquisition — an OPTIMIZATION ONLY (avoids duplicate
   * snapshot writes from N tabs); correctness never depends on it, because
   * generations are complete-or-invisible regardless of who writes.
   * 1. An `ifAvailable` probe decides immediately (lockDecided resolves).
   * 2. When the probe lost, a blocking queued request waits for succession
   *    (the UA grants it when the holder releases or its tab dies).
   */
  private async acquireWriterLock_(
    pathString: string,
    root: TrackedRoot
  ): Promise<void> {
    if (this.webLocks_ === null) {
      root.lockDecided = Promise.resolve();
      return;
    }
    const name =
      'firebase-db-gen|' +
      this.prefix_ +
      '|' +
      this.scopeKey_() +
      '|' +
      pathString;
    const generation = this.authGeneration_;
    const usable = (): boolean =>
      !this.disposed_ &&
      !this.networkSuspended_ &&
      this.authGeneration_ === generation &&
      this.tracked_.get(pathString) === root;
    const holdLock = (): Promise<void> =>
      new Promise<void>(resolve => {
        root.releaseLock = resolve;
        if (root.dirty && root.windowTimer === null) {
          this.armWindow_(pathString, root);
        }
      });

    let decided!: () => void;
    root.lockDecided = new Promise<void>(resolve => {
      decided = resolve;
    });
    try {
      let probeWon = false;
      await this.webLocks_.request(
        name,
        { mode: 'exclusive', ifAvailable: true },
        lock => {
          decided();
          if (lock === null || !usable()) {
            return Promise.resolve();
          }
          probeWon = true;
          return holdLock();
        }
      );
      if (probeWon || !usable()) {
        return;
      }
      await this.webLocks_.request(name, { mode: 'exclusive' }, () => {
        if (!usable()) {
          return Promise.resolve();
        }
        return holdLock();
      });
    } catch (e) {
      decided();
    }
  }

  private isWriter_(root: TrackedRoot): boolean {
    // Without Web Locks every tab writes — a duplicate complete snapshot
    // is wasted work, never a correctness problem (last pointer wins).
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
        for (const legacy of LEGACY_STORES) {
          if (db.objectStoreNames.contains(legacy)) {
            db.deleteObjectStore(legacy);
          }
        }
        // The v2 'meta' store carries incompatible records; recreate it.
        if (db.objectStoreNames.contains(META_STORE)) {
          db.deleteObjectStore(META_STORE);
        }
        db.createObjectStore(META_STORE);
        if (!db.objectStoreNames.contains(CHUNKS_STORE)) {
          db.createObjectStore(CHUNKS_STORE);
        }
      };
      request.onsuccess = () => {
        const db = request.result;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      request.onerror = () => resolve(null);
      request.onblocked = () => resolve(null);
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

  /** meta key for a root: scope·root (both escaped) + terminator. */
  private metaKey_(pathString: string): string {
    return (
      encodeRowSegment(this.scopeKey_()) +
      ROW_KEY_SEPARATOR +
      encodeRowSegment(pathString) +
      ROW_KEY_SEPARATOR
    );
  }

  /** chunk key: metaKey · gen · index (zero-padded for range order). */
  private chunkKey_(pathString: string, gen: string, index: number): string {
    return (
      this.metaKey_(pathString) +
      gen +
      ROW_KEY_SEPARATOR +
      String(index).padStart(6, '0')
    );
  }

  private chunkRange_(pathString: string, gen?: string): IDBKeyRange {
    const start =
      gen === undefined
        ? this.metaKey_(pathString)
        : this.metaKey_(pathString) + gen + ROW_KEY_SEPARATOR;
    const end = start + '\uffff';
    if (typeof IDBKeyRange !== 'undefined') {
      return IDBKeyRange.bound(start, end, false, true);
    }
    // Node (tests): a structurally compatible range for fake stores.
    return {
      lower: start,
      upper: end,
      lowerOpen: false,
      upperOpen: true,
      includes: (key: string) => key >= start && key < end
    } as unknown as IDBKeyRange;
  }

  // ─────────────────────────── peek (boot) ───────────────────────────────

  /**
   * Reads the cached tree for `pathString` without starting the repo — the
   * pre-auth boot peek. One physical read per root; the resolved node is
   * RETAINED under the pre-auth backstop (or the short confirmed grace) so
   * the authenticated listener consumes this same decode.
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
   * consumer (identity-bound).
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

  /** One physical root read: meta → live gen's chunks → sliced assemble. */
  private async readRoot_(
    pathString: string
  ): Promise<{ node: Node; gen: string } | null> {
    const db = await this.open_();
    if (db === null) {
      return null;
    }
    try {
      const txn = db.transaction([CHUNKS_STORE, META_STORE], 'readonly');
      const metaReq = txn
        .objectStore(META_STORE)
        .get(this.metaKey_(pathString));
      const meta = (await this.requestDone_(metaReq)) as RootMeta | undefined;
      if (
        meta === undefined ||
        meta.formatVersion !== META_FORMAT_VERSION ||
        Date.now() - meta.updatedAt > this.maxAgeMs_
      ) {
        return null;
      }
      const chunksReq = txn
        .objectStore(CHUNKS_STORE)
        .getAll(this.chunkRange_(pathString, meta.gen));
      const chunkTexts = (await this.requestDone_(chunksReq)) as string[];
      if (chunkTexts.length !== meta.chunkCount) {
        // A foreign swap GC'd this generation mid-read, or a corrupt store.
        return null;
      }
      const rows: Array<[string[], string]> = [];
      for (let i = 0; i < chunkTexts.length; i++) {
        const parsed = JSON.parse(chunkTexts[i]) as Array<[string[], string]>;
        for (let j = 0; j < parsed.length; j++) {
          rows.push(parsed[j]);
        }
        if (i + 1 < chunkTexts.length) {
          await yieldMacrotask();
        }
      }
      const node = await assembleRowsSliced(
        rows,
        yieldMacrotask,
        RESTORE_SLICE_BYTES
      );
      if (node.isEmpty() && rows.length > 0) {
        return null;
      }
      return { node, gen: meta.gen };
    } catch (e) {
      warn('persistence read failed: ' + (e as Error | null)?.message);
      return null;
    }
  }

  // ───────────────────────── restore (listen) ────────────────────────────

  /**
   * The authenticated listener's restore: consumes the retained peek's
   * decode when one exists, otherwise performs its own read. Resolves
   * within `restoreTimeoutMs_` or reports a timeout miss.
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
      this.dropPeek_(pathString, entry);
      this.scheduleSweep_();
      if (this.authGeneration_ !== generation) {
        return { node: null };
      }
      if (result === null) {
        return timedOut
          ? { node: null, reason: 'timeout' as const }
          : { node: null };
      }
      const root = this.tracked_.get(pathString);
      if (root !== undefined) {
        root.committedNode = result.node;
        root.gen = result.gen;
      }
      return { node: result.node };
    });
  }

  // ───────────────────────── listen hashes ───────────────────────────────

  /**
   * Computes the wire listen hashes from the stored chunks of the exact
   * generation this manager last restored or committed. Immutability makes
   * the snapshot binding structural: the chunks either ARE that generation
   * byte for byte, or some are missing (foreign swap GC'd it — chunkCount
   * mismatch) and the claim declines. Worker-first; sliced main-thread
   * fallback. Null ⇒ the listen goes out plain (full resend, uncertified).
   */
  async computeListenHashes(
    pathString: string
  ): Promise<RowListenHashes | null> {
    const root = this.tracked_.get(pathString);
    const gen = root?.gen ?? null;
    if (gen === null || root === undefined) {
      return null;
    }
    if (root.dirty || root.activeFlush !== null) {
      // The live cache moved past the committed snapshot; a claim would
      // describe bytes the client no longer holds.
      return null;
    }
    const chunkPrefix = this.metaKey_(pathString) + gen + ROW_KEY_SEPARATOR;
    if (this.idbFactory_ !== null && workerHashAvailable()) {
      try {
        const compoundHash = await hashRowsInWorker(
          {
            dbName: DB_NAME,
            storeName: CHUNKS_STORE,
            metaStoreName: META_STORE,
            metaKey: this.metaKey_(pathString),
            expectedGen: gen,
            lowerKey: chunkPrefix,
            upperKey: chunkPrefix + '\uffff',
            chunked: true
          },
          this.workerHashTimeoutMs_
        );
        if (compoundHash.posts.length === 0 && compoundHash.hashes[0] === '') {
          return null;
        }
        return { hash: '', compoundHash };
      } catch (e) {
        // Fall through to the main-thread sliced kernel.
      }
    }
    return this.computeListenHashesOnMainThread_(pathString, gen);
  }

  private async computeListenHashesOnMainThread_(
    pathString: string,
    gen: string
  ): Promise<RowListenHashes | null> {
    const db = await this.open_();
    if (db === null) {
      return null;
    }
    try {
      const txn = db.transaction([CHUNKS_STORE, META_STORE], 'readonly');
      const metaReq = txn
        .objectStore(META_STORE)
        .get(this.metaKey_(pathString));
      const chunksReq = txn
        .objectStore(CHUNKS_STORE)
        .getAll(this.chunkRange_(pathString, gen));
      const [meta, chunkTexts] = await Promise.all([
        this.requestDone_(metaReq),
        this.requestDone_(chunksReq)
      ]);
      if (
        meta === undefined ||
        (meta as RootMeta).gen !== gen ||
        (chunkTexts as string[]).length !== (meta as RootMeta).chunkCount ||
        (chunkTexts as string[]).length === 0
      ) {
        return null;
      }
      const rows: Array<{ path: string[]; json: string }> = [];
      for (const text of chunkTexts as string[]) {
        const parsed = JSON.parse(text) as Array<[string[], string]>;
        for (const [path, json] of parsed) {
          rows.push({ path, json });
        }
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
   * server cache at the tracked root. `changedPaths` is accepted for call
   * compatibility; the snapshot model only needs "did anything change",
   * which the node identity answers exactly ([] = a certification restating
   * known state = nothing new).
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
    if (changedPaths !== undefined && changedPaths.length === 0) {
      // A listen certification: state already accounted, nothing dirty.
      return;
    }
    if (node === root.committedNode) {
      // Identity-equal to the committed snapshot: nothing to write.
      return;
    }
    root.dirty = true;
    if (this.networkSuspended_) {
      return;
    }
    this.armWindow_(pathString, root);
  }

  /**
   * Flushes pending dirt immediately (skipping the debounce window).
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

  /** True while the committed snapshot lags the live cache. */
  hasPendingDirt(pathString: string): boolean {
    const root = this.tracked_.get(pathString);
    if (root === undefined) {
      return false;
    }
    return root.dirty || root.activeFlush !== null;
  }

  private armWindow_(pathString: string, root: TrackedRoot): void {
    if (root.windowTimer !== null) {
      return; // non-restarting window
    }
    const delay =
      root.gen !== null
        ? this.writeDelayMs_
        : Math.min(this.writeDelayMs_, this.firstGenDelayMs_);
    root.windowTimer = setTimeout(() => {
      root.windowTimer = null;
      void this.flushNow_(pathString, root);
    }, delay);
  }

  private flushNow_(pathString: string, root: TrackedRoot): Promise<void> {
    if (root.activeFlush !== null) {
      root.rearm = true;
      return root.activeFlush;
    }
    const promise = this.flushImpl_(pathString, root);
    root.activeFlush = promise;
    void promise.finally(() => {
      if (root.activeFlush === promise) {
        root.activeFlush = null;
      }
      if (root.rearm) {
        root.rearm = false;
        if (root.dirty) {
          this.armWindow_(pathString, root);
        }
      }
    });
    return promise;
  }

  /**
   * One snapshot flush: serialize → write chunks under a fresh gen (any
   * number of transactions; unreferenced chunks are invisible) → swap the
   * meta pointer and delete the previous generation's chunks in ONE final
   * transaction. Everything before the swap is free to fail or race.
   */
  private async flushImpl_(
    pathString: string,
    root: TrackedRoot
  ): Promise<void> {
    if (root.lockDecided !== null) {
      await root.lockDecided;
    }
    const node = root.latest;
    if (
      node === null ||
      !root.dirty ||
      !this.isWriter_(root) ||
      this.networkSuspended_ ||
      root.latestScope !== this.authScope_
    ) {
      return;
    }
    const generation = this.authGeneration_;
    const db = await this.open_();
    if (db === null || this.authGeneration_ !== generation) {
      return;
    }
    root.dirty = false;
    const previousGen = root.gen;
    const gen = newGen();
    try {
      // 1) Serialize into chunk texts, sliced. Per-top-level-child payloads
      //    are cached by node identity, so unchanged children reuse their
      //    serialized text (cost ∝ changed subtrees).
      const chunkTexts: string[] = [];
      let current: string[] = [];
      let currentBytes = 0;
      const pushRows = (rows: Array<[string[], string]>): void => {
        for (const row of rows) {
          const text = JSON.stringify(row);
          current.push(text);
          currentBytes += text.length;
          if (currentBytes >= this.chunkBytes_) {
            chunkTexts.push('[' + current.join(',') + ']');
            current = [];
            currentBytes = 0;
          }
        }
      };
      if (node.isLeafNode() || node.isEmpty()) {
        pushRows(splitNodeIntoRows([], node, this.splitThresholdBytes_));
      } else {
        const priority = node.getPriority();
        if (!priority.isEmpty()) {
          pushRows([[['.priority'], JSON.stringify(priority.val())]]);
        }
        const children: Array<[string, Node]> = [];
        node.forEachChild(PRIORITY_INDEX, (key: string, child: Node) => {
          children.push([key, child]);
        });
        for (const [key, child] of children) {
          if (this.disposed_ || this.authGeneration_ !== generation) {
            root.dirty = true;
            return;
          }
          let payload = childPayloadCache.get(child);
          if (payload === undefined) {
            payload = JSON.stringify(
              splitNodeIntoRows([key], child, this.splitThresholdBytes_)
            );
            childPayloadCache.set(child, payload);
            await yieldMacrotask();
          }
          pushRows(JSON.parse(payload) as Array<[string[], string]>);
        }
      }
      if (current.length > 0 || chunkTexts.length === 0) {
        chunkTexts.push('[' + current.join(',') + ']');
      }

      // 2) Write the chunks under the fresh gen (invisible until the swap).
      for (let i = 0; i < chunkTexts.length; i++) {
        if (this.disposed_ || this.authGeneration_ !== generation) {
          root.dirty = true;
          return;
        }
        const txn = db.transaction(CHUNKS_STORE, 'readwrite');
        txn
          .objectStore(CHUNKS_STORE)
          .put(chunkTexts[i], this.chunkKey_(pathString, gen, i));
        await this.txnDone_(txn);
        if (i + 1 < chunkTexts.length) {
          await yieldMacrotask();
        }
      }

      // 3) The swap: point meta at the new gen and drop the old gen's
      //    chunks, atomically. Readers see the old complete generation
      //    right up to this commit, the new one after it.
      if (this.disposed_ || this.authGeneration_ !== generation) {
        root.dirty = true;
        return;
      }
      const txn = db.transaction([CHUNKS_STORE, META_STORE], 'readwrite');
      txn.objectStore(META_STORE).put(
        {
          gen,
          chunkCount: chunkTexts.length,
          updatedAt: Date.now(),
          formatVersion: META_FORMAT_VERSION
        } as RootMeta,
        this.metaKey_(pathString)
      );
      if (previousGen !== null && previousGen !== gen) {
        txn
          .objectStore(CHUNKS_STORE)
          .delete(this.chunkRange_(pathString, previousGen));
      }
      await this.txnDone_(txn);
      root.committedNode = node;
      root.gen = gen;
    } catch (e) {
      warn('persistence flush failed: ' + (e as Error | null)?.message);
      root.dirty = true;
    }
  }

  // ───────────────────── invalidate / evict / sweep ──────────────────────

  /** Drops the stored cache for the root containing `path` (corrupt). */
  invalidate(path: Path): void {
    const rootString = this.trackedRootFor(path.toString());
    void this.deleteRoot_(rootString ?? path.toString());
  }

  /** Removes the stored cache when a persistent listener is torn down. */
  evict(path: Path): void {
    void this.deleteRoot_(path.toString());
  }

  /**
   * Deletes a root's stored cache. The namespace is captured synchronously
   * (an auth switch during the awaits must not redirect the delete), and
   * local state is invalidated first so no queued flush resurrects it.
   * The one delete transaction removes meta + every chunk of every gen —
   * a concurrent writer's in-flight gen simply becomes orphan chunks that
   * its own swap either re-references (it wins) or the sweep ages out.
   */
  private async deleteRoot_(pathString: string): Promise<void> {
    const metaKey = this.metaKey_(pathString);
    const range = this.chunkRange_(pathString);
    const generation = this.authGeneration_;
    const root = this.tracked_.get(pathString);
    if (root !== undefined) {
      if (root.windowTimer !== null) {
        clearTimeout(root.windowTimer);
        root.windowTimer = null;
      }
      root.dirty = false;
      root.rearm = false;
      root.committedNode = null;
      root.gen = null;
      if (root.activeFlush !== null) {
        await root.activeFlush.catch(() => {});
      }
    }
    const db = await this.open_();
    if (db === null || this.authGeneration_ !== generation) {
      return;
    }
    try {
      const txn = db.transaction([CHUNKS_STORE, META_STORE], 'readwrite');
      txn.objectStore(META_STORE).delete(metaKey);
      txn.objectStore(CHUNKS_STORE).delete(range);
      await this.txnDone_(txn);
    } catch (e) {
      // Fail open.
    }
  }

  /**
   * One deferred sweep per manager lifetime, off the boot path. In ONE
   * readwrite transaction (serializable against writers): delete metas
   * older than maxAge, chunks of non-live generations older than the
   * orphan age (crashed/raced writes — their key embeds no timestamp, so
   * age rides the gen id's time prefix), and chunks with no meta at all.
   */
  private scheduleSweep_(): void {
    if (this.sweepTimer_ !== null || this.sweepDone_ || this.disposed_) {
      return;
    }
    this.sweepTimer_ = setTimeout(() => {
      this.sweepTimer_ = null;
      this.sweepDone_ = true;
      void this.sweep_();
    }, 60 * 1000);
  }

  private async sweep_(): Promise<void> {
    const db = await this.open_();
    if (db === null || this.disposed_) {
      return;
    }
    try {
      const txn = db.transaction([CHUNKS_STORE, META_STORE], 'readwrite');
      const metaStore = txn.objectStore(META_STORE);
      const chunkStore = txn.objectStore(CHUNKS_STORE);
      const [metaKeys, metaValues, chunkKeys] = await Promise.all([
        this.requestDone_(metaStore.getAllKeys()),
        this.requestDone_(metaStore.getAll()),
        this.requestDone_(chunkStore.getAllKeys())
      ]);
      const now = Date.now();
      /** live meta prefix → its live gen. */
      const live = new Map<string, string>();
      let deletions = 0;
      for (let i = 0; i < metaKeys.length; i++) {
        const meta = metaValues[i] as RootMeta | null;
        if (
          meta === null ||
          typeof meta !== 'object' ||
          meta.formatVersion !== META_FORMAT_VERSION ||
          now - meta.updatedAt > this.maxAgeMs_
        ) {
          metaStore.delete(metaKeys[i] as string);
          deletions++;
        } else {
          live.set(metaKeys[i] as string, meta.gen);
        }
      }
      for (let i = 0; i < chunkKeys.length; i++) {
        const key = chunkKeys[i] as string;
        // key = scopeSeg·SEP·rootSeg·SEP·gen·SEP·index — meta prefix is
        // everything through the second separator.
        const second = key.indexOf(
          ROW_KEY_SEPARATOR,
          key.indexOf(ROW_KEY_SEPARATOR) + 1
        );
        const metaPrefix = key.slice(0, second + 1);
        const rest = key.slice(second + 1);
        const gen = rest.slice(0, rest.indexOf(ROW_KEY_SEPARATOR));
        const liveGen = live.get(metaPrefix);
        if (liveGen === gen) {
          continue;
        }
        // Non-live gen: age from the gen id's time prefix (base36 ms).
        const genTime = parseInt(gen.slice(0, 8), 36);
        if (
          liveGen === undefined ||
          !isFinite(genTime) ||
          now - genTime > this.orphanGenAgeMs_
        ) {
          chunkStore.delete(key);
          deletions++;
        }
      }
      if (deletions === 0) {
        return;
      }
      await this.txnDone_(txn);
    } catch (e) {
      // Best-effort; sweep again next session.
    }
  }

  // ───────────────────────────── lifecycle ───────────────────────────────

  /**
   * Deliberate offline (goOffline/repoInterrupt): a suspended tab's server
   * cache is frozen, so it releases writership (the UA grants the lock to
   * an online tab) and stops flushing. Resume re-queues.
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
          const release = root.releaseLock;
          const active = root.activeFlush;
          if (active !== null) {
            void active.finally(() => {
              if (root.releaseLock === release) {
                release();
                root.releaseLock = null;
              }
            });
          } else {
            release();
            root.releaseLock = null;
          }
        }
      } else {
        void this.acquireWriterLock_(pathString, root);
        if (root.dirty) {
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
      this.chunkBytes_,
      this.workerHashTimeoutMs_,
      this.maxAgeMs_,
      this.orphanGenAgeMs_
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

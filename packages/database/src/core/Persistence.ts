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

import { isIndexedDBAvailable } from '@firebase/util';

import {
  compoundHashFromNodeAsync,
  estimateSerializedNodeSize,
  hashFromNodeAsync
} from './CompoundHash';
import { SeedCompoundHash } from './ServerCacheSeed';
import { Node } from './snap/Node';
import { Path, pathParent } from './util/Path';

/**
 * Client-side persistence of the server cache, in the spirit of the mobile
 * SDKs' setPersistenceEnabled(true): the SDK itself stores what the server
 * sent for each listened root and restores it on the next startup, so a
 * reload serves cached data immediately and revalidates with the server via
 * the hash protocol (see ServerCacheSeed) instead of re-downloading.
 *
 * Web-specific shape: IndexedDB is asynchronous, so unlike Android's blocking
 * SQLite reads the restore is a promise. The Repo HOLDS each persisted root's
 * outbound listen until its restore settles (bounded below), applies the
 * restored tree as server data (raising the cached events immediately —
 * mobile persistence semantics: cached data is shown, then corrected by the
 * server when it differs), and then sends the listen carrying the restored
 * tree's hashes. An unchanged tree costs a hash handshake; a changed one
 * costs range-merge deltas; a cold root costs exactly today's full download.
 *
 * What is persisted, per top-level listened ROOT (a complete, unfiltered
 * listen), as TWO records under adjacent keys:
 *   - the data record: the last server-confirmed tree as exported JSON
 *     (val(true) — priorities preserved), written through debounced on
 *     server overwrites / merges / range merges / listen-completes;
 *   - the hash record: the tree's canonical listen hash and compound hash,
 *     recomputed AFTER each write-through in idle-time slices and stored
 *     separately, so a flush serializes the tree exactly once and the hash
 *     lands as a small follow-up write.
 *
 * Coupling stored hashes to stored trees: every write-through stamps the
 * data record with a manager-wide monotonic `revision`; the async recompute
 * carries the revision it hashed and its result is discarded when a newer
 * write-through superseded it. On restore the hash record is joined only
 * when its revision matches the data record's — a mismatch (recompute
 * pending at shutdown) restores WITHOUT hashes: the data still paints, the
 * listen just goes out hashless, exactly a cold load for that root.
 *
 * Storage: one IndexedDB database ('firebase-database-persistence'), one
 * object store, keyed "<repo prefix>|<path>" (data) and
 * "<repo prefix>|<path>#hash" (hashes; '#' cannot appear in a path segment).
 * All storage failures degrade to cold loads; nothing here may ever break
 * the live connection.
 */

const STORE = 'firebase-server-cache';

/**
 * Records older than this are dropped (staleness makes a full download
 * likely anyway; bounded retention caps disk use).
 */
export const PERSISTENCE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

/**
 * How long after the last server update a root's write-through runs. The
 * flush serializes the whole root (val(true) + the structured clone into
 * IndexedDB), so it is deliberately coarse for very large roots.
 * @internal
 */
export const PERSISTENCE_WRITE_DEBOUNCE_MS = 10000;

/**
 * A restore that hasn't settled by this budget attaches the listen unseeded
 * — persistence may add at most this much latency to a root's FIRST listen,
 * and only when IndexedDB is pathologically slow.
 */
export const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;

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

/** The hash follow-up record stored next to a data record. */
interface PersistedHashRecord {
  hash: string;
  compoundHash: SeedCompoundHash;
  updatedAt: number;
  revision: string;
}

/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
export const persistenceStats: {
  restoredRoots: string[];
  restoreMisses: string[];
  writeThroughs: number;
  hashRecomputes: number;
  evictions: number;
  storageFailures: number;
} = {
  restoredRoots: [],
  restoreMisses: [],
  writeThroughs: 0,
  hashRecomputes: 0,
  evictions: 0,
  storageFailures: 0
};

const HASH_KEY_SUFFIX = '#hash';

/**
 * One PersistenceManager per Repo. `prefix` namespaces records so multiple
 * databases/apps sharing the page don't collide. `idbFactory` exists for
 * tests (Node has no IndexedDB); production uses the global.
 */
export class PersistenceManager {
  private db_: Promise<IDBDatabase | null> | null = null;
  /**
   * Roots that flow through persistence (complete default listens).
   */
  private trackedRoots_ = new Set<string>();
  /**
   * Latest server tree per root. Revisions come from a single manager-wide
   * counter, so no revision is ever reissued — an in-flight hash recompute
   * can never collide with a tree that arrived after its root was evicted
   * and re-tracked.
   */
  private latest_ = new Map<string, { node: Node; revision: string }>();
  /**
   * Distinguishes this manager's write tokens from every other tab's and
   * session's — numeric counters restart at zero on reload, which let a new
   * data write pair up with a surviving old hash sidecar.
   */
  private instanceId_ = Math.random().toString(36).slice(2, 10);
  private writeCounter_ = 0;
  private writeTimers_ = new Map<string, ReturnType<typeof setTimeout>>();
  /** In-flight storage operations per root (see enqueue_). */
  private queues_ = new Map<string, Promise<void>>();
  private disposed_ = false;

  constructor(
    private prefix_: string,
    private idbFactory_: IDBFactory | null = isIndexedDBAvailable()
      ? indexedDB
      : null,
    private maxRootBytes_: number = Infinity
  ) {}

  /**
   * A replacement manager for a different key prefix — used when emulator
   * configuration changes the RepoInfo after persistence was enabled but
   * before the repo started (no queues or tracked roots exist yet).
   */
  rebindTo(prefix: string): PersistenceManager {
    this.dispose();
    return new PersistenceManager(prefix, this.idbFactory_, this.maxRootBytes_);
  }

  /**
   * Marks a root as persistence-managed; write-throughs only run for
   * tracked roots (and their descendants' updates).
   */
  track(pathString: string): void {
    this.trackedRoots_.add(pathString);
  }

  /**
   * The root's last listen stopped: flush any pending write-through so
   * IndexedDB holds the final tree for the next session, then release the
   * in-memory copy — only live listens need it.
   */
  untrack(pathString: string): void {
    if (!this.trackedRoots_.has(pathString)) {
      return;
    }
    this.trackedRoots_.delete(pathString);
    // Release the tree only after the final flush settles (flush_ reads
    // latest_ when it runs). Skip the delete if the root was re-tracked
    // meanwhile — the new listen owns the entry now.
    void this.flushNow(pathString).then(() => {
      if (!this.trackedRoots_.has(pathString)) {
        this.latest_.delete(pathString);
      }
    });
  }

  /**
   * The nearest tracked root at-or-above `pathString`, or null.
   */
  trackedRootFor(pathString: string): string | null {
    for (const root of this.trackedRoots_) {
      if (
        pathString === root ||
        (pathString.length > root.length &&
          pathString.startsWith(root === '/' ? root : root + '/'))
      ) {
        return root;
      }
    }
    return null;
  }

  private open_(): Promise<IDBDatabase | null> {
    if (this.db_) {
      return this.db_;
    }
    this.db_ = this.openAtVersion_(undefined).then(db => {
      if (db === null) {
        return null;
      }
      if (db.objectStoreNames.contains(STORE)) {
        return db;
      }
      // The database exists but lacks the store — e.g. it was created by a
      // versionless open from other tooling. Object stores can only be added
      // in a version-change transaction, so reopen one version up.
      const nextVersion = db.version + 1;
      db.close();
      return this.openAtVersion_(nextVersion).then(upgraded => {
        if (upgraded !== null && !upgraded.objectStoreNames.contains(STORE)) {
          persistenceStats.storageFailures++;
          upgraded.close();
          return null;
        }
        return upgraded;
      });
    });
    // One sweep per manager, off the first open: restore() only expires the
    // exact keys it is asked for, so without this, roots that are never
    // listened to again would sit in IndexedDB forever.
    void this.db_.then(db => {
      if (db !== null) {
        this.sweepExpired_(db);
      }
    });
    return this.db_;
  }

  /**
   * Deletes this manager's expired records (see PERSISTENCE_MAX_AGE_MS) by
   * cursor walk. Both record kinds carry `updatedAt`, so data and hash
   * records expire together. Best-effort: any failure leaves the records
   * for the next session's sweep.
   */
  private sweepExpired_(db: IDBDatabase): void {
    const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
    const prefix = this.key_('');
    try {
      const store = db.transaction(STORE, 'readwrite').objectStore(STORE);
      const req = store.openCursor();
      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) {
          return;
        }
        const key = cursor.key;
        const record = cursor.value as { updatedAt?: unknown } | undefined;
        if (
          typeof key === 'string' &&
          key.startsWith(prefix) &&
          (typeof record?.updatedAt !== 'number' || record.updatedAt < cutoff)
        ) {
          persistenceStats.evictions++;
          cursor.delete();
        }
        cursor.continue();
      };
    } catch (e) {
      // Sweeping is opportunistic; never let it surface.
    }
  }

  private openAtVersion_(
    version: number | undefined
  ): Promise<IDBDatabase | null> {
    return new Promise(resolve => {
      if (!this.idbFactory_) {
        resolve(null);
        return;
      }
      try {
        const req =
          version === undefined
            ? this.idbFactory_.open('firebase-database-persistence')
            : this.idbFactory_.open('firebase-database-persistence', version);
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains(STORE)) {
            db.createObjectStore(STORE);
          }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => {
          persistenceStats.storageFailures++;
          resolve(null);
        };
        req.onblocked = () => resolve(null);
      } catch (e) {
        persistenceStats.storageFailures++;
        resolve(null);
      }
    });
  }

  private key_(pathString: string): string {
    return this.prefix_ + '|' + pathString;
  }

  /**
   * Runs `body` against the object store in a transaction of the given mode
   * and resolves with what `body` chose to deliver (via its `done` callback)
   * once the transaction completes. Every failure path — no database, a
   * throwing store call, an aborted transaction — resolves `fallback` and
   * counts one storageFailure (except when IndexedDB is absent altogether,
   * which is a supported cold-load configuration, not a failure).
   */
  private withStore_<T>(
    mode: IDBTransactionMode,
    fallback: T,
    body: (store: IDBObjectStore, done: (value: T) => void) => void
  ): Promise<T> {
    return this.open_().then(
      db =>
        new Promise<T>(resolve => {
          if (!db) {
            resolve(fallback);
            return;
          }
          try {
            const tx = db.transaction(STORE, mode);
            let value = fallback;
            body(tx.objectStore(STORE), v => {
              value = v;
            });
            tx.oncomplete = () => resolve(value);
            tx.onabort = tx.onerror = () => {
              persistenceStats.storageFailures++;
              resolve(fallback);
            };
          } catch (e) {
            persistenceStats.storageFailures++;
            resolve(fallback);
          }
        })
    );
  }

  /**
   * Reads a root's data record and joins its hash record when the revisions
   * match, in one readonly transaction. Expired records resolve null (and
   * are deleted best-effort).
   */
  private readRecord_(pathString: string): Promise<PersistedRecord | null> {
    return this.withStore_<PersistedRecord | null>(
      'readonly',
      null,
      (store, done) => {
        const dataReq = store.get(this.key_(pathString));
        const hashReq = store.get(this.key_(pathString) + HASH_KEY_SUFFIX);
        // Same store, same transaction: requests complete in issue order, so
        // when hashReq's success fires, dataReq.result is safe to read.
        // (Reading a request's result before IT completes throws.)
        hashReq.onsuccess = () => {
          const record = dataReq.result as PersistedRecord | undefined;
          if (!record) {
            done(null);
            return;
          }
          const hashes = hashReq.result as PersistedHashRecord | undefined;
          if (hashes && hashes.revision === record.revision) {
            done({
              json: record.json,
              hash: hashes.hash,
              compoundHash: hashes.compoundHash,
              updatedAt: record.updatedAt,
              revision: record.revision
            });
          } else {
            done(record);
          }
        };
      }
    ).then(record => {
      if (!record) {
        return null;
      }
      if (Date.now() - record.updatedAt > PERSISTENCE_MAX_AGE_MS) {
        persistenceStats.evictions++;
        void this.deleteRecord_(pathString);
        return null;
      }
      return record;
    });
  }

  private deleteRecord_(pathString: string): Promise<void> {
    return this.withStore_<void>('readwrite', undefined, store => {
      store.delete(this.key_(pathString));
      store.delete(this.key_(pathString) + HASH_KEY_SUFFIX);
    });
  }

  /**
   * Restores the persisted record for a root. Resolves null on miss, expiry,
   * storage failure, or timeout — the caller then attaches unseeded.
   */
  restore(pathString: string): Promise<PersistedRecord | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    return this.raceRestoreTimeout_(this.readRecord_(pathString)).then(
      record => {
        if (record) {
          persistenceStats.restoredRoots.push(pathString);
        } else {
          persistenceStats.restoreMisses.push(pathString);
        }
        return record;
      }
    );
  }

  /**
   * The boot-peek read (see getPersistedValue): resolves the record of the
   * DEEPEST persisted ancestor of `pathString` (or of the path itself),
   * fetching the whole ancestor chain in one readonly transaction. Expired
   * ancestors are skipped (and deleted best-effort). Does not touch the
   * restore counters — a peek is not a listen restore.
   */
  restoreNearest(
    pathString: string
  ): Promise<{ root: string; record: PersistedRecord } | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    // Deepest first: the path itself, then each ancestor up to the root.
    const candidates: string[] = [];
    let path: Path | null = new Path(pathString);
    while (path !== null) {
      candidates.push(path.toString());
      path = pathParent(path);
    }
    const read = this.withStore_<Array<PersistedRecord | undefined>>(
      'readonly',
      [],
      (store, done) => {
        const results: Array<PersistedRecord | undefined> = new Array(
          candidates.length
        );
        let remaining = candidates.length;
        candidates.forEach((candidate, i) => {
          const req = store.get(this.key_(candidate));
          req.onsuccess = () => {
            results[i] = req.result as PersistedRecord | undefined;
            if (--remaining === 0) {
              done(results);
            }
          };
        });
      }
    ).then(results => {
      const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
      for (let i = 0; i < results.length; i++) {
        const record = results[i];
        if (!record) {
          continue;
        }
        if (record.updatedAt < cutoff) {
          persistenceStats.evictions++;
          void this.deleteRecord_(candidates[i]);
          continue;
        }
        return { root: candidates[i], record };
      }
      return null;
    });
    return this.raceRestoreTimeout_(read);
  }

  /**
   * Bounds a read by PERSISTENCE_RESTORE_TIMEOUT_MS, clearing the timer as
   * soon as the read settles first (the common case — otherwise every
   * restore would pin its Repo in memory for the full budget).
   */
  private raceRestoreTimeout_<T>(read: Promise<T | null>): Promise<T | null> {
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<null>(resolve => {
      timer = setTimeout(() => resolve(null), PERSISTENCE_RESTORE_TIMEOUT_MS);
    });
    return Promise.race([read, timeout]).then(result => {
      clearTimeout(timer);
      return result;
    });
  }

  /**
   * Write-through: the server confirmed `node` as the state of the tracked
   * root `path`. Debounced per root; hashes recompute afterwards in idle
   * slices against the same revision.
   */
  serverCacheUpdated(path: Path, node: Node): void {
    if (this.disposed_) {
      return;
    }
    const pathString = path.toString();
    if (!this.trackedRoots_.has(pathString)) {
      return;
    }
    this.latest_.set(pathString, {
      node,
      revision: this.instanceId_ + '-' + (++this.writeCounter_).toString(36)
    });
    // Trailing throttle, NOT a resetting debounce: the timer set by the first
    // update in a burst survives later updates, so a root that churns faster
    // than the interval (a chat streaming, an editing session) still flushes
    // every interval instead of never. The flush reads latest_ when it runs,
    // so it always writes the newest tree.
    if (!this.writeTimers_.has(pathString)) {
      this.writeTimers_.set(
        pathString,
        setTimeout(() => {
          this.writeTimers_.delete(pathString);
          this.enqueue_(pathString, () => this.flush_(pathString));
        }, PERSISTENCE_WRITE_DEBOUNCE_MS)
      );
    }
  }

  /**
   * The viewer lost access to a root: a cached copy must not outlive the
   * access that produced it.
   */
  evict(path: Path): void {
    const pathString = path.toString();
    this.latest_.delete(pathString);
    const timer = this.writeTimers_.get(pathString);
    if (timer) {
      clearTimeout(timer);
      this.writeTimers_.delete(pathString);
    }
    persistenceStats.evictions++;
    // Through the queue: a flush already running for this root finishes its
    // writes first, then the delete removes them — never the reverse.
    this.enqueue_(pathString, () => this.deleteRecord_(pathString));
  }

  dispose(): void {
    this.disposed_ = true;
    for (const timer of this.writeTimers_.values()) {
      clearTimeout(timer);
    }
    this.writeTimers_.clear();
    this.latest_.clear();
  }

  /**
   * Test seam: forces a pending debounced flush to run now.
   */
  flushNow(pathString: string): Promise<void> {
    const timer = this.writeTimers_.get(pathString);
    if (timer) {
      clearTimeout(timer);
      this.writeTimers_.delete(pathString);
    }
    return this.enqueue_(pathString, () => this.flush_(pathString));
  }

  /**
   * Chains an operation onto the root's queue. One writer per root at a
   * time: a flush's data and hash records land as a couple before the next
   * flush or delete for that root starts, which is the whole storage
   * consistency argument — no cross-operation races to reason about.
   */
  private enqueue_(pathString: string, op: () => Promise<void>): Promise<void> {
    const next = (this.queues_.get(pathString) ?? Promise.resolve()).then(op);
    // Settle-or-not, the chain must continue; storage failures are already
    // absorbed (and counted) inside withStore_.
    const settled = next.catch(() => {});
    this.queues_.set(pathString, settled);
    void settled.then(() => {
      if (this.queues_.get(pathString) === settled) {
        this.queues_.delete(pathString);
      }
    });
    return next;
  }

  private flush_(pathString: string): Promise<void> {
    const entry = this.latest_.get(pathString);
    if (!entry || this.disposed_) {
      return Promise.resolve();
    }
    const { node, revision } = entry;
    if (
      this.maxRootBytes_ !== Infinity &&
      estimateSerializedNodeSize(node) > this.maxRootBytes_
    ) {
      // Persisting costs a transient serialize + structured-clone + restore
      // parse of the whole root — multiples of its size in peak memory. On
      // constrained devices that is a crash, so oversized roots simply stay
      // unpersisted (their boot is a normal cold load).
      return Promise.resolve();
    }
    persistenceStats.writeThroughs++;
    // The tree is serialized and written exactly once, hashless — a crash
    // before the recompute leaves a restorable tree that seeds without a
    // hash instead of nothing. The hashes follow as a small separate record,
    // and because flushes for a root are serialized (enqueue_), the pair is
    // always coupled: the hash written here describes the data written here,
    // even if newer updates arrived while hashing.
    const record: PersistedRecord = {
      json: node.val(true),
      updatedAt: Date.now(),
      revision
    };
    const put = this.withStore_<void>('readwrite', undefined, store => {
      store.put(record, this.key_(pathString));
      // Atomically invalidate the previous hash sidecar: between this write
      // and the recompute below, the stored state is "data, hashless" — never
      // "new data, old hash".
      store.delete(this.key_(pathString) + HASH_KEY_SUFFIX);
    });
    return put.then(() =>
      Promise.all([
        hashFromNodeAsync(node),
        compoundHashFromNodeAsync(node)
      ]).then(([hash, compoundHash]) => {
        if (this.disposed_) {
          return;
        }
        persistenceStats.hashRecomputes++;
        const hashRecord: PersistedHashRecord = {
          hash,
          compoundHash: {
            hashes: compoundHash.hashes,
            posts: compoundHash.posts
          },
          updatedAt: record.updatedAt,
          revision
        };
        return this.withStore_<void>('readwrite', undefined, store => {
          // Another tab may have replaced the data record while we hashed;
          // only attach the hash if the record still carries OUR token (the
          // read and the put share one transaction, so this is atomic).
          const dataReq = store.get(this.key_(pathString));
          dataReq.onsuccess = () => {
            const current = dataReq.result as PersistedRecord | undefined;
            if (current && current.revision === revision) {
              store.put(hashRecord, this.key_(pathString) + HASH_KEY_SUFFIX);
            }
          };
        });
      })
    );
  }
}

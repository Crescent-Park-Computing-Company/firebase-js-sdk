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

import { compoundHashFromNodeAsync } from './CompoundHash';
import { SeedCompoundHash } from './ServerCacheSeed';
import { Node } from './snap/Node';
import { Path } from './util/Path';

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
 * listen):
 *   - the last server-confirmed tree, as exported JSON (val(true) — priorities
 *     preserved), written through debounced on server overwrites / merges /
 *     range merges / listen-completes;
 *   - the tree's canonical listen hash and compound hash, recomputed AFTER
 *     each write-through in idle-time slices (compoundHashFromNodeAsync) and
 *     stored alongside, so the next startup seeds in O(1) with no tree walk.
 *
 * Coupling stored hashes to stored trees: every write-through bumps a
 * per-root `revision`; the async recompute carries the revision it hashed
 * and its result is discarded when a newer write-through superseded it. A
 * record whose hashes are missing (recompute pending at shutdown) restores
 * WITHOUT hashes — the data still paints, the listen just goes out
 * hashless, exactly a cold load for that root.
 *
 * Storage: one IndexedDB database ('firebase-database-persistence'), one
 * object store, keyed "<repo prefix>|<path>". All storage failures degrade
 * to cold loads; nothing here may ever break the live connection.
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
  revision: number;
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
  staleHashDiscards: number;
  evictions: number;
  storageFailures: number;
} = {
  restoredRoots: [],
  restoreMisses: [],
  writeThroughs: 0,
  hashRecomputes: 0,
  staleHashDiscards: 0,
  evictions: 0,
  storageFailures: 0
};

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
   * Latest server tree per root; revision couples hashes to trees.
   */
  private latest_ = new Map<string, { node: Node; revision: number }>();
  private writeTimers_ = new Map<string, ReturnType<typeof setTimeout>>();
  private disposed_ = false;

  constructor(
    private prefix_: string,
    private idbFactory_: IDBFactory | null = typeof indexedDB !== 'undefined'
      ? indexedDB
      : null
  ) {}

  /**
   * Marks a root as persistence-managed; write-throughs only run for
   * tracked roots (and their descendants' updates).
   */
  track(pathString: string): void {
    this.trackedRoots_.add(pathString);
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
    return this.db_;
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

  private idbGet_(pathString: string): Promise<PersistedRecord | null> {
    return this.open_().then(
      db =>
        new Promise<PersistedRecord | null>(resolve => {
          if (!db) {
            resolve(null);
            return;
          }
          try {
            const tx = db.transaction(STORE, 'readonly');
            const req = tx.objectStore(STORE).get(this.key_(pathString));
            req.onsuccess = () =>
              resolve((req.result as PersistedRecord) ?? null);
            req.onerror = () => resolve(null);
          } catch (e) {
            resolve(null);
          }
        })
    );
  }

  private idbPut_(pathString: string, record: PersistedRecord): Promise<void> {
    return this.open_().then(
      db =>
        new Promise<void>(resolve => {
          if (!db) {
            resolve();
            return;
          }
          try {
            const tx = db.transaction(STORE, 'readwrite');
            tx.objectStore(STORE).put(record, this.key_(pathString));
            tx.oncomplete = () => resolve();
            tx.onabort = tx.onerror = () => {
              persistenceStats.storageFailures++;
              resolve();
            };
          } catch (e) {
            persistenceStats.storageFailures++;
            resolve();
          }
        })
    );
  }

  private idbDelete_(pathString: string): Promise<void> {
    return this.open_().then(
      db =>
        new Promise<void>(resolve => {
          if (!db) {
            resolve();
            return;
          }
          try {
            const tx = db.transaction(STORE, 'readwrite');
            tx.objectStore(STORE).delete(this.key_(pathString));
            tx.oncomplete = () => resolve();
            tx.onabort = tx.onerror = () => resolve();
          } catch (e) {
            resolve();
          }
        })
    );
  }

  /**
   * Restores the persisted record for a root. Resolves null on miss, expiry,
   * storage failure, or timeout — the caller then attaches unseeded.
   */
  restore(pathString: string): Promise<PersistedRecord | null> {
    if (this.disposed_) {
      return Promise.resolve(null);
    }
    const read = this.idbGet_(pathString).then(record => {
      if (!record) {
        return null;
      }
      if (Date.now() - record.updatedAt > PERSISTENCE_MAX_AGE_MS) {
        persistenceStats.evictions++;
        void this.idbDelete_(pathString);
        return null;
      }
      return record;
    });
    const timeout = new Promise<null>(resolve =>
      setTimeout(() => resolve(null), PERSISTENCE_RESTORE_TIMEOUT_MS)
    );
    return Promise.race([read, timeout]).then(record => {
      if (record) {
        persistenceStats.restoredRoots.push(pathString);
      } else {
        persistenceStats.restoreMisses.push(pathString);
      }
      return record;
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
    const prev = this.latest_.get(pathString);
    const revision = (prev ? prev.revision : 0) + 1;
    this.latest_.set(pathString, { node, revision });
    const existing = this.writeTimers_.get(pathString);
    if (existing) {
      clearTimeout(existing);
    }
    this.writeTimers_.set(
      pathString,
      setTimeout(() => {
        this.writeTimers_.delete(pathString);
        this.flush_(pathString);
      }, PERSISTENCE_WRITE_DEBOUNCE_MS)
    );
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
    void this.idbDelete_(pathString);
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
    return this.flush_(pathString);
  }

  private flush_(pathString: string): Promise<void> {
    const entry = this.latest_.get(pathString);
    if (!entry || this.disposed_) {
      return Promise.resolve();
    }
    const { node, revision } = entry;
    persistenceStats.writeThroughs++;
    // Data first, hashless: a crash before the hash recompute leaves a
    // restorable tree that seeds without a hash instead of nothing.
    const record: PersistedRecord = {
      json: node.val(true),
      updatedAt: Date.now(),
      revision
    };
    return this.idbPut_(pathString, record).then(() =>
      compoundHashFromNodeAsync(node).then(compoundHash => {
        const current = this.latest_.get(pathString);
        if (!current || current.revision !== revision || this.disposed_) {
          // A newer server update superseded this tree while it hashed; its
          // own flush persists fresh hashes. Discarding keeps stored hashes
          // coupled to stored trees.
          persistenceStats.staleHashDiscards++;
          return;
        }
        persistenceStats.hashRecomputes++;
        return this.idbPut_(pathString, {
          json: record.json,
          hash: node.hash(),
          compoundHash: {
            hashes: compoundHash.hashes,
            posts: compoundHash.posts
          },
          updatedAt: record.updatedAt,
          revision
        });
      })
    );
  }
}

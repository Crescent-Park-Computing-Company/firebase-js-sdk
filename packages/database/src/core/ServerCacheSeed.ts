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

import { compoundHashFromNode } from './CompoundHash';
import { Node } from './snap/Node';
import { nodeFromJSON } from './snap/nodeFromJSON';

/**
 * Server-cache seeding: apps that persist a copy of their data (e.g. in
 * IndexedDB) can install it as the SDK's initial server cache BEFORE the
 * listener for that path attaches. The first listen then carries the seeded
 * tree's hash (and, when provided, its compound hash) instead of the
 * empty-node hash:
 *
 * - If the server's data still matches, the listen completes with no data
 *   download at all.
 * - If it doesn't and a compound hash was seeded, the server responds with
 *   range merges covering only the parts that changed.
 * - Otherwise the server sends the full tree, exactly as an unseeded listen
 *   would.
 *
 * A seed is installed as an INCOMPLETE server cache, so no value event is
 * raised from it: only a server message (a listen 'ok', a range merge, or a
 * full overwrite) promotes it to complete, server-certified state. Wrong or
 * stale seeded data therefore costs at most a missed hash — it is never
 * surfaced to the app as current data.
 *
 * The optional precomputed hashes exist so callers can compute them off the
 * main thread (e.g. in a worker, via computeCanonicalHash /
 * computeCompoundHash) and stamp them at boot in O(1). Both must be computed
 * from exactly the seeded JSON: the server certifies whatever the listen
 * carries, and on a match the seeded tree is promoted as server state.
 */

/**
 * A compound hash in wire shape: `ch: { hs: hashes, ps: posts }`.
 * @internal
 */
export interface SeedCompoundHash {
  hashes: string[];
  posts: string[];
}

/**
 * The hash accessor a listen carries. The optional compoundHash method
 * returns the compound hash of the seeded server cache when one is installed
 * for the listened path, letting it thread through the existing
 * listen-provider chain without changing any call sites.
 */
export interface ListenHashFn {
  (): string;
  compoundHash?: () => SeedCompoundHash | undefined;
}

interface ServerCacheSeed {
  json: unknown;
  hash?: string;
  compoundHash?: SeedCompoundHash;
}

const seeds = new Map<string, ServerCacheSeed>();

function normalizeSeedPath(path: string): string {
  const trimmed = String(path).replace(/^\/+|\/+$/g, '');
  return trimmed === '' ? '/' : '/' + trimmed;
}

/**
 * Registers cached JSON as the initial server cache for `path`. Must be
 * called before the listener for that exact path attaches — the seed is
 * consumed (once) at listener registration, and only by a default (complete,
 * unfiltered) query: a filtered query's listen hash is computed over the
 * filtered subset, which raw cached JSON is not.
 *
 * @param path - Absolute database path the JSON was cached for.
 * @param json - The cached value. null/undefined clears nothing and seeds
 * nothing (an empty tree's hash is what an unseeded listen sends anyway).
 * @param hash - Optional precomputed canonical hash of `json` (the exact
 * value computeCanonicalHash returns for it).
 * @param compoundHash - Optional precomputed compound hash of `json` (the
 * exact value computeCompoundHash returns for it).
 * @internal
 */
export function seedServerCache(
  path: string,
  json: unknown,
  hash?: string,
  compoundHash?: SeedCompoundHash
): void {
  if (json === null || json === undefined) {
    return;
  }
  seeds.set(normalizeSeedPath(path), { json, hash, compoundHash });
}

/** Removes all registered seeds. * @internal
 */
export function clearServerCacheSeeds(): void {
  seeds.clear();
}

/**
 * Consumes (at most once) the seed registered for exactly `pathString`.
 * Returns undefined when no seed matches.
 */
export function takeServerCacheSeed(
  pathString: string
): ServerCacheSeed | undefined {
  const key = normalizeSeedPath(pathString);
  const seed = seeds.get(key);
  if (seed !== undefined) {
    seeds.delete(key);
  }
  return seed;
}

/**
 * Builds the node for a seed, stamping the precomputed canonical hash into
 * the node's lazy-hash slot (so hash() returns it without an O(tree) walk)
 * and attaching the precomputed compound hash for the listen to send.
 *
 * An empty tree is returned unstamped: nodeFromJSON maps it to the shared
 * ChildrenNode.EMPTY_NODE singleton, and stamping that would poison every
 * empty node in the app.
 */
export function buildSeedNode(seed: ServerCacheSeed): Node {
  const node = nodeFromJSON(seed.json);
  if (node.isEmpty()) {
    return node;
  }
  if (typeof seed.hash === 'string' && seed.hash.length > 0) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const stampable = node as any;
    if (stampable.lazyHash_ === null) {
      stampable.lazyHash_ = seed.hash;
    }
  }
  const compoundHash = seed.compoundHash;
  if (
    compoundHash &&
    Array.isArray(compoundHash.hashes) &&
    Array.isArray(compoundHash.posts) &&
    compoundHash.hashes.length === compoundHash.posts.length + 1
  ) {
    setNodeCompoundHash(node, compoundHash);
  }
  return node;
}

/**
 * The compound hash rides on the seeded node itself: once a server update
 * replaces the cached node the stamp is gone, so re-listens after real data
 * arrived send only the simple hash (which is then correct by construction).
 */
const nodeCompoundHashes = new WeakMap<object, SeedCompoundHash>();

export function setNodeCompoundHash(
  node: Node,
  compoundHash: SeedCompoundHash
): void {
  nodeCompoundHashes.set(node, compoundHash);
}

export function getNodeCompoundHash(node: Node): SeedCompoundHash | undefined {
  return nodeCompoundHashes.get(node);
}

/**
 * The canonical listen hash of a JSON value — exactly what an unseeded
 * client would send for this tree. Exposed so apps can precompute seeds'
 * hashes off the main thread with the SDK's own canonicalization.
 * @internal
 */
export function computeCanonicalHash(json: unknown): string {
  return nodeFromJSON(json).hash();
}

/**
 * The compound hash of a JSON value, in wire shape. Exposed so apps can
 * precompute seeds' compound hashes off the main thread with the SDK's own
 * canonicalization.
 * @internal
 */
export function computeCompoundHash(json: unknown): SeedCompoundHash {
  const compoundHash = compoundHashFromNode(nodeFromJSON(json));
  return { hashes: compoundHash.hashes, posts: compoundHash.posts };
}

/**
 * Counters for observing seeding effectiveness (listens sent with a real
 * hash, server-side hash matches, range merges received, wire bytes).
 * @internal
 */
export const serverCacheSeedStats: {
  listensSentWithHash: number;
  listensSentWithCompoundHash: number;
  listenOks: number;
  hashMatches: number;
  rangeMergesReceived: number;
  seededPaths: string[];
  bytesReceived: number;
  dataPushesByPath: Record<string, number>;
} = {
  listensSentWithHash: 0,
  listensSentWithCompoundHash: 0,
  listenOks: 0,
  hashMatches: 0,
  rangeMergesReceived: 0,
  seededPaths: [],
  bytesReceived: 0,
  dataPushesByPath: {}
};

/** Normalizes a wire path for the per-path push counters. */
export function normalizeStatsPath(path: string): string {
  return normalizeSeedPath(path);
}

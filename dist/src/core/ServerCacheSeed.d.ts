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
import { Node } from './snap/Node';
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
 * Seeds are scoped to one Database instance (one Repo): each Repo owns a
 * ServerCacheSeedStore, and its SyncTree consumes from that store only —
 * two instances listening to the same path never steal each other's seeds.
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
export interface ServerCacheSeed {
    json: unknown;
    hash?: string;
    compoundHash?: SeedCompoundHash;
}
/**
 * The seeds registered for one Repo, keyed by canonical path string
 * (Path.toString() — the same canonicalization the consumer uses, so a seed
 * for 'a//b/' and a listen at '/a/b' cannot drift apart).
 */
export declare class ServerCacheSeedStore {
    private seeds_;
    set(path: string, json: unknown, hash?: string, compoundHash?: SeedCompoundHash): void;
    /**
     * Consumes (at most once) the seed registered for exactly `pathString`.
     * Returns undefined when no seed matches.
     */
    take(pathString: string): ServerCacheSeed | undefined;
    clear(): void;
}
/**
 * Builds the node for a seed, stamping the precomputed hashes (see
 * stampSeedHashes).
 */
export declare function buildSeedNode(seed: ServerCacheSeed): Node;
/**
 * Stamps a precomputed canonical hash into the node's lazy-hash slot (so
 * hash() returns it without an O(tree) walk) and attaches the precomputed
 * compound hash for the listen to send. Both must describe exactly this
 * tree — the server certifies whatever the listen carries.
 *
 * An empty tree is returned unstamped: an empty node is the shared
 * ChildrenNode.EMPTY_NODE singleton, and stamping that would poison every
 * empty node in the app.
 */
export declare function stampSeedHashes(node: Node, hash?: string, compoundHash?: SeedCompoundHash): Node;
export declare function getNodeCompoundHash(node: Node): SeedCompoundHash | undefined;
/**
 * The persisted canonical hash associated with a seeded node. This rides in
 * a WeakMap instead of being stamped into every subtree by node.hash(): a
 * compound-hash-only seed deliberately stores the empty simple hash, letting
 * the server validate its ranges without a full-tree hash pass that would
 * permanently retain one SHA string per node.
 */
export declare function getNodeCanonicalHash(node: Node): string | undefined;
/**
 * The canonical listen hash of a JSON value — exactly what an unseeded
 * client would send for this tree. Exposed so apps can precompute seeds'
 * hashes off the main thread with the SDK's own canonicalization.
 * @internal
 */
export declare function computeCanonicalHash(json: unknown): string;
/**
 * The compound hash of a JSON value, in wire shape. Exposed so apps can
 * precompute seeds' compound hashes off the main thread with the SDK's own
 * canonicalization.
 * @internal
 */
export declare function computeCompoundHash(json: unknown): SeedCompoundHash;
/**
 * Counters for observing seeding effectiveness (listens sent with a real
 * hash, server-side hash matches, range merges received, wire bytes).
 * @internal
 */
export declare const serverCacheSeedStats: {
    listensSentWithHash: number;
    listensSentWithCompoundHash: number;
    listenOks: number;
    hashMatches: number;
    rangeMergesReceived: number;
    seededPaths: string[];
    bytesReceived: number;
};

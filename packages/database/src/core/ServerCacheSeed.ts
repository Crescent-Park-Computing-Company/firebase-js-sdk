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
 * Internal listen-hash protocol types shared by persistence, SyncTree, and
 * PersistentConnection. Application-level manual seeding was removed; the SDK
 * persistence manager is the only source of restored server cache state.
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
export function stampSeedHashes(
  node: Node,
  hash?: string,
  compoundHash?: SeedCompoundHash
): Node {
  if (node.isEmpty()) {
    return node;
  }
  if (typeof hash === 'string') {
    nodeCanonicalHashes.set(node, hash);
    if (hash.length > 0) {
      node.stampLazyHash(hash);
    }
  }
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
const nodeCanonicalHashes = new WeakMap<object, string>();

function setNodeCompoundHash(node: Node, compoundHash: SeedCompoundHash): void {
  nodeCompoundHashes.set(node, compoundHash);
}

export function getNodeCompoundHash(node: Node): SeedCompoundHash | undefined {
  return nodeCompoundHashes.get(node);
}

/**
 * The persisted canonical hash associated with a seeded node. This rides in
 * a WeakMap instead of being stamped into every subtree by node.hash(): a
 * compound-hash-only seed deliberately stores the empty simple hash, letting
 * the server validate its ranges without a full-tree hash pass that would
 * permanently retain one SHA string per node.
 */
export function getNodeCanonicalHash(node: Node): string | undefined {
  return nodeCanonicalHashes.get(node);
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
} = {
  listensSentWithHash: 0,
  listensSentWithCompoundHash: 0,
  listenOks: 0,
  hashMatches: 0,
  rangeMergesReceived: 0,
  seededPaths: [],
  bytesReceived: 0
};

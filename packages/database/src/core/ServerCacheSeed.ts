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
 * The restored plain tree a seeded node was decoded from, when the manifest
 * proved it priority-free — for such a tree, `val()` output is structurally
 * identical to the stored input, so the first snapshot can hand the
 * application the RESTORED OBJECT BY REFERENCE instead of walking the Node
 * tree and materializing a third full copy of the data (the SDK's val() has
 * no memoization of its own). Rides in a WeakMap keyed by the seeded root
 * node: the first server change replaces the root node instance, after
 * which val() naturally materializes from the updated Nodes.
 */
const nodeSeedValues = new WeakMap<object, unknown>();

export function stampSeedValue(node: Node, value: unknown): void {
  nodeSeedValues.set(node, value);
}

export function getNodeSeedValue(node: Node): unknown | undefined {
  return nodeSeedValues.get(node);
}

/**
 * Hashes for a listen that is about to be sent for `pathString` — the
 * manifest-first boot path: the listen goes out BEFORE the restored tree
 * exists in SyncTree, so the hashes cannot ride on the cached node yet.
 * SyncTree's hashFn consults this registry first; the entry is cleared when
 * the restore settles (either the seeded node then carries the hashes, or
 * the restore failed and the next listen must not reuse them).
 *
 * Keyed by the repo-relative listened path. Single-repo keying is safe: two
 * repos listening to the same path string would only ever stamp equivalent
 * hashes for their own stores, and the entry lives for one boot window.
 */
const pendingListenHashes = new Map<
  string,
  { hash: string; compoundHash: SeedCompoundHash }
>();

export function stampNextListenHashes(
  pathString: string,
  hash: string,
  compoundHash: SeedCompoundHash
): void {
  pendingListenHashes.set(pathString, { hash, compoundHash });
}

export function clearNextListenHashes(pathString: string): void {
  pendingListenHashes.delete(pathString);
}

export function getNextListenHashes(
  pathString: string
): { hash: string; compoundHash: SeedCompoundHash } | undefined {
  return pendingListenHashes.get(pathString);
}

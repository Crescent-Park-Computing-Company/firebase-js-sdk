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
 * One-boot materialization handoff. The optimistic pre-auth peek
 * (getPersistedValue) materializes the restored tree to JS objects once;
 * the authenticated listener that adopts the SAME immutable Node then
 * replays it as a child_added burst whose per-child `snapshot.val()` calls
 * would materialize the identical tree a second time — two full JS copies
 * of a large workspace alive at the peak of boot.
 *
 * The peek stamps each materialized value here, keyed by its Node instance;
 * a consumer that OPTS IN via consumePersistedMaterialization() (api/
 * Reference_impl) takes a stamp (get + delete) instead of walking the node.
 * `DataSnapshot.val()` never consumes a stamp — its fresh-objects contract
 * is untouched. Consume-once means only the single designed peek→listener
 * handoff ever receives shared objects (which is the point — the optimistic
 * tree and the live tree then share child identity, so downstream
 * memoization sees unchanged branches as unchanged).
 *
 * Correctness is by construction: a Node is immutable, so a stamp can only
 * ever be returned for exactly the data it was computed from. Any server
 * delta between peek and replay produces a NEW child Node instance, which
 * misses the WeakMap and materializes fresh.
 *
 * Only non-null object values are stamped (a leaf's val() is O(1) already),
 * and never on an empty node — the empty ChildrenNode is a shared singleton
 * and stamping it would leak one boot's subtree to unrelated paths.
 */
const nodeMaterializedValues = new WeakMap<object, object>();

export function stampMaterializedValue(node: Node, value: unknown): void {
  if (value === null || typeof value !== 'object' || node.isEmpty()) {
    return;
  }
  nodeMaterializedValues.set(node, value);
}

export function consumeMaterializedValue(node: Node): object | undefined {
  const value = nodeMaterializedValues.get(node);
  if (value !== undefined) {
    nodeMaterializedValues.delete(node);
  }
  return value;
}

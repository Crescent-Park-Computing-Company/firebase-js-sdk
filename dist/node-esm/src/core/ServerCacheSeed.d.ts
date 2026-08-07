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
/** The hash pair a manifest-first listen can consume before its Node exists. */
export interface PendingListenHashes {
    hash: string;
    compoundHash: SeedCompoundHash;
}
/**
 * Repo-scoped manifest-first hash registry. Different Database instances can
 * listen to the same relative path while holding different caches; keeping
 * this store on Repo prevents one restore from overwriting or clearing
 * another Repo's pending hashes.
 */
export declare class PendingListenHashStore {
    private readonly pending_;
    set(pathString: string, hash: string, compoundHash: SeedCompoundHash): void;
    clear(pathString: string): void;
    get(pathString: string): PendingListenHashes | undefined;
    clearAll(): void;
}

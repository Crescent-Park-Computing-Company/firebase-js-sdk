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

import { KEY_INDEX } from './snap/indexes/KeyIndex';
import { PRIORITY_INDEX } from './snap/indexes/PriorityIndex';
import { LeafNode } from './snap/LeafNode';
import { Node } from './snap/Node';
import {
  hashQuotedString,
  leafHashValueText,
  priorityHashText
} from './snap/snap';
import { nameCompare, sha1 } from './util/util';

/**
 * A compound hash of a node: the node's tree serialized in key order, cut
 * into ranges at the given split posts, with each range hashed separately.
 *
 * Sent on a listen as `ch: { hs: hashes, ps: posts }`, next to the simple
 * hash `h`. When the simple hash misses, the server diffs per range and
 * pushes only the ranges whose hash differs (as range-merge messages)
 * instead of the whole tree.
 *
 * This is a port of the compound hash support in the Android and iOS SDKs
 * (Android: com.google.firebase.database.snapshot.CompoundHash); the wire
 * semantics match them exactly.
 */
export class CompoundHash {
  /**
   * @param posts - The split positions between ranges: the path of the last
   * leaf each range serialized, slash-joined ('/' for the root). Always one
   * shorter than `hashes`.
   * @param hashes - base64(sha1(...)) of each range's serialization, ending
   * with one trailing '' for the open tail range.
   */
  constructor(
    public readonly posts: string[],
    public readonly hashes: string[]
  ) {
    if (posts.length !== hashes.length - 1) {
      throw new Error(
        'Number of posts need to be n-1 for n hashes in CompoundHash'
      );
    }
  }
}

/**
 * The state a split strategy sees after each leaf is serialized.
 */
export interface CompoundHashSplitState {
  /** Length of the current range's serialization so far. */
  hashLength(): number;
  /** Path of the node currently being processed, as key segments. */
  currentPath(): string[];
}

/**
 * Decides after each serialized leaf whether to end the current range.
 */
export type CompoundHashSplitStrategy = (
  state: CompoundHashSplitState
) => boolean;

/**
 * The default strategy: cut a range once its serialized text exceeds
 * max(512, sqrt(estimatedSize * 100)) bytes, so a tree splits into roughly
 * sqrt(size)-sized ranges. Never splits right after a `.priority` leaf: the
 * server treats a priority and the node it belongs to as one unit.
 */
export function simpleSizeSplitStrategy(node: Node): CompoundHashSplitStrategy {
  const estimatedSize = estimateSerializedNodeSize(node);
  const splitThreshold = Math.max(
    512,
    Math.floor(Math.sqrt(estimatedSize * 100))
  );
  return state =>
    state.hashLength() > splitThreshold &&
    state.currentPath()[state.currentPath().length - 1] !== '.priority';
}

/**
 * Computes the compound hash of a node.
 */
export function compoundHashFromNode(
  node: Node,
  splitStrategy?: CompoundHashSplitStrategy
): CompoundHash {
  if (node.isEmpty()) {
    return new CompoundHash([], ['']);
  }
  const strategy = splitStrategy || simpleSizeSplitStrategy(node);
  const builder = new CompoundHashBuilder(strategy);
  const walker = new CompoundHashWalker(node, builder);
  walker.drainUntil(Infinity);
  builder.finishHashing();
  return new CompoundHash(builder.posts, builder.hashes);
}

/**
 * Iterates children in key order with the node's priority interleaved as a
 * `.priority` pseudo-child, matching the serialization the server hashes
 * (Android ChildrenNode.forEachChild(visitor, includePriority = true)): the
 * priority is emitted immediately before the first child key that sorts
 * after '.priority'. A priority that sorts after every child is dropped, as
 * it is on Android and iOS — both ends of the protocol must agree.
 */
export function forEachChildWithPriority(
  node: Node,
  action: (key: string, child: Node, includedInHash: boolean) => void,
  includeTrailingPriority = false
): void {
  if (node.getPriority().isEmpty()) {
    node.forEachChild(KEY_INDEX, (key, child) => action(key, child, true));
    return;
  }
  let passedPriority = false;
  node.forEachChild(KEY_INDEX, (key, child) => {
    if (!passedPriority && nameCompare(key, '.priority') > 0) {
      passedPriority = true;
      action('.priority', node.getPriority(), true);
    }
    action(key, child, true);
  });
  if (!passedPriority && includeTrailingPriority) {
    // Android's compound grammar omits a priority that sorts after every
    // child, but export-format persistence must still serialize it.
    action('.priority', node.getPriority(), false);
  }
}

/**
 * The one definition of the compound-hash traversal, driven as an explicit
 * frame stack — a `child` frame runs builder.startChild, pushes its subtree,
 * and a matching `end` frame runs builder.endChild — so the builder sees the
 * exact call sequence a recursive walk would produce. The synchronous
 * computation drains it in one go; the async one drains it in bounded
 * slices. Either way the resulting hash is identical by construction.
 */
class CompoundHashWalker {
  // Popped last-in-first-out; children are pushed in reverse key order so
  // they pop in key order.
  private stack_: CompoundHashFrame[];

  constructor(node: Node, private builder_: CompoundHashBuilder) {
    this.stack_ = [{ kind: 'node', node }];
  }

  /**
   * Processes frames until the walk completes or `deadline` (an epoch-ms
   * timestamp) passes — always at least one frame, so every slice makes
   * progress no matter how small its budget. Returns true when the walk is
   * complete.
   */
  drainUntil(deadline: number): boolean {
    while (this.stack_.length > 0) {
      this.processFrame_(this.stack_.pop()!);
      if (Date.now() >= deadline) {
        break;
      }
    }
    return this.stack_.length === 0;
  }

  private processFrame_(frame: CompoundHashFrame): void {
    if (frame.kind === 'end') {
      this.builder_.endChild();
      return;
    }
    const current = frame.kind === 'node' ? frame.node : frame.child;
    if (frame.kind === 'child') {
      this.builder_.startChild(frame.key);
      this.stack_.push({ kind: 'end' });
    }
    if (current.isLeafNode()) {
      this.builder_.processLeaf(current as LeafNode);
      // A leaf pushed no 'end' of its own; the pending 'end' (if this was a
      // child frame) already sits on the stack.
      return;
    }
    const children: Array<[string, Node]> = [];
    forEachChildWithPriority(current, (key, child) => {
      children.push([key, child]);
    });
    for (let i = children.length - 1; i >= 0; i--) {
      this.stack_.push({
        kind: 'child',
        key: children[i][0],
        child: children[i][1]
      });
    }
  }
}

type CompoundHashFrame =
  | { kind: 'node'; node: Node }
  | { kind: 'child'; key: string; child: Node }
  | { kind: 'end' };

export class CompoundHashBuilder {
  posts: string[] = [];
  hashes: string[] = [];
  /** Serialized text length of each completed range (same order as posts). */
  sizes: number[] = [];
  /**
   * When set, completed range texts are handed to the sink instead of being
   * hashed synchronously; `hashes` receives a placeholder the caller fills in
   * (the sink receives the index to fill). Lets the persistence flush hash
   * ranges with WebCrypto off the main thread's synchronous path.
   */
  hashSink: ((text: string, index: number) => void) | null = null;

  /** null when not currently inside a range. */
  private currentHash_: string | null = null;
  /**
   * Key stack of the node being processed. Kept beyond currentDepth_ so the
   * path of the last processed leaf survives popping back out of its parent.
   */
  private currentPath_: string[] = [];
  private currentDepth_ = 0;
  private lastLeafDepth_ = -1;
  private needsComma_ = true;

  private readonly splitState_: CompoundHashSplitState = {
    hashLength: () =>
      this.currentHash_ === null ? 0 : this.currentHash_.length,
    currentPath: () => this.currentPath_.slice(0, this.currentDepth_)
  };

  constructor(private splitStrategy_: CompoundHashSplitStrategy) {}

  processLeaf(node: LeafNode): void {
    this.ensureRange_();
    this.lastLeafDepth_ = this.currentDepth_;
    this.currentHash_ += leafHashRepresentation(node);
    this.needsComma_ = true;
    if (this.splitStrategy_(this.splitState_)) {
      this.endRange_();
    }
  }

  startChild(key: string): void {
    this.ensureRange_();
    if (this.needsComma_) {
      this.currentHash_ += ',';
    }
    this.currentHash_ += hashQuotedString(key) + ':(';
    if (this.currentDepth_ === this.currentPath_.length) {
      this.currentPath_.push(key);
    } else {
      this.currentPath_[this.currentDepth_] = key;
    }
    this.currentDepth_++;
    this.needsComma_ = false;
  }

  endChild(): void {
    this.currentDepth_--;
    if (this.currentHash_ !== null) {
      // Add closing parenthesis for the child that was just processed.
      this.currentHash_ += ')';
    }
    this.needsComma_ = true;
  }

  finishHashing(): void {
    if (this.currentHash_ !== null) {
      this.endRange_();
    }
    // Always close with the empty hash for the tail range to allow simple
    // appending at the server.
    this.hashes.push('');
  }

  /**
   * Seeds the builder into the exact state the natural full-tree walk has
   * immediately after ending a range at the leaf `path`: no open range, the
   * walker positioned at that leaf's depth. A subsequent walk of the leaves
   * AFTER `path` then serializes ranges byte-identically to the corresponding
   * portion of a full walk — the next range's opening parenthesis prefix is
   * reconstructed from the common path with this boundary, which is exactly
   * what ensureRange_ derives from currentPath_/currentDepth_.
   */
  seedBoundary(path: string[]): void {
    this.currentPath_ = path.slice();
    this.currentDepth_ = path.length;
    this.lastLeafDepth_ = path.length;
    this.currentHash_ = null;
    this.needsComma_ = true;
  }

  /**
   * Ends the open range at the last processed leaf regardless of the split
   * strategy — used by the stable-range rewalk to close a dirty run exactly
   * at a preserved boundary post so the following clean range's interval is
   * untouched. No-op when no range is open.
   */
  forceEndRange(): void {
    if (this.currentHash_ !== null) {
      this.endRange_();
    }
  }

  private ensureRange_(): void {
    if (this.currentHash_ === null) {
      let hash = '(';
      for (let i = 0; i < this.currentDepth_; i++) {
        hash += hashQuotedString(this.currentPath_[i]) + ':(';
      }
      this.currentHash_ = hash;
      this.needsComma_ = false;
    }
  }

  private endRange_(): void {
    let hash = this.currentHash_!;
    for (let i = 0; i < this.currentDepth_; i++) {
      hash += ')';
    }
    hash += ')';
    this.sizes.push(hash.length);
    if (this.hashSink !== null) {
      const index = this.hashes.length;
      this.hashes.push('');
      this.hashSink(hash, index);
    } else {
      this.hashes.push(sha1(hash));
    }
    const post = this.currentPath_.slice(0, this.lastLeafDepth_).join('/');
    this.posts.push(post === '' ? '/' : post);
    this.currentHash_ = null;
    this.needsComma_ = true;
  }
}

/**
 * Builds the protocol compound hash while serializing disjoint persistence
 * entries in traversal order. The first full cache write therefore walks each
 * Node once: the returned JSON is stored in the chunk and the same visit feeds
 * the wire hash builder.
 */

/**
 * ============================ STABLE RANGES ============================
 *
 * A committed persistence generation stores its compound hash as a list of
 * ranges [{ post, hash, size }] whose BOUNDARIES ARE PRESERVED across
 * generations. A flush re-hashes only ranges whose leaf interval intersects
 * a changed subtree; clean ranges keep their stored hash without their bytes
 * ever being read. The wire protocol permits this: posts are arbitrary
 * client-chosen markers, and the server recomputes each interval's hash from
 * the posts alone — boundaries never expire, only balance matters.
 *
 * Balance is kept with a half/double hysteresis around the ideal size `s`
 * from simpleSizeSplitStrategy: a rewalked run re-splits naturally at ~s (so
 * a range that grew past ~2s divides), and a clean range smaller than s/2
 * adjacent to a dirty run is absorbed into the run and re-emitted merged.
 * Occasional under-sized survivors are harmless — a small range is valid,
 * merely suboptimal — so rebalancing is amortized, never a correctness step.
 */

/** One stored range: interval end marker, its hash, serialized text length. */
export interface StableRange {
  post: string;
  hash: string;
  size: number;
}

/**
 * Compares two range markers (slash-joined leaf paths) in compound-hash leaf
 * order: segment-wise nameCompare, a strict prefix sorting first. Posts are
 * ordering markers only — they need not exist as leaves in the current tree,
 * so the comparison must be total over arbitrary paths.
 */
export function compareRangeMarkers(a: string[], b: string[]): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const cmp = nameCompare(a[i], b[i]);
    if (cmp !== 0) {
      return cmp;
    }
  }
  return a.length - b.length;
}

const ROOT_POST = '/';

function markerToPath(post: string): string[] {
  return post === ROOT_POST || post === '' ? [] : post.split('/');
}

/**
 * Relation of the subtree rooted at `path` to the marker `post`:
 *   -1 → every leaf in the subtree sorts before-or-at the marker
 *    0 → the marker lies inside (or at the root of) the subtree
 *    1 → every leaf in the subtree sorts after the marker
 */
function subtreeVsMarker(path: string[], post: string[]): -1 | 0 | 1 {
  const n = Math.min(path.length, post.length);
  for (let i = 0; i < n; i++) {
    const cmp = nameCompare(path[i], post[i]);
    if (cmp < 0) {
      return -1;
    }
    if (cmp > 0) {
      return 1;
    }
  }
  if (path.length <= post.length) {
    // path is a (possibly equal) prefix of post: marker inside subtree. An
    // exactly-equal leaf path counts as inside; the walk emits it and the
    // interval's half-open bounds decide inclusion.
    return 0;
  }
  // post is a strict prefix of path: markers sort before their extensions,
  // so the whole subtree sorts after the marker.
  return 1;
}

/**
 * Walks the leaves of `node` whose paths lie in the half-open marker interval
 * (fromPost, toPost], feeding the builder exactly the startChild / endChild /
 * processLeaf sequence the natural full-tree walk produces for those leaves.
 * The builder must have been seeded at `fromPost` (seedBoundary) so the first
 * emitted range opens with the same common-ancestor prefix the full walk
 * would write. `toPost === null` walks to the end of the tree.
 *
 * Subtrees entirely outside the interval are pruned without reading them —
 * the cost is O(interval bytes + pruned fanout), not O(tree).
 */
export function walkLeafInterval(
  node: Node,
  fromPost: string[] | null,
  toPost: string[] | null,
  builder: CompoundHashBuilder
): void {
  // Transition state: the path of the previously emitted leaf (or the seeded
  // boundary), from which endChild/startChild transitions are derived.
  let openPath: string[] = fromPost === null ? [] : fromPost;
  let openDepth = openPath.length;
  let started = fromPost !== null;
  let stopped = false;

  const emitLeaf = (path: string[], leaf: LeafNode): void => {
    if (!started) {
      // First leaf of a from-the-start walk: descend from the root.
      for (let i = 0; i < path.length; i++) {
        builder.startChild(path[i]);
      }
      started = true;
    } else {
      let common = 0;
      while (
        common < openDepth &&
        common < path.length &&
        openPath[common] === path[common]
      ) {
        common++;
      }
      for (let i = openDepth; i > common; i--) {
        builder.endChild();
      }
      for (let i = common; i < path.length; i++) {
        builder.startChild(path[i]);
      }
    }
    builder.processLeaf(leaf);
    // Copy: `path` is the walker's live mutable array.
    openPath = path.slice();
    openDepth = openPath.length;
  };

  const walk = (current: Node, path: string[]): void => {
    if (stopped) {
      return;
    }
    if (fromPost !== null) {
      const rel = subtreeVsMarker(path, fromPost);
      if (rel === -1) {
        return; // entirely at-or-before the opening boundary
      }
      if (rel === 0 && current.isLeafNode()) {
        // The boundary leaf itself: excluded (interval is open at fromPost).
        if (compareRangeMarkers(path, fromPost) <= 0) {
          return;
        }
      }
    }
    if (toPost !== null) {
      const rel = subtreeVsMarker(path, toPost);
      if (rel === 1) {
        stopped = true; // entirely after the closing boundary
        return;
      }
    }
    if (current.isLeafNode()) {
      emitLeaf(path, current as LeafNode);
      return;
    }
    forEachChildWithPriority(current, (key, child) => {
      if (stopped) {
        return;
      }
      path.push(key);
      walk(child, path);
      path.pop();
    });
  };

  walk(node, []);
  // Pop back out of the last emitted leaf's ancestry so a caller chaining
  // further work sees a balanced builder; endChild is a no-op on text when
  // no range is open.
  builder.forceEndRange();
}

/**
 * The identity-diff: collects the paths of maximal subtrees that differ
 * between two versions of an immutable, structurally shared tree. Unchanged
 * subtrees are recognized by object identity and never descended. A child
 * present in only one version reports that child's path. Descends at most
 * `maxDepth` levels before treating a differing subtree as wholly changed —
 * dirty mapping only needs interval bounds, not precise leaves.
 */
export function collectChangedSubtreePaths(
  before: Node,
  after: Node,
  maxDepth = 8,
  maxPaths = 512
): string[][] | null {
  const changed: string[][] = [];
  let overflow = false;
  const visit = (a: Node, b: Node, path: string[], depth: number): void => {
    if (overflow || a === b) {
      return;
    }
    if (
      depth >= maxDepth ||
      a.isLeafNode() ||
      b.isLeafNode() ||
      a.isEmpty() ||
      b.isEmpty()
    ) {
      if (changed.length >= maxPaths) {
        overflow = true;
        return;
      }
      changed.push(path.slice());
      return;
    }
    // Union of child keys in sorted order; nodes are index-sorted by key.
    const aKeys: string[] = [];
    const bKeys: string[] = [];
    a.forEachChild(KEY_INDEX, key => {
      aKeys.push(key);
    });
    b.forEachChild(KEY_INDEX, key => {
      bKeys.push(key);
    });
    let i = 0;
    let j = 0;
    while ((i < aKeys.length || j < bKeys.length) && !overflow) {
      let key: string;
      let cmp: number;
      if (i >= aKeys.length) {
        cmp = 1;
        key = bKeys[j];
      } else if (j >= bKeys.length) {
        cmp = -1;
        key = aKeys[i];
      } else {
        cmp = nameCompare(aKeys[i], bKeys[j]);
        key = cmp <= 0 ? aKeys[i] : bKeys[j];
      }
      path.push(key);
      if (cmp === 0) {
        visit(
          a.getImmediateChild(key),
          b.getImmediateChild(key),
          path,
          depth + 1
        );
        i++;
        j++;
      } else {
        if (changed.length >= maxPaths) {
          overflow = true;
        } else {
          changed.push(path.slice());
        }
        if (cmp < 0) {
          i++;
        } else {
          j++;
        }
      }
      path.pop();
    }
    // A priority change on an interior node serializes into its range too.
    if (!overflow && a.getPriority() !== b.getPriority()) {
      if (
        a.getPriority().isEmpty() !== b.getPriority().isEmpty() ||
        (!a.getPriority().isEmpty() &&
          a.getPriority().val() !== b.getPriority().val())
      ) {
        if (changed.length >= maxPaths) {
          overflow = true;
        } else {
          changed.push(path.slice());
        }
      }
    }
  };
  visit(before, after, [], 0);
  return overflow ? null : changed;
}

/**
 * Marks the ranges whose leaf interval intersects any changed subtree. Range
 * i covers the half-open marker interval (posts[i-1], posts[i]]; the virtual
 * tail after the last post is reported via the returned `tailDirty` (leaves
 * appended after the previously last leaf fall there).
 */
export function markDirtyRanges(
  ranges: StableRange[],
  changedPaths: string[][]
): { dirty: boolean[]; tailDirty: boolean } {
  const dirty = new Array<boolean>(ranges.length).fill(false);
  let tailDirty = false;
  const posts = ranges.map(r => markerToPath(r.post));
  for (const path of changedPaths) {
    if (path.length === 0) {
      dirty.fill(true);
      tailDirty = true;
      break;
    }
    // First range not entirely before the subtree: subtree's leaves start at
    // marker `path` (a prefix sorts before its extensions), so binary-search
    // the first post >= path.
    let lo = 0;
    let hi = ranges.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (compareRangeMarkers(posts[mid], path) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo === ranges.length) {
      tailDirty = true;
      continue;
    }
    // Mark ranges from lo while their interval intersects the subtree: the
    // interval (posts[i-1], posts[i]] intersects until the PREVIOUS post
    // already sorts past every leaf under `path` (after it, not inside it).
    for (let i = lo; i < ranges.length; i++) {
      if (i > lo) {
        // Stop once the subtree's leaves all sort at-or-before the PREVIOUS
        // post: the interval (posts[i-1], posts[i]] can no longer intersect.
        if (subtreeVsMarker(path, posts[i - 1]) === -1) {
          break;
        }
      }
      dirty[i] = true;
      if (
        i === ranges.length - 1 &&
        subtreeVsMarker(path, posts[ranges.length - 1]) !== -1
      ) {
        // The subtree extends past the last post into the virtual tail.
        tailDirty = true;
      }
    }
  }
  return { dirty, tailDirty };
}

/**
 * Produces the next generation's stable ranges: clean ranges carry over
 * verbatim; each maximal dirty run (pre-extended over undersized clean
 * neighbors) is re-serialized over the current tree between its preserved
 * outer boundaries, re-splitting naturally at the current ideal size. Ranges
 * are emitted through `builder`, whose hashSink/hashes the caller owns —
 * pass a sink to hash the dirty texts with WebCrypto afterwards.
 *
 * Returns the new range list with hashes for SINK-DEFERRED entries empty
 * (the caller fills them from the sink's completions, matching indexes in
 * builder.hashes/sizes/posts order for the dirty emissions).
 */
export function rebuildStableRanges(
  node: Node,
  previous: StableRange[],
  dirty: boolean[],
  tailDirty: boolean,
  builder: CompoundHashBuilder
): StableRange[] {
  const ideal = Math.max(
    512,
    Math.floor(Math.sqrt(estimateSerializedNodeSize(node) * 100))
  );
  const minSize = ideal >> 1;
  // Absorb undersized clean neighbors into adjacent dirty runs (merge side of
  // the hysteresis): they re-emit merged with the run's bytes.
  const effectiveDirty = dirty.slice();
  for (let i = 0; i < effectiveDirty.length; i++) {
    if (!effectiveDirty[i]) {
      continue;
    }
    for (
      let p = i - 1;
      p >= 0 && !effectiveDirty[p] && previous[p].size < minSize;
      p--
    ) {
      effectiveDirty[p] = true;
    }
    for (
      let n = i + 1;
      n < effectiveDirty.length &&
      !effectiveDirty[n] &&
      previous[n].size < minSize;
      n++
    ) {
      effectiveDirty[n] = true;
      i = n;
    }
  }

  const result: StableRange[] = [];
  let i = 0;
  while (i < previous.length) {
    if (!effectiveDirty[i]) {
      result.push(previous[i]);
      i++;
      continue;
    }
    let j = i;
    while (j < previous.length && effectiveDirty[j]) {
      j++;
    }
    const runEndsAtTail = j === previous.length && tailDirty;
    const fromPost = i === 0 ? null : markerToPath(previous[i - 1].post);
    const toPost = runEndsAtTail ? null : markerToPath(previous[j - 1].post);
    const firstEmitIndex = builder.posts.length;
    if (fromPost !== null) {
      builder.seedBoundary(fromPost);
    }
    walkLeafInterval(node, fromPost, toPost, builder);
    for (let k = firstEmitIndex; k < builder.posts.length; k++) {
      result.push({
        post: builder.posts[k],
        hash: builder.hashes[k],
        size: builder.sizes[k]
      });
    }
    i = j;
  }
  if (tailDirty && previous.length > 0) {
    // Tail handled by extending the last run (runEndsAtTail) when the last
    // range was dirty; when it was clean, walk the pure tail interval.
    const lastWasClean = !effectiveDirty[previous.length - 1];
    if (lastWasClean) {
      const fromPost = markerToPath(previous[previous.length - 1].post);
      const firstEmitIndex = builder.posts.length;
      builder.seedBoundary(fromPost);
      walkLeafInterval(node, fromPost, null, builder);
      for (let k = firstEmitIndex; k < builder.posts.length; k++) {
        result.push({
          post: builder.posts[k],
          hash: builder.hashes[k],
          size: builder.sizes[k]
        });
      }
    }
  }
  if (previous.length === 0) {
    // First-ever generation: one natural full walk.
    const firstEmitIndex = builder.posts.length;
    walkLeafInterval(node, null, null, builder);
    for (let k = firstEmitIndex; k < builder.posts.length; k++) {
      result.push({
        post: builder.posts[k],
        hash: builder.hashes[k],
        size: builder.sizes[k]
      });
    }
  }
  return result;
}

export class CompoundHashAccumulator {
  private readonly builder_: CompoundHashBuilder;
  private openPath_: string[] = [];

  constructor(root: Node) {
    this.builder_ = new CompoundHashBuilder(simpleSizeSplitStrategy(root));
  }

  serializeEntry(path: string[], node: Node, includedInHash = true): unknown {
    if (!includedInHash) {
      return node.val(true);
    }
    this.moveToPath_(path);
    return this.serializeNode_(node);
  }

  hashEntry(path: string[], node: Node, includedInHash = true): void {
    if (!includedInHash) {
      return;
    }
    this.moveToPath_(path);
    this.hashNode_(node);
  }

  finish(): CompoundHash {
    this.moveToPath_([]);
    this.builder_.finishHashing();
    return new CompoundHash(this.builder_.posts, this.builder_.hashes);
  }

  private moveToPath_(next: string[]): void {
    let common = 0;
    while (
      common < this.openPath_.length &&
      common < next.length &&
      this.openPath_[common] === next[common]
    ) {
      common++;
    }
    for (let i = this.openPath_.length; i > common; i--) {
      this.builder_.endChild();
    }
    for (let i = common; i < next.length; i++) {
      this.builder_.startChild(next[i]);
    }
    this.openPath_ = next.slice();
  }

  private hashNode_(node: Node): void {
    if (node.isEmpty()) {
      return;
    }
    if (node.isLeafNode()) {
      this.builder_.processLeaf(node as LeafNode);
      return;
    }
    forEachChildWithPriority(node, (key, child, included) => {
      if (!included) {
        return;
      }
      this.builder_.startChild(key);
      this.hashNode_(child);
      this.builder_.endChild();
    });
  }

  private serializeNode_(node: Node): unknown {
    if (node.isEmpty()) {
      return null;
    }
    if (node.isLeafNode()) {
      this.builder_.processLeaf(node as LeafNode);
      if (!node.getPriority().isEmpty()) {
        return {
          '.value': node.val(),
          '.priority': node.getPriority().val()
        };
      }
      return node.val();
    }

    const out: Record<string, unknown> = {};
    forEachChildWithPriority(
      node,
      (key, child, included) => {
        if (included) {
          this.builder_.startChild(key);
          out[key] = this.serializeNode_(child);
          this.builder_.endChild();
        } else {
          out[key] = child.val(true);
        }
      },
      true
    );
    return out;
  }
}

/**
 * The compound-hash representation of a leaf: the V2 grammar (strings and
 * keys JSON-quoted so range serializations are unambiguous to reparse; see
 * leafHashValueText), with the priority prefixed exactly as in Node.hash().
 */
function leafHashRepresentation(node: LeafNode): string {
  let representation = '';
  const priority = node.getPriority();
  if (!priority.isEmpty()) {
    representation +=
      'priority:' +
      leafHashValueText(priority.val() as string | number, true) +
      ':';
  }
  representation += leafHashValueText(
    node.val() as string | number | boolean,
    true
  );
  return representation;
}

/**
 * Sizes computed for interior (children) nodes, keyed by node identity.
 * Nodes are immutable and structurally shared across server updates, so a
 * subtree's estimate stays valid for as long as the subtree object lives —
 * repeated estimations of a large mostly-unchanged tree (the persistence
 * write path re-plans its chunks on every flush) only walk the changed
 * spine. Leaves are cheap to size and are not cached.
 */
const serializedSizeCache = new WeakMap<Node, number>();

/**
 * Estimates the serialized size of a node in bytes — a cheap approximation
 * that only drives the default split threshold and the persistence chunk
 * planner, never a wire value (port of Android NodeSizeEstimator).
 */
export function estimateSerializedNodeSize(node: Node): number {
  if (node.isEmpty()) {
    return 4; // null keyword
  } else if (node.isLeafNode()) {
    let valueSize: number;
    const value = node.val();
    if (typeof value === 'number') {
      valueSize = 8; // estimate each float with 8 bytes
    } else if (typeof value === 'boolean') {
      valueSize = 4; // true or false need roughly 4 bytes
    } else {
      // string: two quotes plus the payload
      valueSize = 2 + String(value).length;
    }
    if (node.getPriority().isEmpty()) {
      return valueSize;
    }
    // Account for the extra overhead of the ".value" and ".priority" keys.
    return 24 + valueSize + estimateSerializedNodeSize(node.getPriority());
  } else {
    const cached = serializedSizeCache.get(node);
    if (cached !== undefined) {
      return cached;
    }
    let sum = 1; // opening brace
    node.forEachChild(KEY_INDEX, (key, child) => {
      // key, quotes, colon, comma
      sum += key.length + 4 + estimateSerializedNodeSize(child);
    });
    if (!node.getPriority().isEmpty()) {
      sum += 12 + estimateSerializedNodeSize(node.getPriority());
    }
    serializedSizeCache.set(node, sum);
    return sum;
  }
}

/**
 * Schedules the next slice of a background computation: idle time where the
 * platform offers it, a macrotask otherwise.
 */
export function scheduleSlice(fn: () => void): void {
  if (typeof requestIdleCallback === 'function') {
    requestIdleCallback(() => fn(), { timeout: 200 });
  } else {
    setTimeout(fn, 0);
  }
}

/**
 * Computes a compound hash in bounded slices of main-thread time, yielding
 * to the event loop between slices, so hashing a large tree for persistence
 * never blocks the UI the way a monolithic walk would. Same traversal as
 * compoundHashFromNode (see CompoundHashWalker), so the result is identical.
 */
export function compoundHashFromNodeAsync(
  node: Node,
  splitStrategy?: CompoundHashSplitStrategy,
  sliceMs = 12,
  onProgress: () => void = () => {}
): Promise<CompoundHash> {
  if (node.isEmpty()) {
    return Promise.resolve(new CompoundHash([], ['']));
  }
  const strategy = splitStrategy || simpleSizeSplitStrategy(node);
  const builder = new CompoundHashBuilder(strategy);
  const walker = new CompoundHashWalker(node, builder);
  return new Promise((resolve, reject) => {
    const step = (): void => {
      try {
        if (!walker.drainUntil(Date.now() + sliceMs)) {
          onProgress();
          scheduleSlice(step);
          return;
        }
        builder.finishHashing();
        resolve(new CompoundHash(builder.posts, builder.hashes));
      } catch (e) {
        reject(e);
      }
    };
    step();
  });
}

/**
 * Computes the canonical Node hash without populating every subtree's
 * lazyHash_. Only frames on the current depth-first path are retained; each
 * child hash is folded into its parent and released. Persistence uses this at
 * write time, stores the resulting root hash in the manifest, and stamps only
 * the restored root on the next boot.
 */
export function canonicalHashFromNodeAsync(
  node: Node,
  sliceMs = 12,
  onProgress: () => void = () => {}
): Promise<string> {
  if (node.isEmpty()) {
    return Promise.resolve('');
  }
  interface CanonicalFrame {
    node: Node;
    key: string | null;
    children: Array<[string, Node]> | null;
    nextChild: number;
    toHash: string;
  }
  const stack: CanonicalFrame[] = [
    { node, key: null, children: null, nextChild: 0, toHash: '' }
  ];
  return new Promise((resolve, reject) => {
    const completeFrame = (hash: string) => {
      const finished = stack.pop();
      if (!finished) {
        resolve(hash);
        return;
      }
      const parent = stack[stack.length - 1];
      if (!parent) {
        resolve(hash);
      } else if (hash !== '' && finished.key !== null) {
        parent.toHash += ':' + finished.key + ':' + hash;
      }
    };
    const step = (): void => {
      try {
        const deadline = Date.now() + sliceMs;
        while (stack.length > 0) {
          const frame = stack[stack.length - 1];
          if (frame.node.isLeafNode()) {
            const leaf = frame.node as LeafNode;
            let text = '';
            const priority = leaf.getPriority();
            if (!priority.isEmpty()) {
              text +=
                'priority:' +
                priorityHashText(priority.val() as string | number) +
                ':';
            }
            text += leafHashValueText(
              leaf.val() as string | number | boolean,
              false
            );
            completeFrame(sha1(text));
          } else {
            if (frame.children === null) {
              const priority = frame.node.getPriority();
              if (!priority.isEmpty()) {
                frame.toHash =
                  'priority:' +
                  priorityHashText(priority.val() as string | number) +
                  ':';
              }
              const children: Array<[string, Node]> = [];
              frame.children = children;
              frame.node.forEachChild(PRIORITY_INDEX, (key, child) => {
                children.push([key, child]);
              });
            }
            if (frame.nextChild < frame.children.length) {
              const [key, child] = frame.children[frame.nextChild++];
              stack.push({
                node: child,
                key,
                children: null,
                nextChild: 0,
                toHash: ''
              });
            } else {
              completeFrame(frame.toHash === '' ? '' : sha1(frame.toHash));
            }
          }
          if (stack.length > 0 && Date.now() >= deadline) {
            onProgress();
            scheduleSlice(step);
            return;
          }
        }
      } catch (e) {
        reject(e);
      }
    };
    step();
  });
}

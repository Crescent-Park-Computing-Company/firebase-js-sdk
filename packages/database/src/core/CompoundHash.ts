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
import { LeafNode } from './snap/LeafNode';
import { Node } from './snap/Node';
import { hashQuotedString, leafHashValueText } from './snap/snap';
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

/**
 * Incrementally builds the compound hash while a walk feeds it the
 * startChild / processLeaf / endChild sequence of the tree's leaves in key
 * order. A port of Android's CompoundHash.CompoundHashBuilder; the range
 * grammar (quoted keys, parenthesized nesting, V2 leaf text) matches the
 * server byte for byte.
 */
export class CompoundHashBuilder {
  posts: string[] = [];
  hashes: string[] = [];

  /**
   * When set (by compoundHashFromNodeAsync), endRange_ collects each range's
   * grammar text here and leaves a placeholder in `hashes` instead of
   * running the pure-JS sha1 inline. The async driver then digests the
   * texts with the platform's native SHA-1 (crypto.subtle + TextEncoder),
   * which profiles an order of magnitude faster than the JS fallback and
   * allocates nothing on the JS heap. The trailing empty hash is appended
   * by finishHashing as usual and is never a placeholder.
   */
  rangeTexts: string[] | null = null;

  /**
   * The current range's grammar text, as parts joined once per range:
   * per-leaf string concatenation builds a rope chain per append, and on a
   * multi-megabyte tree the rope churn (allocation + flattening) costs more
   * GC time than the hashing itself. null when not currently inside a range.
   */
  private currentParts_: string[] | null = null;
  private currentLength_ = 0;
  /**
   * Key stack of the node being processed. Kept beyond currentDepth_ so the
   * path of the last processed leaf survives popping back out of its parent.
   */
  private currentPath_: string[] = [];
  private currentDepth_ = 0;
  private lastLeafDepth_ = -1;
  private needsComma_ = true;

  private readonly splitState_: CompoundHashSplitState = {
    hashLength: () => this.currentLength_,
    currentPath: () => this.currentPath_.slice(0, this.currentDepth_)
  };

  constructor(private splitStrategy_: CompoundHashSplitStrategy) {}

  private append_(text: string): void {
    this.currentParts_!.push(text);
    this.currentLength_ += text.length;
  }

  processLeaf(node: LeafNode): void {
    this.ensureRange_();
    this.lastLeafDepth_ = this.currentDepth_;
    this.append_(leafHashRepresentation(node));
    this.needsComma_ = true;
    if (this.splitStrategy_(this.splitState_)) {
      this.endRange_();
    }
  }

  startChild(key: string): void {
    this.ensureRange_();
    if (this.needsComma_) {
      this.append_(',');
    }
    this.append_(hashQuotedString(key) + ':(');
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
    if (this.currentParts_ !== null) {
      // Add closing parenthesis for the child that was just processed.
      this.append_(')');
    }
    this.needsComma_ = true;
  }

  finishHashing(): void {
    if (this.currentParts_ !== null) {
      this.endRange_();
    }
    // Always close with the empty hash for the tail range to allow simple
    // appending at the server.
    this.hashes.push('');
  }

  private ensureRange_(): void {
    if (this.currentParts_ === null) {
      this.currentParts_ = [];
      this.currentLength_ = 0;
      let hash = '(';
      for (let i = 0; i < this.currentDepth_; i++) {
        hash += hashQuotedString(this.currentPath_[i]) + ':(';
      }
      this.append_(hash);
      this.needsComma_ = false;
    }
  }

  private endRange_(): void {
    for (let i = 0; i < this.currentDepth_; i++) {
      this.append_(')');
    }
    this.append_(')');
    const hash = this.currentParts_!.join('');
    if (this.rangeTexts !== null) {
      this.rangeTexts.push(hash);
      this.hashes.push('');
    } else {
      this.hashes.push(sha1(hash));
    }
    const post = this.currentPath_.slice(0, this.lastLeafDepth_).join('/');
    this.posts.push(post === '' ? '/' : post);
    this.currentParts_ = null;
    this.currentLength_ = 0;
    this.needsComma_ = true;
  }
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
  /**
   * Returns true when the caller must collapse this branch to stay within the
   * global path budget. A large atomic subtree update should dirty that
   * subtree's ranges, never fall back to dirtying the entire persisted root.
   */
  const visit = (a: Node, b: Node, path: string[], depth: number): boolean => {
    if (a === b) {
      return false;
    }
    const branchStart = changed.length;
    const collapseBranch = (): boolean => {
      changed.splice(branchStart);
      changed.push(path.slice());
      return changed.length > maxPaths;
    };
    if (
      depth >= maxDepth ||
      a.isLeafNode() ||
      b.isLeafNode() ||
      a.isEmpty() ||
      b.isEmpty()
    ) {
      changed.push(path.slice());
      return changed.length > maxPaths;
    }
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
    while (i < aKeys.length || j < bKeys.length) {
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
      let overBudget = false;
      if (cmp === 0) {
        overBudget = visit(
          a.getImmediateChild(key),
          b.getImmediateChild(key),
          path,
          depth + 1
        );
        i++;
        j++;
      } else {
        changed.push(path.slice());
        overBudget = changed.length > maxPaths;
        if (cmp < 0) {
          i++;
        } else {
          j++;
        }
      }
      path.pop();
      if (overBudget) {
        return collapseBranch();
      }
    }
    if (a.getPriority() !== b.getPriority()) {
      if (
        a.getPriority().isEmpty() !== b.getPriority().isEmpty() ||
        (!a.getPriority().isEmpty() &&
          a.getPriority().val() !== b.getPriority().val())
      ) {
        changed.push(path.slice());
        if (changed.length > maxPaths) {
          return collapseBranch();
        }
      }
    }
    return false;
  };
  visit(before, after, [], 0);
  return changed;
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
  const subtle =
    typeof crypto !== 'undefined' && crypto.subtle ? crypto.subtle : null;
  if (subtle !== null) {
    // Defer range hashing to the platform's native SHA-1: the pure-JS
    // fallback (stringToByteArray + compress_) profiles as two thirds of
    // the whole hash cost on a large tree AND churns the GC; TextEncoder +
    // crypto.subtle do the same work natively off the JS heap.
    builder.rangeTexts = [];
  }
  const walker = new CompoundHashWalker(node, builder);
  const encoder = subtle !== null ? new TextEncoder() : null;
  const pendingDigests: Array<Promise<void>> = [];
  let digested = 0;
  // Digest ranges AS THE WALK PRODUCES THEM: the native hash runs off-thread
  // while the walk continues, and each range's (potentially large) grammar
  // text is released right after encoding instead of accumulating until the
  // end of the traversal.
  const drainTexts = (): void => {
    const texts = builder.rangeTexts!;
    while (digested < texts.length) {
      const index = digested++;
      const text = texts[index];
      texts[index] = '';
      pendingDigests.push(
        digestRangeText(subtle!, encoder!, text).then(hash => {
          builder.hashes[index] = hash;
        })
      );
    }
  };
  return new Promise((resolve, reject) => {
    const step = (): void => {
      try {
        if (!walker.drainUntil(Date.now() + sliceMs)) {
          if (builder.rangeTexts !== null) {
            drainTexts();
          }
          onProgress();
          scheduleSlice(step);
          return;
        }
        builder.finishHashing();
        if (builder.rangeTexts === null) {
          resolve(new CompoundHash(builder.posts, builder.hashes));
          return;
        }
        drainTexts();
        resolve(
          Promise.all(pendingDigests).then(
            () => new CompoundHash(builder.posts, builder.hashes)
          )
        );
      } catch (e) {
        reject(e);
      }
    };
    step();
  });
}

/**
 * base64(sha1(text)) via WebCrypto. Identical output to util.sha1 (locked
 * in by the async-matches-sync test).
 */
function digestRangeText(
  subtle: SubtleCrypto,
  encoder: TextEncoder,
  text: string
): Promise<string> {
  return subtle.digest('SHA-1', encoder.encode(text)).then(buffer => {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    // btoa is universal in browsers; Node ≥16 has it global too.
    return btoa(binary);
  });
}

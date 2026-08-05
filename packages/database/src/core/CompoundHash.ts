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
function forEachChildWithPriority(
  node: Node,
  action: (key: string, child: Node) => void
): void {
  if (node.getPriority().isEmpty()) {
    node.forEachChild(KEY_INDEX, action);
    return;
  }
  let passedPriority = false;
  node.forEachChild(KEY_INDEX, (key, child) => {
    if (!passedPriority && nameCompare(key, '.priority') > 0) {
      passedPriority = true;
      action('.priority', node.getPriority());
    }
    action(key, child);
  });
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

class CompoundHashBuilder {
  posts: string[] = [];
  hashes: string[] = [];

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
    this.hashes.push(sha1(hash));
    const post = this.currentPath_.slice(0, this.lastLeafDepth_).join('/');
    this.posts.push(post === '' ? '/' : post);
    this.currentHash_ = null;
    this.needsComma_ = true;
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
function scheduleSlice(fn: () => void): void {
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
  sliceMs = 12
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
  sliceMs = 12
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

/**
 * Computes node.hash() — the canonical listen hash — in bounded slices.
 * Node hashes cache per node (lazyHash_) and nodes are immutable, so the
 * walk primes every subtree's hash bottom-up across slices; the final
 * root hash() then assembles from cached children in one cheap pass, and
 * unchanged subtrees stay primed for the next flush.
 */
export function hashFromNodeAsync(node: Node, sliceMs = 12): Promise<string> {
  interface HashFrame {
    node: Node;
    childrenPrimed: boolean;
  }
  const stack: HashFrame[] = [{ node, childrenPrimed: false }];
  const processFrame = (frame: HashFrame): void => {
    if (frame.childrenPrimed || frame.node.isLeafNode()) {
      frame.node.hash();
      return;
    }
    stack.push({ node: frame.node, childrenPrimed: true });
    frame.node.forEachChild(KEY_INDEX, (_key, child) => {
      stack.push({ node: child, childrenPrimed: false });
    });
  };
  return new Promise((resolve, reject) => {
    const step = (): void => {
      try {
        const deadline = Date.now() + sliceMs;
        // At least one frame per slice: progress is guaranteed even with a
        // zero budget.
        while (stack.length > 0) {
          processFrame(stack.pop()!);
          if (Date.now() >= deadline) {
            break;
          }
        }
        if (stack.length > 0) {
          scheduleSlice(step);
          return;
        }
        resolve(node.hash());
      } catch (e) {
        reject(e);
      }
    };
    step();
  });
}

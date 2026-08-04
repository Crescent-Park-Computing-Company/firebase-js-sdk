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
import { doubleToIEEE754String, nameCompare, sha1 } from './util/util';

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
  processNode(node, builder);
  builder.finishHashing();
  return new CompoundHash(builder.posts, builder.hashes);
}

function processNode(node: Node, builder: CompoundHashBuilder): void {
  if (node.isLeafNode()) {
    builder.processLeaf(node as LeafNode);
  } else {
    forEachChildWithPriority(node, (key, child) => {
      builder.startChild(key);
      processNode(child, builder);
      builder.endChild();
    });
  }
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
    this.currentHash_ += quoted(key) + ':(';
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
        hash += quoted(this.currentPath_[i]) + ':(';
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
 * The compound-hash representation of a leaf value. Unlike the text hashed by
 * Node.hash(), strings and keys are JSON-quoted so range serializations are
 * unambiguous to reparse (Android calls this the "V2" hash representation).
 */
export function leafHashRepresentation(node: Node): string {
  let representation = '';
  if (!node.getPriority().isEmpty()) {
    representation +=
      'priority:' + leafHashRepresentation(node.getPriority()) + ':';
  }
  const value = node.val();
  const type = typeof value;
  representation += type + ':';
  if (type === 'number') {
    representation += doubleToIEEE754String(value as number);
  } else if (type === 'string') {
    representation += quoted(value as string);
  } else {
    representation += String(value);
  }
  return representation;
}

/**
 * JSON-style quoting with only backslash and double quote escaped.
 */
function quoted(value: string): string {
  let escaped = value;
  if (escaped.indexOf('\\') !== -1) {
    escaped = escaped.replace(/\\/g, '\\\\');
  }
  if (escaped.indexOf('"') !== -1) {
    escaped = escaped.replace(/"/g, '\\"');
  }
  return '"' + escaped + '"';
}

/**
 * Estimates the serialized size of a node in bytes — a cheap approximation
 * that only drives the default split threshold, never a wire value (port of
 * Android NodeSizeEstimator).
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
    let sum = 1; // opening brace
    node.forEachChild(KEY_INDEX, (key, child) => {
      // key, quotes, colon, comma
      sum += key.length + 4 + estimateSerializedNodeSize(child);
    });
    if (!node.getPriority().isEmpty()) {
      sum += 12 + estimateSerializedNodeSize(node.getPriority());
    }
    return sum;
  }
}

/**
 * Computes a compound hash in bounded slices of main-thread time, yielding to
 * the event loop between slices, so hashing a large tree for persistence
 * never blocks the UI the way a monolithic walk would.
 *
 * The recursive walk of compoundHashFromNode is driven as an explicit frame
 * stack — a `child` frame runs builder.startChild, pushes its subtree, and a
 * matching `end` frame runs builder.endChild — so the builder sees the exact
 * call sequence the recursion produces and the resulting hash is identical.
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

  type Frame =
    | { kind: 'node'; node: Node }
    | { kind: 'child'; key: string; child: Node }
    | { kind: 'end' };
  // Popped last-in-first-out; children are pushed in reverse key order so
  // they pop in key order.
  const stack: Frame[] = [{ kind: 'node', node }];

  const processFrame = (frame: Frame): void => {
    if (frame.kind === 'end') {
      builder.endChild();
      return;
    }
    const current = frame.kind === 'node' ? frame.node : frame.child;
    if (frame.kind === 'child') {
      builder.startChild(frame.key);
      stack.push({ kind: 'end' });
    }
    if (current.isLeafNode()) {
      builder.processLeaf(current as LeafNode);
      // A leaf pushed no 'end' of its own; the pending 'end' (if this was a
      // child frame) already sits on the stack.
      return;
    }
    const children: Array<[string, Node]> = [];
    forEachChildWithPriority(current, (key, child) => {
      children.push([key, child]);
    });
    for (let i = children.length - 1; i >= 0; i--) {
      stack.push({ kind: 'child', key: children[i][0], child: children[i][1] });
    }
  };

  return new Promise((resolve, reject) => {
    const schedule =
      typeof requestIdleCallback === 'function'
        ? (fn: () => void) => requestIdleCallback(() => fn(), { timeout: 200 })
        : (fn: () => void) => setTimeout(fn, 0);
    const step = (): void => {
      try {
        const deadline = Date.now() + sliceMs;
        while (stack.length > 0 && Date.now() < deadline) {
          processFrame(stack.pop()!);
        }
        if (stack.length > 0) {
          schedule(step);
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

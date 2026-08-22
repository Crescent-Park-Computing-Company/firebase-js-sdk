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

import {
  newEmptyPath,
  Path,
  pathChild,
  pathCompare,
  pathContains
} from '../util/Path';

import { ChildrenNode } from './ChildrenNode';
import { KEY_INDEX } from './indexes/KeyIndex';
import { Node } from './Node';

/**
 * Applies a server range merge against locally cached data: every leaf whose
 * path lies strictly after `optExclusiveStart` and at-or-before
 * `optInclusiveEnd` is replaced by (or, when absent from the update, deleted
 * in favor of) the corresponding leaves of the update node; everything
 * outside the range is kept. A null bound is open (-/+ infinity). Priorities
 * of children nodes are treated as leaf children of that node.
 *
 * The server sends range merges when a listen carried a compound hash and
 * only some of its ranges differed — each merge covers one differing range.
 *
 * This is a port of the range merge support in the Android and iOS SDKs
 * (Android: com.google.firebase.database.snapshot.RangeMerge); the
 * semantics match them exactly.
 */
export class RangeMerge {
  constructor(
    private optExclusiveStart_: Path | null,
    private optInclusiveEnd_: Path | null,
    private snap_: Node
  ) {}

  applyTo(node: Node): Node {
    return this.updateRangeInNode_(newEmptyPath(), node, this.snap_);
  }

  private updateRangeInNode_(
    currentPath: Path,
    node: Node,
    updateNode: Node
  ): Node {
    const startComparison =
      this.optExclusiveStart_ === null
        ? 1
        : pathCompare(currentPath, this.optExclusiveStart_);
    const endComparison =
      this.optInclusiveEnd_ === null
        ? -1
        : pathCompare(currentPath, this.optInclusiveEnd_);
    const startInNode =
      this.optExclusiveStart_ !== null &&
      pathContains(currentPath, this.optExclusiveStart_);
    const endInNode =
      this.optInclusiveEnd_ !== null &&
      pathContains(currentPath, this.optInclusiveEnd_);
    if (startComparison > 0 && endComparison < 0 && !endInNode) {
      // node is completely contained in the range
      return updateNode;
    } else if (startComparison > 0 && endInNode && updateNode.isLeafNode()) {
      return updateNode;
    } else if (startComparison > 0 && endComparison === 0) {
      // Exactly the inclusive end and the update is not a leaf: any leaf at
      // this position was consumed by the range (deleted); deeper structure
      // is outside the range.
      if (node.isLeafNode()) {
        return ChildrenNode.EMPTY_NODE;
      }
      return node;
    } else if (startInNode || endInNode) {
      // The range starts or ends within this node: update the union of both
      // nodes' children.
      const allChildren = new Set<string>();
      node.forEachChild(KEY_INDEX, key => {
        allChildren.add(key);
      });
      updateNode.forEachChild(KEY_INDEX, key => {
        allChildren.add(key);
      });
      const inOrder = Array.from(allChildren);
      // Add priority last, so the node is not empty when it is applied.
      if (
        !updateNode.getPriority().isEmpty() ||
        !node.getPriority().isEmpty()
      ) {
        inOrder.push('.priority');
      }
      let newNode = node;
      for (const key of inOrder) {
        const currentChild = node.getImmediateChild(key);
        const updatedChild = this.updateRangeInNode_(
          pathChild(currentPath, key),
          currentChild,
          updateNode.getImmediateChild(key)
        );
        if (updatedChild !== currentChild) {
          newNode = newNode.updateImmediateChild(key, updatedChild);
        }
      }
      return newNode;
    } else {
      // Unaffected by this range
      return node;
    }
  }
}

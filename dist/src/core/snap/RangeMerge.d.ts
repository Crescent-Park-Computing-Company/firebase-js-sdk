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
import { Path } from '../util/Path';
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
export declare class RangeMerge {
    private optExclusiveStart_;
    private optInclusiveEnd_;
    private snap_;
    constructor(optExclusiveStart_: Path | null, optInclusiveEnd_: Path | null, snap_: Node);
    applyTo(node: Node): Node;
    private updateRangeInNode_;
}

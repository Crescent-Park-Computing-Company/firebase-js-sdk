/**
 * @license
 * Copyright 2017 Google LLC
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
import { Node } from '../snap/Node';
import { Path } from '../util/Path';
/**
 * A cache node only stores complete children. Additionally it holds a flag whether the node can be considered fully
 * initialized in the sense that we know at one point in time this represented a valid state of the world, e.g.
 * initialized with data from the server, or a complete overwrite by the client. The filtered flag also tracks
 * whether a node potentially had children removed due to a filter.
 */
export declare class CacheNode {
    private node_;
    private fullyInitialized_;
    private filtered_;
    private verified_;
    constructor(node_: Node, fullyInitialized_: boolean, filtered_: boolean, verified_?: boolean);
    /**
     * Returns whether this node was fully initialized with either server data or a complete overwrite by the client
     */
    isFullyInitialized(): boolean;
    /**
     * Whether the data came from (or was confirmed by) the server. False only
     * while a view holds a persisted tree installed ahead of the server's
     * answer (OperationVerification 'restore'); a full server overwrite or the
     * listen's completion flips it back. Travels with the cache: a view seeded
     * from another view's complete cache inherits the bit. get() serves a
     * cached value only from a verified server cache.
     */
    isVerified(): boolean;
    /**
     * Returns whether this node is potentially missing children due to a filter applied to the node
     */
    isFiltered(): boolean;
    isCompleteForPath(path: Path): boolean;
    isCompleteForChild(key: string): boolean;
    getNode(): Node;
}

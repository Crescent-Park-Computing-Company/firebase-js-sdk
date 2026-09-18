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
import { Operation } from '../operation/Operation';
import { Node } from '../snap/Node';
import { Path } from '../util/Path';
import { WriteTreeRef } from '../WriteTree';
import { CacheNode } from './CacheNode';
import { Event } from './Event';
import { EventGenerator } from './EventGenerator';
import { EventRegistration, QueryContext } from './EventRegistration';
import { ViewCache } from './ViewCache';
import { ViewProcessor } from './ViewProcessor';
/**
 * A view represents a specific location and query that has 1 or more event registrations.
 *
 * It does several things:
 *  - Maintains the list of event registrations for this location/query.
 *  - Maintains a cache of the data visible for this location/query.
 *  - Applies new operations (via applyOperation), updates the cache, and based on the event
 *    registrations returns the set of events to be raised.
 */
export declare class View {
    private query_;
    processor_: ViewProcessor;
    viewCache_: ViewCache;
    eventRegistrations_: EventRegistration[];
    eventGenerator_: EventGenerator;
    constructor(query_: QueryContext, initialViewCache: ViewCache);
    get query(): QueryContext;
}
export declare function viewGetServerCache(view: View): Node | null;
export declare function viewGetCompleteNode(view: View): Node | null;
export declare function viewGetCompleteServerCache(view: View, path: Path): Node | null;
/**
 * The complete server cache this view holds for `path`, as a CacheNode that
 * carries the view's `verified` bit, so a view seeded from it inherits the
 * provenance along with the data.
 */
export declare function viewGetCompleteServerCacheNode(view: View, path: Path): CacheNode | null;
/**
 * Whether the value this view returns (its event cache: server data plus
 * local writes) is built only from verified data. False while the view's
 * server cache is a restored tree, or while a local write's refill of a
 * filtered window borrowed from an unverified covering cache
 * (see viewProcessorApplyOperation).
 */
export declare function viewIsEventCacheVerified(view: View): boolean;
export declare function viewIsEmpty(view: View): boolean;
export declare function viewAddEventRegistration(view: View, eventRegistration: EventRegistration): void;
/**
 * @param eventRegistration - If null, remove all callbacks.
 * @param cancelError - If a cancelError is provided, appropriate cancel events will be returned.
 * @returns Cancel events, if cancelError was provided.
 */
export declare function viewRemoveEventRegistration(view: View, eventRegistration: EventRegistration | null, cancelError?: Error): Event[];
/**
 * Applies the given Operation, updates our cache, and returns the appropriate events.
 */
export declare function viewApplyOperation(view: View, operation: Operation, writesCache: WriteTreeRef, completeServerCache: CacheNode | null): Event[];
export declare function viewGetInitialEvents(view: View, registration: EventRegistration): Event[];

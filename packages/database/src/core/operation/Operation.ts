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

import { Path } from '../util/Path';

/**
 *
 * @enum
 */
export enum OperationType {
  OVERWRITE,
  MERGE,
  ACK_USER_WRITE,
  LISTEN_COMPLETE
}

/**
 * @interface
 */
export interface Operation {
  source: OperationSource;

  type: OperationType;

  path: Path;

  operationForChild(childName: string): Operation | null;
}

/**
 * What a server-sourced operation says about the data it carries, for the
 * `verified` bit of the server cache it lands in (see CacheNode.isVerified):
 *
 * - 'verify': ordinary server data. A full overwrite of a view, or a listen
 *   completing at it, marks the view's server cache verified.
 * - 'keep': a correction folded over the current cache (range merges). The
 *   view keeps whatever bit it had; the listen completion that follows the
 *   merges is what verifies it.
 * - 'restore': a persisted tree being installed as a listen's initial cache
 *   before the server has answered. A full overwrite of a view marks it
 *   UNverified; a view the operation only partially covers (an ancestor
 *   view) is left untouched, so unverified data never grafts into a view
 *   the server did verify.
 */
export type OperationVerification = 'verify' | 'keep' | 'restore';

export interface OperationSource {
  fromUser: boolean;
  fromServer: boolean;
  queryId: string | null;
  tagged: boolean;
  verification: OperationVerification;
}

export function newOperationSourceUser(): OperationSource {
  return {
    fromUser: true,
    fromServer: false,
    queryId: null,
    tagged: false,
    verification: 'verify'
  };
}

export function newOperationSourceServer(
  verification: OperationVerification = 'verify'
): OperationSource {
  return {
    fromUser: false,
    fromServer: true,
    queryId: null,
    tagged: false,
    verification
  };
}

export function newOperationSourceServerTaggedQuery(
  queryId: string,
  verification: OperationVerification = 'verify'
): OperationSource {
  return {
    fromUser: false,
    fromServer: true,
    queryId,
    tagged: true,
    verification
  };
}

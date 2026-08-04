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
import { Node } from './Node';
export declare function setMaxNode(val: Node): void;
/**
 * The hash text of a leaf value: `<typeof>:<serialized value>`. Numbers
 * serialize as IEEE-754 hex; everything else via String(). This is the one
 * definition of the leaf grammar shared by Node.hash() (v2 = false) and the
 * compound-hash range serialization (v2 = true, where strings are
 * JSON-quoted so ranges are unambiguous to reparse — Android calls this the
 * "V2" hash representation).
 */
export declare function leafHashValueText(value: string | number | boolean, v2: boolean): string;
/**
 * JSON-style quoting with only backslash and double quote escaped (the V2
 * hash grammar's string form).
 */
export declare function hashQuotedString(value: string): string;
export declare const priorityHashText: (priority: string | number) => string;
/**
 * Validates that a priority snapshot Node is valid.
 */
export declare const validatePriorityNode: (priorityNode: Node) => void;

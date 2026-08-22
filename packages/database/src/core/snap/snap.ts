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

import { assert, contains } from '@firebase/util';

import { Indexable } from '../util/misc';
import { doubleToIEEE754String } from '../util/util';

import { Node } from './Node';

let MAX_NODE: Node;

export function setMaxNode(val: Node) {
  MAX_NODE = val;
}

/**
 * The hash text of a leaf value: `<typeof>:<serialized value>`. Numbers
 * serialize as IEEE-754 hex; everything else via String(). This is the one
 * definition of the leaf grammar shared by Node.hash() (v2 = false) and the
 * compound-hash range serialization (v2 = true, where strings are
 * JSON-quoted so ranges are unambiguous to reparse — Android calls this the
 * "V2" hash representation).
 */
export function leafHashValueText(
  value: string | number | boolean,
  v2: boolean
): string {
  const type = typeof value;
  let text = type + ':';
  if (type === 'number') {
    text += doubleToIEEE754String(value as number);
  } else if (v2 && type === 'string') {
    text += hashQuotedString(value as string);
  } else {
    text += String(value);
  }
  return text;
}

/**
 * JSON-style quoting with only backslash and double quote escaped (the V2
 * hash grammar's string form).
 */
export function hashQuotedString(value: string): string {
  let escaped = value;
  if (escaped.indexOf('\\') !== -1) {
    escaped = escaped.replace(/\\/g, '\\\\');
  }
  if (escaped.indexOf('"') !== -1) {
    escaped = escaped.replace(/"/g, '\\"');
  }
  return '"' + escaped + '"';
}

export const priorityHashText = function (priority: string | number): string {
  return leafHashValueText(priority, /* v2= */ false);
};

/**
 * Validates that a priority snapshot Node is valid.
 */
export const validatePriorityNode = function (priorityNode: Node) {
  if (priorityNode.isLeafNode()) {
    const val = priorityNode.val();
    assert(
      typeof val === 'string' ||
        typeof val === 'number' ||
        (typeof val === 'object' && contains(val as Indexable, '.sv')),
      'Priority must be a string or number.'
    );
  } else {
    assert(
      priorityNode === MAX_NODE || priorityNode.isEmpty(),
      'priority of unexpected type.'
    );
  }
  // Don't call getPriority() on MAX_NODE to avoid hitting assertion.
  assert(
    priorityNode === MAX_NODE || priorityNode.getPriority().isEmpty(),
    "Priority nodes can't have a priority of their own."
  );
};

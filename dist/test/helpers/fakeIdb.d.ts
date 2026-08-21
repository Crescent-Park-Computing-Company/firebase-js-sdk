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
/**
 * Fake IndexedDB for the row manager: string keys, getAll/getAllKeys with
 * ranges, range deletes, multi-store transactions, versioned open with
 * upgrade. Shared `stores` gives multi-manager (multi-tab) tests one
 * storage substrate.
 *
 * Transaction semantics mirror the real API where the manager depends on
 * them: WRITES BUFFER per transaction and land on the shared Maps only at
 * commit (oncomplete); abort() discards the buffer and fires onabort, and
 * no further requests in that transaction run. Reads see the transaction's
 * own uncommitted writes layered over the committed state (IndexedDB
 * read-your-own-writes), while other transactions never observe them —
 * so tests CAN detect torn/partially-visible generations.
 */
export declare function makeFakeIdb(shared?: Map<string, Map<string, unknown>>, log?: {
    puts: string[];
    deletes: string[];
}): IDBFactory;
export declare const flushMicrotasks: (rounds?: number) => Promise<void>;
export declare const wait: (ms: number) => Promise<void>;

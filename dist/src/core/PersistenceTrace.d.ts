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
 * Diagnostic trace events published to an app-installed global sink
 * (`globalThis.__firebaseDatabasePersistenceTrace`). Observability only:
 * every emission is exception-guarded, and no wire or persistence behavior
 * depends on a sink being installed.
 *
 * The three event kinds cover the questions a production incident needs
 * answered without a DevTools trace:
 *
 * - `listen-outcome` — how each persistent default listen started
 *   (restored/cold/fallback) and its certification, with total wire bytes.
 * - `wire-message` — one event per server data operation as it arrives,
 *   with its payload size and which ingestion route it took. This is the
 *   event that attributes "who shipped the giant message" to a path.
 * - `flush` — one event per committed persistence generation, with range
 *   reuse counters and whether the flush baseline still shares identity
 *   with the live tree (a divorced baseline retains a second full tree
 *   until the next flush adopts the new one — the dominant steady-state
 *   memory cost of persistence).
 */
export type PersistenceTraceEvent = {
    type: 'listen-outcome';
    path: string;
    outcome: {
        mode: 'restored' | 'cold' | 'fallback';
        certified: boolean;
        bytes: number;
        reason?: string;
    };
} | {
    type: 'wire-message';
    path: string;
    kind: 'data' | 'merge' | 'rm';
    wireBytes: number;
    tagged: boolean;
    /**
     * First routing decision at arrival: applied synchronously, deferred
     * into the ordered ingest queue, or diverted to a sliced ingest. A
     * queued operation is not re-reported when the drain later applies it.
     */
    decision: 'sync' | 'queued' | 'sliced';
} | {
    type: 'flush';
    path: string;
    /** `commit` stored a generation; `empty` deleted the record. */
    mode: 'commit' | 'empty';
    ranges: number;
    rangesHashed: number;
    rangesReused: number;
    /**
     * Of the new tree's immediate children, how many ARE the baseline's
     * child objects (identity). 0-of-N with a baseline present = a full
     * divorce — the memory-doubling shape; N≈total = a cheap incremental
     * flush.
     */
    sharedChildren: number;
    totalChildren: number;
};
export declare function emitPersistenceTrace(event: PersistenceTraceEvent): void;
/**
 * Whether a trace sink is currently installed. Callers whose event
 * CONSTRUCTION is itself non-trivial (e.g. the flush event's baseline
 * identity scan) check this first so an uninstrumented session pays
 * nothing on the hot path.
 */
export declare function persistenceTraceSinkInstalled(): boolean;

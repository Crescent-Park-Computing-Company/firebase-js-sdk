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

/* istanbul ignore file — createRowHashKernel/workerMain are serialized
 * with Function.toString() into the Blob worker; instrumented bodies would
 * reference module-scope coverage counters that do not exist in the worker. */

/**
 * Streaming compound-hash kernel over persisted rows.
 *
 * Computes the wire compound hash ({posts, hashes}) directly from RowStore
 * rows — export-format JSON text keyed by relative path — without building
 * Node trees or materializing the workspace. Peak memory is one parsed row
 * plus the open range's canonical text.
 *
 * The kernel is a single self-contained factory with zero imports so the
 * SAME code runs in two places:
 *   - main thread / tests: `createRowHashKernel(sha1)` called directly;
 *   - the hash worker: `createRowHashKernel.toString()` is embedded in the
 *     worker script text and instantiated there (see HashWorker.ts).
 * Nothing inside the factory may reference module-scope identifiers.
 *
 * Grammar parity: emits byte-identical canonical range text to
 * CompoundHashBuilder walking the assembled Node (verified by tests):
 *   - children in nameCompare order, priority interleaved as a '.priority'
 *     pseudo-child before the first key sorting after it, and dropped when
 *     it would sort after every child (Android parity);
 *   - leaves as `type:value` with IEEE754-hex numbers and quoted strings,
 *     `priority:P:` prefix for leaf priorities;
 *   - ranges split when the open range's text exceeds the threshold, never
 *     directly after a '.priority' leaf; posts are last-leaf paths.
 *
 * Rows must be DISJOINT (the RowStore invariant). A violated invariant is
 * detected during the walk (a row nested inside the previous row's subtree)
 * and surfaces as an `overlap` error — the caller falls back to
 * assemble-then-hash, so the reported hash is always truthful.
 */

/** One row: relative path segments plus export-format JSON text. */
export interface KernelRow {
  path: string[];
  json: string;
}

export interface KernelCompoundHash {
  posts: string[];
  hashes: string[];
}

export interface RowHashKernel {
  /**
   * Hashes rows of ONE root. `splitThreshold` overrides the size-derived
   * default (tests pin it to compare against fixedSizeSplitStrategy).
   * When the factory received a `yieldFn`, the walk awaits it after every
   * `sliceBudgetBytes` of processed row text (default 256 KiB) so a large
   * root hashed on the main thread stays in bounded slices; without a
   * yieldFn the walk is one synchronous pass (worker / tests).
   * Rejects with an Error whose `message` is 'overlap' when the rows are
   * not disjoint.
   */
  hashRows(
    rows: KernelRow[],
    splitThreshold?: number,
    sliceBudgetBytes?: number
  ): Promise<KernelCompoundHash>;
}

export function createRowHashKernel(
  sha1Base64: (text: string) => Promise<string>,
  yieldFn?: () => Promise<void>
): RowHashKernel {
  // ---- name ordering (verbatim port of core/util nameCompare) ----
  const MIN_NAME = '[MIN_NAME]';
  const MAX_NAME = '[MAX_NAME]';
  const INTEGER_32_MIN = -2147483648;
  const INTEGER_32_MAX = 2147483647;
  // EXACT port of core/util INTEGER_REGEXP_: leading zeros are allowed
  // before up to ten significant digits ('00000000001' IS integer key 1).
  // A narrower pattern here diverges the kernel's child ordering from the
  // canonical nameCompare and breaks range-merge certification.
  const INTEGER_REGEXP = new RegExp('^-?(0*)\\d{1,10}$');
  const tryParseInt = (str: string): number | null => {
    if (INTEGER_REGEXP.test(str)) {
      const intVal = Number(str);
      if (intVal >= INTEGER_32_MIN && intVal <= INTEGER_32_MAX) {
        return intVal;
      }
    }
    return null;
  };
  const nameCompare = (a: string, b: string): number => {
    if (a === b) {
      return 0;
    } else if (a === MIN_NAME || b === MAX_NAME) {
      return -1;
    } else if (b === MIN_NAME || a === MAX_NAME) {
      return 1;
    } else {
      const aAsInt = tryParseInt(a),
        bAsInt = tryParseInt(b);
      if (aAsInt !== null) {
        if (bAsInt !== null) {
          return aAsInt - bAsInt === 0 ? a.length - b.length : aAsInt - bAsInt;
        } else {
          return -1;
        }
      } else if (bAsInt !== null) {
        return 1;
      } else {
        return a < b ? -1 : 1;
      }
    }
  };
  const comparePaths = (a: string[], b: string[]): number => {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) {
      const cmp = nameCompare(a[i], b[i]);
      if (cmp !== 0) {
        return cmp;
      }
    }
    return a.length - b.length;
  };

  // ---- leaf text (verbatim ports of doubleToIEEE754String and snap.ts) ----
  const ieee754Buffer = new DataView(new ArrayBuffer(8));
  const ieee754HexBytes: string[] = [];
  for (let i = 0; i < 256; i++) {
    ieee754HexBytes[i] = (i < 16 ? '0' : '') + i.toString(16);
  }
  const doubleToIEEE754String = (v: number): string => {
    ieee754Buffer.setFloat64(0, v);
    return (
      ieee754HexBytes[ieee754Buffer.getUint8(0)] +
      ieee754HexBytes[ieee754Buffer.getUint8(1)] +
      ieee754HexBytes[ieee754Buffer.getUint8(2)] +
      ieee754HexBytes[ieee754Buffer.getUint8(3)] +
      ieee754HexBytes[ieee754Buffer.getUint8(4)] +
      ieee754HexBytes[ieee754Buffer.getUint8(5)] +
      ieee754HexBytes[ieee754Buffer.getUint8(6)] +
      ieee754HexBytes[ieee754Buffer.getUint8(7)]
    );
  };
  const hashQuotedString = (value: string): string => {
    let escaped = value;
    if (escaped.indexOf('\\') !== -1) {
      escaped = escaped.replace(/\\/g, '\\\\');
    }
    if (escaped.indexOf('"') !== -1) {
      escaped = escaped.replace(/"/g, '\\"');
    }
    return '"' + escaped + '"';
  };
  const leafHashValueText = (value: string | number | boolean): string => {
    const type = typeof value;
    let text = type + ':';
    if (type === 'number') {
      text += doubleToIEEE754String(value as number);
    } else if (type === 'string') {
      text += hashQuotedString(value as string);
    } else {
      text += String(value);
    }
    return text;
  };

  // ---- export-format helpers ----
  type ExportValue = string | number | boolean | ExportObject;
  interface ExportObject {
    [key: string]: ExportValue;
  }
  const isLeafValue = (v: ExportValue): boolean =>
    v === null || typeof v !== 'object' || '.value' in (v as ExportObject);
  const leafRepresentation = (v: ExportValue): string => {
    if (typeof v === 'object' && v !== null) {
      const wrapped = v as ExportObject;
      const priority = wrapped['.priority'];
      let text = '';
      if (priority !== undefined) {
        text +=
          'priority:' + leafHashValueText(priority as string | number) + ':';
      }
      return (
        text + leafHashValueText(wrapped['.value'] as string | number | boolean)
      );
    }
    return leafHashValueText(v as string | number | boolean);
  };

  // ---- range builder (verbatim CompoundHashBuilder text semantics) ----
  class Builder {
    posts: string[] = [];
    hashTexts: string[] = [];
    private currentHash_: string | null = null;
    private currentPath_: string[] = [];
    private currentDepth_ = 0;
    private lastLeafDepth_ = -1;
    private needsComma_ = true;

    constructor(private splitThreshold_: number) {}

    private ensureRange_(): void {
      if (this.currentHash_ === null) {
        let hash = '(';
        for (let i = 0; i < this.currentDepth_; i++) {
          hash += hashQuotedString(this.currentPath_[i]) + ':(';
        }
        this.currentHash_ = hash;
        this.needsComma_ = false;
      }
    }

    startChild(key: string): void {
      this.ensureRange_();
      if (this.needsComma_) {
        this.currentHash_ += ',';
      }
      this.currentHash_ += hashQuotedString(key) + ':(';
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
        this.currentHash_ += ')';
      }
      this.needsComma_ = true;
    }

    processLeaf(leafText: string): void {
      this.ensureRange_();
      this.lastLeafDepth_ = this.currentDepth_;
      this.currentHash_ += leafText;
      this.needsComma_ = true;
      if (
        this.currentHash_!.length > this.splitThreshold_ &&
        this.currentPath_[this.currentDepth_ - 1] !== '.priority'
      ) {
        this.endRange_();
      }
    }

    finish(): void {
      if (this.currentHash_ !== null) {
        this.endRange_();
      }
    }

    private endRange_(): void {
      let hash = this.currentHash_!;
      for (let i = 0; i < this.currentDepth_; i++) {
        hash += ')';
      }
      hash += ')';
      this.hashTexts.push(hash);
      const post = this.currentPath_.slice(0, this.lastLeafDepth_).join('/');
      this.posts.push(post === '' ? '/' : post);
      this.currentHash_ = null;
      this.needsComma_ = true;
    }
  }

  /**
   * Walks one export value at the builder's current position. `hasLaterRowSibling`
   * reports whether, for the CURRENT row's root object, a later row exists
   * under the same parent whose key sorts after '.priority' — needed for the
   * trailing-priority drop rule when a split node's priority lives in its own
   * pseudo-row (handled by the caller); inside one row the rule is local.
   */
  const walkValue = (builder: Builder, value: ExportValue): void => {
    if (isLeafValue(value)) {
      builder.processLeaf(leafRepresentation(value));
      return;
    }
    const obj = value as ExportObject;
    const keys: string[] = [];
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        keys.push(key);
      }
    }
    keys.sort(nameCompare);
    // Trailing-priority drop (Android grammar): a '.priority' key sorting
    // after every other child is omitted from the hash.
    let emitKeys = keys;
    if (keys.length > 0 && keys[keys.length - 1] === '.priority') {
      emitKeys = keys.slice(0, keys.length - 1);
    }
    for (let i = 0; i < emitKeys.length; i++) {
      const key = emitKeys[i];
      builder.startChild(key);
      if (key === '.priority') {
        builder.processLeaf(leafHashValueText(obj[key] as string | number));
      } else {
        walkValue(builder, obj[key]);
      }
      builder.endChild();
    }
  };

  const hashRows = async (
    rows: KernelRow[],
    splitThreshold?: number,
    sliceBudgetBytes?: number
  ): Promise<KernelCompoundHash> => {
    if (rows.length === 0) {
      return { posts: [], hashes: [''] };
    }
    const sliceBudget =
      sliceBudgetBytes !== undefined ? sliceBudgetBytes : 256 * 1024;
    let sliceSpent = 0;
    const sorted = rows.slice().sort((a, b) => comparePaths(a.path, b.path));
    let threshold: number;
    if (splitThreshold !== undefined) {
      threshold = Math.max(512, Math.floor(splitThreshold));
    } else {
      // Android's SimpleSizeSplitStrategy, sized from total row bytes (a
      // serialized-size estimate of the whole tree).
      let totalBytes = 0;
      for (let i = 0; i < sorted.length; i++) {
        totalBytes += sorted[i].json.length;
      }
      threshold = Math.max(512, Math.floor(Math.sqrt(totalBytes * 100)));
    }
    const builder = new Builder(threshold);
    let openDepth = 0;
    for (let i = 0; i < sorted.length; i++) {
      const path = sorted[i].path;
      if (i > 0) {
        const prev = sorted[i - 1].path;
        let common = 0;
        while (
          common < prev.length &&
          common < path.length &&
          prev[common] === path[common]
        ) {
          common++;
        }
        if (common === prev.length) {
          // This row's path is inside (or equal to) the previous row's
          // subtree — the disjointness invariant is violated and streaming
          // would interleave two serializations of one subtree.
          throw new Error('overlap');
        }
        while (openDepth > common) {
          builder.endChild();
          openDepth--;
        }
      }
      const isPriorityRow =
        path.length > 0 && path[path.length - 1] === '.priority';
      if (isPriorityRow) {
        // A split node's priority pseudo-row. Trailing-drop rule against ROW
        // siblings: emit only when a later row still sits under the same
        // parent (rows are in nameCompare order, so any such row's key
        // sorts after '.priority').
        const parentLen = path.length - 1;
        const next = i + 1 < sorted.length ? sorted[i + 1].path : null;
        let hasLaterSibling = next !== null && next.length > parentLen;
        for (let d = 0; hasLaterSibling && d < parentLen; d++) {
          if (next![d] !== path[d]) {
            hasLaterSibling = false;
          }
        }
        if (!hasLaterSibling) {
          continue;
        }
      }
      while (openDepth < path.length) {
        builder.startChild(path[openDepth]);
        openDepth++;
      }
      if (isPriorityRow) {
        builder.processLeaf(
          leafHashValueText(JSON.parse(sorted[i].json) as string | number)
        );
      } else {
        walkValue(builder, JSON.parse(sorted[i].json) as ExportValue);
      }
      if (yieldFn !== undefined) {
        sliceSpent += sorted[i].json.length;
        if (sliceSpent >= sliceBudget) {
          sliceSpent = 0;
          await yieldFn();
        }
      }
    }
    while (openDepth > 0) {
      builder.endChild();
      openDepth--;
    }
    builder.finish();
    const hashes = await Promise.all(
      builder.hashTexts.map(text => sha1Base64(text))
    );
    hashes.push('');
    return { posts: builder.posts, hashes };
  };

  return { hashRows };
}

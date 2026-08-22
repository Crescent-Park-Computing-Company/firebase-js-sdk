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

import { createRowHashKernel } from '../src/core/RowHashKernel';
import { RowPersistenceManager } from '../src/core/RowPersistence';
import { splitNodeIntoRows } from '../src/core/RowStore';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { Path } from '../src/core/util/Path';
import { sha1 } from '../src/core/util/util';

import { makeFakeIdb } from './helpers/fakeIdb';

const locks = {
  request: (
    _n: string,
    _o: { mode: 'exclusive' },
    cb: (l: unknown) => Promise<unknown>
  ): Promise<unknown> => Promise.resolve().then(() => cb({}))
};

describe('perf smoke v3 (node, fake IDB)', function () {
  this.timeout(120000);
  it('measures snapshot flush / identity skip / delta flush / restore / hash on a ~43MB tree', async () => {
    const workspace: Record<string, unknown> = {};
    for (let s = 0; s < 40; s++) {
      const section: Record<string, unknown> = {};
      for (let d = 0; d < 60; d++) {
        section['doc' + d] = {
          title: 'Document ' + s + '-' + d,
          body: 'lorem ipsum '.repeat(1500) + d,
          meta: { createdAt: 1700000000000 + d, tags: { '0': 'a', '1': 'b' } }
        };
      }
      workspace['section' + s] = section;
    }
    const node = nodeFromJSON(workspace);
    const shared = new Map<string, Map<string, unknown>>();
    const manager = new RowPersistenceManager(
      'perf',
      makeFakeIdb(shared),
      locks as never,
      1,
      1,
      16 * 1024,
      30000,
      300000,
      60000,
      2 * 1024 * 1024
    );
    manager.setAuthScope('u');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');

    let maxGap = 0;
    let last = Date.now();
    let watching = true;
    const tick = (): void => {
      const now = Date.now();
      maxGap = Math.max(maxGap, now - last);
      last = now;
      if (watching) {
        setTimeout(tick, 0);
      }
    };
    setTimeout(tick, 0);

    const t0 = Date.now();
    manager.serverCacheUpdated(new Path('/ws'), node);
    await manager.flushNow('/ws');
    const tFirst = Date.now() - t0;

    // Identity-equal update: must be free (no writes, no serialization).
    const t1 = Date.now();
    manager.serverCacheUpdated(new Path('/ws'), node);
    await manager.flushNow('/ws');
    const tSkip = Date.now() - t1;

    // One changed leaf: the WeakMap cache reuses every unchanged top-level
    // child's serialized payload — cost tracks the CHANGED subtree.
    const node2 = node.updateChild(
      new Path('section3/doc7/title'),
      nodeFromJSON('CHANGED')
    );
    const t2 = Date.now();
    manager.serverCacheUpdated(new Path('/ws'), node2, [
      ['section3', 'doc7', 'title']
    ]);
    await manager.flushNow('/ws');
    const tDelta = Date.now() - t2;
    watching = false;

    const reader = new RowPersistenceManager(
      'perf',
      makeFakeIdb(shared),
      locks as never,
      1,
      1,
      16 * 1024,
      30000,
      300000,
      60000,
      2 * 1024 * 1024
    );
    reader.setAuthScope('u');
    reader.setPersistentPath('/ws', true);
    reader.track('/ws');
    const t3 = Date.now();
    const restored = await reader.restoreForListen('/ws');
    const tRestore = Date.now() - t3;

    const rows = splitNodeIntoRows([], node2, 16 * 1024).map(
      ([path, json]) => ({
        path,
        json
      })
    );
    const t4 = Date.now();
    const kernel = createRowHashKernel(text => Promise.resolve(sha1(text)));
    const hash = await kernel.hashRows(rows);
    const tHash = Date.now() - t4;
    // eslint-disable-next-line no-console
    console.log(
      `      firstFlush=${tFirst}ms identitySkip=${tSkip}ms deltaFlush=${tDelta}ms restore=${tRestore}ms kernelHash=${tHash}ms ranges=${
        hash.posts.length
      } maxStall=${maxGap}ms chunks=${shared.get('chunks')!.size}`
    );
    // eslint-disable-next-line no-console
    console.log(
      `      restored equals: ${
        restored.node !== null && restored.node.equals(node2)
      }`
    );
    manager.dispose();
    reader.dispose();
  });
});

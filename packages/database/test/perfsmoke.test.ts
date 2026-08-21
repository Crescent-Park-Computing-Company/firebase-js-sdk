import { RowPersistenceManager } from '../src/core/RowPersistence';
import { createRowHashKernel } from '../src/core/RowHashKernel';
import { splitNodeIntoRows } from '../src/core/RowStore';
import { nodeFromJSON } from '../src/core/snap/nodeFromJSON';
import { sha1 } from '../src/core/util/util';
import { Path } from '../src/core/util/Path';
import { makeFakeIdb, flushMicrotasks, wait } from './helpers/fakeIdb';

describe('perf smoke (node, fake IDB)', function () {
  this.timeout(120000);
  it('measures split / restore / incremental flush / kernel hash on a ~50MB tree', async () => {
    // Build a Mana-shaped workspace: many mid-size documents.
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
    const t0 = Date.now();
    const node = nodeFromJSON(workspace);
    const tBuild = Date.now() - t0;

    const t1 = Date.now();
    const rows = splitNodeIntoRows([], node, 16 * 1024);
    let totalBytes = 0;
    for (const [, json] of rows) {
      totalBytes += json.length;
    }
    const tSplit = Date.now() - t1;
    // eslint-disable-next-line no-console
    console.log(
      `      tree=${(totalBytes / 1e6).toFixed(1)}MB rows=${rows.length} build=${tBuild}ms split=${tSplit}ms`
    );

    const shared = new Map<string, Map<string, unknown>>();
    const manager = new RowPersistenceManager(
      'perf', makeFakeIdb(shared), null, 1, 1, 16 * 1024, 30000, 300000, 60000, 4 << 20
    );
    manager.setAuthScope('u');
    manager.setPersistentPath('/ws', true);
    manager.track('/ws');
    const t2 = Date.now();
    manager.serverCacheUpdated(new Path('/ws'), node);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const tFirstGen = Date.now() - t2;

    // Incremental: one leaf change.
    const node2 = node.updateChild(
      new Path('section3/doc7/title'),
      nodeFromJSON('CHANGED')
    );
    const t3 = Date.now();
    manager.serverCacheUpdated(new Path('/ws'), node2, [
      ['section3', 'doc7', 'title']
    ]);
    await manager.flushNow('/ws');
    await flushMicrotasks();
    const tIncr = Date.now() - t3;

    // Restore.
    const reader = new RowPersistenceManager(
      'perf', makeFakeIdb(shared), null, 1, 1, 16 * 1024, 30000, 300000, 60000, 4 << 20
    );
    reader.setAuthScope('u');
    reader.setPersistentPath('/ws', true);
    reader.track('/ws');
    const t4 = Date.now();
    const restored = await reader.restoreForListen('/ws');
    const tRestore = Date.now() - t4;

    // Kernel hash (main-thread run; the worker does the identical walk).
    const kernelRows = rows.map(([path, json]) => ({ path, json }));
    const t5 = Date.now();
    const kernel = createRowHashKernel(text => Promise.resolve(sha1(text)));
    const hash = await kernel.hashRows(kernelRows);
    const tHash = Date.now() - t5;
    // eslint-disable-next-line no-console
    console.log(
      `      firstGen=${tFirstGen}ms incrementalFlush=${tIncr}ms restore=${tRestore}ms kernelHash=${tHash}ms ranges=${hash.posts.length}`
    );
    // eslint-disable-next-line no-console
    console.log(
      `      restored equals: ${restored.node !== null && restored.node.equals(node2)}`
    );
    manager.dispose();
    reader.dispose();
  });
});

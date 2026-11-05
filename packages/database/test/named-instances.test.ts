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

import { deleteApp, FirebaseApp, initializeApp } from '@firebase/app';
import { expect } from 'chai';

import { ref } from '../src/api/Reference_impl';
import {
  getDatabase,
  setPersistenceAuthScope,
  setPersistenceEnabled
} from '../src/api/Database';
import { _setWebLocksForTesting } from '../src/core/Persistence';

_setWebLocksForTesting(null);

const URL = 'https://named-instances-test.firebaseio.com';
let appSeq = 0;

describe('Named database instances', () => {
  let app: FirebaseApp;

  beforeEach(() => {
    app = initializeApp({ databaseURL: URL }, 'named-instances-' + appSeq++);
  });

  afterEach(() => deleteApp(app));

  it('returns one instance per name, separate from the default', () => {
    const main = getDatabase(app);
    const chats = getDatabase(app, undefined, 'chats');
    const meta = getDatabase(app, undefined, 'meta');
    expect(getDatabase(app, undefined, 'chats')).to.equal(chats);
    expect(getDatabase(app)).to.equal(main);
    expect(new Set([main, chats, meta]).size).to.equal(3);
    const repos = [main, chats, meta].map(db => db._repoInternal);
    expect(new Set(repos).size).to.equal(3);
    expect(new Set(repos.map(repo => repo.key)).size).to.equal(3);
  });

  it('keeps the wire target and the default instance key unchanged', () => {
    const main = getDatabase(app);
    const chats = getDatabase(app, undefined, 'chats');
    expect(main._repoInternal.key).to.equal(URL + '/');
    expect(chats._repoInternal.repoInfo_.toURLString()).to.equal(URL + '/');
    expect(ref(chats, 'a/b').toString()).to.equal(URL + '/a/b');
  });

  it('a named instance of a custom URL is keyed by both', () => {
    const other = 'https://other-db.firebaseio.com';
    const a = getDatabase(app, other, 'a');
    expect(getDatabase(app, other, 'a')).to.equal(a);
    expect(getDatabase(app, undefined, 'a')).to.not.equal(a);
    expect(ref(a).toString()).to.equal(other + '/');
  });

  it("shares the app's Auth", () => {
    const repos = [getDatabase(app), getDatabase(app, undefined, 'chats')].map(
      db => db._repoInternal
    );
    const names = repos.map(
      repo =>
        (repo.authTokenProvider_ as unknown as { appName_: string }).appName_
    );
    expect(names).to.deep.equal([app.name, app.name]);
  });

  it('persists each instance under its own namespace', () => {
    const dbs = [
      getDatabase(app),
      getDatabase(app, undefined, 'chats'),
      getDatabase(app, undefined, 'meta')
    ];
    for (const db of dbs) {
      setPersistenceEnabled(db, true);
      setPersistenceAuthScope(db, 'u1');
    }
    const prefixes = dbs.map(
      db =>
        (db._repoInternal.persistence_ as unknown as { prefix_: string })
          .prefix_
    );
    expect(prefixes[0]).to.equal(URL + '/');
    expect(new Set(prefixes).size).to.equal(3);
    expect(prefixes[1]).to.equal(URL + '/<chats>');
  });

  it('rejects names that could collide with the instance key', () => {
    expect(() => getDatabase(app, undefined, 'a<b>')).to.throw(
      /instance names/
    );
    expect(() => getDatabase(app, undefined, '')).to.throw(/instance names/);
  });

  it('deleting the app releases every instance', async () => {
    const name = app.name;
    getDatabase(app);
    getDatabase(app, undefined, 'chats');
    await deleteApp(app);
    app = initializeApp({ databaseURL: URL }, name);
    expect(() => getDatabase(app, undefined, 'chats')).to.not.throw();
  });
});

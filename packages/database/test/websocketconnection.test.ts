/**
 * @license
 * Copyright 2022 Google LLC
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

import { expect } from 'chai';

import { RepoInfo } from '../src/core/RepoInfo';
import { APPLICATION_ID_PARAM } from '../src/realtime/Constants';
import { WebSocketConnection } from '../src/realtime/WebSocketConnection';

function testRepoInfo(): RepoInfo {
  return new RepoInfo(
    'test-ns.firebaseio.com',
    true,
    'test-ns',
    false,
    undefined,
    undefined,
    false,
    false
  );
}

describe('WebSocketConnection', () => {
  it('should add an applicationId to the query parameter', () => {
    const repoInfo = testRepoInfo();
    const applicationId = 'myID';
    const websocketConnection = new WebSocketConnection(
      'connId',
      repoInfo,
      applicationId
    );
    const searchParams = new URL(websocketConnection.connURL).searchParams;
    expect(searchParams.get(APPLICATION_ID_PARAM)).to.equal(applicationId);
  });
  it('should not add an applicationId to the query parameter if applicationId is empty', () => {
    const repoInfo = testRepoInfo();
    const applicationId = '';
    const websocketConnection = new WebSocketConnection(
      'connId',
      repoInfo,
      applicationId
    );
    const searchParams = new URL(websocketConnection.connURL).searchParams;
    expect(searchParams.get(APPLICATION_ID_PARAM)).to.be.null;
  });
  it('delivers original frame bytes beside the parsed message', () => {
    const repoInfo = testRepoInfo();
    const connection = new WebSocketConnection('connId', repoInfo, 'app');
    const seen: Array<{ message: unknown; bytes: number | undefined }> = [];
    // Avoid opening a real socket; handleIncomingFrame only needs a non-null marker.
    connection.mySock = {} as WebSocket;
    connection.onMessage = (message, bytes) => seen.push({ message, bytes });
    const payload = JSON.stringify({ t: 'd', d: { a: 'd', b: { p: '/x' } } });
    connection.handleIncomingFrame({ data: payload });
    expect(seen).to.deep.equal([
      { message: JSON.parse(payload), bytes: payload.length }
    ]);
  });

  it('sums split WebSocket frame bytes without reserializing the payload', () => {
    const repoInfo = testRepoInfo();
    const connection = new WebSocketConnection('connId', repoInfo, 'app');
    let bytes: number | undefined;
    connection.mySock = {} as WebSocket;
    connection.onMessage = (_message, received) => {
      bytes = received;
    };
    const payload = JSON.stringify({ t: 'd', d: { value: 'x'.repeat(100) } });
    const midpoint = Math.ceil(payload.length / 2);
    connection.handleIncomingFrame({ data: '2' });
    connection.handleIncomingFrame({ data: payload.slice(0, midpoint) });
    connection.handleIncomingFrame({ data: payload.slice(midpoint) });
    expect(bytes).to.equal(payload.length + 1);
  });

  it('counts non-ASCII payloads in UTF-8 wire bytes', () => {
    const connection = new WebSocketConnection('connId', testRepoInfo(), 'app');
    let bytes: number | undefined;
    connection.mySock = {} as WebSocket;
    connection.onMessage = (_message, received) => {
      bytes = received;
    };
    const payload = JSON.stringify({ t: 'd', d: { value: 'سلام 🌍' } });
    connection.handleIncomingFrame({ data: payload });
    expect(bytes).to.equal(new TextEncoder().encode(payload).length);
  });
});

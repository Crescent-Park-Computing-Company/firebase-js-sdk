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

  it('does not tear down and re-arm the keepalive timer on every frame', () => {
    // A large message arrives as thousands of 16KB frames; recreating the
    // interval timer per frame burned ~38% of the receive window in
    // clearInterval/setInterval churn (measured on a ~90MB push). Activity
    // must be tracked without re-arming the timer.
    const connection = new WebSocketConnection('connId', testRepoInfo(), 'app');
    connection.mySock = {} as WebSocket;
    connection.onMessage = () => {};
    const originalSetInterval = global.setInterval;
    let arms = 0;
    (global as unknown as Record<string, unknown>).setInterval = ((
      ...args: Parameters<typeof setInterval>
    ) => {
      arms++;
      return originalSetInterval(...args);
    }) as typeof setInterval;
    try {
      connection.handleIncomingFrame({ data: '3' });
      const payload = JSON.stringify({ t: 'd', d: { value: 'x'.repeat(64) } });
      const mid = Math.ceil(payload.length / 2);
      connection.handleIncomingFrame({ data: payload.slice(0, mid) });
      connection.handleIncomingFrame({ data: payload.slice(mid) });
      expect(arms).to.be.at.most(1, 'one timer arm for any number of frames');
    } finally {
      (global as unknown as Record<string, unknown>).setInterval =
        originalSetInterval;
      if (connection.keepaliveTimer !== null) {
        clearInterval(connection.keepaliveTimer);
        connection.keepaliveTimer = null;
      }
    }
  });

  it('still sends the no-op ping after a full quiet interval', () => {
    const connection = new WebSocketConnection('connId', testRepoInfo(), 'app');
    const sent: string[] = [];
    connection.mySock = {
      send: (s: string) => {
        sent.push(s);
      }
    } as unknown as WebSocket;
    connection.onMessage = () => {};
    const originalSetInterval = global.setInterval;
    let tick: (() => void) | null = null;
    (global as unknown as Record<string, unknown>).setInterval = ((
      handler: () => void
    ) => {
      tick = handler;
      return 1 as unknown as ReturnType<typeof setInterval>;
    }) as typeof setInterval;
    const originalNow = Date.now;
    let now = 1_000_000;
    Date.now = () => now;
    try {
      connection.resetKeepAlive();
      expect(tick).to.not.equal(null);
      // Activity 1s ago: tick must NOT ping.
      now += 1_000;
      tick!();
      expect(sent).to.deep.equal([]);
      // A full quiet interval (45s): tick pings exactly once...
      now += 45_000;
      tick!();
      expect(sent).to.deep.equal(['0']);
      // ...and the ping itself resets the quiet window.
      now += 1_000;
      tick!();
      expect(sent).to.deep.equal(['0']);
    } finally {
      Date.now = originalNow;
      (global as unknown as Record<string, unknown>).setInterval =
        originalSetInterval;
      connection.keepaliveTimer = null;
    }
  });
});

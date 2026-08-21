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
 * Yields one macrotask. MessageChannel where available: unlike setTimeout(0),
 * ports are exempt from the nested-timer clamp (~4ms after a few levels),
 * which would otherwise stretch a many-slice computation by whole seconds
 * exactly on the slow boots it is meant to help.
 *
 * Node port lifecycle: a referenced MessagePort keeps the Node event loop
 * alive, so the ports are referenced only while yields are pending and
 * unref'd once the queue drains — an idle channel must not block a Node
 * consumer's otherwise-clean shutdown. The direction matters both ways: a
 * PERMANENTLY unref'd port is wrong too, because Node drops delivery when
 * no other handle holds the loop and the yield would never resolve.
 * Browsers have no ref/unref on ports; the optional calls are no-ops there.
 */
interface UnrefablePort {
  ref?: () => void;
  unref?: () => void;
}
let yieldChannel: MessageChannel | null = null;
const yieldResolvers: Array<() => void> = [];
function setPortsReferenced(referenced: boolean): void {
  for (const port of [yieldChannel!.port1, yieldChannel!.port2]) {
    const p = port as unknown as UnrefablePort;
    if (referenced) {
      p.ref?.();
    } else {
      p.unref?.();
    }
  }
}
export function yieldMacrotask(): Promise<void> {
  if (typeof MessageChannel === 'undefined') {
    return new Promise(resolve => setTimeout(resolve, 0));
  }
  if (yieldChannel === null) {
    yieldChannel = new MessageChannel();
    // Installing onmessage references the port in Node; start idle-unref'd.
    yieldChannel.port1.onmessage = () => {
      yieldResolvers.shift()?.();
      if (yieldResolvers.length === 0) {
        setPortsReferenced(false);
      }
    };
    setPortsReferenced(false);
  }
  return new Promise(resolve => {
    yieldResolvers.push(resolve);
    setPortsReferenced(true);
    yieldChannel!.port2.postMessage(null);
  });
}

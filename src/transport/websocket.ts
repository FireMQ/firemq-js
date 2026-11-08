// WebSocket transport: one binary message per frame, without the length prefix and without
// the `FMQ1` magic. Works with the global WebSocket (browsers, Deno, Bun, Node 22+), the `ws`
// package, and the built-in Node client.

import { decodeFrame, MAX_HELLO_FRAME } from '../codec.js';
import { ClosedError, ConnectionError, describeError, ProtocolError } from '../errors.js';
import type { ConnectOptions, WebSocketConstructor, WebSocketLike } from '../options.js';
import { type ConnectUrl, webSocketUrl } from '../url.js';
import type { Transport } from './types.js';

/** WebSocket subprotocol of the FireMQ protocol, version 1. */
export const SUBPROTOCOL = 'firemq.v1';

const HIGH_WATER = 1024 * 1024;
const OPEN = 1;

function toBytes(data: unknown): Uint8Array | undefined {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  return undefined;
}

/** Opens a WebSocket transport with the given implementation. */
export function dialWebSocket(
  url: ConnectUrl,
  options: ConnectOptions,
  Impl: WebSocketConstructor,
): Promise<Transport> {
  const target = webSocketUrl(url);
  const timeoutMs = options.connectTimeout ?? 10_000;
  return new Promise<Transport>((resolve, reject) => {
    let ws: WebSocketLike;
    try {
      ws = new Impl(target, SUBPROTOCOL);
    } catch (e) {
      reject(new ConnectionError(`cannot open WebSocket to ${target}: ${(e as Error).message}`, { cause: e }));
      return;
    }
    ws.binaryType = 'arraybuffer';
    let maxFrame = MAX_HELLO_FRAME;
    let opened = false;
    let closed = false;
    let lastError: unknown;

    const transport: Transport = {
      send(frame) {
        if (ws.readyState !== OPEN) return true;
        ws.send(frame.subarray(4));
        return ws.bufferedAmount < HIGH_WATER;
      },
      async drain() {
        while (!closed && ws.readyState === OPEN && ws.bufferedAmount >= HIGH_WATER / 2) {
          await new Promise((r) => setTimeout(r, 5));
        }
      },
      setMaxFrame(bytes) {
        maxFrame = bytes;
      },
      close() {
        try {
          ws.close(1000);
        } catch {
          // already closing
        }
      },
      onframe: () => {},
      onclose: () => {},
    };

    const timer = setTimeout(() => {
      if (opened) return;
      fail(new ConnectionError(`connecting to ${target} timed out`));
      try {
        ws.close();
      } catch {
        // ignore
      }
    }, timeoutMs);

    const fail = (err: Error) => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      if (opened) transport.onclose(err);
      else reject(err);
    };

    ws.onopen = () => {
      opened = true;
      clearTimeout(timer);
      resolve(transport);
    };
    ws.onerror = (ev) => {
      lastError = ev?.error ?? ev?.message ?? lastError;
    };
    ws.onclose = (ev) => {
      if (!opened) {
        const detail = describeError(lastError) || (ev?.code ? `closed with code ${ev.code}` : '');
        fail(new ConnectionError(`cannot open WebSocket to ${target}${detail ? `: ${detail}` : ''}`, { cause: lastError }));
        return;
      }
      const reason = ev?.reason ? `: ${ev.reason}` : '';
      fail(new ClosedError(`WebSocket closed (code ${ev?.code ?? 1006})${reason}`));
    };
    ws.onmessage = (ev) => {
      if (closed) return;
      const bytes = toBytes(ev.data);
      try {
        if (!bytes) throw new ProtocolError('received a text WebSocket message; FireMQ frames are binary');
        if (bytes.byteLength > maxFrame) {
          throw new ProtocolError(`frame of ${bytes.byteLength} bytes exceeds the maximum of ${maxFrame} bytes`);
        }
        transport.onframe(decodeFrame(bytes));
      } catch (e) {
        fail(e as Error);
        transport.close();
      }
    };
  });
}

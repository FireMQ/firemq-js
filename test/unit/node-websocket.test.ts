import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import * as net from 'node:net';
import { describe, it } from 'node:test';

import { NodeWebSocket } from '../../src/transport/node-websocket.js';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function serverFrame(opcode: number, payload: Uint8Array, fin = true): Buffer {
  const len = payload.length;
  const head = len < 126 ? Buffer.from([0, len]) : len < 65536 ? Buffer.alloc(4) : Buffer.alloc(10);
  if (len >= 126 && len < 65536) {
    head[1] = 126;
    head.writeUInt16BE(len, 2);
  } else if (len >= 65536) {
    head[1] = 127;
    head.writeBigUInt64BE(BigInt(len), 2);
  }
  head[0] = (fin ? 0x80 : 0) | opcode;
  return Buffer.concat([head, payload]);
}

/** Reads one masked client frame from the buffer, or returns undefined. */
function clientFrame(buf: Buffer): { opcode: number; payload: Buffer; size: number } | undefined {
  if (buf.length < 2) return undefined;
  let len = buf[1]! & 0x7f;
  let off = 2;
  if (len === 126) {
    len = buf.readUInt16BE(2);
    off = 4;
  } else if (len === 127) {
    len = Number(buf.readBigUInt64BE(2));
    off = 10;
  }
  assert.ok(buf[1]! & 0x80, 'client frames must be masked');
  if (buf.length < off + 4 + len) return undefined;
  const mask = buf.subarray(off, off + 4);
  const payload = Buffer.from(buf.subarray(off + 4, off + 4 + len));
  for (let i = 0; i < payload.length; i++) payload[i]! ^= mask[i & 3]!;
  return { opcode: buf[0]! & 0x0f, payload, size: off + 4 + len };
}

/** A one-connection WebSocket server running `script` after the handshake. */
async function withServer(
  script: (socket: net.Socket, frames: AsyncGenerator<{ opcode: number; payload: Buffer }>) => Promise<void>,
): Promise<{ url: string; done: Promise<void>; requestHead: Promise<string> }> {
  let resolveHead!: (h: string) => void;
  const requestHead = new Promise<string>((r) => (resolveHead = r));
  let finished!: Promise<void>;
  const server = net.createServer((socket) => {
    let buf = Buffer.alloc(0);
    let wake: (() => void) | undefined;
    socket.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      wake?.();
    });
    const more = () => new Promise<void>((r) => (wake = r));
    async function* frames() {
      for (;;) {
        const f = clientFrame(buf);
        if (f) {
          buf = buf.subarray(f.size);
          yield f;
        } else {
          await more();
        }
      }
    }
    finished = (async () => {
      while (!buf.includes('\r\n\r\n')) await more();
      const end = buf.indexOf('\r\n\r\n');
      const head = buf.subarray(0, end).toString();
      buf = buf.subarray(end + 4);
      resolveHead(head);
      const key = /sec-websocket-key: (.+)/i.exec(head)![1]!.trim();
      const accept = createHash('sha1').update(key + GUID).digest('base64');
      socket.write(
        `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\nSec-WebSocket-Protocol: firemq.v1\r\n\r\n`,
      );
      await script(socket, frames());
      server.close();
    })();
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as net.AddressInfo).port;
  return { url: `ws://127.0.0.1:${port}/fire?vhost=dev`, done: new Promise((r) => setTimeout(r, 0)).then(() => finished), requestHead };
}

function open(ws: NodeWebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = (e) => reject(e.error);
  });
}

describe('NodeWebSocket', () => {
  it('handshakes, exchanges binary messages, reassembles fragments and answers pings', async () => {
    const big = Buffer.alloc(70_000, 7);
    const { url, done, requestHead } = await withServer(async (socket, frames) => {
      // Echo the first client message back in three fragments with a ping in between.
      const first = (await frames.next()).value!;
      assert.equal(first.opcode, 0x2);
      socket.write(serverFrame(0x2, first.payload.subarray(0, 2), false));
      socket.write(serverFrame(0x9, Buffer.from('hb')));
      socket.write(serverFrame(0x0, first.payload.subarray(2, 4), false));
      socket.write(serverFrame(0x0, first.payload.subarray(4), true));
      const pong = (await frames.next()).value!;
      assert.equal(pong.opcode, 0xa);
      assert.equal(pong.payload.toString(), 'hb');
      // A large message with a 64-bit length, split across writes.
      const second = (await frames.next()).value!;
      assert.equal(second.payload.length, big.length);
      const frame = serverFrame(0x2, second.payload);
      socket.write(frame.subarray(0, 5));
      socket.write(frame.subarray(5));
      // Close from the server; the client must echo the close.
      socket.write(serverFrame(0x8, Buffer.from([0x03, 0xe8, 0x62, 0x79, 0x65])));
      const close = (await frames.next()).value!;
      assert.equal(close.opcode, 0x8);
      assert.equal(close.payload.readUInt16BE(0), 1000);
      socket.end();
    });
    const ws = new NodeWebSocket(url, 'firemq.v1');
    const messages: Uint8Array[] = [];
    const closed = new Promise<{ code: number; reason: string }>((r) => (ws.onclose = r));
    ws.onmessage = (ev) => messages.push(ev.data as Uint8Array);
    await open(ws);
    assert.equal(ws.protocol, 'firemq.v1');
    const head = await requestHead;
    assert.match(head, /^GET \/fire\?vhost=dev HTTP\/1\.1/);
    assert.match(head, /Sec-WebSocket-Protocol: firemq\.v1/);
    ws.send(new Uint8Array([1, 2, 3, 4, 5, 6]));
    while (messages.length < 1) await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual([...messages[0]!], [1, 2, 3, 4, 5, 6]);
    ws.send(big);
    while (messages.length < 2) await new Promise((r) => setTimeout(r, 5));
    assert.equal(messages[1]!.length, big.length);
    assert.equal(messages[1]![69_999], 7);
    const ev = await closed;
    assert.equal(ev.code, 1000);
    assert.equal(ev.reason, 'bye');
    assert.equal(ws.readyState, NodeWebSocket.CLOSED);
    await done;
  });

  it('reports a failed handshake', async () => {
    const server = net.createServer((s) => s.end('HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n'));
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as net.AddressInfo).port;
    const ws = new NodeWebSocket(`ws://127.0.0.1:${port}/fire`);
    const err = await new Promise<Error>((resolve) => (ws.onerror = (e) => resolve(e.error)));
    assert.match(err.message, /404/);
    await new Promise<void>((r) => (ws.onclose = () => r()));
    server.close();
  });
});

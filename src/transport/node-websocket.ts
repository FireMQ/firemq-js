// A small RFC 6455 WebSocket client over node:net and node:tls, used on Node versions without
// a global WebSocket (18 and 20) and for `wss://` with custom TLS options. It implements the
// subset of the standard WebSocket API that the transport uses: binary messages only.

import { createHash, randomBytes, randomFillSync } from 'node:crypto';
import * as net from 'node:net';
import * as tls from 'node:tls';

import type { TlsOptions } from '../options.js';
import { nodeTlsOptions, SOCKET_OPTIONS } from './tcp.js';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;

export interface NodeWebSocketOptions {
  tls?: TlsOptions;
  /** Largest message accepted, in bytes. Defaults to 64 MiB. */
  maxPayload?: number;
}

export class NodeWebSocket {
  static readonly CONNECTING = CONNECTING;
  static readonly OPEN = OPEN;
  static readonly CLOSING = CLOSING;
  static readonly CLOSED = CLOSED;

  readyState = CONNECTING;
  binaryType = 'arraybuffer';
  protocol = '';
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string; wasClean: boolean }) => void) | null = null;
  onerror: ((ev: { error: Error; message: string }) => void) | null = null;

  private readonly socket: net.Socket;
  private readonly key = randomBytes(16).toString('base64');
  private readonly maxPayload: number;
  private chunks: Buffer[] = [];
  private size = 0;
  private fragments: Buffer[] = [];
  private fragmentSize = 0;
  private fragmentOp = 0;
  private closeSent = false;
  private closeCode = 1006;
  private closeReason = '';
  private closeTimer: NodeJS.Timeout | undefined;
  private failed = false;

  constructor(url: string, protocols?: string | string[], options: NodeWebSocketOptions = {}) {
    const u = new URL(url);
    const secure = u.protocol === 'wss:';
    if (!secure && u.protocol !== 'ws:') throw new SyntaxError(`not a WebSocket URL: ${url}`);
    const host = u.hostname.startsWith('[') ? u.hostname.slice(1, -1) : u.hostname;
    const port = u.port ? Number(u.port) : secure ? 443 : 80;
    this.maxPayload = options.maxPayload ?? 64 * 1024 * 1024;
    const wanted = protocols === undefined ? [] : Array.isArray(protocols) ? protocols : [protocols];
    this.socket = secure
      ? tls.connect({ host, port, ...nodeTlsOptions(host, options.tls) })
      : net.connect({ host, port, ...SOCKET_OPTIONS });
    this.socket.once(secure ? 'secureConnect' : 'connect', () => {
      this.socket.setNoDelay(true);
      const lines = [
        `GET ${u.pathname || '/'}${u.search} HTTP/1.1`,
        `Host: ${u.host}`,
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Key: ${this.key}`,
        'Sec-WebSocket-Version: 13',
      ];
      if (wanted.length > 0) lines.push(`Sec-WebSocket-Protocol: ${wanted.join(', ')}`);
      this.socket.write(lines.join('\r\n') + '\r\n\r\n');
    });
    this.socket.on('data', (chunk: Buffer) => this.onData(chunk));
    this.socket.on('error', (error) => this.emitError(error));
    this.socket.on('close', () => {
      clearTimeout(this.closeTimer);
      const wasOpen = this.readyState !== CONNECTING;
      if (!wasOpen && !this.failed) this.emitError(new Error('connection closed during the WebSocket handshake'));
      this.readyState = CLOSED;
      this.onclose?.({ code: this.closeCode, reason: this.closeReason, wasClean: wasOpen && this.closeCode !== 1006 });
    });
  }

  get bufferedAmount(): number {
    return this.socket.writableLength;
  }

  send(data: Uint8Array): void {
    if (this.readyState !== OPEN) return;
    this.writeFrame(0x2, data);
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === CLOSED || this.readyState === CLOSING) return;
    if (this.readyState === CONNECTING) {
      this.readyState = CLOSING;
      this.socket.destroy();
      return;
    }
    this.readyState = CLOSING;
    this.sendClose(code, reason);
    this.closeTimer = setTimeout(() => this.socket.destroy(), 1000);
    this.closeTimer.unref();
  }

  private emitError(error: Error): void {
    this.failed = true;
    this.onerror?.({ error, message: error.message });
  }

  private fail(message: string, code = 1002): void {
    this.emitError(new Error(message));
    if (this.readyState === OPEN) {
      this.sendClose(code, '');
      this.readyState = CLOSING;
    }
    this.socket.destroy();
  }

  private sendClose(code: number, reason: string): void {
    if (this.closeSent) return;
    this.closeSent = true;
    const r = Buffer.from(reason);
    const payload = Buffer.alloc(2 + r.length);
    payload.writeUInt16BE(code, 0);
    r.copy(payload, 2);
    this.writeFrame(0x8, payload);
  }

  private writeFrame(opcode: number, payload: Uint8Array): void {
    const len = payload.byteLength;
    const headerLen = 2 + (len < 126 ? 0 : len < 65536 ? 2 : 8) + 4;
    const frame = Buffer.allocUnsafe(headerLen + len);
    frame[0] = 0x80 | opcode;
    let offset = 2;
    if (len < 126) {
      frame[1] = 0x80 | len;
    } else if (len < 65536) {
      frame[1] = 0x80 | 126;
      frame.writeUInt16BE(len, 2);
      offset = 4;
    } else {
      frame[1] = 0x80 | 127;
      frame.writeUInt32BE(Math.floor(len / 2 ** 32), 2);
      frame.writeUInt32BE(len >>> 0, 6);
      offset = 10;
    }
    const mask = frame.subarray(offset, offset + 4);
    randomFillSync(mask);
    offset += 4;
    for (let i = 0; i < len; i++) frame[offset + i] = payload[i]! ^ mask[i & 3]!;
    this.socket.write(frame);
  }

  private onData(chunk: Buffer): void {
    this.chunks.push(chunk);
    this.size += chunk.length;
    if (this.readyState === CONNECTING) {
      const buf = this.chunks.length === 1 ? this.chunks[0]! : Buffer.concat(this.chunks);
      const end = buf.indexOf('\r\n\r\n');
      if (end < 0) {
        this.chunks = [buf];
        if (buf.length > 16 * 1024) this.fail('WebSocket handshake response too large');
        return;
      }
      const rest = buf.subarray(end + 4);
      this.chunks = rest.length > 0 ? [rest] : [];
      this.size = rest.length;
      if (!this.handshake(buf.subarray(0, end).toString('latin1'))) return;
    }
    this.readFrames();
  }

  private handshake(head: string): boolean {
    const [status, ...lines] = head.split('\r\n');
    const m = /^HTTP\/1\.[01] (\d{3})/.exec(status ?? '');
    if (!m || m[1] !== '101') {
      this.fail(`WebSocket handshake failed: ${status ?? 'no status line'}`);
      return false;
    }
    const headers = new Map<string, string>();
    for (const line of lines) {
      const i = line.indexOf(':');
      if (i > 0) headers.set(line.slice(0, i).trim().toLowerCase(), line.slice(i + 1).trim());
    }
    const accept = createHash('sha1').update(this.key + GUID).digest('base64');
    if (headers.get('upgrade')?.toLowerCase() !== 'websocket' || headers.get('sec-websocket-accept') !== accept) {
      this.fail('WebSocket handshake failed: invalid upgrade response');
      return false;
    }
    this.protocol = headers.get('sec-websocket-protocol') ?? '';
    this.readyState = OPEN;
    this.onopen?.({});
    return this.readyState === OPEN;
  }

  private peek(n: number): Buffer {
    const first = this.chunks[0]!;
    if (first.length >= n) return first;
    const all = Buffer.concat(this.chunks);
    this.chunks = [all];
    return all;
  }

  private take(n: number): Buffer {
    let out: Buffer;
    const first = this.chunks[0]!;
    if (first.length >= n) {
      out = first.subarray(0, n);
      if (first.length === n) this.chunks.shift();
      else this.chunks[0] = first.subarray(n);
    } else {
      const all = Buffer.concat(this.chunks, this.size);
      out = all.subarray(0, n);
      this.chunks = all.length > n ? [all.subarray(n)] : [];
    }
    this.size -= n;
    return out;
  }

  private readFrames(): void {
    while (this.size >= 2 && (this.readyState === OPEN || this.readyState === CLOSING)) {
      const head = this.peek(Math.min(this.size, 14));
      const b0 = head[0]!;
      const b1 = head[1]!;
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (this.size < 4) return;
        len = head.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.size < 10) return;
        len = head.readUInt32BE(2) * 2 ** 32 + head.readUInt32BE(6);
        offset = 10;
      }
      if (len > this.maxPayload || this.fragmentSize + len > this.maxPayload) {
        this.fail('WebSocket message too large', 1009);
        return;
      }
      const maskOffset = offset;
      if (masked) offset += 4;
      if (this.size < offset + len) return;
      const frame = this.take(offset + len);
      const payload = frame.subarray(offset);
      if (masked) {
        const mask = frame.subarray(maskOffset, maskOffset + 4);
        for (let i = 0; i < payload.length; i++) payload[i] = payload[i]! ^ mask[i & 3]!;
      }
      this.onFrame(fin, opcode, payload);
    }
  }

  private onFrame(fin: boolean, opcode: number, payload: Buffer): void {
    switch (opcode) {
      case 0x0:
        if (this.fragmentOp === 0) return this.fail('unexpected continuation frame');
        this.fragments.push(payload);
        this.fragmentSize += payload.length;
        if (fin) {
          const data = Buffer.concat(this.fragments, this.fragmentSize);
          const op = this.fragmentOp;
          this.fragments = [];
          this.fragmentSize = 0;
          this.fragmentOp = 0;
          this.deliver(op, data);
        }
        return;
      case 0x1:
      case 0x2:
        if (this.fragmentOp !== 0) return this.fail('expected a continuation frame');
        if (fin) return this.deliver(opcode, payload);
        this.fragmentOp = opcode;
        this.fragments = [payload];
        this.fragmentSize = payload.length;
        return;
      case 0x8: {
        if (payload.length >= 2) {
          this.closeCode = payload.readUInt16BE(0);
          this.closeReason = payload.subarray(2).toString('utf8');
        } else {
          this.closeCode = 1005;
        }
        this.sendClose(this.closeCode === 1005 ? 1000 : this.closeCode, '');
        this.readyState = CLOSING;
        this.socket.end();
        return;
      }
      case 0x9:
        if (this.readyState === OPEN) this.writeFrame(0xa, payload);
        return;
      case 0xa:
        return;
      default:
        this.fail(`unknown WebSocket opcode ${opcode}`);
    }
  }

  private deliver(opcode: number, data: Buffer): void {
    if (this.readyState !== OPEN) return;
    const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    this.onmessage?.({ data: opcode === 0x1 ? data.toString('utf8') : bytes });
  }
}

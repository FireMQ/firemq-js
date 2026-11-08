// Frames of the FireMQ protocol: `len: u32 | op: u8 | hlen: u32 | header | payload`.
// The header is a MessagePack map with string keys; the payload is raw bytes.

import { Decoder, Encoder } from '@msgpack/msgpack';

import { ProtocolError } from './errors.js';

/** Protocol major version spoken by this client. */
export const PROTOCOL_VERSION = 1;

/** Bytes a TCP client sends before its first frame: ASCII `FMQ1`. */
export const MAGIC = new Uint8Array([0x46, 0x4d, 0x51, 0x31]);

/** Largest frame accepted before `welcome`. */
export const MAX_HELLO_FRAME = 65_536;

/** Default largest frame after `welcome`. */
export const DEFAULT_MAX_FRAME = 16 * 1024 * 1024;

/** Op codes. */
export const Op = {
  Hello: 0x01,
  Welcome: 0x02,
  Ping: 0x03,
  Pong: 0x04,
  Close: 0x05,
  Ok: 0x06,
  Error: 0x07,
  Flow: 0x08,
  DeclareExchange: 0x10,
  DeleteExchange: 0x11,
  DeclareQueue: 0x12,
  DeleteQueue: 0x13,
  PurgeQueue: 0x14,
  Bind: 0x15,
  Unbind: 0x16,
  Publish: 0x20,
  Confirm: 0x21,
  Return: 0x22,
  Subscribe: 0x30,
  Unsubscribe: 0x31,
  Credit: 0x32,
  Deliver: 0x33,
  Cancelled: 0x34,
  Ack: 0x35,
  Nack: 0x36,
  Get: 0x37,
  GetOk: 0x38,
} as const;

const OP_NAMES: Record<number, string> = {
  0x01: 'hello',
  0x02: 'welcome',
  0x03: 'ping',
  0x04: 'pong',
  0x05: 'close',
  0x06: 'ok',
  0x07: 'error',
  0x08: 'flow',
  0x10: 'declare_exchange',
  0x11: 'delete_exchange',
  0x12: 'declare_queue',
  0x13: 'delete_queue',
  0x14: 'purge_queue',
  0x15: 'bind',
  0x16: 'unbind',
  0x20: 'publish',
  0x21: 'confirm',
  0x22: 'return',
  0x30: 'subscribe',
  0x31: 'unsubscribe',
  0x32: 'credit',
  0x33: 'deliver',
  0x34: 'cancelled',
  0x35: 'ack',
  0x36: 'nack',
  0x37: 'get',
  0x38: 'get_ok',
};

/** The op's name as written in the spec, for example `declare_queue`, or `unknown`. */
export function opName(op: number): string {
  return OP_NAMES[op] ?? 'unknown';
}

/** A MessagePack map with string keys. */
export type Header = Record<string, unknown>;

/** A decoded frame. */
export interface Frame {
  op: number;
  header: Header;
  payload: Uint8Array;
}

const encoder = new Encoder({ ignoreUndefined: true, useBigInt64: true });
const decoder = new Decoder();
const EMPTY = new Uint8Array(0);

/**
 * Encodes a frame with its 4-byte length prefix. On WebSocket, send `frame.subarray(4)`.
 * Pass `header` as `undefined` for an empty header (`hlen` 0), as `ping` and `pong` do.
 */
export function encodeFrame(op: number, header?: Header, payload?: Uint8Array): Uint8Array {
  const h = header === undefined ? EMPTY : encoder.encodeSharedRef(header);
  const plen = payload ? payload.byteLength : 0;
  const len = 1 + 4 + h.byteLength + plen;
  const out = new Uint8Array(4 + len);
  const view = new DataView(out.buffer);
  view.setUint32(0, len);
  out[4] = op;
  view.setUint32(5, h.byteLength);
  out.set(h, 9);
  if (payload && plen > 0) out.set(payload, 9 + h.byteLength);
  return out;
}

/** Decodes a MessagePack header. An empty header is an empty map. */
export function decodeHeader(bytes: Uint8Array): Header {
  if (bytes.byteLength === 0) return {};
  let value: unknown;
  try {
    value = decoder.decode(bytes);
  } catch (e) {
    throw new ProtocolError(`invalid frame header: ${(e as Error).message}`);
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value) || value instanceof Uint8Array) {
    throw new ProtocolError('frame header is not a MessagePack map');
  }
  return value as Header;
}

/** Decodes a frame without its length prefix, as carried by one WebSocket message. */
export function decodeFrame(bytes: Uint8Array): Frame {
  if (bytes.byteLength < 5) throw new ProtocolError('frame shorter than 5 bytes');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const op = bytes[0]!;
  const hlen = view.getUint32(1);
  if (hlen > bytes.byteLength - 5) throw new ProtocolError('header length exceeds frame');
  const header = decodeHeader(bytes.subarray(5, 5 + hlen));
  return { op, header, payload: bytes.subarray(5 + hlen) };
}

/**
 * Splits length-prefixed frames off a TCP byte stream. Feed it chunks as they arrive; it
 * returns every frame that is complete. Large frames are copied once, when complete.
 */
export class FrameReader {
  /** Largest accepted value of the length prefix. */
  maxFrame: number;
  private chunks: Uint8Array[] = [];
  private size = 0;

  constructor(maxFrame = MAX_HELLO_FRAME) {
    this.maxFrame = maxFrame;
  }

  /** Buffered bytes that do not form a whole frame yet. */
  get buffered(): number {
    return this.size;
  }

  push(chunk: Uint8Array): Frame[] {
    if (chunk.byteLength > 0) {
      this.chunks.push(chunk);
      this.size += chunk.byteLength;
    }
    const frames: Frame[] = [];
    while (this.size >= 4) {
      const prefix = this.peek(4);
      const len = new DataView(prefix.buffer, prefix.byteOffset, 4).getUint32(0);
      if (len > this.maxFrame) {
        throw new ProtocolError(`frame of ${len} bytes exceeds the maximum of ${this.maxFrame} bytes`);
      }
      if (len < 5) throw new ProtocolError('frame shorter than 5 bytes');
      if (this.size < 4 + len) break;
      const frame = this.take(4 + len);
      frames.push(decodeFrame(frame.subarray(4)));
    }
    return frames;
  }

  private peek(n: number): Uint8Array {
    const first = this.chunks[0]!;
    if (first.byteLength >= n) return first.subarray(0, n);
    const out = new Uint8Array(n);
    let filled = 0;
    for (const c of this.chunks) {
      const part = c.subarray(0, n - filled);
      out.set(part, filled);
      filled += part.byteLength;
      if (filled === n) break;
    }
    return out;
  }

  private take(n: number): Uint8Array {
    this.size -= n;
    const first = this.chunks[0]!;
    if (first.byteLength >= n) {
      if (first.byteLength === n) this.chunks.shift();
      else this.chunks[0] = first.subarray(n);
      return first.subarray(0, n);
    }
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const c = this.chunks[0]!;
      const want = n - filled;
      if (c.byteLength <= want) {
        out.set(c, filled);
        filled += c.byteLength;
        this.chunks.shift();
      } else {
        out.set(c.subarray(0, want), filled);
        this.chunks[0] = c.subarray(want);
        filled += want;
      }
    }
    return out;
  }
}

/**
 * Drops top-level fields that hold their zero value: `undefined`, `null`, `false`, `0`, the
 * empty string, and empty maps and arrays. The protocol treats an omitted field as its zero
 * value, so the frame means the same and is smaller.
 */
export function compact(header: Header): Header {
  const out: Header = {};
  for (const [k, v] of Object.entries(header)) {
    if (v === undefined || v === null || v === false || v === 0 || v === '') continue;
    if (Array.isArray(v) && v.length === 0) continue;
    if (isPlainObject(v) && Object.keys(v).length === 0) continue;
    out[k] = v;
  }
  return out;
}

export function isPlainObject(v: unknown): v is Record<string, unknown> {
  if (v === null || typeof v !== 'object') return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}

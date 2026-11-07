import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { compact, decodeFrame, encodeFrame, FrameReader, MAX_HELLO_FRAME, Op, opName } from '../../src/codec.js';
import { ProtocolError } from '../../src/errors.js';

interface Vector {
  name: string;
  op: number;
  op_name: string;
  frame_hex: string;
  header: Record<string, unknown>;
  payload_hex: string;
}

const fixture = new URL('../../../test/fixtures/frames.json', import.meta.url);
const vectors = (JSON.parse(readFileSync(fixture, 'utf8')) as { vectors: Vector[] }).vectors;

const unhex = (s: string) => new Uint8Array(Buffer.from(s, 'hex'));
const hex = (b: Uint8Array) => Buffer.from(b).toString('hex');

describe('test vectors', () => {
  it('covers every op', () => {
    const ops = new Set(vectors.map((v) => v.op));
    for (const op of Object.values(Op)) assert.ok(ops.has(op), `no vector for ${opName(op)}`);
  });

  for (const v of vectors) {
    it(`decodes ${v.name} from a TCP stream`, () => {
      const frames = new FrameReader(1 << 30).push(unhex(v.frame_hex));
      assert.equal(frames.length, 1);
      const f = frames[0]!;
      assert.equal(f.op, v.op);
      assert.equal(opName(f.op), v.op_name);
      assert.deepEqual(f.header, v.header);
      assert.equal(hex(f.payload), v.payload_hex);
    });

    it(`decodes ${v.name} from a WebSocket message`, () => {
      const f = decodeFrame(unhex(v.frame_hex).subarray(4));
      assert.equal(f.op, v.op);
      assert.deepEqual(f.header, v.header);
      assert.equal(hex(f.payload), v.payload_hex);
    });

    it(`round-trips ${v.name}`, () => {
      const header = Object.keys(v.header).length === 0 ? undefined : v.header;
      const encoded = encodeFrame(v.op, header, unhex(v.payload_hex));
      const [f] = new FrameReader(1 << 30).push(encoded);
      assert.equal(f!.op, v.op);
      assert.deepEqual(f!.header, v.header);
      assert.equal(hex(f!.payload), v.payload_hex);
    });
  }

  it('encodes ping and pong exactly as the vectors', () => {
    for (const name of ['ping', 'pong']) {
      const v = vectors.find((x) => x.name === name)!;
      assert.equal(hex(encodeFrame(v.op)), v.frame_hex);
    }
  });
});

describe('FrameReader', () => {
  it('splits frames delivered one byte at a time', () => {
    const stream = Buffer.concat([
      encodeFrame(Op.Publish, { seq: 1 }, new TextEncoder().encode('hello')),
      encodeFrame(Op.Ping),
      encodeFrame(Op.Confirm, { upto: 1 }),
    ]);
    const reader = new FrameReader();
    const frames = [];
    for (const byte of stream) frames.push(...reader.push(new Uint8Array([byte])));
    assert.deepEqual(
      frames.map((f) => f.op),
      [Op.Publish, Op.Ping, Op.Confirm],
    );
    assert.equal(new TextDecoder().decode(frames[0]!.payload), 'hello');
    assert.deepEqual(frames[1]!.header, {});
    assert.equal(reader.buffered, 0);
  });

  it('reassembles a large frame from chunks', () => {
    const body = new Uint8Array(1024 * 1024).map((_, i) => i & 0xff);
    const frame = encodeFrame(Op.Deliver, { sub: 1, tag: 9 }, body);
    const reader = new FrameReader(16 * 1024 * 1024);
    const out = [];
    for (let i = 0; i < frame.length; i += 65_536) out.push(...reader.push(frame.subarray(i, i + 65_536)));
    assert.equal(out.length, 1);
    assert.deepEqual(out[0]!.payload, body);
  });

  it('rejects frames above the limit', () => {
    const frame = encodeFrame(Op.Publish, {}, new Uint8Array(MAX_HELLO_FRAME));
    assert.throws(() => new FrameReader().push(frame), ProtocolError);
  });

  it('rejects frames shorter than five bytes', () => {
    assert.throws(() => new FrameReader().push(new Uint8Array([0, 0, 0, 3, 1, 0, 0])), ProtocolError);
  });

  it('rejects a header longer than the frame', () => {
    assert.throws(() => decodeFrame(new Uint8Array([0x20, 0, 0, 0, 9, 1])), ProtocolError);
  });

  it('rejects a header that is not a map', () => {
    const notMap = new Uint8Array([0x06, 0, 0, 0, 1, 0x01]);
    assert.throws(() => decodeFrame(notMap), ProtocolError);
  });

  it('ignores unknown header keys', () => {
    const [f] = new FrameReader().push(encodeFrame(Op.Ack, { tag: 5, future_field: [1, 2] }));
    assert.equal(f!.header.tag, 5);
  });
});

describe('compact', () => {
  it('drops zero values at the top level only', () => {
    assert.deepEqual(
      compact({
        rid: 1,
        name: '',
        durable: false,
        passive: undefined,
        args: {},
        caps: [],
        n: 0,
        headers: { keep: false, zero: 0 },
        exclusive: true,
      }),
      { rid: 1, headers: { keep: false, zero: 0 }, exclusive: true },
    );
  });
});

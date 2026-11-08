// An in-memory broker stand-in: records what the client sends and lets tests answer.

import { decodeFrame, encodeFrame, type Frame, type Header, Op } from '../../src/codec.js';
import { ClosedError } from '../../src/errors.js';
import type { Dialer, Transport } from '../../src/transport/types.js';

export class FakeServer {
  /** Frames the client sent, in order. */
  readonly received: Frame[] = [];
  /** Answers client frames. Return true when handled. */
  handler: (frame: Frame) => boolean = () => false;
  welcome: Header = { ver: 1, server: { name: 'firemq', version: 'test' }, conn_id: 'fake', heartbeat: 0, max_frame: 1 << 20 };
  /** Answer requests with `ok` automatically. */
  autoOk = true;
  closed = false;
  private transport: Transport | undefined;
  private waiters: Array<{ op: number; resolve: (f: Frame) => void }> = [];

  dial: Dialer = async () => {
    const t: Transport = {
      send: (frame) => {
        if (this.closed) return true;
        const f = decodeFrame(frame.subarray(4));
        this.received.push(f);
        queueMicrotask(() => this.onClientFrame(f));
        return true;
      },
      drain: async () => {},
      setMaxFrame: () => {},
      close: () => {
        if (this.closed) return;
        this.closed = true;
        queueMicrotask(() => t.onclose(new ClosedError('transport closed')));
      },
      onframe: () => {},
      onclose: () => {},
    };
    this.transport = t;
    return t;
  };

  /** Sends a frame to the client. */
  push(op: number, header?: Header, payload?: Uint8Array): void {
    this.transport!.onframe(decodeFrame(encodeFrame(op, header, payload).subarray(4)));
  }

  /** Simulates the socket dropping. */
  drop(): void {
    this.closed = true;
    this.transport!.onclose(new Error('socket reset'));
  }

  /** Resolves with the next client frame with this op (or one already received and not yet awaited). */
  next(op: number): Promise<Frame> {
    return new Promise((resolve) => this.waiters.push({ op, resolve }));
  }

  sent(op: number): Frame[] {
    return this.received.filter((f) => f.op === op);
  }

  private onClientFrame(f: Frame): void {
    const i = this.waiters.findIndex((w) => w.op === f.op);
    if (i >= 0) this.waiters.splice(i, 1)[0]!.resolve(f);
    if (this.handler(f)) return;
    if (f.op === Op.Hello) {
      this.push(Op.Welcome, this.welcome);
      return;
    }
    if (this.autoOk && typeof f.header.rid === 'number') {
      if (f.op === Op.DeclareQueue) {
        this.push(Op.Ok, { rid: f.header.rid, name: f.header.name || 'amq.gen-x', messages: 0, consumers: 0 });
      } else {
        this.push(Op.Ok, { rid: f.header.rid });
      }
    }
    if (f.op === Op.Close) {
      this.push(Op.Close, { code: 200 });
    }
  }
}

export const tick = () => new Promise((r) => setTimeout(r, 0));

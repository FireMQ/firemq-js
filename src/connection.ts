// One FireMQ connection: handshake, request/reply matching, publisher confirms, deliveries,
// heartbeats and shutdown. `Client` is the public face of this class.

import {
  compact,
  DEFAULT_MAX_FRAME,
  encodeFrame,
  type Frame,
  type Header,
  MAX_HELLO_FRAME,
  Op,
  opName,
  PROTOCOL_VERSION,
} from './codec.js';
import {
  ClosedError,
  ConnectionError,
  FireMQError,
  NackedError,
  ProtocolError,
  ServerError,
  UnroutableError,
} from './errors.js';
import type { Settler } from './message.js';
import type { ConnectOptions, ServerInfo } from './options.js';
import type { Dialer, Transport } from './transport/types.js';
import type { ConnectUrl } from './url.js';
import { VERSION } from './version.js';

interface Waiter<T> {
  resolve(value: T): void;
  reject(error: Error): void;
}

/** Receives the deliveries and the end of one subscription. */
export interface SubscriptionSink {
  deliver(header: Header, body: Uint8Array): void;
  /** The server ended the subscription, for example because its queue was deleted. */
  serverCancelled(code: number, reason: string): void;
  /** The connection is gone. `error` is undefined after a graceful `close()`. */
  connectionClosed(error: Error | undefined): void;
}

const num = (v: unknown): number => (typeof v === 'number' ? v : typeof v === 'bigint' ? Number(v) : 0);
const str = (v: unknown): string => (typeof v === 'string' ? v : '');

function platform(): string {
  const g = globalThis as { Deno?: unknown; Bun?: unknown; process?: { versions?: { node?: string } } };
  if (g.Deno !== undefined) return 'deno';
  if (g.Bun !== undefined) return 'bun';
  if (g.process?.versions?.node) return 'node';
  return 'browser';
}

function asClosed(error: Error): Error {
  return error instanceof FireMQError ? error : new ClosedError(error.message, { cause: error });
}

interface Handshake {
  welcome: Header;
  early: Frame[];
  earlyClose: Error | undefined;
}

/** Sends `hello` and waits for `welcome`. Frames that arrive right after it are kept. */
function handshake(transport: Transport, url: ConnectUrl, options: ConnectOptions): Promise<Handshake> {
  const client: Header = {
    product: 'firemq-js',
    version: VERSION,
    lang: 'javascript',
    platform: platform(),
  };
  if (options.connectionName) client.connection_name = options.connectionName;
  const hello = compact({
    ver: PROTOCOL_VERSION,
    vhost: url.vhost,
    auth: { mech: 'plain', user: url.user, pass: url.pass },
    heartbeat: options.heartbeat ?? 30,
    client,
    caps: ['confirms', 'credit'],
  });
  return new Promise<Handshake>((resolve, reject) => {
    let done = false;
    const state: Handshake = { welcome: {}, early: [], earlyClose: undefined };
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      transport.close();
      reject(new ConnectionError('handshake timed out'));
    }, options.connectTimeout ?? 10_000);
    transport.onframe = (frame) => {
      if (done) {
        state.early.push(frame);
        return;
      }
      done = true;
      clearTimeout(timer);
      if (frame.op === Op.Welcome) {
        state.welcome = frame.header;
        resolve(state);
      } else if (frame.op === Op.Error) {
        transport.close();
        reject(new ServerError(num(frame.header.code), str(frame.header.msg)));
      } else {
        transport.close();
        reject(new ProtocolError(`expected welcome, got ${opName(frame.op)}`));
      }
    };
    transport.onclose = (error) => {
      if (done) {
        state.earlyClose = error;
        return;
      }
      done = true;
      clearTimeout(timer);
      reject(
        error instanceof FireMQError
          ? error
          : new ClosedError('server closed the connection during handshake', { cause: error }),
      );
    };
    transport.send(encodeFrame(Op.Hello, hello));
  });
}

export class Connection implements Settler {
  readonly server: ServerInfo;
  /** Resolves with the reason once the connection is closed. */
  readonly closed: Promise<Error>;
  readonly subs = new Map<number, SubscriptionSink>();

  private readonly transport: Transport;
  private readonly options: ConnectOptions;
  private readonly requests = new Map<number, Waiter<Frame>>();
  private readonly confirms = new Map<number, Waiter<void>>();
  private readonly returned = new Map<number, [exchange: string, key: string]>();
  private reason: Error | undefined;
  private closing = false;
  private rid = 1;
  private seq = 1;
  private subId = 1;
  private lastSend = Date.now();
  private lastRecv = Date.now();
  private timer: ReturnType<typeof setInterval> | undefined;
  private paused = false;
  private resumeWaiters: Array<() => void> = [];
  private resolveClosed!: (reason: Error) => void;

  static async open(url: ConnectUrl, options: ConnectOptions, dial: Dialer): Promise<Connection> {
    const transport = await dial(url, options);
    const hs = await handshake(transport, url, options);
    return new Connection(transport, hs, options);
  }

  private constructor(transport: Transport, hs: Handshake, options: ConnectOptions) {
    this.transport = transport;
    this.options = options;
    const w = hs.welcome;
    const srv = (w.server ?? {}) as Header;
    const maxFrame = Math.max(num(w.max_frame) || DEFAULT_MAX_FRAME, MAX_HELLO_FRAME);
    this.server = {
      protocolVersion: num(w.ver),
      name: str(srv.name),
      version: str(srv.version),
      connectionId: str(w.conn_id),
      heartbeat: num(w.heartbeat),
      maxFrame,
      capabilities: Array.isArray(w.caps) ? w.caps.filter((c): c is string => typeof c === 'string') : [],
    };
    this.closed = new Promise<Error>((r) => (this.resolveClosed = r));
    transport.setMaxFrame(maxFrame);
    transport.onframe = (frame) => this.dispatch(frame);
    transport.onclose = (error) =>
      this.shutdown(this.closing ? new ClosedError('client closed the connection') : asClosed(error), this.closing);
    this.startHeartbeat(this.server.heartbeat);
    for (const frame of hs.early) this.dispatch(frame);
    if (hs.earlyClose) transport.onclose(hs.earlyClose);
  }

  get isClosed(): boolean {
    return this.reason !== undefined;
  }

  /** Throws the reason the connection closed, if it did. */
  check(): void {
    if (this.reason) throw this.reason;
  }

  /** Sends a frame. Returns false when the socket buffer is full. */
  send(op: number, header?: Header, payload?: Uint8Array): boolean {
    return this.sendFrame(encodeFrame(op, header, payload));
  }

  sendFrame(frame: Uint8Array): boolean {
    this.check();
    this.lastSend = Date.now();
    return this.transport.send(frame);
  }

  drain(): Promise<void> {
    return this.transport.drain();
  }

  /** Sends a request with a fresh `rid` and resolves with its `ok` or `get_ok` frame. */
  request(op: number, build: (rid: number) => Header): Promise<Frame> {
    if (this.reason) return Promise.reject(this.reason);
    const rid = this.rid++;
    return new Promise<Frame>((resolve, reject) => {
      this.requests.set(rid, { resolve, reject });
      try {
        this.send(op, compact(build(rid)));
      } catch (e) {
        this.requests.delete(rid);
        reject(e as Error);
      }
    });
  }

  /** The sequence number the next confirmed publish will use. */
  peekSeq(): number {
    return this.seq;
  }

  /** Claims the next sequence number and returns a promise for its confirm. */
  expectConfirm(): { seq: number; confirmed: Promise<void> } {
    this.check();
    const seq = this.seq++;
    const confirmed = new Promise<void>((resolve, reject) => this.confirms.set(seq, { resolve, reject }));
    return { seq, confirmed };
  }

  /** Forgets a confirm whose publish could not be sent. */
  dropConfirm(seq: number): void {
    this.confirms.delete(seq);
  }

  nextSubId(): number {
    return this.subId++;
  }

  /** Resolves once the broker lifts a `flow` pause, or right away. */
  waitForFlow(): Promise<void> {
    if (!this.paused || this.reason) return Promise.resolve();
    return new Promise<void>((r) => this.resumeWaiters.push(r));
  }

  /** Acknowledges or rejects a delivery. */
  settle(tag: number, ack: boolean, requeue: boolean): void {
    if (ack) this.send(Op.Ack, { tag });
    else this.send(Op.Nack, compact({ tag, requeue }));
  }

  async close(): Promise<void> {
    if (this.reason) return;
    this.closing = true;
    try {
      this.send(Op.Close, { code: 200, msg: 'client closed' });
    } catch {
      // the transport is already gone
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([this.closed, new Promise<void>((r) => (timer = setTimeout(r, 2000)))]);
    clearTimeout(timer);
    this.shutdown(new ClosedError('client closed the connection'), true);
  }

  private startHeartbeat(heartbeat: number): void {
    if (heartbeat <= 0) return;
    const idleMs = Math.max(1, Math.floor(heartbeat / 2)) * 1000;
    const deadMs = heartbeat * 2000;
    this.timer = setInterval(
      () => {
        const now = Date.now();
        if (now - this.lastRecv > deadMs) {
          this.shutdown(new ClosedError('server missed heartbeats'));
          return;
        }
        if (now - this.lastSend >= idleMs) {
          try {
            this.send(Op.Ping);
          } catch {
            // closed
          }
        }
      },
      Math.min(idleMs, 1000),
    );
    (this.timer as { unref?: () => void }).unref?.();
  }

  private shutdown(reason: Error, graceful = false): void {
    if (this.reason) return;
    this.reason = reason;
    clearInterval(this.timer);
    const requests = [...this.requests.values()];
    const confirms = [...this.confirms.values()];
    const subs = [...this.subs.values()];
    this.requests.clear();
    this.confirms.clear();
    this.returned.clear();
    this.subs.clear();
    for (const w of requests) w.reject(reason);
    for (const w of confirms) w.reject(reason);
    for (const s of subs) s.connectionClosed(graceful ? undefined : reason);
    for (const r of this.resumeWaiters.splice(0)) r();
    this.transport.close();
    this.resolveClosed(reason);
  }

  private dispatch(frame: Frame): void {
    this.lastRecv = Date.now();
    const h = frame.header;
    switch (frame.op) {
      case Op.Ok:
      case Op.GetOk: {
        const rid = num(h.rid);
        const w = this.requests.get(rid);
        if (w) {
          this.requests.delete(rid);
          w.resolve(frame);
        }
        return;
      }
      case Op.Error: {
        const err = new ServerError(num(h.code), str(h.msg));
        const w = h.rid === undefined ? undefined : this.requests.get(num(h.rid));
        if (w) {
          this.requests.delete(num(h.rid));
          w.reject(err);
        } else if (!h.fatal) {
          try {
            this.options.onError?.(err);
          } catch {
            // a throwing callback must not break the connection
          }
        }
        if (h.fatal) this.shutdown(err);
        return;
      }
      case Op.Confirm: {
        if (h.nack) {
          const seq = num(h.seq);
          this.returned.delete(seq);
          const w = this.confirms.get(seq);
          if (w) {
            this.confirms.delete(seq);
            w.reject(new NackedError(num(h.code), str(h.msg)));
          }
        } else if (h.upto !== undefined) {
          const upto = num(h.upto);
          // Map iteration follows insertion order, which is sequence order.
          for (const [seq, w] of this.confirms) {
            if (seq > upto) break;
            this.confirms.delete(seq);
            const ret = this.returned.get(seq);
            if (ret) {
              this.returned.delete(seq);
              w.reject(new UnroutableError(ret[0], ret[1]));
            } else {
              w.resolve();
            }
          }
        }
        return;
      }
      case Op.Return: {
        const seq = num(h.seq);
        if (seq > 0) this.returned.set(seq, [str(h.exchange), str(h.key)]);
        return;
      }
      case Op.Deliver:
        this.subs.get(num(h.sub))?.deliver(h, frame.payload);
        return;
      case Op.Cancelled: {
        const sub = num(h.sub);
        const s = this.subs.get(sub);
        this.subs.delete(sub);
        s?.serverCancelled(num(h.code), str(h.msg));
        return;
      }
      case Op.Ping:
        try {
          this.send(Op.Pong);
        } catch {
          // closed
        }
        return;
      case Op.Flow:
        this.paused = h.pause === true;
        if (!this.paused) for (const r of this.resumeWaiters.splice(0)) r();
        return;
      case Op.Close: {
        if (this.closing) {
          this.shutdown(new ClosedError('client closed the connection'), true);
          return;
        }
        try {
          this.send(Op.Close, { code: 200 });
        } catch {
          // closed
        }
        const msg = str(h.msg);
        this.shutdown(new ClosedError(msg || `closed by server (code ${num(h.code)})`));
        return;
      }
      default:
        // pong and anything unknown
        return;
    }
  }
}

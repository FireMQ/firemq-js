import { compact, encodeFrame, type Header, Op } from './codec.js';
import { Connection } from './connection.js';
import { ProtocolError } from './errors.js';
import { type Message, messageFrom, Subscription } from './message.js';
import {
  type Body,
  bodyBytes,
  type ConnectOptions,
  type DeleteExchangeOptions,
  type DeleteQueueOptions,
  type ExchangeOptions,
  type ExchangeType,
  type GetOptions,
  propertiesToWire,
  type PublishOptions,
  type QueueInfo,
  type QueueOptions,
  type ServerInfo,
  type SubscribeOptions,
  type Table,
} from './options.js';
import type { Dialer } from './transport/types.js';
import { parseUrl } from './url.js';

const num = (v: unknown): number => (typeof v === 'number' ? v : typeof v === 'bigint' ? Number(v) : 0);

/**
 * A connection to FireMQ. Create one with `connect()`. One client is one connection; share it
 * across your application and call `close()` when done.
 */
export class Client {
  readonly #conn: Connection;

  /** Clients come from `connect()`. */
  constructor(conn: Connection) {
    this.#conn = conn;
  }

  /** Server name, version and limits from the handshake. */
  get server(): ServerInfo {
    return this.#conn.server;
  }

  /** True once the connection is closed. */
  get isClosed(): boolean {
    return this.#conn.isClosed;
  }

  /** Resolves with the reason once the connection closes, for any reason. Never rejects. */
  get closed(): Promise<Error> {
    return this.#conn.closed;
  }

  /** Declares a queue. An empty name asks the broker to generate one; read it from the result. */
  async declareQueue(name: string, options: QueueOptions = {}): Promise<QueueInfo> {
    const t = options.temporary === true;
    const ok = await this.#conn.request(Op.DeclareQueue, (rid) => ({
      rid,
      name,
      durable: options.durable ?? !t,
      exclusive: options.exclusive ?? t,
      auto_delete: options.autoDelete ?? t,
      passive: options.passive,
      args: options.args,
    }));
    const h = ok.header;
    return {
      name: typeof h.name === 'string' && h.name !== '' ? h.name : name,
      messages: num(h.messages),
      consumers: num(h.consumers),
    };
  }

  /** Deletes a queue and returns how many messages it held. */
  async deleteQueue(name: string, options: DeleteQueueOptions = {}): Promise<number> {
    const ok = await this.#conn.request(Op.DeleteQueue, (rid) => ({
      rid,
      name,
      if_unused: options.ifUnused,
      if_empty: options.ifEmpty,
    }));
    return num(ok.header.messages);
  }

  /** Removes every ready message from a queue and returns how many were removed. */
  async purgeQueue(name: string): Promise<number> {
    const ok = await this.#conn.request(Op.PurgeQueue, (rid) => ({ rid, name }));
    return num(ok.header.messages);
  }

  /** Declares an exchange of type `direct`, `fanout`, `topic` or `headers`. */
  async declareExchange(name: string, type: ExchangeType, options: ExchangeOptions = {}): Promise<void> {
    await this.#conn.request(Op.DeclareExchange, (rid) => ({
      rid,
      name,
      type,
      durable: options.durable ?? true,
      auto_delete: options.autoDelete,
      internal: options.internal,
      passive: options.passive,
      args: options.args,
    }));
  }

  async deleteExchange(name: string, options: DeleteExchangeOptions = {}): Promise<void> {
    await this.#conn.request(Op.DeleteExchange, (rid) => ({ rid, name, if_unused: options.ifUnused }));
  }

  /** Binds `queue` to `exchange` with a routing key or pattern. `args` holds, for example, `x-match`. */
  async bindQueue(queue: string, exchange: string, key = '', args?: Table): Promise<void> {
    await this.#conn.request(Op.Bind, (rid) => ({ rid, exchange, queue, key, args }));
  }

  async unbindQueue(queue: string, exchange: string, key = '', args?: Table): Promise<void> {
    await this.#conn.request(Op.Unbind, (rid) => ({ rid, exchange, queue, key, args }));
  }

  /** Binds exchange `destination` to exchange `source`: messages routed by `source` flow on. */
  async bindExchange(destination: string, source: string, key = '', args?: Table): Promise<void> {
    await this.#conn.request(Op.Bind, (rid) => ({ rid, exchange: source, to_exchange: destination, key, args }));
  }

  async unbindExchange(destination: string, source: string, key = '', args?: Table): Promise<void> {
    await this.#conn.request(Op.Unbind, (rid) => ({ rid, exchange: source, to_exchange: destination, key, args }));
  }

  /**
   * Publishes a message and resolves when the broker confirms it. Use exchange `''` to send
   * straight to the queue named by `key`. Messages are persistent unless
   * `persistent: false`; with `confirm: false` the promise resolves once the frame is sent.
   *
   * Rejects with `UnroutableError` for a `mandatory` message no queue received, and with
   * `NackedError` when the broker refused it.
   */
  async publish(exchange: string, key: string, body: Body, options: PublishOptions = {}): Promise<void> {
    const conn = this.#conn;
    const payload = bodyBytes(body);
    await conn.waitForFlow();
    conn.check();
    const confirm = options.confirm ?? true;
    const header: Header = compact({
      seq: confirm ? conn.peekSeq() : 0,
      exchange,
      key,
      mandatory: options.mandatory,
      persistent: options.persistent ?? true,
      props: propertiesToWire(options.properties),
      headers: options.headers,
    });
    const frame = encodeFrame(Op.Publish, header, payload);
    if (frame.byteLength - 4 > conn.server.maxFrame) {
      throw new ProtocolError(
        `message of ${payload.byteLength} bytes does not fit in a frame; the broker accepts frames up to ${conn.server.maxFrame} bytes`,
      );
    }
    if (!confirm) {
      if (!conn.sendFrame(frame)) await conn.drain();
      return;
    }
    const { seq, confirmed } = conn.expectConfirm();
    try {
      conn.sendFrame(frame);
    } catch (e) {
      conn.dropConfirm(seq);
      throw e;
    }
    await confirmed;
  }

  /** Publishes a value as JSON with content type `application/json`. */
  publishJson(exchange: string, key: string, value: unknown, options: PublishOptions = {}): Promise<void> {
    const body = JSON.stringify(value);
    if (body === undefined) return Promise.reject(new TypeError('value cannot be encoded as JSON'));
    return this.publish(exchange, key, body, {
      ...options,
      properties: { contentType: 'application/json', ...options.properties },
    });
  }

  /** Starts consuming from a queue. Iterate the result with `for await`. */
  async subscribe(queue: string, options: SubscribeOptions = {}): Promise<Subscription> {
    const conn = this.#conn;
    conn.check();
    const id = conn.nextSubId();
    const window = Math.max(1, Math.floor(options.prefetch ?? 100));
    const noAck = options.noAck === true;
    const sub = new Subscription(conn, id, queue, window, noAck);
    conn.subs.set(id, sub);
    try {
      await conn.request(Op.Subscribe, (rid) => ({
        rid,
        sub: id,
        queue,
        credit: window,
        no_ack: noAck,
        exclusive: options.exclusive,
        tag: options.tag,
        args: options.args,
      }));
    } catch (e) {
      conn.subs.delete(id);
      throw e;
    }
    return sub;
  }

  /** Takes one message from a queue, or `null` when it is empty. */
  async get(queue: string, options: GetOptions = {}): Promise<Message | null> {
    const noAck = options.noAck === true;
    const reply = await this.#conn.request(Op.Get, (rid) => ({ rid, queue, no_ack: noAck }));
    if (reply.op !== Op.GetOk) return null;
    return messageFrom(reply.header, reply.payload, noAck ? undefined : this.#conn);
  }

  /** Closes the connection gracefully. Unacknowledged messages return to their queues. */
  close(): Promise<void> {
    return this.#conn.close();
  }
}

/** Parses the URL, connects and completes the handshake. */
export async function openClient(url: string, options: ConnectOptions, dial: Dialer): Promise<Client> {
  const conn = await Connection.open(parseUrl(url), options, dial);
  return new Client(conn);
}

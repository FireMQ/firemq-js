import type { Header } from './codec.js';
import { Op } from './codec.js';
import type { Connection, SubscriptionSink } from './connection.js';
import { type Properties, propertiesFromWire, type Table } from './options.js';

/** Sends `ack` and `nack` for a message. */
export interface Settler {
  settle(deliveryTag: number, ack: boolean, requeue: boolean): void;
}

/** The fields of a message. */
export interface MessageFields {
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  properties: Properties;
  headers: Table;
  redelivered: boolean;
  deliveryTag: number;
}

const utf8 = new TextDecoder();

/** A delivered message. */
export class Message implements MessageFields {
  /** The raw body. */
  readonly body: Uint8Array;
  /** Exchange the message was published to; empty for the default exchange. */
  readonly exchange: string;
  /** Routing key it was published with. */
  readonly routingKey: string;
  readonly properties: Properties;
  /** Application headers. */
  readonly headers: Table;
  /** True when the message was delivered before and not acknowledged. */
  readonly redelivered: boolean;
  /** Delivery tag, unique on the connection. */
  readonly deliveryTag: number;
  #settler: Settler | undefined;

  /** Messages come from `subscribe` and `get`; constructing one is useful in tests. */
  constructor(fields: MessageFields, settler?: Settler) {
    this.body = fields.body;
    this.exchange = fields.exchange;
    this.routingKey = fields.routingKey;
    this.properties = fields.properties;
    this.headers = fields.headers;
    this.redelivered = fields.redelivered;
    this.deliveryTag = fields.deliveryTag;
    this.#settler = settler;
  }

  /** The body decoded as UTF-8. */
  text(): string {
    return utf8.decode(this.body);
  }

  /** The body parsed as JSON. */
  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  /**
   * Acknowledges the message. Does nothing for `noAck` messages or when the message was
   * already acknowledged or rejected. Throws `ClosedError` when the connection is closed.
   */
  ack(): void {
    this.#settle(true, false);
  }

  /**
   * Rejects the message. With `requeue` (the default) it is delivered again; otherwise it is
   * dead-lettered when the queue has a dead-letter exchange, and dropped if not.
   */
  nack(requeue = true): void {
    this.#settle(false, requeue);
  }

  #settle(ack: boolean, requeue: boolean): void {
    const s = this.#settler;
    if (!s) return;
    this.#settler = undefined;
    s.settle(this.deliveryTag, ack, requeue);
  }
}

/** Builds a message from a `deliver` or `get_ok` header and body. */
export function messageFrom(h: Header, body: Uint8Array, settler: Settler | undefined): Message {
  return new Message(
    {
      body,
      exchange: typeof h.exchange === 'string' ? h.exchange : '',
      routingKey: typeof h.key === 'string' ? h.key : '',
      properties: propertiesFromWire(h.props),
      headers: h.headers !== null && typeof h.headers === 'object' ? (h.headers as Table) : {},
      redelivered: h.redelivered === true,
      deliveryTag: typeof h.tag === 'number' ? h.tag : Number(h.tag ?? 0),
    },
    settler,
  );
}

interface Waiter {
  resolve(message: Message | undefined): void;
  reject(error: Error): void;
  timer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Messages from a queue. Iterate it with `for await`, or call `receive()`. Leaving a
 * `for await` loop early cancels the subscription.
 *
 * The broker sends at most `prefetch` messages ahead. Credit is granted back in batches of
 * half that window as messages are acknowledged or rejected, or, with `noAck`, as they are
 * taken from the subscription.
 */
export class Subscription implements AsyncIterableIterator<Message>, SubscriptionSink, Settler {
  /** The queue this subscription consumes from. */
  readonly queue: string;
  /** Subscription id on the connection. */
  readonly id: number;
  /** True when messages count as acknowledged once sent. */
  readonly noAck: boolean;

  readonly #conn: Connection;
  readonly #window: number;
  #buffer: Message[] = [];
  #waiters: Waiter[] = [];
  #state: 'active' | 'cancelling' | 'ended' = 'active';
  #error: Error | undefined;
  #released = 0;

  /** Subscriptions come from `Client.subscribe`. */
  constructor(conn: Connection, id: number, queue: string, window: number, noAck: boolean) {
    this.#conn = conn;
    this.id = id;
    this.queue = queue;
    this.#window = window;
    this.noAck = noAck;
  }

  /** False once the subscription was cancelled or ended. */
  get active(): boolean {
    return this.#state === 'active';
  }

  /**
   * Waits for the next message. Resolves `undefined` when the subscription ends, or when
   * `timeoutMs` passes first. Rejects when the connection is lost.
   */
  receive(timeoutMs?: number): Promise<Message | undefined> {
    const next = this.#buffer.shift();
    if (next) {
      this.#taken();
      return Promise.resolve(next);
    }
    if (this.#state !== 'active') {
      return this.#error ? Promise.reject(this.#error) : Promise.resolve(undefined);
    }
    return new Promise<Message | undefined>((resolve, reject) => {
      const w: Waiter = { resolve, reject, timer: undefined };
      if (timeoutMs !== undefined) {
        w.timer = setTimeout(() => {
          const i = this.#waiters.indexOf(w);
          if (i >= 0) this.#waiters.splice(i, 1);
          resolve(undefined);
        }, timeoutMs);
      }
      this.#waiters.push(w);
    });
  }

  async next(): Promise<IteratorResult<Message, undefined>> {
    const message = await this.receive();
    return message ? { value: message, done: false } : { value: undefined, done: true };
  }

  /** Called when a `for await` loop exits early: cancels the subscription. */
  async return(): Promise<IteratorResult<Message, undefined>> {
    await this.cancel().catch(() => {});
    return { value: undefined, done: true };
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  /**
   * Stops the subscription and ends iteration. Messages received but not yet taken are
   * requeued (with `noAck` they are dropped). Messages already taken can still be
   * acknowledged.
   */
  async cancel(): Promise<void> {
    if (this.#state !== 'active') return;
    this.#state = 'cancelling';
    const pending = this.#buffer.splice(0);
    for (const w of this.#waiters.splice(0)) {
      clearTimeout(w.timer);
      w.resolve(undefined);
    }
    try {
      if (!this.noAck) for (const m of pending) m.nack(true);
      await this.#conn.request(Op.Unsubscribe, (rid) => ({ rid, sub: this.id }));
    } catch (e) {
      if (!this.#conn.isClosed) throw e;
    } finally {
      this.#state = 'ended';
      if (this.#conn.subs.get(this.id) === this) this.#conn.subs.delete(this.id);
    }
  }

  /** @internal Settler: sends ack or nack and grants credit back. */
  settle(tag: number, ack: boolean, requeue: boolean): void {
    this.#conn.settle(tag, ack, requeue);
    this.#release(1);
  }

  /** @internal SubscriptionSink */
  deliver(header: Header, body: Uint8Array): void {
    const message = messageFrom(header, body, this.noAck ? undefined : this);
    if (this.#state !== 'active') {
      // In flight while cancelling: hand it back to the queue.
      if (!this.noAck) {
        try {
          message.nack(true);
        } catch {
          // closed
        }
      }
      return;
    }
    const w = this.#waiters.shift();
    if (w) {
      clearTimeout(w.timer);
      this.#taken();
      w.resolve(message);
    } else {
      this.#buffer.push(message);
    }
  }

  /** @internal SubscriptionSink */
  serverCancelled(): void {
    // Buffered messages stay available; iteration ends after them.
    this.#end(undefined, false);
  }

  /** @internal SubscriptionSink */
  connectionClosed(error: Error | undefined): void {
    this.#end(error, true);
  }

  #end(error: Error | undefined, dropBuffer: boolean): void {
    if (this.#state === 'ended') return;
    this.#state = 'ended';
    this.#error = error;
    if (dropBuffer) this.#buffer = [];
    for (const w of this.#waiters.splice(0)) {
      clearTimeout(w.timer);
      if (error) w.reject(error);
      else w.resolve(undefined);
    }
  }

  #taken(): void {
    if (this.noAck) this.#release(1);
  }

  #release(n: number): void {
    if (this.#state !== 'active') return;
    this.#released += n;
    const batch = Math.max(1, Math.floor(this.#window / 2));
    if (this.#released >= batch) {
      const credit = this.#released;
      this.#released = 0;
      try {
        this.#conn.send(Op.Credit, { sub: this.id, credit });
      } catch {
        // closed
      }
    }
  }
}

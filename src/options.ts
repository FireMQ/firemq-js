import type { ServerError } from './errors.js';

/** Application headers and arguments: a map whose values are any MessagePack type. */
export type Table = Record<string, unknown>;

/** Message properties, mirroring AMQP's basic properties. */
export interface Properties {
  contentType?: string;
  contentEncoding?: string;
  correlationId?: string;
  replyTo?: string;
  /** Time to live in milliseconds. */
  expiration?: number;
  messageId?: string;
  /** Seconds since the Unix epoch. */
  timestamp?: number;
  type?: string;
  /** Must equal the connection's user when set. */
  userId?: string;
  appId?: string;
  /** 0 to 255. */
  priority?: number;
}

/** The camelCase property name and its key on the wire. */
export const PROPERTY_KEYS: ReadonlyArray<readonly [keyof Properties, string]> = [
  ['contentType', 'content_type'],
  ['contentEncoding', 'content_encoding'],
  ['correlationId', 'correlation_id'],
  ['replyTo', 'reply_to'],
  ['expiration', 'expiration'],
  ['messageId', 'message_id'],
  ['timestamp', 'timestamp'],
  ['type', 'type'],
  ['userId', 'user_id'],
  ['appId', 'app_id'],
  ['priority', 'priority'],
];

export function propertiesToWire(p: Properties | undefined): Table | undefined {
  if (!p) return undefined;
  const out: Table = {};
  let any = false;
  for (const [name, key] of PROPERTY_KEYS) {
    const v = p[name];
    if (v !== undefined && v !== null) {
      out[key] = v;
      any = true;
    }
  }
  return any ? out : undefined;
}

export function propertiesFromWire(wire: unknown): Properties {
  const out: Properties = {};
  if (wire === null || typeof wire !== 'object') return out;
  const w = wire as Table;
  for (const [name, key] of PROPERTY_KEYS) {
    const v = w[key];
    if (v !== undefined && v !== null) (out as Table)[name] = v;
  }
  return out;
}

/** TLS settings for `fires://`, and for `wss://` on Node. */
export interface TlsOptions {
  /** Extra trusted CA certificates (PEM), for example a self-signed one. */
  ca?: string | Uint8Array | Array<string | Uint8Array>;
  /** Client certificate chain (PEM), for mutual TLS. */
  cert?: string | Uint8Array;
  /** Client private key (PEM), for mutual TLS. */
  key?: string | Uint8Array;
  /** Verify the server certificate. Defaults to true. Set false only for local development. */
  rejectUnauthorized?: boolean;
  /** Server name for SNI and certificate checks. Defaults to the URL's host. */
  servername?: string;
}

/** The parts of the standard `WebSocket` class the client uses. */
export interface WebSocketLike {
  binaryType: string;
  readonly readyState: number;
  readonly bufferedAmount: number;
  // Event types differ between implementations, so the handlers take `any`.
  onopen: ((ev: any) => unknown) | null;
  onmessage: ((ev: any) => unknown) | null;
  onclose: ((ev: any) => unknown) | null;
  onerror: ((ev: any) => unknown) | null;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
}

/** A `WebSocket` constructor, such as the global one or the one from the `ws` package. */
export type WebSocketConstructor = new (url: string, protocols?: string | string[]) => WebSocketLike;

/** How to connect. */
export interface ConnectOptions {
  /** Heartbeat interval in seconds. 0 disables heartbeats. Defaults to 30. */
  heartbeat?: number;
  /** Shown in the management UI. */
  connectionName?: string;
  /** How long to wait for the connection and the handshake, in milliseconds. Defaults to 10 000. */
  connectTimeout?: number;
  /** TLS settings for `fires://`, and for `wss://` on Node. */
  tls?: TlsOptions;
  /**
   * WebSocket implementation for `ws://` and `wss://`. Defaults to the global `WebSocket`, and
   * on Node versions without one, to a small built-in client.
   */
  WebSocket?: WebSocketConstructor;
  /**
   * Called for errors the broker reports outside any request, such as acknowledging an
   * unknown delivery tag. These errors do not close the connection.
   */
  onError?: (error: ServerError) => void;
}

/** Options for `declareQueue`. Queues are durable by default. */
export interface QueueOptions {
  /** Survives a broker restart. Defaults to true. */
  durable?: boolean;
  /** Used only by this connection and deleted when it closes. */
  exclusive?: boolean;
  /** Deleted when its last subscription is removed. */
  autoDelete?: boolean;
  /** Only check that the queue exists. */
  passive?: boolean;
  /** Shorthand for `durable: false, exclusive: true, autoDelete: true`. */
  temporary?: boolean;
  /** RabbitMQ queue arguments, for example `x-message-ttl` or `x-dead-letter-exchange`. */
  args?: Table;
}

export const QueueOptions = {
  /** A queue that lives only as long as this connection. */
  temporary(extra: Omit<QueueOptions, 'temporary'> = {}): QueueOptions {
    return { durable: false, exclusive: true, autoDelete: true, ...extra };
  },
};

/** Exchange types. Other strings are passed through for plugins. */
export type ExchangeType = 'direct' | 'fanout' | 'topic' | 'headers' | (string & {});

/** Options for `declareExchange`. Exchanges are durable by default. */
export interface ExchangeOptions {
  /** Survives a broker restart. Defaults to true. */
  durable?: boolean;
  /** Deleted when its last binding is removed. */
  autoDelete?: boolean;
  /** Cannot be published to directly. */
  internal?: boolean;
  /** Only check that the exchange exists. */
  passive?: boolean;
  /** Exchange arguments, for example `alternate-exchange`. */
  args?: Table;
}

/** Options for `publish`. Messages are persistent and confirmed by default. */
export interface PublishOptions {
  /** Write the message to disk. Defaults to true. */
  persistent?: boolean;
  /** Fail with `UnroutableError` when no queue receives the message. */
  mandatory?: boolean;
  /**
   * Wait for the broker's confirm. Defaults to true. When false, `publish` resolves once the
   * frame is handed to the socket.
   */
  confirm?: boolean;
  properties?: Properties;
  /** Application headers. */
  headers?: Table;
}

/** Options for `subscribe`. */
export interface SubscribeOptions {
  /** How many unacknowledged messages the broker may send at once. Defaults to 100. */
  prefetch?: number;
  /** Treat messages as acknowledged when sent. Faster, but messages are lost if the consumer crashes. */
  noAck?: boolean;
  /** Fail with 405 if the queue has other consumers, and block new ones. */
  exclusive?: boolean;
  /** Consumer tag shown in the management UI. */
  tag?: string;
  /** Consumer arguments. */
  args?: Table;
}

/** Options for `get`. */
export interface GetOptions {
  /** Treat the message as acknowledged when sent. */
  noAck?: boolean;
}

/** Options for `deleteQueue`. */
export interface DeleteQueueOptions {
  /** Only delete the queue when it has no consumers. */
  ifUnused?: boolean;
  /** Only delete the queue when it has no messages. */
  ifEmpty?: boolean;
}

/** Options for `deleteExchange`. */
export interface DeleteExchangeOptions {
  /** Only delete the exchange when it has no bindings. */
  ifUnused?: boolean;
}

/** What `declareQueue` reports. */
export interface QueueInfo {
  name: string;
  messages: number;
  consumers: number;
}

/** What the broker said in `welcome`. */
export interface ServerInfo {
  protocolVersion: number;
  /** Server product name, `firemq`. */
  name: string;
  version: string;
  /** Connection name, as shown in the management API. */
  connectionId: string;
  /** Negotiated heartbeat interval in seconds. */
  heartbeat: number;
  /** Largest frame the server accepts, in bytes. */
  maxFrame: number;
  capabilities: string[];
}

/** A message body. Strings are encoded as UTF-8. */
export type Body = string | Uint8Array | ArrayBuffer | ArrayBufferView;

const utf8 = new TextEncoder();

export function bodyBytes(body: Body): Uint8Array {
  if (typeof body === 'string') return utf8.encode(body);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  if (ArrayBuffer.isView(body)) return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
  throw new TypeError('body must be a string, Uint8Array, Buffer, ArrayBuffer or ArrayBufferView');
}

/** Base class of every error the client raises. */
export class FireMQError extends Error {
  /** The AMQP reply code, when the broker sent one. */
  readonly code: number | undefined;

  constructor(message: string, code?: number, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'FireMQError';
    this.code = code;
  }
}

/** The broker refused a request. `code` is an AMQP reply code such as 404 or 406. */
export class ServerError extends FireMQError {
  declare readonly code: number;
  /** The broker's reason, without the code. */
  readonly reason: string;

  constructor(code: number, reason: string) {
    super(`${reason} (code ${code})`, code);
    this.name = 'ServerError';
    this.reason = reason;
  }
}

/** The broker rejected a publish, for example because a queue with `x-overflow: reject-publish` was full. */
export class NackedError extends FireMQError {
  declare readonly code: number;
  readonly reason: string;

  constructor(code: number, reason: string) {
    super(`publish rejected: ${reason} (code ${code})`, code);
    this.name = 'NackedError';
    this.reason = reason;
  }
}

/** A mandatory publish matched no queue. The code is always 312 (no-route). */
export class UnroutableError extends FireMQError {
  declare readonly code: number;
  readonly exchange: string;
  readonly routingKey: string;

  constructor(exchange: string, routingKey: string) {
    super(
      `message returned: no queue is bound to exchange '${exchange}' with key '${routingKey}'`,
      312,
    );
    this.name = 'UnroutableError';
    this.exchange = exchange;
    this.routingKey = routingKey;
  }
}

/** The connection is closed, or closed while the operation was in flight. */
export class ClosedError extends FireMQError {
  constructor(reason: string, options?: { cause?: unknown }) {
    super(`connection closed: ${reason}`, undefined, options);
    this.name = 'ClosedError';
  }
}

/** The client could not reach the broker: DNS, TCP, TLS or WebSocket failures and timeouts. */
export class ConnectionError extends FireMQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, undefined, options);
    this.name = 'ConnectionError';
  }
}

/** The broker sent something this client does not understand, or a frame was too large. */
export class ProtocolError extends FireMQError {
  constructor(message: string) {
    super(message);
    this.name = 'ProtocolError';
  }
}

/** A connection URL could not be parsed. */
export class UrlError extends FireMQError {
  constructor(message: string) {
    super(message);
    this.name = 'UrlError';
  }
}

/** A readable reason for any thrown value, including Node's AggregateError from happy eyeballs. */
export function describeError(e: unknown): string {
  if (e === undefined || e === null) return '';
  if (typeof e === 'string') return e;
  const err = e as { message?: unknown; code?: unknown; errors?: unknown; error?: unknown };
  if (typeof err.message === 'string' && err.message !== '') return err.message;
  if (Array.isArray(err.errors) && err.errors.length > 0) {
    return [...new Set(err.errors.map(describeError).filter((s) => s !== ''))].join('; ');
  }
  if (err.error !== undefined && err.error !== e) return describeError(err.error);
  if (typeof err.code === 'string') return err.code;
  return '';
}

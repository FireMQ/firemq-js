# firemq-js

TypeScript client for [FireMQ](https://github.com/FireMQ/firemq), a RabbitMQ-compatible message broker. It speaks the [FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md) over TCP, TLS and WebSocket, and runs on Node.js 18+, Deno, Bun and browsers.

- One dependency, [`@msgpack/msgpack`](https://www.npmjs.com/package/@msgpack/msgpack). No native code.
- Safe defaults: queues and exchanges are durable, messages are persistent, and `publish` resolves only when the broker confirms.
- Consumers are async iterables with credit-based flow control.
- Passes all 19 scenarios of the FireMQ SDK conformance suite.

## Install

```bash
npm install @firemq/client
```

The package ships ES modules and a CommonJS build with type declarations.

## Quickstart

Start a broker, for example with `firemq --dev`, then:

```js
import { connect } from '@firemq/client';

const client = await connect('fire://guest:guest@localhost:5673');
await client.declareQueue('jobs');

// Resolves once the broker has the message.
await client.publish('', 'jobs', 'hello');

const jobs = await client.subscribe('jobs');
for await (const msg of jobs) {
  console.log(msg.text());
  msg.ack();
  break; // leaving the loop cancels the subscription
}

await client.close();
```

### In the browser

Browsers connect over WebSocket. The broker serves it on its HTTP port at `/fire`:

```js
import { connect, QueueOptions } from '@firemq/client';

const client = await connect('wss://mq.example.com:15671/fire?vhost=prod');
const { name } = await client.declareQueue('', QueueOptions.temporary());
```

Bundlers such as Vite, webpack and esbuild pick the browser build automatically; it contains only the WebSocket transport. [`examples/browser.html`](examples/browser.html) is a complete page that runs without a bundler.

## Connection URLs

| Scheme | Transport | Default port | Runs on |
|---|---|---|---|
| `fire://` | TCP | 5673 | Node, Deno, Bun |
| `fires://` | TLS | 5674 | Node, Deno, Bun |
| `ws://` | WebSocket | 15672 | everywhere |
| `wss://` | WebSocket over TLS | 15671 | everywhere |

The user and password default to `guest` and `guest`. For `fire` and `fires`, the path is the vhost, percent-decoded (`fire://host/prod%2Feu` is vhost `prod/eu`); an empty path is vhost `/`. For `ws` and `wss`, the path is the endpoint (`/fire` when empty) and the vhost is the `vhost` query parameter: `ws://localhost:15672/fire?vhost=dev`.

## API

### `connect(url, options?)`

Opens a connection and completes the handshake. Returns a `Client`. One client is one connection; share it across your application.

| Option | Default | Meaning |
|---|---|---|
| `heartbeat` | `30` | Heartbeat interval in seconds; `0` disables heartbeats. The client pings after `heartbeat / 2` seconds without sending, and closes the connection after `2 × heartbeat` seconds without hearing from the broker. |
| `connectionName` | | Shown in the management UI. |
| `connectTimeout` | `10000` | Milliseconds to wait for the connection and the handshake. |
| `tls` | | TLS settings; see [TLS](#tls). |
| `WebSocket` | global `WebSocket` | WebSocket implementation for `ws://` and `wss://`; see [WebSocket](#websocket). |
| `onError` | | Called with a `ServerError` for errors the broker reports outside any request, such as acknowledging an unknown delivery tag. They do not close the connection. |

`client.server` holds what the broker said in its welcome: `name`, `version`, `connectionId`, `heartbeat`, `maxFrame` and `capabilities`. `client.isClosed` tells whether the connection is closed, and `client.closed` is a promise that resolves with the reason once it closes, for any reason.

`client.close()` closes the connection gracefully. Unacknowledged messages return to their queues.

### Queues and exchanges

```js
await client.declareQueue('orders', { args: { 'x-message-ttl': 60_000 } }); // → { name, messages, consumers }
await client.declareQueue('', QueueOptions.temporary()); // server-named, deleted with the connection
await client.deleteQueue('orders', { ifUnused: true, ifEmpty: true }); // → messages deleted
await client.purgeQueue('orders'); // → messages purged

await client.declareExchange('events', 'topic'); // direct, fanout, topic or headers
await client.deleteExchange('events', { ifUnused: true });

await client.bindQueue('orders', 'events', 'order.*');
await client.unbindQueue('orders', 'events', 'order.*');
await client.bindExchange('audit', 'events', '#'); // destination, source, key
await client.unbindExchange('audit', 'events', '#');
```

`declareQueue` options: `durable` (default `true`), `exclusive`, `autoDelete`, `passive`, `args` with RabbitMQ's queue arguments, and `temporary: true` as a shorthand for `durable: false, exclusive: true, autoDelete: true`. `declareExchange` options: `durable` (default `true`), `autoDelete`, `internal`, `passive` and `args`. The binding methods take an optional fourth argument with binding arguments, for example `x-match` for headers exchanges.

### Publishing

```js
await client.publish('events', 'order.created', JSON.stringify(order), {
  properties: { contentType: 'application/json', messageId: order.id },
  headers: { 'x-tenant': 'acme' },
});

await client.publishJson('events', 'order.created', order); // sets contentType for you
```

`publish(exchange, routingKey, body, options?)` resolves when the broker confirms the message. Use exchange `''` to send straight to the queue named by the routing key. The body is a string (sent as UTF-8), `Uint8Array`, `Buffer`, `ArrayBuffer` or any typed array.

| Option | Default | Meaning |
|---|---|---|
| `persistent` | `true` | Write the message to disk. |
| `mandatory` | `false` | Reject with `UnroutableError` when no queue receives the message. |
| `confirm` | `true` | Wait for the broker's confirm. With `false`, the promise resolves once the frame is handed to the socket. |
| `properties` | | `contentType`, `contentEncoding`, `correlationId`, `replyTo`, `expiration` (ms), `messageId`, `timestamp` (seconds), `type`, `userId`, `appId`, `priority`. |
| `headers` | | Application headers. Values can be any MessagePack type: strings, numbers, booleans, `null`, arrays, objects and `Uint8Array`. |

Confirms are cumulative, so publishing many messages concurrently and awaiting them together is fast:

```js
await Promise.all(batch.map((m) => client.publish('', 'jobs', m)));
```

When the broker raises a memory or disk alarm, it asks clients to pause; `publish` waits until the alarm clears.

### Consuming

```js
const sub = await client.subscribe('jobs', { prefetch: 50 });
for await (const msg of sub) {
  try {
    await handle(msg.json());
    msg.ack();
  } catch {
    msg.nack(false); // dead-letter it, or drop it when the queue has no dead-letter exchange
  }
}
```

`subscribe(queue, options?)` returns a `Subscription`, an async iterable of messages.

| Option | Default | Meaning |
|---|---|---|
| `prefetch` | `100` | How many unacknowledged messages the broker may send ahead. |
| `noAck` | `false` | Messages count as acknowledged once sent. Faster, but a crash loses them. |
| `exclusive` | `false` | Fail with code 405 if the queue has other consumers, and keep new ones out. |
| `tag` | | Consumer tag shown in the management UI. |

The client grants the whole `prefetch` window at subscribe time and grants credit back in batches of half the window as you acknowledge or reject messages (with `noAck`, as you take them from the subscription).

`sub.receive(timeoutMs?)` waits for one message and resolves `undefined` on timeout or when the subscription ends. `sub.cancel()` stops the subscription: iteration ends, messages that arrived but were not taken go back to the queue, and messages you already took can still be acknowledged. Leaving a `for await` loop with `break`, `return` or an exception cancels the subscription too. When the broker ends the subscription, for example because the queue was deleted, iteration ends after the messages already received. When the connection is lost, the loop throws the `ClosedError`; after `client.close()` it ends quietly.

### Getting one message

```js
const msg = await client.get('jobs'); // a Message, or null when the queue is empty
msg?.ack();
```

Pass `{ noAck: true }` to take the message without acknowledging it.

### Messages

A `Message` has `body` (a `Uint8Array`), `text()`, `json()`, `exchange`, `routingKey`, `properties`, `headers`, `redelivered` and `deliveryTag`.

`msg.ack()` acknowledges it. `msg.nack(requeue = true)` rejects it: requeued messages are delivered again with `redelivered: true`; the others are dead-lettered when the queue has a dead-letter exchange, and dropped otherwise. Settling a message twice, or settling a `noAck` message, does nothing.

## Errors

Every error extends `FireMQError`, which has an optional `code` with the AMQP reply code.

| Error | When |
|---|---|
| `ServerError` | The broker refused a request. `code` is, for example, 403 (access refused), 404 (not found), 405 (resource locked) or 406 (precondition failed, such as redeclaring a queue with other properties). The connection stays usable unless the error was fatal. |
| `UnroutableError` | A `mandatory` publish reached no queue. `code` is 312; `exchange` and `routingKey` say where it went. |
| `NackedError` | The broker rejected a publish, for example because a queue with `x-overflow: reject-publish` is full. |
| `ClosedError` | The connection is closed, or closed while the call was in flight. |
| `ConnectionError` | The broker could not be reached: DNS, TCP, TLS or WebSocket failures, and timeouts. |
| `ProtocolError` | The broker sent something the client does not understand, or a message is larger than the broker's frame limit. |
| `UrlError` | The connection URL is invalid. |

```js
import { ServerError, UnroutableError } from '@firemq/client';

try {
  await client.publish('events', 'nobody.listens', 'hi', { mandatory: true });
} catch (e) {
  if (e instanceof UnroutableError) console.warn(`no queue for ${e.routingKey}`);
  else throw e;
}
```

## TLS

Use `fires://` for TLS. The client verifies the server's certificate against Node's built-in list of trusted CAs. To trust a private or self-signed CA, pass it in `tls.ca`:

```js
import { readFileSync } from 'node:fs';

const client = await connect('fires://mq.internal:5674', {
  tls: { ca: readFileSync('ca.pem') },
});
```

`tls` also takes `cert` and `key` for client certificates, `servername` to override the name checked against the certificate, and `rejectUnauthorized: false` to skip verification, which is only for local development. On Node, the `tls` settings also apply to `wss://`.

## WebSocket

The WebSocket transport sends one binary message per frame, with the subprotocol `firemq.v1`. Browsers, Deno, Bun and Node 22+ use their global `WebSocket`. Node 18 and 20 have no global `WebSocket`, so the client uses a small built-in implementation instead; the built-in one is also used for `wss://` when you pass `tls` settings, because the global one cannot take a custom CA.

To use another implementation, such as the [`ws`](https://www.npmjs.com/package/ws) package, pass its constructor:

```js
import WebSocket from 'ws';

const client = await connect('ws://localhost:15672/fire', { WebSocket });
```

## Deno and Bun

Both run the Node build: `fire://` and `fires://` go through their Node compatibility layer and `ws://` through their global `WebSocket`. In Deno, import it with `import { connect } from 'npm:@firemq/client'`.

## Examples

[`examples/`](examples) has runnable versions of the RabbitMQ tutorials: [hello world](examples/hello_world.mjs), [work queues](examples/work_queues.mjs), [publish/subscribe](examples/publish_subscribe.mjs), [routing](examples/routing.mjs), [topics](examples/topics.mjs) and [RPC](examples/rpc.mjs), plus a [browser page](examples/browser.html). Each one reads the broker URL from `FIREMQ_URL`:

```bash
npm install && npm run build
FIREMQ_URL=fire://localhost node examples/hello_world.mjs
```

## Development

```bash
npm install
npm run lint                # type-check
npm test                    # unit tests, including every protocol test vector
npm run build               # dist/esm and dist/cjs
npm run test:integration    # against a real broker
```

The integration tests start the broker binary at `FIREMQ_BIN` (default `../.bin/firemq`) on free ports, or use a running broker when `FIREMQ_URL` is set; see [`test/integration/broker.ts`](test/integration/broker.ts) for the other variables.

The conformance driver in [`conformance/driver.mjs`](conformance/driver.mjs) runs the SDK through the [FireMQ conformance suite](https://github.com/FireMQ/firemq/blob/main/spec/conformance.md). Build first, then:

```bash
firemq-conformance run --driver "node conformance/driver.mjs"
FIREMQ_DRIVER_TRANSPORT=ws firemq-conformance run --driver "node conformance/driver.mjs"
```

The second run sends every scenario over WebSocket.

## License

Apache-2.0. See [LICENSE](LICENSE).

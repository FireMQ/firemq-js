// Runs the client against a real broker over every transport the broker offers.

import assert from 'node:assert/strict';
import * as net from 'node:net';
import { after, describe, it } from 'node:test';
import * as tls from 'node:tls';

import {
  type Client,
  ClosedError,
  type ConnectOptions,
  connect,
  NackedError,
  QueueOptions,
  ServerError,
  UnroutableError,
} from '../../src/index.js';
import { NodeWebSocket } from '../../src/transport/node-websocket.js';
import { startBroker, unique } from './broker.js';

const broker = await startBroker();
if (!broker) console.error('no FireMQ broker: set FIREMQ_URL or FIREMQ_BIN to run the integration tests');

interface TransportCase {
  name: string;
  url: string;
  options: ConnectOptions;
}

const cases: TransportCase[] = [];
let proxy: tls.Server | undefined;

if (broker) {
  cases.push({ name: 'fire://', url: broker.fireUrl, options: {} });
  if (broker.wsUrl) {
    if ((globalThis as { WebSocket?: unknown }).WebSocket) {
      cases.push({ name: 'ws:// (global WebSocket)', url: broker.wsUrl, options: {} });
    }
    cases.push({ name: 'ws:// (built-in client)', url: broker.wsUrl, options: { WebSocket: NodeWebSocket } });
  }
  if (broker.tlsUrl && broker.tlsCa) {
    cases.push({ name: 'fires://', url: broker.tlsUrl, options: { tls: { ca: broker.tlsCa } } });
  }
  if (broker.tlsKey && broker.tlsCert && broker.httpAddress) {
    // The broker's HTTP listener is plain; terminate TLS in front of it to test wss://.
    const upstream = broker.httpAddress;
    proxy = tls.createServer({ key: broker.tlsKey, cert: broker.tlsCert }, (client) => {
      const server = net.connect(upstream.port, upstream.host);
      client.pipe(server).pipe(client);
      client.on('error', () => server.destroy());
      server.on('error', () => client.destroy());
    });
    await new Promise<void>((r) => proxy!.listen(0, '127.0.0.1', r));
    const port = (proxy.address() as net.AddressInfo).port;
    cases.push({ name: 'wss:// (built-in client, TLS)', url: `wss://localhost:${port}/fire`, options: { tls: { ca: broker.tlsCa! } } });
  }
}

after(async () => {
  proxy?.close();
  await broker?.stop();
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

for (const tc of cases) {
  describe(tc.name, () => {
    const open = (opts: ConnectOptions = {}) => connect(tc.url, { ...tc.options, ...opts });

    async function withClient(fn: (c: Client) => Promise<void>): Promise<void> {
      const c = await open();
      try {
        await fn(c);
      } finally {
        await c.close();
      }
    }

    it('connects and reports the server', () =>
      withClient(async (c) => {
        assert.equal(c.server.protocolVersion, 1);
        assert.equal(c.server.name, 'firemq');
        assert.ok(c.server.capabilities.includes('confirms'));
        assert.ok(c.server.heartbeat > 0);
        assert.ok(c.server.maxFrame >= 65_536);
      }));

    it('refuses a wrong password with 403', async () => {
      const bad = tc.url.replace('guest:guest@', '').replace('://', '://guest:wrong@');
      await assert.rejects(connect(bad, tc.options), (e: unknown) => e instanceof ServerError && e.code === 403);
    });

    it('publishes and gets', () =>
      withClient(async (c) => {
        const q = unique('it.get');
        await c.declareQueue(q, { durable: false });
        assert.equal(await c.get(q), null);
        await c.publish('', q, 'hello');
        const m = await c.get(q, { noAck: true });
        assert.equal(m?.text(), 'hello');
        assert.equal(m?.exchange, '');
        assert.equal(m?.routingKey, q);
        assert.equal(m?.redelivered, false);
        assert.equal(await c.get(q), null);
        await c.deleteQueue(q);
      }));

    it('consumes in order with acks and leaves the queue empty', () =>
      withClient(async (c) => {
        const q = unique('it.consume');
        await c.declareQueue(q, { durable: false });
        for (let i = 0; i < 50; i++) await c.publish('', q, `m${i}`);
        const sub = await c.subscribe(q, { prefetch: 10 });
        let i = 0;
        for await (const m of sub) {
          assert.equal(m.text(), `m${i}`);
          m.ack();
          if (++i === 50) break;
        }
        const info = await c.declareQueue(q, { passive: true });
        assert.equal(info.messages, 0);
        await c.deleteQueue(q);
      }));

    it('stops at the prefetch window until a message is acknowledged', () =>
      withClient(async (c) => {
        const q = unique('it.prefetch');
        await c.declareQueue(q, { durable: false });
        for (const b of ['a', 'b', 'c']) await c.publish('', q, b);
        const sub = await c.subscribe(q, { prefetch: 2 });
        const a = await sub.receive(3000);
        const b = await sub.receive(3000);
        assert.deepEqual([a?.text(), b?.text()], ['a', 'b']);
        assert.equal(await sub.receive(300), undefined);
        a!.ack();
        assert.equal((await sub.receive(3000))?.text(), 'c');
        await sub.cancel();
        await c.deleteQueue(q);
      }));

    it('redelivers a message nacked with requeue', () =>
      withClient(async (c) => {
        const q = unique('it.requeue');
        await c.declareQueue(q, { durable: false });
        await c.publish('', q, 'again');
        const sub = await c.subscribe(q, { prefetch: 1 });
        const first = await sub.receive(3000);
        assert.equal(first?.redelivered, false);
        first!.nack(true);
        const second = await sub.receive(3000);
        assert.equal(second?.text(), 'again');
        assert.equal(second?.redelivered, true);
        second!.ack();
        await sub.cancel();
        await c.deleteQueue(q);
      }));

    it('dead-letters a message nacked without requeue', () =>
      withClient(async (c) => {
        const dlx = unique('it.dlx');
        const dead = unique('it.dead');
        const work = unique('it.work');
        await c.declareExchange(dlx, 'fanout', { durable: false });
        await c.declareQueue(dead, { durable: false });
        await c.bindQueue(dead, dlx);
        await c.declareQueue(work, { durable: false, args: { 'x-dead-letter-exchange': dlx } });
        await c.publish('', work, 'bad');
        const m = await c.get(work);
        m!.nack(false);
        let deadMsg = null;
        for (let i = 0; i < 50 && !deadMsg; i++) {
          deadMsg = await c.get(dead, { noAck: true });
          if (!deadMsg) await sleep(20);
        }
        assert.equal(deadMsg?.text(), 'bad');
        assert.equal(deadMsg?.exchange, dlx);
        assert.ok(Array.isArray(deadMsg?.headers['x-death']));
        await c.deleteQueue(work);
        await c.deleteQueue(dead);
        await c.deleteExchange(dlx);
      }));

    it('rejects a mandatory publish nobody receives with UnroutableError', () =>
      withClient(async (c) => {
        const key = unique('it.nowhere');
        await assert.rejects(c.publish('', key, 'lost', { mandatory: true }), (e: unknown) => {
          assert.ok(e instanceof UnroutableError);
          assert.equal(e.code, 312);
          assert.equal(e.routingKey, key);
          return true;
        });
        await c.publish('', key, 'dropped');
      }));

    it('rejects a publish to a full reject-publish queue with NackedError', () =>
      withClient(async (c) => {
        const q = unique('it.full');
        await c.declareQueue(q, { durable: false, args: { 'x-max-length': 1, 'x-overflow': 'reject-publish' } });
        await c.publish('', q, 'fits');
        await assert.rejects(c.publish('', q, 'rejected'), (e: unknown) => e instanceof NackedError && e.code === 406);
        await c.deleteQueue(q);
      }));

    it('routes by topic pattern and through exchange-to-exchange bindings', () =>
      withClient(async (c) => {
        const up = unique('it.up');
        const topic = unique('it.topic');
        const q = unique('it.routed');
        await c.declareExchange(up, 'direct', { durable: false });
        await c.declareExchange(topic, 'topic', { durable: false });
        await c.declareQueue(q, { durable: false });
        await c.bindQueue(q, topic, '*.critical.#');
        await c.bindExchange(topic, up, 'kern.critical');
        await c.publish(topic, 'kern.critical.disk', '1');
        await c.publish(topic, 'kern.info', '2');
        await c.publish(up, 'kern.critical', '3');
        await c.publish(up, 'other', '4');
        const got = [];
        for (let m = await c.get(q, { noAck: true }); m; m = await c.get(q, { noAck: true })) got.push(m.text());
        assert.deepEqual(got, ['1', '3']);
        await c.unbindQueue(q, topic, '*.critical.#');
        await c.publish(topic, 'kern.critical', '5');
        assert.equal(await c.get(q), null);
        await c.deleteQueue(q);
        await c.deleteExchange(up);
        await c.deleteExchange(topic);
      }));

    it('round-trips every property and header type', () =>
      withClient(async (c) => {
        const q = unique('it.props');
        await c.declareQueue(q, { durable: false });
        const properties = {
          contentType: 'application/json',
          contentEncoding: 'gzip',
          correlationId: 'c-1',
          replyTo: 'replies',
          expiration: 600_000,
          messageId: 'm-1',
          timestamp: 1_767_225_600,
          type: 'order.created',
          appId: 'shop',
          priority: 3,
        };
        const headers = {
          bool: true,
          int: 42,
          negative: -7,
          big: 2 ** 40,
          float: 1.5,
          nil: null,
          string: 's',
          list: [1, 'two'],
          map: { k: 'v' },
          bytes: new Uint8Array([0, 255]),
        };
        await c.publishJson('', q, { id: 42 }, { properties, headers });
        const m = await c.get(q, { noAck: true });
        assert.deepEqual(m?.properties, properties);
        assert.deepEqual(m?.headers, headers);
        assert.deepEqual(m?.json(), { id: 42 });
        await c.deleteQueue(q);
      }));

    it('carries binary and 1 MiB bodies intact', () =>
      withClient(async (c) => {
        const q = unique('it.bodies');
        await c.declareQueue(q, { durable: false });
        const binary = new Uint8Array(256).map((_, i) => i);
        const large = new Uint8Array(1024 * 1024).map((_, i) => (i * 31) & 0xff);
        await c.publish('', q, binary);
        await c.publish('', q, Buffer.from(large));
        const sub = await c.subscribe(q, { noAck: true });
        assert.deepEqual((await sub.receive(5000))?.body, binary);
        assert.deepEqual((await sub.receive(5000))?.body, large);
        await sub.cancel();
        await c.deleteQueue(q);
      }));

    it('reports 406 and 404 and keeps the connection usable', () =>
      withClient(async (c) => {
        const q = unique('it.errors');
        await c.declareQueue(q, { durable: true });
        await assert.rejects(c.declareQueue(q, { durable: false }), (e: unknown) => e instanceof ServerError && e.code === 406);
        await assert.rejects(c.bindQueue(q, unique('it.missing')), (e: unknown) => e instanceof ServerError && e.code === 404);
        await c.publish('', q, 'still works');
        assert.equal((await c.get(q, { noAck: true }))?.text(), 'still works');
        await c.deleteQueue(q);
      }));

    it('reports purge and delete counts', () =>
      withClient(async (c) => {
        const q = unique('it.counts');
        await c.declareQueue(q, { durable: false });
        for (let i = 0; i < 3; i++) await c.publish('', q, 'x');
        assert.equal(await c.purgeQueue(q), 3);
        await c.publish('', q, 'y');
        assert.equal(await c.deleteQueue(q), 1);
      }));

    it('returns unacknowledged messages to the queue when the connection closes', async () => {
      const q = unique('it.unacked');
      await withClient(async (c) => {
        await c.declareQueue(q, { durable: false });
        await c.publish('', q, 'work');
        const sub = await c.subscribe(q, { prefetch: 1 });
        assert.equal((await sub.receive(3000))?.text(), 'work');
      });
      await withClient(async (c) => {
        const m = await c.get(q, { noAck: true });
        assert.equal(m?.text(), 'work');
        assert.equal(m?.redelivered, true);
        await c.deleteQueue(q);
      });
    });

    it('deletes an exclusive temporary queue with its connection', async () => {
      let name = '';
      await withClient(async (c) => {
        name = (await c.declareQueue('', QueueOptions.temporary())).name;
        assert.match(name, /^amq\.gen-/);
      });
      await withClient(async (c) => {
        await assert.rejects(c.declareQueue(name, { passive: true }), (e: unknown) => e instanceof ServerError && e.code === 404);
      });
    });

    it('stops deliveries on cancel and requeues what was not taken', () =>
      withClient(async (c) => {
        const q = unique('it.cancel');
        await c.declareQueue(q, { durable: false });
        for (const b of ['1', '2', '3']) await c.publish('', q, b);
        const sub = await c.subscribe(q, { prefetch: 10 });
        const first = await sub.receive(3000);
        first!.ack();
        await sleep(50); // let 2 and 3 arrive in the buffer
        await sub.cancel();
        assert.equal(await sub.receive(), undefined);
        await c.publish('', q, '4');
        const rest = [];
        for (let m = await c.get(q, { noAck: true }); m; m = await c.get(q, { noAck: true })) rest.push(m.text());
        assert.deepEqual(rest.sort(), ['2', '3', '4']);
        await c.deleteQueue(q);
      }));

    it('publishes without confirms', () =>
      withClient(async (c) => {
        const q = unique('it.noconfirm');
        await c.declareQueue(q, { durable: false });
        for (let i = 0; i < 100; i++) await c.publish('', q, `n${i}`, { confirm: false, persistent: false });
        await c.publish('', q, 'last'); // confirms are ordered after the earlier publishes
        const info = await c.declareQueue(q, { passive: true });
        assert.equal(info.messages, 101);
        await c.deleteQueue(q);
      }));

    it('fails calls with ClosedError after close()', async () => {
      const c = await open();
      await c.close();
      assert.equal(c.isClosed, true);
      await assert.rejects(c.declareQueue(unique('it.closed')), ClosedError);
    });

    if (tc.name === 'fire://' || tc.name.includes('built-in')) {
      it('keeps an idle connection alive with heartbeats', async () => {
        const c = await open({ heartbeat: 1 });
        assert.equal(c.server.heartbeat, 1);
        await sleep(2600);
        assert.equal(c.isClosed, false);
        await c.declareQueue(unique('it.hb'), QueueOptions.temporary());
        await c.close();
      });
    }
  });
}

if (cases.length > 1) {
  it('interoperates across transports', async () => {
    const [a, b] = [await connect(cases[0]!.url, cases[0]!.options), await connect(cases[1]!.url, cases[1]!.options)];
    try {
      const q = unique('it.interop');
      await a.declareQueue(q, { durable: false });
      await a.publish('', q, 'across', { headers: { from: cases[0]!.name } });
      const m = await b.get(q, { noAck: true });
      assert.equal(m?.text(), 'across');
      assert.equal(m?.headers.from, cases[0]!.name);
      await b.deleteQueue(q);
    } finally {
      await a.close();
      await b.close();
    }
  });
}

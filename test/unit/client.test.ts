import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { type Client, openClient } from '../../src/client.js';
import { Op } from '../../src/codec.js';
import { ClosedError, NackedError, ProtocolError, ServerError, UnroutableError } from '../../src/errors.js';
import { Message } from '../../src/message.js';
import { QueueOptions } from '../../src/options.js';
import { FakeServer, tick } from './fake-server.js';

const utf8 = new TextEncoder();

async function setup(configure?: (s: FakeServer) => void, options = {}): Promise<{ server: FakeServer; client: Client }> {
  const server = new FakeServer();
  configure?.(server);
  const client = await openClient('fire://localhost', options, server.dial);
  return { server, client };
}

function deliver(server: FakeServer, sub: number, tag: number, body = `m${tag}`) {
  server.push(Op.Deliver, { sub, tag, key: 'q' }, utf8.encode(body));
}

describe('handshake', () => {
  it('sends hello with credentials, vhost, heartbeat and client info', async () => {
    const server = new FakeServer();
    const client = await openClient('fire://app:secret@h/dev', { heartbeat: 0, connectionName: 'worker-1' }, server.dial);
    const hello = server.sent(Op.Hello)[0]!.header;
    assert.equal(hello.ver, 1);
    assert.equal(hello.vhost, 'dev');
    assert.deepEqual(hello.auth, { mech: 'plain', user: 'app', pass: 'secret' });
    assert.equal(hello.heartbeat, undefined); // 0 is the zero value and is omitted
    assert.deepEqual(hello.caps, ['confirms', 'credit']);
    const info = hello.client as Record<string, unknown>;
    assert.equal(info.product, 'firemq-js');
    assert.equal(info.connection_name, 'worker-1');
    assert.equal(client.server.connectionId, 'fake');
    assert.equal(client.server.maxFrame, 1 << 20);
    await client.close();
  });

  it('fails with ServerError when the broker refuses the login', async () => {
    const server = new FakeServer();
    server.handler = (f) => {
      if (f.op !== Op.Hello) return false;
      server.push(Op.Error, { code: 403, msg: 'ACCESS_REFUSED', fatal: true });
      return true;
    };
    await assert.rejects(openClient('fire://localhost', {}, server.dial), (e: unknown) => {
      assert.ok(e instanceof ServerError);
      assert.equal(e.code, 403);
      return true;
    });
  });

  it('fails when the server answers hello with something else', async () => {
    const server = new FakeServer();
    server.handler = (f) => {
      if (f.op !== Op.Hello) return false;
      server.push(Op.Ok, { rid: 1 });
      return true;
    };
    await assert.rejects(openClient('fire://localhost', {}, server.dial), ProtocolError);
  });
});

describe('requests', () => {
  it('declares queues with safe defaults and the temporary shorthand', async () => {
    const { server, client } = await setup();
    const info = await client.declareQueue('jobs', { args: { 'x-max-length': 10 } });
    assert.deepEqual(info, { name: 'jobs', messages: 0, consumers: 0 });
    const tmp = await client.declareQueue('', QueueOptions.temporary());
    assert.equal(tmp.name, 'amq.gen-x');
    const [durable, temporary] = server.sent(Op.DeclareQueue).map((f) => f.header);
    assert.deepEqual(durable, { rid: 1, name: 'jobs', durable: true, args: { 'x-max-length': 10 } });
    assert.deepEqual(temporary, { rid: 2, exclusive: true, auto_delete: true });
    await client.declareQueue('t', { temporary: true });
    assert.deepEqual(server.sent(Op.DeclareQueue)[2]!.header, { rid: 3, name: 't', exclusive: true, auto_delete: true });
    await client.close();
  });

  it('encodes bindings to queues and exchanges', async () => {
    const { server, client } = await setup();
    await client.bindQueue('q', 'ex', 'k.*');
    await client.bindExchange('down', 'up', 'audit');
    await client.unbindQueue('q', 'ex', 'k.*');
    const [bq, be] = server.sent(Op.Bind).map((f) => f.header);
    assert.deepEqual(bq, { rid: 1, exchange: 'ex', queue: 'q', key: 'k.*' });
    assert.deepEqual(be, { rid: 2, exchange: 'up', to_exchange: 'down', key: 'audit' });
    assert.deepEqual(server.sent(Op.Unbind)[0]!.header, { rid: 3, exchange: 'ex', queue: 'q', key: 'k.*' });
    await client.close();
  });

  it('rejects a request with ServerError and keeps the connection', async () => {
    const { client } = await setup((s) => {
      s.handler = (f) => {
        if (f.op !== Op.Bind) return false;
        s.push(Op.Error, { rid: f.header.rid, code: 404, msg: "no exchange 'missing'" });
        return true;
      };
    });
    await assert.rejects(client.bindQueue('q', 'missing'), (e: unknown) => e instanceof ServerError && e.code === 404);
    assert.equal(client.isClosed, false);
    await client.purgeQueue('q');
    await client.close();
  });

  it('reports errors outside requests to onError', async () => {
    const errors: ServerError[] = [];
    const { server, client } = await setup(undefined, { onError: (e: ServerError) => errors.push(e) });
    server.push(Op.Error, { code: 406, msg: 'unknown delivery tag 9' });
    assert.equal(errors.length, 1);
    assert.equal(errors[0]!.code, 406);
    assert.equal(client.isClosed, false);
    await client.close();
  });

  it('closes on a fatal error and rejects everything in flight', async () => {
    const { server, client } = await setup((s) => {
      s.autoOk = false;
    });
    const pending = client.purgeQueue('q');
    await tick();
    server.push(Op.Error, { code: 541, msg: 'internal error', fatal: true });
    await assert.rejects(pending, (e: unknown) => e instanceof ServerError && e.code === 541);
    assert.equal(client.isClosed, true);
    await assert.rejects(client.purgeQueue('q'), ServerError);
  });
});

describe('publish', () => {
  it('resolves on cumulative confirms', async () => {
    const { server, client } = await setup();
    const settled: number[] = [];
    const ps = [1, 2, 3].map((n) => client.publish('', 'q', `m${n}`).then(() => settled.push(n)));
    await tick();
    const seqs = server.sent(Op.Publish).map((f) => f.header.seq);
    assert.deepEqual(seqs, [1, 2, 3]);
    server.push(Op.Confirm, { upto: 2 });
    await tick();
    assert.deepEqual(settled, [1, 2]);
    server.push(Op.Confirm, { upto: 3 });
    await Promise.all(ps);
    assert.deepEqual(settled, [1, 2, 3]);
    await client.close();
  });

  it('encodes defaults, properties and headers', async () => {
    const { server, client } = await setup();
    const p = client.publish('orders', 'order.created', '{"id":1}', {
      mandatory: true,
      properties: { contentType: 'application/json', correlationId: 'c-1', expiration: 60_000, priority: 0 },
      headers: { 'x-attempt': 2, trace: 'abc' },
    });
    await tick();
    const f = server.sent(Op.Publish)[0]!;
    assert.deepEqual(f.header, {
      seq: 1,
      exchange: 'orders',
      key: 'order.created',
      mandatory: true,
      persistent: true,
      props: { content_type: 'application/json', correlation_id: 'c-1', expiration: 60_000, priority: 0 },
      headers: { 'x-attempt': 2, trace: 'abc' },
    });
    assert.equal(new TextDecoder().decode(f.payload), '{"id":1}');
    server.push(Op.Confirm, { upto: 1 });
    await p;
    await client.close();
  });

  it('rejects one publish on a nack and confirms the rest', async () => {
    const { server, client } = await setup();
    const a = client.publish('', 'q', 'a');
    const b = client.publish('', 'q', 'b');
    const c = client.publish('', 'q', 'c');
    await tick();
    server.push(Op.Confirm, { seq: 2, nack: true, code: 406, msg: "queue 'q' is full" });
    server.push(Op.Confirm, { upto: 3 });
    await a;
    await c;
    await assert.rejects(b, (e: unknown) => e instanceof NackedError && e.code === 406);
    await client.close();
  });

  it('rejects a mandatory publish that was returned before its confirm', async () => {
    const { server, client } = await setup();
    const lost = client.publish('ex', 'nowhere', 'x', { mandatory: true });
    const ok = client.publish('ex', 'somewhere', 'y', { mandatory: true });
    await tick();
    server.push(Op.Return, { seq: 1, code: 312, msg: 'NO_ROUTE', exchange: 'ex', key: 'nowhere' }, utf8.encode('x'));
    server.push(Op.Confirm, { upto: 2 });
    await assert.rejects(lost, (e: unknown) => {
      assert.ok(e instanceof UnroutableError);
      assert.equal(e.code, 312);
      assert.equal(e.exchange, 'ex');
      assert.equal(e.routingKey, 'nowhere');
      return true;
    });
    await ok;
    await client.close();
  });

  it('does not wait for a confirm when confirm is false', async () => {
    const { server, client } = await setup();
    await client.publish('', 'q', 'fire and forget', { confirm: false, persistent: false });
    const h = server.sent(Op.Publish)[0]!.header;
    assert.equal(h.seq, undefined);
    assert.equal(h.persistent, undefined);
    // The next confirmed publish still starts at 1.
    const p = client.publish('', 'q', 'confirmed');
    await tick();
    assert.equal(server.sent(Op.Publish)[1]!.header.seq, 1);
    server.push(Op.Confirm, { upto: 1 });
    await p;
    await client.close();
  });

  it('refuses bodies larger than the negotiated frame size', async () => {
    const { client } = await setup((s) => {
      s.welcome = { ...s.welcome, max_frame: 70_000 };
    });
    await assert.rejects(client.publish('', 'q', new Uint8Array(80_000)), ProtocolError);
    await client.close();
  });

  it('waits while the broker pauses publishers', async () => {
    const { server, client } = await setup();
    server.push(Op.Flow, { pause: true });
    let done = false;
    const p = client.publish('', 'q', 'x').then(() => (done = true));
    await tick();
    assert.equal(server.sent(Op.Publish).length, 0);
    server.push(Op.Flow, { pause: false });
    await tick();
    assert.equal(server.sent(Op.Publish).length, 1);
    server.push(Op.Confirm, { upto: 1 });
    await p;
    assert.ok(done);
    await client.close();
  });

  it('rejects outstanding confirms when the connection drops', async () => {
    const { server, client } = await setup();
    const p = client.publish('', 'q', 'x');
    await tick();
    server.drop();
    await assert.rejects(p, ClosedError);
    await assert.rejects(client.publish('', 'q', 'y'), ClosedError);
    assert.ok((await client.closed) instanceof ClosedError);
  });
});

describe('subscribe', () => {
  it('grants the window at subscribe and tops up by half on ack', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q', { prefetch: 4 });
    assert.deepEqual(server.sent(Op.Subscribe)[0]!.header, { rid: 1, sub: 1, queue: 'q', credit: 4 });
    for (let tag = 1; tag <= 4; tag++) deliver(server, sub.id, tag);
    const m1 = (await sub.receive())!;
    const m2 = (await sub.receive())!;
    m1.ack();
    assert.equal(server.sent(Op.Credit).length, 0);
    m2.nack(false);
    assert.deepEqual(server.sent(Op.Credit)[0]?.header, { sub: 1, credit: 2 });
    assert.deepEqual(server.sent(Op.Ack)[0]!.header, { tag: 1 });
    assert.deepEqual(server.sent(Op.Nack)[0]!.header, { tag: 2 });
    m2.ack(); // already settled: no-op
    assert.equal(server.sent(Op.Ack).length, 1);
    await client.close();
  });

  it('with noAck grants credit as messages are taken', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q', { prefetch: 2, noAck: true });
    assert.equal(server.sent(Op.Subscribe)[0]!.header.no_ack, true);
    deliver(server, sub.id, 1);
    deliver(server, sub.id, 2);
    assert.equal(server.sent(Op.Credit).length, 0);
    const m = (await sub.receive())!;
    assert.deepEqual(server.sent(Op.Credit)[0]?.header, { sub: 1, credit: 1 });
    m.ack(); // noAck: no-op
    assert.equal(server.sent(Op.Ack).length, 0);
    await client.close();
  });

  it('iterates with for await and cancels when the loop exits', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q', { prefetch: 10 });
    for (let tag = 1; tag <= 3; tag++) deliver(server, sub.id, tag);
    const seen: string[] = [];
    for await (const m of sub) {
      seen.push(m.text());
      m.ack();
      if (seen.length === 2) break;
    }
    assert.deepEqual(seen, ['m1', 'm2']);
    // The unconsumed third message goes back to the queue.
    assert.deepEqual(server.sent(Op.Nack)[0]!.header, { tag: 3, requeue: true });
    assert.deepEqual(server.sent(Op.Unsubscribe)[0]!.header, { rid: 2, sub: 1 });
    assert.equal(sub.active, false);
    assert.equal(await sub.receive(), undefined);
    await client.close();
  });

  it('requeues deliveries that arrive while cancelling', async () => {
    const { server, client } = await setup((s) => {
      s.handler = (f) => {
        if (f.op !== Op.Unsubscribe) return false;
        deliver(s, f.header.sub as number, 7);
        s.push(Op.Ok, { rid: f.header.rid });
        return true;
      };
    });
    const sub = await client.subscribe('q');
    await sub.cancel();
    assert.deepEqual(server.sent(Op.Nack)[0]!.header, { tag: 7, requeue: true });
    await client.close();
  });

  it('receive times out without losing the next message', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q');
    assert.equal(await sub.receive(20), undefined);
    deliver(server, sub.id, 1);
    assert.equal((await sub.receive(20))?.text(), 'm1');
    await client.close();
  });

  it('drains buffered messages, then ends, when the server cancels', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q');
    deliver(server, sub.id, 1);
    server.push(Op.Cancelled, { sub: sub.id, code: 404, msg: "queue 'q' was deleted" });
    const seen = [];
    for await (const m of sub) seen.push(m.text());
    assert.deepEqual(seen, ['m1']);
    await client.close();
  });

  it('throws from the iterator when the connection is lost', async () => {
    const { server, client } = await setup();
    const sub = await client.subscribe('q');
    const loop = (async () => {
      for await (const m of sub) m.ack();
    })();
    await tick();
    server.drop();
    await assert.rejects(loop, ClosedError);
  });

  it('ends the iterator quietly on close()', async () => {
    const { client } = await setup();
    const sub = await client.subscribe('q');
    const loop = (async () => {
      let n = 0;
      for await (const _ of sub) n++;
      return n;
    })();
    await client.close();
    assert.equal(await loop, 0);
    assert.equal((await client.closed).message, 'connection closed: client closed the connection');
  });

  it('removes the subscription when subscribe fails', async () => {
    const { client } = await setup((s) => {
      s.handler = (f) => {
        if (f.op !== Op.Subscribe) return false;
        s.push(Op.Error, { rid: f.header.rid, code: 405, msg: 'RESOURCE_LOCKED' });
        return true;
      };
    });
    await assert.rejects(client.subscribe('q', { exclusive: true }), (e: unknown) => e instanceof ServerError && e.code === 405);
    await client.close();
  });
});

describe('get', () => {
  it('returns a message, or null when the queue is empty', async () => {
    const { server, client } = await setup((s) => {
      let calls = 0;
      s.handler = (f) => {
        if (f.op !== Op.Get) return false;
        if (calls++ === 0) {
          s.push(
            Op.GetOk,
            {
              rid: f.header.rid,
              tag: 9,
              redelivered: true,
              exchange: 'ex',
              key: 'k',
              remaining: 0,
              props: { content_type: 'text/plain', timestamp: 1767225600 },
              headers: { n: 1 },
            },
            utf8.encode('work'),
          );
        } else {
          s.push(Op.Ok, { rid: f.header.rid, empty: true });
        }
        return true;
      };
    });
    const m = await client.get('q');
    assert.ok(m instanceof Message);
    assert.equal(m.text(), 'work');
    assert.equal(m.exchange, 'ex');
    assert.equal(m.routingKey, 'k');
    assert.equal(m.redelivered, true);
    assert.equal(m.deliveryTag, 9);
    assert.deepEqual(m.properties, { contentType: 'text/plain', timestamp: 1767225600 });
    assert.deepEqual(m.headers, { n: 1 });
    m.ack();
    assert.deepEqual(server.sent(Op.Ack)[0]!.header, { tag: 9 });
    assert.equal(await client.get('q'), null);
    await client.close();
  });
});

describe('connection', () => {
  it('answers ping with pong', async () => {
    const { server, client } = await setup();
    server.push(Op.Ping);
    assert.equal(server.sent(Op.Pong).length, 1);
    await client.close();
  });

  it('replies to a server close and rejects later calls', async () => {
    const { server, client } = await setup();
    server.push(Op.Close, { code: 320, msg: 'CONNECTION_FORCED - shutdown' });
    assert.equal(server.sent(Op.Close).length, 1);
    assert.equal(client.isClosed, true);
    await assert.rejects(client.declareQueue('q'), (e: unknown) => e instanceof ClosedError && /shutdown/.test(e.message));
  });

  it('sends close on close() and waits for the reply', async () => {
    const { server, client } = await setup();
    await client.close();
    assert.deepEqual(server.sent(Op.Close)[0]!.header, { code: 200, msg: 'client closed' });
    assert.equal(client.isClosed, true);
    await client.close(); // idempotent
  });

  it('sends pings when idle and closes when the server goes silent', async () => {
    const { server, client } = await setup((s) => {
      s.welcome = { ...s.welcome, heartbeat: 1 };
    });
    // The heartbeat timer is unref'd and the fake transport has no socket, so keep the event
    // loop alive while waiting.
    const keepAlive = setInterval(() => {}, 100);
    const reason = await client.closed;
    clearInterval(keepAlive);
    assert.ok(reason instanceof ClosedError);
    assert.match(reason.message, /missed heartbeats/);
    assert.ok(server.sent(Op.Ping).length >= 1);
  });
});

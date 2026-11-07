// Conformance driver for the TypeScript SDK. Build first (`npm run build`), then:
//
//   firemq-conformance run --driver "node conformance/driver.mjs"
//
// Reads one JSON command per line on stdin and writes exactly one JSON reply per command on
// stdout, in order. Logs go to stderr.
//
// Set FIREMQ_DRIVER_TRANSPORT=ws to run every scenario over WebSocket instead of TCP. The
// runner only hands out a fire:// URL, so the driver finds the broker's HTTP port by trying
// the ports next to the fire:// port (or uses FIREMQ_DRIVER_WS_PORT when set).

import { createInterface } from 'node:readline';

import {
  ClosedError,
  connect,
  NackedError,
  ServerError,
  UnroutableError,
  VERSION,
} from '../dist/esm/index.js';

const useWs = process.env.FIREMQ_DRIVER_TRANSPORT === 'ws';

let client = null;
const subs = new Map(); // "sub-N" -> Subscription
const messages = new Map(); // tag string -> Message
let nextId = 1;

const PROPS = [
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

function propsFromJson(p = {}) {
  const out = {};
  for (const [name, key] of PROPS) if (p[key] !== undefined && p[key] !== null) out[name] = p[key];
  return out;
}

function propsToJson(p) {
  const out = {};
  for (const [name, key] of PROPS) if (p[name] !== undefined) out[key] = p[name];
  return out;
}

/** Makes MessagePack values JSON-safe: bytes become base64 strings, bigints numbers. */
function jsonSafe(v) {
  if (v instanceof Uint8Array) return Buffer.from(v).toString('base64');
  if (typeof v === 'bigint') return Number(v);
  if (v instanceof Date) return Math.floor(v.getTime() / 1000);
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v !== null && typeof v === 'object') {
    const out = {};
    for (const [k, x] of Object.entries(v)) out[k] = jsonSafe(x);
    return out;
  }
  return v;
}

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

function messageJson(m) {
  if (!m) return null;
  const tag = String(nextId++);
  messages.set(tag, m);
  let body = null;
  try {
    body = strictUtf8.decode(m.body);
  } catch {
    body = null;
  }
  return {
    body,
    body_base64: Buffer.from(m.body).toString('base64'),
    exchange: m.exchange,
    routing_key: m.routingKey,
    redelivered: m.redelivered,
    tag,
    properties: propsToJson(m.properties),
    headers: jsonSafe(m.headers),
  };
}

function errorJson(e) {
  if (e instanceof UnroutableError) return { kind: 'unroutable', code: e.code, message: e.message };
  if (e instanceof NackedError) return { kind: 'nacked', code: e.code, message: e.message };
  if (e instanceof ServerError) return { kind: 'server', code: e.code, message: e.message };
  if (e instanceof ClosedError) return { kind: 'closed', message: e.message };
  if (e && e.unsupported) return { kind: 'unsupported', message: e.message };
  return { kind: 'other', message: e instanceof Error ? e.message : String(e) };
}

function need() {
  if (!client) throw new ClosedError('not connected');
  return client;
}

let wsPort = process.env.FIREMQ_DRIVER_WS_PORT ? Number(process.env.FIREMQ_DRIVER_WS_PORT) : undefined;

/** Turns the runner's fire:// URL into a ws:// URL on the broker's HTTP port. */
async function wsUrlFor(fireUrl) {
  const u = new URL(fireUrl);
  const creds = u.username ? `${u.username}:${u.password}@` : '';
  const vhost = u.pathname.replace(/^\/+/, '');
  const query = vhost ? `?vhost=${encodeURIComponent(decodeURIComponent(vhost))}` : '';
  const build = (port) => `ws://${creds}${u.hostname}:${port}/fire${query}`;
  if (wsPort !== undefined) return build(wsPort);
  const base = Number(u.port || 5673);
  for (const port of [base + 1, base - 1, base + 2, base - 2]) {
    try {
      // Guest credentials only to find the port; the real connect uses the runner's URL.
      const probe = await connect(`ws://${u.hostname}:${port}/fire`, { connectTimeout: 2000, heartbeat: 0 });
      await probe.close();
      wsPort = port;
      return build(port);
    } catch (e) {
      if (e instanceof ServerError) {
        wsPort = port;
        return build(port);
      }
    }
  }
  throw new Error(`no FireMQ WebSocket endpoint next to port ${base}`);
}

const handlers = {
  hello: async () => ({ sdk: 'firemq-js', version: VERSION, capabilities: [] }),

  connect: async ({ url }) => {
    if (client) await client.close().catch(() => {});
    subs.clear();
    messages.clear();
    client = null;
    const target = useWs ? await wsUrlFor(url) : url;
    if (useWs) console.error(`driver: ${url} -> ${target.replace(/\/\/[^@/]*@/, '//')}`);
    client = await connect(target);
    return {};
  },

  close: async () => {
    const c = client;
    client = null;
    subs.clear();
    messages.clear();
    if (c) await c.close();
    return {};
  },

  declare_queue: async ({ name, durable, exclusive, auto_delete, args }) => {
    const info = await need().declareQueue(name, { durable, exclusive, autoDelete: auto_delete, args });
    return { name: info.name, messages: info.messages, consumers: info.consumers };
  },

  delete_queue: async ({ name }) => ({ messages: await need().deleteQueue(name) }),

  purge_queue: async ({ name }) => ({ messages: await need().purgeQueue(name) }),

  declare_exchange: async ({ name, type, durable }) => {
    await need().declareExchange(name, type, { durable });
    return {};
  },

  delete_exchange: async ({ name }) => {
    await need().deleteExchange(name);
    return {};
  },

  bind_queue: async ({ queue, exchange, key, args }) => {
    await need().bindQueue(queue, exchange, key, args);
    return {};
  },

  unbind_queue: async ({ queue, exchange, key }) => {
    await need().unbindQueue(queue, exchange, key);
    return {};
  },

  bind_exchange: async ({ destination, source, key }) => {
    await need().bindExchange(destination, source, key);
    return {};
  },

  publish: async (cmd) => {
    const body = cmd.body_base64 !== undefined && cmd.body_base64 !== null
      ? new Uint8Array(Buffer.from(cmd.body_base64, 'base64'))
      : cmd.body ?? '';
    await need().publish(cmd.exchange, cmd.key, body, {
      persistent: cmd.persistent,
      mandatory: cmd.mandatory,
      properties: propsFromJson(cmd.properties),
      headers: cmd.headers ?? {},
    });
    return {};
  },

  subscribe: async ({ queue, prefetch, no_ack }) => {
    const sub = await need().subscribe(queue, { prefetch, noAck: no_ack });
    const id = `sub-${nextId++}`;
    subs.set(id, sub);
    return { sub: id };
  },

  next: async ({ sub, timeout_ms }) => {
    const s = subs.get(sub);
    if (!s) throw new Error(`unknown subscription ${sub}`);
    return { message: messageJson(await s.receive(timeout_ms)) };
  },

  ack: async ({ tag }) => {
    const m = messages.get(tag);
    if (!m) throw new Error(`unknown tag ${tag}`);
    messages.delete(tag);
    m.ack();
    return {};
  },

  nack: async ({ tag, requeue }) => {
    const m = messages.get(tag);
    if (!m) throw new Error(`unknown tag ${tag}`);
    messages.delete(tag);
    m.nack(requeue);
    return {};
  },

  unsubscribe: async ({ sub }) => {
    const s = subs.get(sub);
    if (!s) throw new Error(`unknown subscription ${sub}`);
    subs.delete(sub);
    await s.cancel();
    return {};
  },

  get: async ({ queue, no_ack }) => ({ message: messageJson(await need().get(queue, { noAck: no_ack })) }),
};

const out = (obj) => process.stdout.write(JSON.stringify(obj) + '\n');

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  if (line.trim() === '') continue;
  let cmd;
  try {
    cmd = JSON.parse(line);
  } catch (e) {
    console.error(`driver: invalid JSON: ${line}`);
    continue;
  }
  const handler = handlers[cmd.op];
  try {
    if (!handler) {
      const e = new Error(`op '${cmd.op}' is not supported`);
      e.unsupported = true;
      throw e;
    }
    out({ id: cmd.id, ok: true, result: await handler(cmd) });
  } catch (e) {
    console.error(`driver: ${cmd.op} failed: ${e instanceof Error ? e.message : e}`);
    out({ id: cmd.id, ok: false, error: errorJson(e) });
  }
}

if (client) await client.close().catch(() => {});
process.exit(0);

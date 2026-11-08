// Finds or starts a FireMQ broker for the integration tests.
//
// With FIREMQ_URL set, the tests use that broker; FIREMQ_WS_URL, FIREMQ_TLS_URL and
// FIREMQ_TLS_CA (a PEM file) enable the WebSocket and TLS tests, and FIREMQ_TLS_CERT and
// FIREMQ_TLS_KEY (PEM files) the wss:// test through a local TLS proxy. Otherwise they start the
// binary at FIREMQ_BIN (default ../.bin/firemq) on free ports, with a self-signed certificate
// when openssl is available, and stop it afterwards. Without either, the tests are skipped.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import * as net from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export interface Broker {
  fireUrl: string;
  wsUrl?: string;
  tlsUrl?: string;
  tlsCa?: string;
  /** host:port of the HTTP listener, for the wss:// proxy test. */
  httpAddress?: { host: string; port: number };
  tlsKey?: string;
  tlsCert?: string;
  stop(): Promise<void>;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');

function freePort(): Promise<number> {
  return new Promise((res, rej) => {
    const s = net.createServer();
    s.once('error', rej);
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address() as net.AddressInfo;
      s.close(() => res(port));
    });
  });
}

async function waitForPort(port: number, child: ChildProcess): Promise<void> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`firemq exited with code ${child.exitCode}`);
    const ok = await new Promise<boolean>((r) => {
      const s = net.connect(port, '127.0.0.1');
      s.once('connect', () => {
        s.destroy();
        r(true);
      });
      s.once('error', () => r(false));
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`firemq did not listen on port ${port}`);
}

function makeCert(dir: string): { cert: string; key: string } | undefined {
  const cert = join(dir, 'cert.pem');
  const key = join(dir, 'key.pem');
  if (existsSync(cert) && existsSync(key)) return { cert, key };
  try {
    mkdirSync(dir, { recursive: true });
    execFileSync(
      'openssl',
      [
        'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '7',
        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
      ],
      { stdio: 'ignore' },
    );
    return { cert, key };
  } catch {
    return undefined;
  }
}

export async function startBroker(): Promise<Broker | undefined> {
  const env = process.env;
  if (env.FIREMQ_URL) {
    const read = (file: string | undefined) => (file ? readFileSync(file, 'utf8') : undefined);
    const ws = env.FIREMQ_WS_URL ? new URL(env.FIREMQ_WS_URL) : undefined;
    return {
      fireUrl: env.FIREMQ_URL,
      wsUrl: env.FIREMQ_WS_URL,
      tlsUrl: env.FIREMQ_TLS_URL,
      tlsCa: read(env.FIREMQ_TLS_CA),
      tlsCert: read(env.FIREMQ_TLS_CERT),
      tlsKey: read(env.FIREMQ_TLS_KEY),
      httpAddress: ws ? { host: ws.hostname === 'localhost' ? '127.0.0.1' : ws.hostname, port: Number(ws.port || 15672) } : undefined,
      stop: async () => {},
    };
  }
  const bin = env.FIREMQ_BIN ?? resolve(root, '../.bin/firemq');
  if (!existsSync(bin)) return undefined;
  const [amqp, fire, http, fires] = [await freePort(), await freePort(), await freePort(), await freePort()];
  const tls = makeCert(join(root, '.tmp', 'tls'));
  const args = [
    '--ephemeral',
    '--amqp', `127.0.0.1:${amqp}`,
    '--fire', `127.0.0.1:${fire}`,
    '--http', `127.0.0.1:${http}`,
    '--amqps', 'off',
    '--log-level', 'warn',
  ];
  if (tls) args.push('--fires', `127.0.0.1:${fires}`, '--tls-cert', tls.cert, '--tls-key', tls.key);
  else args.push('--fires', 'off');
  const child = spawn(bin, args, { stdio: ['ignore', 'ignore', 'inherit'] });
  const kill = () => child.kill('SIGKILL');
  process.once('exit', kill);
  await waitForPort(fire, child);
  await waitForPort(http, child);
  if (tls) await waitForPort(fires, child);
  return {
    fireUrl: `fire://guest:guest@localhost:${fire}`,
    wsUrl: `ws://localhost:${http}/fire`,
    tlsUrl: tls ? `fires://guest:guest@localhost:${fires}` : undefined,
    tlsCa: tls ? readFileSync(tls.cert, 'utf8') : undefined,
    tlsCert: tls ? readFileSync(tls.cert, 'utf8') : undefined,
    tlsKey: tls ? readFileSync(tls.key, 'utf8') : undefined,
    httpAddress: { host: '127.0.0.1', port: http },
    stop: async () => {
      process.off('exit', kill);
      if (child.exitCode !== null) return;
      const exited = new Promise((r) => child.once('exit', r));
      child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 3000);
      await exited;
      clearTimeout(timer);
    },
  };
}

/** A queue name no other test or run uses. */
export function unique(prefix: string): string {
  return `${prefix}.${Date.now().toString(36)}.${Math.random().toString(36).slice(2, 8)}`;
}

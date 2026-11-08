// TCP and TLS transport for Node, Deno and Bun: `FMQ1`, then length-prefixed frames.

import * as net from 'node:net';
import * as tls from 'node:tls';

import { FrameReader, MAGIC } from '../codec.js';
import { ClosedError, ConnectionError, describeError } from '../errors.js';
import type { ConnectOptions, TlsOptions } from '../options.js';
import { authority, type ConnectUrl } from '../url.js';
import type { Transport } from './types.js';

type Pem = string | Buffer;

function pem(v: string | Uint8Array): Pem {
  return typeof v === 'string' ? v : Buffer.from(v.buffer, v.byteOffset, v.byteLength);
}

/**
 * Connect options shared by TCP and TLS sockets. `autoSelectFamily` tries IPv6 and IPv4
 * addresses, so `localhost` reaches a broker bound to 127.0.0.1 when it resolves to ::1 first.
 * It is the default from Node 20; Node 18 needs it spelled out.
 */
export const SOCKET_OPTIONS = { autoSelectFamily: true } as Record<string, unknown>;

/** Node `tls.connect` options for a host and our `TlsOptions`. */
export function nodeTlsOptions(host: string, o: TlsOptions = {}): tls.ConnectionOptions {
  const opts: tls.ConnectionOptions = { ...SOCKET_OPTIONS };
  if (o.ca !== undefined) opts.ca = Array.isArray(o.ca) ? o.ca.map(pem) : pem(o.ca);
  if (o.cert !== undefined) opts.cert = pem(o.cert);
  if (o.key !== undefined) opts.key = pem(o.key);
  if (o.rejectUnauthorized !== undefined) opts.rejectUnauthorized = o.rejectUnauthorized;
  const servername = o.servername ?? (net.isIP(host) === 0 ? host : undefined);
  if (servername) opts.servername = servername;
  return opts;
}

/** Opens a TCP socket, or a TLS socket for `fires`, and sends the `FMQ1` magic. */
export function dialTcp(url: ConnectUrl, options: ConnectOptions): Promise<Transport> {
  const where = authority(url);
  const timeoutMs = options.connectTimeout ?? 10_000;
  return new Promise<Transport>((resolve, reject) => {
    const secure = url.transport === 'fires';
    const socket: net.Socket = secure
      ? tls.connect({ host: url.host, port: url.port, ...nodeTlsOptions(url.host, options.tls) })
      : net.connect({ host: url.host, port: url.port, ...SOCKET_OPTIONS });
    const reader = new FrameReader();
    let connected = false;
    let closed = false;
    let lastError: Error | undefined;

    const transport: Transport = {
      send(frame) {
        if (socket.destroyed) return true;
        return socket.write(frame);
      },
      drain() {
        if (!socket.writableNeedDrain || socket.destroyed) return Promise.resolve();
        return new Promise<void>((r) => {
          const done = () => {
            socket.off('drain', done);
            socket.off('close', done);
            r();
          };
          socket.once('drain', done);
          socket.once('close', done);
        });
      },
      setMaxFrame(bytes) {
        reader.maxFrame = bytes;
      },
      close() {
        if (socket.destroyed) return;
        socket.end();
        setTimeout(() => socket.destroy(), 1000).unref();
      },
      onframe: () => {},
      onclose: () => {},
    };

    const timer = setTimeout(() => {
      if (connected) return;
      socket.destroy(new ConnectionError(`connecting to ${where} timed out`));
    }, timeoutMs);

    socket.once(secure ? 'secureConnect' : 'connect', () => {
      connected = true;
      clearTimeout(timer);
      socket.setNoDelay(true);
      socket.write(MAGIC);
      resolve(transport);
    });
    socket.on('data', (chunk: Buffer) => {
      if (closed) return;
      try {
        const bytes = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
        for (const frame of reader.push(bytes)) {
          transport.onframe(frame);
          if (closed) return;
        }
      } catch (e) {
        lastError = e as Error;
        socket.destroy();
      }
    });
    socket.on('error', (e) => {
      lastError = e;
    });
    socket.on('close', () => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      if (!connected) {
        const err = lastError;
        reject(
          err instanceof ConnectionError
            ? err
            : new ConnectionError(`cannot connect to ${where}: ${describeError(err) || 'connection closed'}`, { cause: err }),
        );
        return;
      }
      transport.onclose(lastError ?? new ClosedError('server closed the connection'));
    });
  });
}

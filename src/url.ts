import { UrlError } from './errors.js';

/** How the client reaches the broker. */
export type Transport = 'fire' | 'fires' | 'ws' | 'wss';

/** A parsed `fire://`, `fires://`, `ws://` or `wss://` connection URL. */
export interface ConnectUrl {
  transport: Transport;
  host: string;
  port: number;
  vhost: string;
  user: string;
  pass: string;
  /** WebSocket endpoint path, `/fire` by default. Empty for `fire` and `fires`. */
  path: string;
  /** The query string including `?`, or empty. Kept on WebSocket URLs. */
  search: string;
}

const DEFAULT_PORTS: Record<Transport, number> = {
  fire: 5673,
  fires: 5674,
  ws: 15672,
  wss: 15671,
};

function decode(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/**
 * Parses a connection URL and fills in defaults: user and password `guest`, vhost `/`, and
 * ports 5673 (`fire`), 5674 (`fires`), 15672 (`ws`) and 15671 (`wss`).
 *
 * For `fire` and `fires` the path is the vhost (`fire://host/my-vhost`). For `ws` and `wss`
 * the path is the endpoint (`/fire` when empty) and the vhost is the `vhost` query parameter.
 */
export function parseUrl(input: string): ConnectUrl {
  const match = /^([a-zA-Z][a-zA-Z0-9+.-]*):/.exec(input);
  if (!match) throw new UrlError(`invalid URL '${input}'`);
  const scheme = match[1]!.toLowerCase();
  if (!(scheme in DEFAULT_PORTS)) {
    throw new UrlError(`unsupported scheme '${scheme}'; use fire, fires, ws or wss`);
  }
  const transport = scheme as Transport;
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new UrlError(`invalid URL '${input}'`);
  }
  let host = url.hostname;
  if (host.startsWith('[') && host.endsWith(']')) host = host.slice(1, -1);
  if (host === '') host = 'localhost';

  // WHATWG URL hides a port equal to the scheme's web default (80 for ws, 443 for wss), so
  // read an explicit port from the authority as written.
  let port = DEFAULT_PORTS[transport];
  if (url.port !== '') {
    port = Number(url.port);
  } else {
    const authority = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/(?:[^@/?#]*@)?(\[[^\]]*\]|[^:/?#]*)(?::(\d+))?/.exec(input);
    if (authority?.[2]) port = Number(authority[2]);
  }
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new UrlError(`invalid port in '${input}'`);

  const user = url.username === '' ? 'guest' : decode(url.username);
  const hasPassword = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/[^@/?#]*:[^@/?#]*@/.test(input);
  const pass = hasPassword ? decode(url.password) : 'guest';
  const queryVhost = url.searchParams.get('vhost');

  let vhost: string;
  let path = '';
  if (transport === 'fire' || transport === 'fires') {
    const raw = url.pathname.replace(/^\/+/, '');
    vhost = raw === '' ? (queryVhost ?? '/') : decode(raw);
  } else {
    path = url.pathname === '' || url.pathname === '/' ? '/fire' : url.pathname;
    vhost = queryVhost ?? '/';
  }
  return { transport, host, port, vhost, user, pass, path, search: url.search };
}

/** `host:port`, with IPv6 hosts bracketed. */
export function authority(url: ConnectUrl): string {
  return url.host.includes(':') ? `[${url.host}]:${url.port}` : `${url.host}:${url.port}`;
}

/** The WebSocket URL for a `ws` or `wss` connection URL, without credentials. */
export function webSocketUrl(url: ConnectUrl): string {
  return `${url.transport}://${authority(url)}${url.path}${url.search}`;
}

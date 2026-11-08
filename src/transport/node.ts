// Transports available on Node, Deno and Bun.

import { ConnectionError } from '../errors.js';
import type { ConnectOptions, WebSocketConstructor } from '../options.js';
import type { ConnectUrl } from '../url.js';
import { NodeWebSocket } from './node-websocket.js';
import { dialTcp } from './tcp.js';
import type { Transport } from './types.js';
import { dialWebSocket } from './websocket.js';

function builtinWebSocket(options: ConnectOptions): WebSocketConstructor {
  return class extends NodeWebSocket {
    constructor(url: string, protocols?: string | string[]) {
      super(url, protocols, { tls: options.tls });
    }
  };
}

/** Picks the WebSocket implementation: the option, else the global one, else the built-in client. */
export function nodeWebSocket(url: ConnectUrl, options: ConnectOptions): WebSocketConstructor {
  if (options.WebSocket) return options.WebSocket;
  // The global WebSocket cannot take a custom CA, so TLS options select the built-in client.
  if (url.transport === 'wss' && options.tls) return builtinWebSocket(options);
  const global = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
  return global ?? builtinWebSocket(options);
}

export function dialNode(url: ConnectUrl, options: ConnectOptions): Promise<Transport> {
  switch (url.transport) {
    case 'fire':
    case 'fires':
      return dialTcp(url, options);
    case 'ws':
    case 'wss':
      return dialWebSocket(url, options, nodeWebSocket(url, options));
    default:
      return Promise.reject(new ConnectionError(`unsupported transport '${String(url.transport)}'`));
  }
}

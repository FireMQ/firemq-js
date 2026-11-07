// Transports available in browsers: WebSocket only.

import { ConnectionError } from '../errors.js';
import type { ConnectOptions, WebSocketConstructor } from '../options.js';
import type { ConnectUrl } from '../url.js';
import type { Transport } from './types.js';
import { dialWebSocket } from './websocket.js';

export function dialBrowser(url: ConnectUrl, options: ConnectOptions): Promise<Transport> {
  if (url.transport === 'fire' || url.transport === 'fires') {
    return Promise.reject(
      new ConnectionError(`${url.transport}:// needs TCP, which browsers do not have; use ws:// or wss://`),
    );
  }
  const Impl = options.WebSocket ?? (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (!Impl) {
    return Promise.reject(new ConnectionError('no WebSocket implementation; pass one in options.WebSocket'));
  }
  return dialWebSocket(url, options, Impl);
}

// Entry point for browsers: ws:// and wss:// only.

import { type Client, openClient } from './client.js';
import type { ConnectOptions } from './options.js';
import { dialBrowser } from './transport/browser.js';

export * from './api.js';

/** Connects to FireMQ over WebSocket with a URL such as `wss://host:15671/fire?vhost=dev`. */
export function connect(url: string, options: ConnectOptions = {}): Promise<Client> {
  return openClient(url, options, dialBrowser);
}

// Entry point for Node, Deno and Bun: fire://, fires://, ws:// and wss://.

import { type Client, openClient } from './client.js';
import type { ConnectOptions } from './options.js';
import { dialNode } from './transport/node.js';

export * from './api.js';

/**
 * Connects to FireMQ with a URL such as `fire://user:pass@host:5673/vhost`,
 * `fires://host` (TLS) or `ws://host:15672/fire?vhost=dev`.
 */
export function connect(url: string, options: ConnectOptions = {}): Promise<Client> {
  return openClient(url, options, dialNode);
}

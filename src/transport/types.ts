import type { Frame } from '../codec.js';
import type { ConnectOptions } from '../options.js';
import type { ConnectUrl } from '../url.js';

/** A connected byte pipe that carries FireMQ frames. */
export interface Transport {
  /**
   * Sends one frame. `frame` starts with the 4-byte length prefix; WebSocket transports drop
   * it. Returns false when the caller should wait for `drain()` before sending more.
   */
  send(frame: Uint8Array): boolean;
  /** Resolves when buffered data has been written, or the transport closed. */
  drain(): Promise<void>;
  /** Largest frame accepted from the peer. Raised after `welcome`. */
  setMaxFrame(bytes: number): void;
  /** Closes the transport. Safe to call more than once. */
  close(): void;
  /** Called for every frame received. */
  onframe: (frame: Frame) => void;
  /** Called once when the transport is gone, with the reason. */
  onclose: (error: Error) => void;
}

/** Opens a transport for a parsed URL. */
export type Dialer = (url: ConnectUrl, options: ConnectOptions) => Promise<Transport>;

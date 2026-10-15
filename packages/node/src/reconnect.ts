/**
 * Reconnecting a worker's client after its connection drops.
 */

import {
  ConnectionError,
  NotConnectedError,
  TimeoutError,
  UnexpectedEOFError,
  type Logger,
} from "@floruntime/core";
import type { FloClient } from "./client.js";
import { newWorkerBackoff, pause } from "./poll-backoff.js";

/**
 * Whether `err` means the connection is gone, so only a reconnect helps. A
 * poll that times out counts: its deadline is its own wait plus the
 * client's timeout, so the connection is half-open.
 */
export function isConnectionError(err: unknown): boolean {
  return (
    err instanceof NotConnectedError ||
    err instanceof UnexpectedEOFError ||
    err instanceof ConnectionError ||
    err instanceof TimeoutError
  );
}

/**
 * Redial `client`, waiting between failed tries as the poll loop does after
 * a failed poll. Returns false if `signal` aborts first, or the client was
 * closed by its owner (a worker doesn't reopen it).
 */
export async function reconnect(client: FloClient, signal: AbortSignal, logger: Logger): Promise<boolean> {
  const wait = newWorkerBackoff();
  for (let attempt = 1; !signal.aborted && !client.isClosed(); attempt++) {
    try {
      await client.redial();
      if (signal.aborted) return false;
      logger.warn(`Reconnected (attempt ${attempt})`);
      return true;
    } catch (err) {
      const d = wait.next();
      logger.warn(`Reconnect attempt ${attempt} failed: ${err}; retrying in ${d} ms`);
      await pause(d, signal);
    }
  }
  return false;
}

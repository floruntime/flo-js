/**
 * Reconnecting a worker's client after its connection drops.
 */

import {
  ConnectionError,
  NotConnectedError,
  UnexpectedEOFError,
  type Logger,
} from "@floruntime/core";
import type { FloClient } from "./client.js";
import { newWorkerBackoff, pause } from "./poll-backoff.js";

/** Whether `err` means the connection is gone, so only a reconnect helps. */
export function isConnectionError(err: unknown): boolean {
  return (
    err instanceof NotConnectedError ||
    err instanceof UnexpectedEOFError ||
    err instanceof ConnectionError
  );
}

/**
 * Reconnect `client`, waiting between tries as the poll loop does after a
 * failed poll. Returns false if `signal` aborts first.
 */
export async function reconnect(client: FloClient, signal: AbortSignal, logger: Logger): Promise<boolean> {
  // Drop whatever is left of the old socket so connect() dials afresh.
  await client.close();
  const wait = newWorkerBackoff();
  for (let attempt = 1; !signal.aborted; attempt++) {
    try {
      await client.connect();
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

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
import { pause } from "./poll-backoff.js";

export const RECONNECT_MIN_MS = 1000;
export const RECONNECT_MAX_MS = 30000;

/** Whether `err` means the connection is gone, so only a reconnect helps. */
export function isConnectionError(err: unknown): boolean {
  return (
    err instanceof NotConnectedError ||
    err instanceof UnexpectedEOFError ||
    err instanceof ConnectionError
  );
}

/**
 * Reconnect `client`, retrying with backoff from 1 s doubling to 30 s.
 * Returns false if `signal` aborts first.
 */
export async function reconnect(client: FloClient, signal: AbortSignal, logger: Logger): Promise<boolean> {
  // Drop whatever is left of the old socket so connect() dials afresh.
  await client.close();
  let delay = RECONNECT_MIN_MS;
  for (let attempt = 1; !signal.aborted; attempt++) {
    try {
      await client.connect();
      logger.warn(`Reconnected (attempt ${attempt})`);
      return true;
    } catch (err) {
      logger.warn(`Reconnect attempt ${attempt} failed: ${err}; retrying in ${delay} ms`);
    }
    await pause(delay, signal);
    delay = Math.min(delay * 2, RECONNECT_MAX_MS);
  }
  return false;
}

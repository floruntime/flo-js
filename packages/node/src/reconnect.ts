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
 * client's timeout, so the connection is most likely half-open. It may
 * instead be a slow link or a loaded server; redialing then fails the
 * requests in flight, which callers resend (delivery is at-least-once).
 */
export function isConnectionError(err: unknown): boolean {
  return (
    err instanceof NotConnectedError ||
    err instanceof UnexpectedEOFError ||
    err instanceof ConnectionError ||
    err instanceof TimeoutError
  );
}

/** What a worker knows of its client when it reconnects. */
export interface Seen {
  /** The connection the failed request went on (client.connection()). */
  connection: number;
  /** client.closes() when the worker started. */
  closes: number;
}

/** Whether the client's owner closed it since the worker started. */
export function closedSince(client: FloClient, seen: Seen): boolean {
  return client.isClosed() || client.closes() !== seen.closes;
}

/**
 * Redial `client`, waiting between failed tries as the poll loop does after
 * a failed poll. Only the connection the worker saw fail is dropped: another
 * worker sharing the client may have redialed already. Returns false if
 * `signal` aborts first, or the client's owner closed it (a worker doesn't
 * reopen it, even if the owner connected it again).
 */
export async function reconnect(client: FloClient, signal: AbortSignal, logger: Logger, seen: Seen): Promise<boolean> {
  const wait = newWorkerBackoff();
  for (let attempt = 1; !signal.aborted && !closedSince(client, seen); attempt++) {
    try {
      await client.redial(seen.connection);
      if (signal.aborted || closedSince(client, seen)) return false;
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

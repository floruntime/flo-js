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

/** Sends of one result in all, the first included. */
export const RESULT_SENDS = 3;

/**
 * Send a worker's result (complete, fail, ack or nack) with `send`, which
 * builds the same request each call. If the connection drops or the send
 * times out, the worker reconnects and sends it again: the server may never
 * have seen it, and a resend at worst repeats a result it already has. After
 * RESULT_SENDS sends, or if the worker is stopping or the client's owner
 * closed it, the result is dropped with an error log naming `what`; the
 * server then hands the work out again once its lease runs out. A refusal is
 * the server's answer and is not resent. Returns whether it was delivered.
 */
export async function sendResult(
  client: FloClient,
  signal: AbortSignal,
  logger: Logger,
  closes: number,
  what: string,
  send: () => Promise<unknown>,
): Promise<boolean> {
  for (let sends = 1; ; sends++) {
    const seen = { connection: client.connection(), closes };
    try {
      await send();
      return true;
    } catch (err) {
      if (!isConnectionError(err)) {
        logger.error(`Dropped ${what}: ${err}`);
        return false;
      }
      if (sends >= RESULT_SENDS) {
        logger.error(`Dropped ${what} after ${sends} sends: ${err}`);
        return false;
      }
      if (signal.aborted || closedSince(client, seen)) {
        logger.error(`Dropped ${what}: ${err}; the worker is stopping or its client was closed`);
        return false;
      }
      logger.warn(`Sending ${what} failed: ${err}; reconnecting to send it again`);
      if (!(await reconnect(client, signal, logger, seen))) {
        logger.error(`Dropped ${what}: ${err}; the worker is stopping or its client was closed`);
        return false;
      }
    }
  }
}

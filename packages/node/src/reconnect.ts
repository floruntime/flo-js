/**
 * Reconnecting a worker's client after its connection drops.
 */

import {
  ConnectionError,
  InvalidChecksumError,
  ServerError,
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

/** How long after stop() a worker keeps reconnecting to resend a result. */
export const RESULT_GRACE_MS = 2000;

/**
 * A signal that aborts `ms` after `stop` does, so a stopping worker still
 * resends a lost result for a while but stop() is never held up for long.
 * dispose() clears the timer once the worker is done with it.
 */
export function graceAfter(stop: AbortSignal, ms: number): { signal: AbortSignal; dispose: () => void } {
  const grace = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const start = () => {
    timer = setTimeout(() => grace.abort(), ms);
    timer.unref?.();
  };
  if (stop.aborted) start();
  else stop.addEventListener("abort", start, { once: true });
  return {
    signal: grace.signal,
    dispose: () => {
      stop.removeEventListener("abort", start);
      clearTimeout(timer);
    },
  };
}

/**
 * Whether a result's send failed so that the server may never have seen it,
 * or its answer can't be trusted: the connection dropped or timed out, the
 * reply failed its checksum, or the connection is gone since the send (a bad
 * reply header closes it). Anything else is the server's answer.
 */
function lost(err: unknown, client: FloClient, seen: Seen): boolean {
  // The server answered: a refusal is final, even if the connection changed
  // since (another worker sharing the client may have redialed).
  if (err instanceof ServerError) return false;
  return (
    isConnectionError(err) ||
    err instanceof InvalidChecksumError ||
    !client.isConnected() ||
    client.connection() !== seen.connection
  );
}

/**
 * Send a worker's result (complete, fail, ack or nack) with `send`, which
 * builds the same request each call. If it is lost (see lost()), the worker
 * reconnects and sends it again: a resend at worst repeats a result the
 * server already has. After RESULT_SENDS sends, once `grace` aborts (some
 * time after stop()), or if the client's owner closed it, the result is
 * dropped with an error log naming `what`; the server then hands the work
 * out again once its lease runs out. A refusal is not resent. Returns
 * whether it was delivered.
 */
export async function sendResult(
  client: FloClient,
  grace: AbortSignal,
  logger: Logger,
  closes: number,
  what: string,
  send: () => Promise<unknown>,
): Promise<boolean> {
  for (let sends = 1; ; sends++) {
    // Read before the send: a client shared with another worker may be
    // redialed meanwhile, and only this connection should be dropped.
    const seen = { connection: client.connection(), closes };
    try {
      await send();
      return true;
    } catch (err) {
      if (!lost(err, client, seen)) {
        logger.error(`Dropped ${what}: ${err}`);
        return false;
      }
      if (sends >= RESULT_SENDS) {
        logger.error(`Dropped ${what} after ${sends} sends: ${err}`);
        return false;
      }
      const gone = `Dropped ${what}: ${err}; the worker stopped ${RESULT_GRACE_MS} ms ago, or its client was closed`;
      if (grace.aborted || closedSince(client, seen)) {
        logger.error(gone);
        return false;
      }
      logger.warn(`Sending ${what} failed: ${err}; reconnecting to send it again`);
      if (!(await reconnect(client, grace, logger, seen))) {
        logger.error(gone);
        return false;
      }
    }
  }
}

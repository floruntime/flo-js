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

/** How long a result sent once a worker is leaving (stopping or draining) may wait on reconnects. */
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
 * server already has. After RESULT_SENDS sends, RESULT_GRACE_MS into the
 * send once `leaving` has aborted (a stop or drain, so neither waits on a
 * server that is gone), or if the client's owner closed it, the result is
 * dropped with an error log naming `what`; the server then hands the work
 * out again once its lease runs out. A refusal is not resent. Returns
 * whether it was delivered.
 */
export async function sendResult(
  client: FloClient,
  leaving: AbortSignal,
  logger: Logger,
  closes: number,
  what: string,
  send: () => Promise<unknown>,
): Promise<boolean> {
  const grace = graceAfter(leaving, RESULT_GRACE_MS);
  try {
    return await sendWithin(client, grace.signal, logger, closes, what, send);
  } finally {
    grace.dispose();
  }
}

async function sendWithin(
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
      const gone = `Dropped ${what}: ${err}; it found no connection within ${RESULT_GRACE_MS} ms of the worker stopping or draining, or its client was closed`;
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

/** How often a draining worker tries again to tell the server. */
export const DRAIN_NOTICE_INTERVAL_MS = 1000;

/**
 * `work`, unless `signal` aborts first: then false, and `work` is left to
 * finish on its own, its result or error dropped.
 */
async function unlessAborted(work: Promise<unknown>, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) {
    work.catch(() => {});
    return false;
  }
  let onAbort = () => {};
  const aborted = new Promise<false>((resolve) => {
    onAbort = () => resolve(false);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([work.then(() => true), aborted]);
  } finally {
    signal.removeEventListener("abort", onAbort);
    work.catch(() => {});
  }
}

/**
 * Tell the server `workerId` is draining, once. Returns whether that is
 * settled: sent, refused for a reason another try won't fix, or cut short by
 * `stop`, which a request or redial held up on a hung server mustn't delay. A
 * notice lost with its connection redials that connection (nothing else does
 * once the poll loop has left), unless the client's owner closed it.
 */
export async function sendDrainNotice(client: FloClient, workerId: string, logger: Logger, closes: number, stop: AbortSignal): Promise<boolean> {
  const seen = { connection: client.connection(), closes };
  try {
    await unlessAborted(client.worker.drain(workerId), stop);
    return true;
  } catch (err) {
    if (!lost(err, client, seen)) {
      logger.warn(`Drain notice refused: ${err}`);
      return true;
    }
    logger.warn(`Drain notice lost: ${err}; trying again`);
    if (!closedSince(client, seen)) {
      try {
        await unlessAborted(client.redial(seen.connection), stop);
      } catch {
        // Tried again next round.
      }
    }
    return stop.aborted;
  }
}

/** Resolves when one of `tasks` settles, after `ms`, or when `signal` aborts. */
export async function nextTaskOrTick(tasks: Iterable<Promise<void>>, ms: number, signal: AbortSignal): Promise<void> {
  const settled = [...tasks].map((t) => t.catch(() => {}));
  await Promise.race([...settled, pause(ms, signal)]);
}

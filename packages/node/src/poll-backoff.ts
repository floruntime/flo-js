import { Backoff, type Logger, Reason, ServerError } from "@floruntime/core";

/** The longest delay Node's timers keep; past it they fire after 1 ms. */
export const MAX_TIMER_MS = 2_147_483_647;

/**
 * Pacing for a worker's long-poll loop.
 *
 * The server answers a blocking poll empty at once when its waiter pool is
 * full, and also wakes every parked group read empty on each append so the
 * consumers re-read. The wire can't tell these apart, so timing does: a
 * full pool answers within about a round trip, while append wakes come
 * whenever data arrives. An empty answer counts as early only under
 * min(250 ms, blockMs / 2) (the first early empty is re-polled at once: it
 * is usually an append wake).
 */
export class EmptyPollBackoff {
  static readonly EARLY_MS = 250;
  static readonly MIN_PAUSE_MS = 50;
  static readonly MAX_PAUSE_MS = 1000;

  private streak = 0;

  /** How long to pause before the next poll. */
  next(empty: boolean, elapsedMs: number, blockMs: number): number {
    const early = elapsedMs < Math.min(EmptyPollBackoff.EARLY_MS, blockMs / 2);
    if (!empty || !early) {
      this.streak = 0;
      return 0;
    }
    this.streak++;
    if (this.streak === 1) return 0;
    return Math.min(EmptyPollBackoff.MAX_PAUSE_MS, EmptyPollBackoff.MIN_PAUSE_MS * 2 ** (this.streak - 2));
  }
}

/** Wait `ms`, or less if `signal` aborts first. */
export function pause(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done);
  });
}

// Worker poll loops wait this way after a failed poll, and start again from
// the bottom after a poll that succeeds.
export const WORKER_INITIAL_BACKOFF_MS = 100;
export const WORKER_MAX_BACKOFF_MS = 5000;

export function newWorkerBackoff(): Backoff {
  return new Backoff(WORKER_INITIAL_BACKOFF_MS, WORKER_MAX_BACKOFF_MS);
}

// While a worker's shard is halted, its supervision calls wait this way.
export const HALT_INITIAL_BACKOFF_MS = 100;
export const HALT_MAX_BACKOFF_MS = 30_000;

/** Whether err is a refusal from a halted shard. */
export function isShardHalted(err: unknown): err is ServerError {
  return err instanceof ServerError && err.reason === Reason.ShardHalted;
}

/**
 * Pacing for a worker's supervision calls (registering, joining its group,
 * polling) while their shard is halted. They are safe to repeat, so a
 * worker keeps trying and carries on by itself once an operator repairs the
 * shard. A client's own requests are never retried on a halt.
 */
export class HaltBackoff {
  private readonly backoff = new Backoff(HALT_INITIAL_BACKOFF_MS, HALT_MAX_BACKOFF_MS);

  constructor(private readonly logger: Logger) {}

  /**
   * If err is a halt, log it, wait (less if `signal` aborts) and return
   * true; otherwise return false at once.
   */
  async waitIf(err: unknown, call: string, signal: AbortSignal): Promise<boolean> {
    if (!isShardHalted(err)) return false;
    const d = this.backoff.next();
    this.logger.warn(`${call} refused, shard halted: ${err.serverMessage}; retrying in ${d}ms`);
    await pause(d, signal);
    return true;
  }

  /** Start the waits again from the bottom, after a call that went through. */
  reset(): void {
    this.backoff.reset();
  }
}

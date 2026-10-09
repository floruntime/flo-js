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

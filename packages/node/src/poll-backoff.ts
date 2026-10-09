/**
 * Pacing for a worker's long-poll loop.
 *
 * A blocking poll that comes back empty well before its blockMs usually
 * means the server could not park it (its waiter pool is full) and answered
 * at once; re-polling straight away would spin against the server. One
 * early empty is retried at once, since a stream group read is also woken
 * empty when records arrive, but a run of them backs off, doubling up to a
 * cap. Any poll that returns work, or waits most of its blockMs, resets it.
 */
export class EmptyPollBackoff {
  private streak = 0;

  constructor(
    private readonly minMs = 50,
    private readonly maxMs = 1000
  ) {}

  /** How long to wait before the next poll. */
  next(empty: boolean, elapsedMs: number, blockMs: number): number {
    if (!empty || elapsedMs >= blockMs / 2) {
      this.streak = 0;
      return 0;
    }
    this.streak++;
    if (this.streak === 1) return 0;
    return Math.min(this.maxMs, this.minMs * 2 ** (this.streak - 2));
  }
}

/**
 * Retrying requests the server refused without running them.
 */

import { createServerError, ServerError } from "./errors.js";
import { Ran, type RawResponse, StatusCode } from "./types.js";

/**
 * How often a request the server refused without running it is sent again.
 * Only a refusal whose status is overloaded or unavailable and whose `ran`
 * is `Ran.No` is retried: the server says it never ran, so resending is safe
 * for any operation, writes included. Each retry is a new request with a
 * new id; the server routes it afresh.
 */
export interface RetryPolicy {
  /** Tries in all, the first included; 1 (or less) turns retrying off. */
  maxAttempts: number;
  /**
   * Ceiling of the first wait; each later ceiling doubles, up to
   * `maxBackoffMs`. Each wait is uniform in [0, ceiling].
   */
  initialBackoffMs: number;
  maxBackoffMs: number;
}

/** Waits at most about 350 ms in all before returning the refusal. */
export const DEFAULT_RETRY_POLICY: Readonly<RetryPolicy> = Object.freeze({
  maxAttempts: 4,
  initialBackoffMs: 50,
  maxBackoffMs: 1000,
});

/**
 * Fills unset fields from the defaults, clamps negative waits to 0, and
 * raises a max below the initial wait to the initial, so a wait's bound is
 * never negative and the ceiling only ever grows.
 */
export function normalizeRetryPolicy(p?: Partial<RetryPolicy>): RetryPolicy {
  const initial = Math.max(0, p?.initialBackoffMs ?? DEFAULT_RETRY_POLICY.initialBackoffMs);
  const max = Math.max(initial, p?.maxBackoffMs ?? DEFAULT_RETRY_POLICY.maxBackoffMs);
  return {
    maxAttempts: p?.maxAttempts ?? DEFAULT_RETRY_POLICY.maxAttempts,
    initialBackoffMs: initial,
    maxBackoffMs: max,
  };
}

/**
 * Whether a response is a refusal the server says it didn't run, of a kind
 * worth sending again. A refusal whose body doesn't say whether it ran is
 * not retried.
 */
export function refusedUnrun(resp: RawResponse): boolean {
  if (resp.status !== StatusCode.Overloaded && resp.status !== StatusCode.Unavailable) {
    return false;
  }
  return isRefusedUnrun(createServerError(resp.status, resp.data));
}

/** Whether err is a refusal the server says it didn't run, worth trying again later. */
export function isRefusedUnrun(err: unknown): boolean {
  return (
    err instanceof ServerError &&
    (err.status === StatusCode.Overloaded || err.status === StatusCode.Unavailable) &&
    err.ran === Ran.No
  );
}

/** A capped, doubling, fully jittered wait. */
export class Backoff {
  private ceiling = 0;
  private started = false;

  /** Expects a normalised pair: 0 <= initialMs <= maxMs. */
  constructor(
    private readonly initialMs: number,
    private readonly maxMs: number
  ) {}

  /** The next wait in ms; doubles the ceiling for the one after. */
  next(): number {
    if (!this.started) {
      this.ceiling = this.initialMs;
      this.started = true;
    }
    const d = Math.floor(Math.random() * (this.ceiling + 1));
    this.ceiling = Math.min(this.ceiling * 2, this.maxMs);
    return d;
  }

  /** Start the waits again from the initial ceiling. */
  reset(): void {
    this.started = false;
  }
}

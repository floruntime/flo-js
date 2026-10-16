/**
 * High-level ActionWorker API for Flo.
 *
 * Provides an easy-to-use ActionWorker class for executing actions with:
 * - Automatic connection management
 * - Concurrency control
 * - Error handling and retries
 * - Heartbeat with drain detection
 * - Graceful shutdown with deregister
 *
 * @example
 * ```typescript
 * import { ActionWorker, ActionContext } from "@floruntime/node";
 *
 * const worker = new ActionWorker({
 *   endpoint: "localhost:9000",
 *   namespace: "myapp",
 * });
 *
 * worker.action("process-order", async (ctx) => {
 *   const order = ctx.json<{ orderId: string }>();
 *   // Process the order...
 *   return ctx.toBytes({ status: "completed", orderId: order.orderId });
 * });
 *
 * await worker.start();
 * ```
 */

import {
  ActionType,
  type ActionRegisterOptions,
  Logger,
  ProcessKind,
  ServerError,
  StatusCode,
  WorkerStatus,
  WorkerType,
  consoleLogger,
  silentLogger,
  type ProcessEntry,
  type RetryPolicy,
  workerBlockMs,
} from "@floruntime/core";
import { FloClient } from "./client.js";
import { EmptyPollBackoff, newWorkerBackoff, pause } from "./poll-backoff.js";
import {
  RESULT_GRACE_MS,
  closedSince,
  graceAfter,
  isConnectionError,
  reconnect,
  sendResult,
} from "./reconnect.js";
import crypto from "crypto";
import os from "os";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Configuration for a Flo action worker.
 */
export interface ActionWorkerConfig {
  /** Server endpoint in "host:port" format */
  endpoint: string;

  /** Namespace for operations (default: "default") */
  namespace?: string;

  /** Unique worker identifier (auto-generated if not provided) */
  workerId?: string;

  /** Maximum number of concurrent actions (default: 10) */
  concurrency?: number;

  /** Timeout for action handlers in milliseconds (default: 300000 = 5 minutes) */
  actionTimeoutMs?: number;

  /**
   * Long-poll wait per await in ms, at most 300000 (default: 30000).
   * 0 also means 30000, since 0 (don't wait) would spin the poll loop.
   */
  blockMs?: number;

  /** Machine ID for grouping workers on the same host (auto-detected if not provided) */
  machineId?: string;

  /**
   * Heartbeat interval in milliseconds (default: 30000 = 30 seconds).
   * Set to 0 to disable heartbeats.
   */
  heartbeatIntervalMs?: number;

  /** How its connection retries refused-but-not-run requests (default: the client default). */
  retry?: Partial<RetryPolicy>;

  /**
   * Touch each run every third of its action's timeout while the handler
   * runs (default false). Without it, a handler that runs past the action's
   * timeout without calling `ctx.touch()` is taken back and run again.
   */
  autoTouch?: boolean;

  /**
   * Logger for worker messages.
   * - `true`: use console logger
   * - `false` or omitted: silent (no logging)
   * - Logger object: use custom logger implementation
   */
  logger?: boolean | Logger;
}

/** autoTouch never touches more often than this, whatever the timeout. */
const MIN_TOUCH_INTERVAL_MS = 100;

/** The longest lease a touch may ask for; the server refuses more. */
const MAX_TOUCH_EXTEND_MS = 3_600_000;

/** Touch refusals that mean the run moved on: touching again can't help. */
const TOUCH_FINAL: StatusCode[] = [StatusCode.Conflict, StatusCode.NotFound, StatusCode.BadRequest];

/**
 * Action handler function signature.
 *
 * Return a plain object (auto-serialized to JSON bytes), `Uint8Array` for
 * pre-serialized data, or an `ActionResult` for named outcome routing.
 */
export type ActionHandler = (
  ctx: ActionContext
) => Promise<Record<string, unknown> | Uint8Array | ActionResult>;

/**
 * Named outcome result from an action handler.
 *
 * Use this when the workflow needs to route based on the action's
 * business outcome (e.g., "approved", "rejected", "needs_review").
 *
 * @example
 * ```typescript
 * worker.action("review-order", async (ctx) => {
 *   const order = ctx.json<{ amount: number }>();
 *   if (order.amount > 10000) {
 *     return ctx.result("needs_review", { reason: "high value" });
 *   }
 *   return ctx.result("approved", { orderId: order.orderId });
 * });
 * ```
 */
export class ActionResult {
  constructor(
    /** Named outcome string (maps to workflow transition keys) */
    readonly outcome: string,
    /** Result payload */
    readonly data: Uint8Array
  ) {}
}

/**
 * Context passed to action handlers.
 *
 * Provides access to task information and helper methods for
 * parsing input and formatting output.
 */
export class ActionContext {
  /** Task ID */
  readonly taskId: string;

  /** Action name being executed */
  readonly actionName: string;

  /** Raw input payload */
  readonly payload: Uint8Array;

  /** Attempt number (starts at 1) */
  readonly attempt: number;

  /** Timestamp when task was created */
  readonly createdAt: bigint;

  /** Namespace */
  readonly namespace: string;

  private readonly worker: ActionWorker;
  private _cancelled = false;

  constructor(
    taskId: string,
    actionName: string,
    payload: Uint8Array,
    attempt: number,
    createdAt: bigint,
    namespace: string,
    worker: ActionWorker
  ) {
    this.taskId = taskId;
    this.actionName = actionName;
    this.payload = payload;
    this.attempt = attempt;
    this.createdAt = createdAt;
    this.namespace = namespace;
    this.worker = worker;
  }

  /**
   * Get the raw input bytes.
   */
  input(): Uint8Array {
    return this.payload;
  }

  /**
   * Get the input as a UTF-8 string.
   */
  text(): string {
    return textDecoder.decode(this.payload);
  }

  /**
   * Parse input as JSON and return the result.
   * @template T - Expected type of the parsed JSON
   */
  json<T = unknown>(): T {
    if (this.payload.length === 0) {
      throw new Error("No input data");
    }
    return JSON.parse(textDecoder.decode(this.payload)) as T;
  }

  /**
   * Serialize a value to JSON bytes.
   * Convenience method for creating action results.
   */
  toBytes(value: unknown): Uint8Array {
    return textEncoder.encode(JSON.stringify(value));
  }

  /**
   * Create a named outcome result for workflow routing.
   *
   * Use this when the workflow definition has transitions keyed on
   * custom outcome names (e.g. "approved", "rejected", "needs_review").
   *
   * @param outcome - Named outcome string (maps to workflow transition keys)
   * @param value - Result data (will be JSON-serialized)
   * @returns ActionResult with the named outcome
   *
   * @example
   * ```typescript
   * // In workflow YAML:
   * //   transitions:
   * //     approved: process_payment
   * //     rejected: notify_rejection
   * //     needs_review: manual_review
   *
   * worker.action("review-order", async (ctx) => {
   *   const order = ctx.json<{ amount: number }>();
   *   if (order.amount > 10000) return ctx.result("needs_review", { reason: "high value" });
   *   if (order.amount < 0)     return ctx.result("rejected", { reason: "invalid amount" });
   *   return ctx.result("approved", { orderId: "ORD-123" });
   * });
   * ```
   */
  result(outcome: string, value: unknown): ActionResult {
    return new ActionResult(outcome, textEncoder.encode(JSON.stringify(value)));
  }

  /**
   * Extend the lease on this task.
   *
   * Use this for long-running tasks to prevent timeout.
   * Call periodically to keep the task alive.
   *
   * @param extendMs - How long to extend the lease in milliseconds (default: 30000)
   */
  async touch(extendMs = 30000): Promise<void> {
    await this.worker._touchTask(this.actionName, this.taskId, extendMs);
  }

  /**
   * Check if the task has been cancelled.
   */
  get cancelled(): boolean {
    return this._cancelled;
  }

  /**
   * Mark the context as cancelled (internal use).
   */
  _setCancelled(): void {
    this._cancelled = true;
  }

  /**
   * Check if cancelled and throw if so.
   */
  checkCancelled(): void {
    if (this._cancelled) {
      throw new Error("Task was cancelled");
    }
  }
}

/**
 * High-level Flo action worker.
 *
 * The ActionWorker class provides a convenient way to process actions:
 * - Register action handlers using `action()` method
 * - Start processing with `start()` which runs until `stop()` is called
 * - Automatic concurrency control and error handling
 * - Heartbeats with drain detection
 * - Graceful deregister on shutdown
 *
 * @example
 * ```typescript
 * const worker = new ActionWorker({
 *   endpoint: "localhost:9000",
 *   namespace: "myapp",
 *   concurrency: 5,
 * });
 *
 * // Register handlers
 * worker.action("send-email", async (ctx) => {
 *   const { to, subject, body } = ctx.json<EmailRequest>();
 *   await sendEmail(to, subject, body);
 *   return ctx.toBytes({ sent: true });
 * });
 *
 * // Start processing (blocks until stop() is called)
 * await worker.start();
 * ```
 */
export class ActionWorker {
  private readonly config: Required<Omit<ActionWorkerConfig, 'logger'>>;
  private readonly logger: Logger;
  private client: FloClient | null = null;
  private readonly handlers: Map<string, ActionHandler> = new Map();
  private readonly actionOptions: Map<string, ActionRegisterOptions> = new Map();
  private running = false;
  private stopRequested = false;
  private stopSignal = new AbortController();
  /** Aborts RESULT_GRACE_MS after stopSignal: until then lost results are resent. */
  private resultGrace = graceAfter(this.stopSignal.signal, RESULT_GRACE_MS);
  /** client.closes() at start(): once it moves, the owner closed the client. */
  private clientCloses = 0;
  private activeTaskCount = 0;
  private readonly pendingTasks: Set<Promise<void>> = new Set();
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(config: ActionWorkerConfig) {
    // Validate required fields
    if (!config.endpoint) {
      throw new Error("endpoint is required");
    }

    // Apply defaults
    this.config = {
      endpoint: config.endpoint,
      namespace: config.namespace ?? "default",
      workerId: config.workerId ?? this.generateWorkerId(),
      concurrency: config.concurrency ?? 10,
      actionTimeoutMs: config.actionTimeoutMs ?? 300000,
      blockMs: workerBlockMs(config.blockMs),
      machineId: config.machineId ?? (os.hostname() || ""),
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 30000,
      retry: config.retry ?? {},
      autoTouch: config.autoTouch ?? false,
    };

    // Setup logger
    if (config.logger === true) {
      this.logger = consoleLogger;
    } else if (config.logger === false || config.logger === undefined) {
      this.logger = silentLogger;
    } else {
      this.logger = config.logger;
    }
  }

  private generateWorkerId(): string {
    const hostname = os.hostname() || "unknown";
    const random = crypto.randomBytes(4).toString("hex");
    return `${hostname}-${random}`;
  }

  private log(message: string, ...args: unknown[]): void {
    this.logger.debug(message, ...args);
  }

  /**
   * Register an action handler.
   *
   * The worker registers the action with the server when it starts, with
   * exactly these options (the server's defaults when none: a 30 s timeout,
   * 3 attempts). Registering replaces the action's settings, so workers
   * registering one action with different options leave it with whichever
   * registered last. The options' namespace is ignored: the action goes in
   * the worker's.
   *
   * @param name - The action name to register
   * @param handler - Async function that handles the action
   * @param options - The action's timeout, attempts and owner
   * @throws Error if action is already registered
   */
  action(name: string, handler: ActionHandler, options: ActionRegisterOptions = {}): void {
    if (this.handlers.has(name)) {
      throw new Error(`Action '${name}' is already registered`);
    }
    for (const field of ["timeoutMs", "maxRetries"] as const) {
      const v = options[field];
      // Both go on the wire as u32.
      if (v !== undefined && !(Number.isInteger(v) && v >= 0 && v < 2 ** 32)) {
        throw new RangeError(`${field} must be an integer in [0, 2^32), not ${v}`);
      }
    }
    this.handlers.set(name, handler);
    this.actionOptions.set(name, { ...options, namespace: undefined });
    this.log(`Registered action: ${name}`);
  }

  /**
   * Start the worker and begin processing actions.
   *
   * This method blocks until `stop()` is called or an error occurs.
   *
   * @throws Error if no handlers are registered
   * @throws Error if connection to server fails
   */
  async start(): Promise<void> {
    if (this.handlers.size === 0) {
      throw new Error("No action handlers registered");
    }

    this.log(
      `Starting Flo worker (id=${this.config.workerId}, ` +
        `namespace=${this.config.namespace}, concurrency=${this.config.concurrency})`
    );

    // Connect to server
    this.client = new FloClient(this.config.endpoint, {
      namespace: this.config.namespace,
      logger: this.logger,
      retry: this.config.retry,
    });
    await this.client.connect();
    this.clientCloses = this.client.closes();

    const actionNames = Array.from(this.handlers.keys());
    try {
      await this.register(actionNames);

      // Initialize state
      this.running = true;
      this.stopRequested = false;
      this.stopSignal = new AbortController();
      this.resultGrace = graceAfter(this.stopSignal.signal, RESULT_GRACE_MS);

      // Start heartbeat interval
      this.startHeartbeat();

      // Main polling loop
      await this.pollLoop(actionNames);
    } finally {
      // Stop heartbeat
      this.stopHeartbeat();

      // Wait for running tasks
      if (this.pendingTasks.size > 0) {
        this.log(`Waiting for ${this.pendingTasks.size} tasks to complete...`);
        await Promise.allSettled(this.pendingTasks);
      }
      this.resultGrace.dispose();

      // Deregister from worker registry
      if (this.client) {
        try {
          await this.client.worker.deregister(this.config.workerId);
        } catch {
          // Best-effort deregister
        }
      }

      await this.client?.close();
      this.client = null;
      this.running = false;
      this.log("Worker stopped");
    }
  }

  /** Register the actions and this worker with the server. */
  private async register(actionNames: string[]): Promise<void> {
    for (const actionName of actionNames) {
      await this.client!.action.register(actionName, ActionType.User, this.actionOptions.get(actionName));
      this.log(`Registered action with server: ${actionName}`);
    }

    const processes: ProcessEntry[] = actionNames.map((name) => ({
      name,
      kind: ProcessKind.Action,
    }));
    await this.client!.worker.register(this.config.workerId, actionNames, {
      workerType: WorkerType.Action,
      maxConcurrency: this.config.concurrency,
      processes,
      machineId: this.config.machineId || undefined,
    });
    this.log(`Worker registered with ${actionNames.length} actions`);
  }

  /** Reconnect after a dropped connection, then register again. */
  private async recover(actionNames: string[], cause: unknown, connection: number): Promise<void> {
    this.logger.warn(`Connection lost: ${cause}; reconnecting...`);
    const seen = { connection, closes: this.clientCloses };
    if (!(await reconnect(this.client!, this.stopSignal.signal, this.logger, seen))) {
      if (closedSince(this.client!, seen) && !this.stopRequested) {
        this.logger.warn("The client was closed by its owner; stopping the worker");
        this.stop();
      }
      return;
    }
    try {
      await this.register(actionNames);
    } catch (err) {
      // A connection error here surfaces again on the next poll.
      this.logger.warn(`Re-register after reconnect failed: ${err}`);
    }
  }

  private async pollLoop(actionNames: string[]): Promise<void> {
    const backoff = new EmptyPollBackoff();
    const failed = newWorkerBackoff();
    let unproven = false;
    while (this.running && !this.stopRequested) {
      // The connection this iteration's poll goes on, to redial only it.
      const connection = this.client!.connection();
      try {
        // Check concurrency limit
        if (this.activeTaskCount >= this.config.concurrency) {
          // Wait a bit before checking again
          await this.sleep(100);
          continue;
        }

        // Check if we should stop
        if (this.stopRequested) {
          break;
        }

        // Await task from server
        const started = performance.now();
        const result = await this.client!.worker.awaitTask(
          this.config.workerId,
          actionNames,
          { blockMs: this.config.blockMs }
        );
        failed.reset();
        unproven = false;

        const wait = backoff.next(result.task === null, performance.now() - started, this.config.blockMs);
        if (result.task === null) {
          if (wait > 0) await pause(wait, this.stopSignal.signal);
          continue;
        }

        // Execute task in background
        this.activeTaskCount++;
        const taskPromise = this.executeTask(result.task).finally(() => {
          this.activeTaskCount--;
          this.pendingTasks.delete(taskPromise);
        });
        this.pendingTasks.add(taskPromise);
      } catch (err) {
        if (this.stopRequested) break;
        if (isConnectionError(err)) {
          // A server that takes the connection and drops it again is not
          // redialed at once each time: until a poll succeeds after a
          // reconnect, each one waits first.
          if (unproven) await pause(failed.next(), this.stopSignal.signal);
          if (this.stopRequested) break;
          await this.recover(actionNames, err, connection);
          unproven = true;
        } else {
          const d = failed.next();
          this.log(`Await error: ${err}, retrying in ${d}ms...`);
          await pause(d, this.stopSignal.signal);
        }
      }
    }
  }

  private async executeTask(task: {
    taskId: string;
    taskType: string;
    payload: Uint8Array;
    createdAt: bigint;
    attempt: number;
  }): Promise<void> {
    const client = this.client!;
    const { workerId } = this.config;
    const { taskType, taskId } = task;
    this.log(`Executing action: ${taskType} (task=${taskId}, attempt=${task.attempt})`);

    // The handler's outcome alone picks complete or fail; sending it is
    // separate, so a lost complete is resent, never turned into a fail.
    let what: string;
    let report: () => Promise<unknown>;
    const handler = this.handlers.get(taskType);
    if (!handler) {
      this.log(`No handler registered for action: ${taskType}`);
      what = "fail";
      report = () => client.worker.fail(workerId, taskType, taskId, `No handler for: ${taskType}`);
    } else {
      const ctx = new ActionContext(
        taskId,
        taskType,
        task.payload,
        task.attempt,
        task.createdAt,
        this.config.namespace,
        this
      );
      const timeoutMs = this.actionOptions.get(taskType)?.timeoutMs ?? 30000;
      const stopTouching = this.config.autoTouch && timeoutMs > 0 ? this.keepTouching(taskType, taskId, timeoutMs) : () => {};
      try {
        const result = await this.withTimeout(handler(ctx), this.config.actionTimeoutMs).finally(stopTouching);
        // Plain object → JSON bytes, ActionResult → named outcome
        what = "complete";
        if (result instanceof ActionResult) {
          report = () => client.worker.complete(workerId, taskType, taskId, result.data, { outcome: result.outcome });
        } else {
          const bytes = result instanceof Uint8Array ? result : textEncoder.encode(JSON.stringify(result));
          report = () => client.worker.complete(workerId, taskType, taskId, bytes);
        }
      } catch (err) {
        what = "fail";
        if (err instanceof TimeoutError) {
          this.log(`Action timed out: ${taskType}`);
          report = () => client.worker.fail(workerId, taskType, taskId, "Action timed out", { retry: false });
        } else if (err instanceof NonRetryableError) {
          this.log(`Action failed (non-retryable): ${taskType} - ${err}`);
          report = () => client.worker.fail(workerId, taskType, taskId, String(err), { retry: false });
        } else {
          this.log(`Action failed: ${taskType} - ${err}`);
          report = () => client.worker.fail(workerId, taskType, taskId, String(err), { retry: true });
        }
      }
    }

    const sent = await sendResult(
      client,
      this.resultGrace.signal,
      this.logger,
      this.clientCloses,
      `${what} for task ${taskId}`,
      report
    );
    if (sent) this.log(`Action ${what} sent: ${taskType} (task=${taskId})`);
  }

  /**
   * Touch a run until the returned function is called. Each touch asks for
   * this worker's timeout, at most an hour, so the lease covers it even if
   * another worker's registration shortened the action's, and goes every
   * third of that (at least 100 ms apart). Conflict, not found or bad
   * request means the run moved on (finished, cancelled or taken back), so
   * touching stops; any other refusal, an overloaded leader say, or a touch
   * lost with its connection, is tried again next interval. A touch in
   * flight when this stops is left to finish.
   */
  private keepTouching(actionName: string, taskId: string, timeoutMs: number): () => void {
    const extendMs = Math.min(timeoutMs, MAX_TOUCH_EXTEND_MS);
    // A third of the lease each touch asks for, so two can fail in a row.
    const intervalMs = Math.max(extendMs / 3, MIN_TOUCH_INTERVAL_MS);
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async (): Promise<void> => {
      try {
        await this._touchTask(actionName, taskId, extendMs);
      } catch (err) {
        if (err instanceof ServerError && TOUCH_FINAL.includes(err.status)) {
          this.logger.warn(`Touch for task ${taskId} was refused, stopping: ${err}`);
          return;
        }
        this.logger.warn(`Touch for task ${taskId} failed: ${err}; trying again`);
      }
      if (!stopped) timer = setTimeout(tick, intervalMs);
    };
    timer = setTimeout(tick, intervalMs);
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }

  /**
   * Extend lease on a task (internal method, called by ActionContext).
   */
  async _touchTask(actionName: string, taskId: string, extendMs: number): Promise<void> {
    if (!this.client) {
      throw new Error("Worker not connected");
    }
    await this.client.worker.touch(this.config.workerId, actionName, taskId, { extendMs });
  }

  /**
   * Signal the worker to stop.
   *
   * This sets a flag that will cause the polling loop to exit
   * after the current iteration completes.
   */
  stop(): void {
    this.log("Stopping worker...");
    this.running = false;
    this.stopRequested = true;
    this.stopSignal.abort();
  }

  /**
   * Stop and close the worker.
   */
  async close(): Promise<void> {
    this.stop();
    if (this.client) {
      await this.client.close();
    }
  }

  /**
   * Initiate graceful drain — no new tasks will be assigned.
   * In-flight tasks will continue to completion.
   */
  async drain(): Promise<void> {
    if (!this.client) return;
    await this.client.worker.drain(this.config.workerId);
    this.log("Worker drain requested");
  }

  private startHeartbeat(): void {
    if (this.config.heartbeatIntervalMs <= 0) return;

    this.heartbeatTimer = setInterval(async () => {
      if (!this.client || !this.running) return;
      try {
        const status = await this.client.worker.heartbeat(
          this.config.workerId,
          this.activeTaskCount
        );
        if (status === WorkerStatus.Draining) {
          this.log("Worker is draining, stopping...");
          this.stop();
        }
      } catch {
        // Heartbeat failures are non-fatal
      }
    }, this.config.heartbeatIntervalMs);

    // Don't let the heartbeat timer prevent process exit
    if (this.heartbeatTimer && typeof this.heartbeatTimer === "object" && "unref" in this.heartbeatTimer) {
      this.heartbeatTimer.unref();
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
    let timeoutId: NodeJS.Timeout;

    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new TimeoutError(`Operation timed out after ${ms}ms`));
      }, ms);
    });

    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      clearTimeout(timeoutId!);
    }
  }
}

/**
 * Error thrown when an action handler runs past `actionTimeoutMs`. It fails
 * the task without retry. Not core's `TimeoutError`, which the transport
 * throws when a request goes unanswered; a worker treats that as a lost
 * connection.
 */
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

/**
 * Error thrown by action handlers for deterministic / business-logic failures
 * that should NOT be retried (e.g. validation errors, invalid input).
 *
 * Throwing a regular `Error` will cause the task to be retried.
 * Throwing a `NonRetryableError` will mark the task as permanently failed.
 *
 * @example
 * ```typescript
 * async function validateOrder(ctx: ActionContext): Promise<Uint8Array> {
 *   const data = ctx.json<{ amount: number }>();
 *   if (data.amount > 2000) {
 *     throw new NonRetryableError(`amount $${data.amount} exceeds limit`);
 *   }
 *   return ctx.toBytes({ valid: true });
 * }
 * ```
 */
export class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NonRetryableError";
  }
}

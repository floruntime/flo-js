/**
 * High-level Worker API for Flo.
 *
 * Provides an easy-to-use Worker class for executing actions with:
 * - Automatic connection management
 * - Concurrency control
 * - Error handling and retries
 * - Graceful shutdown
 *
 * @example
 * ```typescript
 * import { Worker, ActionContext } from "@floruntime/node";
 *
 * const worker = new Worker({
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

import { ActionType, Logger, consoleLogger, silentLogger } from "@floruntime/core";
import { FloClient } from "./client.js";
import crypto from "crypto";
import os from "os";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Configuration for a Flo worker.
 */
export interface WorkerConfig {
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

  /** Timeout for blocking dequeue in milliseconds (default: 30000) */
  blockMs?: number;

  /**
   * Logger for worker messages.
   * - `true`: use console logger
   * - `false` or omitted: silent (no logging)
   * - Logger object: use custom logger implementation
   */
  logger?: boolean | Logger;
}

/**
 * Action handler function signature.
 */
export type ActionHandler = (ctx: ActionContext) => Promise<Uint8Array>;

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

  private readonly worker: Worker;
  private _cancelled = false;

  constructor(
    taskId: string,
    actionName: string,
    payload: Uint8Array,
    attempt: number,
    createdAt: bigint,
    namespace: string,
    worker: Worker
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
   * Extend the lease on this task.
   *
   * Use this for long-running tasks to prevent timeout.
   * Call periodically to keep the task alive.
   *
   * @param extendMs - How long to extend the lease in milliseconds (default: 30000)
   */
  async touch(extendMs = 30000): Promise<void> {
    await this.worker._touchTask(this.taskId, extendMs);
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
 * High-level Flo worker for executing actions.
 *
 * The Worker class provides a convenient way to process actions:
 * - Register action handlers using `action()` method
 * - Start processing with `start()` which runs until `stop()` is called
 * - Automatic concurrency control and error handling
 *
 * @example
 * ```typescript
 * const worker = new Worker({
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
export class Worker {
  private readonly config: Required<Omit<WorkerConfig, 'logger'>>;
  private readonly logger: Logger;
  private client: FloClient | null = null;
  private readonly handlers: Map<string, ActionHandler> = new Map();
  private running = false;
  private stopRequested = false;
  private activeTaskCount = 0;
  private readonly pendingTasks: Set<Promise<void>> = new Set();

  constructor(config: WorkerConfig) {
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
      blockMs: config.blockMs ?? 30000,
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
   * @param name - The action name to register
   * @param handler - Async function that handles the action
   * @throws Error if action is already registered
   */
  action(name: string, handler: ActionHandler): void {
    if (this.handlers.has(name)) {
      throw new Error(`Action '${name}' is already registered`);
    }
    this.handlers.set(name, handler);
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
    });
    await this.client.connect();

    try {
      // Register actions with the server
      const actionNames = Array.from(this.handlers.keys());
      for (const actionName of actionNames) {
        await this.client.action.register(actionName, ActionType.User);
        this.log(`Registered action with server: ${actionName}`);
      }

      // Register worker
      await this.client.worker.register(this.config.workerId, actionNames);
      this.log(`Worker registered with ${actionNames.length} actions`);

      // Initialize state
      this.running = true;
      this.stopRequested = false;

      // Main polling loop
      await this.pollLoop(actionNames);
    } finally {
      // Wait for running tasks
      if (this.pendingTasks.size > 0) {
        this.log(`Waiting for ${this.pendingTasks.size} tasks to complete...`);
        await Promise.allSettled(this.pendingTasks);
      }

      await this.client.close();
      this.client = null;
      this.running = false;
      this.log("Worker stopped");
    }
  }

  private async pollLoop(actionNames: string[]): Promise<void> {
    while (this.running && !this.stopRequested) {
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
        const result = await this.client!.worker.awaitTask(
          this.config.workerId,
          actionNames,
          { blockMs: this.config.blockMs }
        );

        if (result.task === null) {
          // No task available, continue polling
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
        if (!this.stopRequested) {
          this.log(`Await error: ${err}, retrying...`);
          await this.sleep(1000);
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
    try {
      this.log(
        `Executing action: ${task.taskType} (task=${task.taskId}, attempt=${task.attempt})`
      );

      // Get handler
      const handler = this.handlers.get(task.taskType);
      if (!handler) {
        this.log(`No handler registered for action: ${task.taskType}`);
        await this.client!.worker.fail(
          this.config.workerId,
          task.taskId,
          `No handler for: ${task.taskType}`
        );
        return;
      }

      // Create action context
      const ctx = new ActionContext(
        task.taskId,
        task.taskType,
        task.payload,
        task.attempt,
        task.createdAt,
        this.config.namespace,
        this
      );

      // Execute with timeout
      try {
        const result = await this.withTimeout(
          handler(ctx),
          this.config.actionTimeoutMs
        );

        // Success - complete the task
        await this.client!.worker.complete(
          this.config.workerId,
          task.taskId,
          result
        );
        this.log(`Action completed: ${task.taskType}`);
      } catch (err) {
        if (err instanceof TimeoutError) {
          this.log(`Action timed out: ${task.taskType}`);
          await this.client!.worker.fail(
            this.config.workerId,
            task.taskId,
            "Action timed out",
            { retry: false }
          );
        } else {
          this.log(`Action failed: ${task.taskType} - ${err}`);
          await this.client!.worker.fail(
            this.config.workerId,
            task.taskId,
            String(err),
            { retry: true }
          );
        }
      }
    } catch (err) {
      this.log(`Failed to report task result: ${err}`);
    }
  }

  /**
   * Extend lease on a task (internal method, called by ActionContext).
   */
  async _touchTask(taskId: string, extendMs: number): Promise<void> {
    if (!this.client) {
      throw new Error("Worker not connected");
    }
    await this.client.worker.touch(this.config.workerId, taskId, { extendMs });
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
 * Error thrown when an operation times out.
 */
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

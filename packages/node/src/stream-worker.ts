/**
 * High-level StreamWorker API for Flo.
 *
 * Consumes stream records via consumer groups with:
 * - Automatic group join/leave
 * - Worker registry integration (register, heartbeat, deregister)
 * - Concurrency control
 * - Automatic ack/nack on handler success/failure
 * - Graceful drain support
 *
 * @example
 * ```typescript
 * import { StreamWorker, StreamContext } from "@floruntime/node";
 * import { FloClient } from "@floruntime/node";
 *
 * const client = new FloClient("localhost:9000", { namespace: "myapp" });
 * await client.connect();
 *
 * const worker = new StreamWorker(client, {
 *   stream: "events",
 *   group: "processors",
 * }, async (ctx) => {
 *   const event = ctx.json<{ type: string }>();
 *   console.log(`Processing event: ${event.type}`);
 * });
 *
 * await worker.start();
 * ```
 */

import {
  Logger,
  ProcessKind,
  WorkerStatus,
  WorkerType,
  consoleLogger,
  isRefusedUnrun,
  silentLogger,
  type ProcessEntry,
  type RetryPolicy,
  type StreamID,
  type StreamRecord,
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

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

/**
 * Configuration for a Flo stream worker.
 */
export interface StreamWorkerConfig {
  /** Stream name to consume from (required) */
  stream: string;

  /** Consumer group name (default: "default") */
  group?: string;

  /** Consumer name within the group (default: workerId) */
  consumer?: string;

  /** Unique worker identifier (auto-generated if not provided) */
  workerId?: string;

  /** Machine ID for grouping workers on the same host (auto-detected if not provided) */
  machineId?: string;

  /** Maximum number of concurrent message handlers (default: 10) */
  concurrency?: number;

  /** Number of records to read per poll (default: 10) */
  batchSize?: number;

  /**
   * Long-poll wait per read in ms, at most 300000 (default: 30000).
   * 0 also means 30000, since 0 (don't wait) would spin the poll loop.
   */
  blockMs?: number;

  /** Maximum duration for a message handler in milliseconds (default: 300000 = 5 minutes) */
  messageTimeoutMs?: number;

  /** Heartbeat interval in milliseconds (default: 30000). Set to 0 to disable. */
  heartbeatIntervalMs?: number;

  /**
   * How the client it creates for an endpoint retries refused-but-not-run
   * requests (default: the client default). A worker given a client uses
   * that client's policy.
   */
  retry?: Partial<RetryPolicy>;

  /**
   * Logger for worker messages.
   * - `true`: use console logger
   * - `false` or omitted: silent (no logging)
   * - Logger object: use custom logger
   */
  logger?: boolean | Logger;
}

/**
 * Handler function for processing stream records.
 * Return normally for success (auto-ack).
 * Throw an error to nack and redeliver.
 */
export type StreamRecordHandler = (ctx: StreamContext) => Promise<void>;

/**
 * Context passed to stream record handlers.
 *
 * Provides access to the stream record and metadata,
 * along with helpers for parsing payloads.
 */
export class StreamContext {
  /** The stream record being processed */
  readonly record: StreamRecord;

  /** Namespace */
  readonly namespace: string;

  /** Stream name */
  readonly stream: string;

  /** Consumer group name */
  readonly group: string;

  /** Consumer name */
  readonly consumer: string;

  constructor(
    record: StreamRecord,
    namespace: string,
    stream: string,
    group: string,
    consumer: string
  ) {
    this.record = record;
    this.namespace = namespace;
    this.stream = stream;
    this.group = group;
    this.consumer = consumer;
  }

  /** Get the record's StreamID. */
  get streamId(): StreamID {
    return this.record.id;
  }

  /** Get the raw record payload. */
  get payload(): Uint8Array {
    return this.record.payload;
  }

  /** Get the record headers. */
  get headers(): Record<string, string> {
    return this.record.headers ?? {};
  }

  /** Get the payload as a UTF-8 string. */
  text(): string {
    return textDecoder.decode(this.record.payload);
  }

  /**
   * Parse the payload as JSON.
   * @template T - Expected type of the parsed JSON
   */
  json<T = unknown>(): T {
    if (this.record.payload.length === 0) {
      throw new Error("No payload data");
    }
    return JSON.parse(textDecoder.decode(this.record.payload)) as T;
  }

  /**
   * Serialize a value to JSON bytes.
   * Convenience method for producing output.
   */
  toBytes(value: unknown): Uint8Array {
    return textEncoder.encode(JSON.stringify(value));
  }
}

/**
 * High-level Flo stream worker.
 *
 * Consumes records from a stream via consumer groups,
 * with automatic ack/nack, heartbeats, and drain support.
 */
export class StreamWorker {
  private readonly config: Required<
    Omit<StreamWorkerConfig, "logger">
  >;
  private readonly logger: Logger;
  private client: FloClient | null = null;
  private readonly ownedClient: boolean;
  private readonly handler: StreamRecordHandler;
  private running = false;
  private stopRequested = false;
  private stopSignal = new AbortController();
  /** Aborts RESULT_GRACE_MS after stopSignal: until then lost results are resent. */
  private resultGrace = graceAfter(this.stopSignal.signal, RESULT_GRACE_MS);
  /** client.closes() at start(): once it moves, the owner closed the client. */
  private clientCloses = 0;
  private activeCount = 0;
  private readonly pendingTasks: Set<Promise<void>> = new Set();
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  /**
   * Create a stream worker.
   *
   * @param clientOrEndpoint - An existing FloClient instance, or an endpoint string ("host:port")
   * @param config - Stream worker configuration
   * @param handler - Handler function for each stream record
   */
  constructor(
    clientOrEndpoint: FloClient | string,
    config: StreamWorkerConfig,
    handler: StreamRecordHandler
  ) {
    if (!config.stream) {
      throw new Error("stream is required");
    }
    if (!handler) {
      throw new Error("handler is required");
    }

    const workerId =
      config.workerId ?? `${os.hostname() || "unknown"}-${crypto.randomBytes(4).toString("hex")}`;

    this.config = {
      stream: config.stream,
      group: config.group ?? "default",
      consumer: config.consumer ?? workerId,
      workerId,
      machineId: config.machineId ?? (os.hostname() || ""),
      concurrency: config.concurrency ?? 10,
      batchSize: config.batchSize ?? 10,
      blockMs: workerBlockMs(config.blockMs),
      messageTimeoutMs: config.messageTimeoutMs ?? 300000,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 30000,
      retry: config.retry ?? {},
    };

    this.handler = handler;

    if (typeof clientOrEndpoint === "string") {
      this.client = new FloClient(clientOrEndpoint, { namespace: "default", retry: this.config.retry });
      this.ownedClient = true;
    } else {
      this.client = clientOrEndpoint;
      this.ownedClient = false;
    }

    // Setup logger
    if (config.logger === true) {
      this.logger = consoleLogger;
    } else if (config.logger && typeof config.logger === "object") {
      this.logger = config.logger;
    } else {
      this.logger = silentLogger;
    }
  }

  /**
   * Start consuming stream records.
   *
   * This method blocks until `stop()` is called or an error occurs.
   */
  async start(): Promise<void> {
    this.log(
      `Starting stream worker (id=${this.config.workerId}, stream=${this.config.stream}, ` +
        `group=${this.config.group}, consumer=${this.config.consumer})`
    );

    // Connect if we own the client
    if (this.ownedClient && this.client && !this.client.isConnected()) {
      await this.client.connect();
    }

    if (!this.client) {
      throw new Error("Client not available");
    }
    this.clientCloses = this.client.closes();

    // Set before the join so stop() can end a join's retry wait.
    this.stopRequested = false;
    this.stopSignal = new AbortController();
    this.resultGrace = graceAfter(this.stopSignal.signal, RESULT_GRACE_MS);

    try {
      if (!(await this.joinGroup())) return;

      // Initialize state
      this.running = true;

      // Start heartbeat
      this.startHeartbeat();

      // Main polling loop
      await this.pollLoop();
    } finally {
      // Stop heartbeat
      this.stopHeartbeat();

      // Wait for in-flight tasks
      if (this.pendingTasks.size > 0) {
        this.log(`Waiting for ${this.pendingTasks.size} records to complete...`);
        await Promise.allSettled(this.pendingTasks);
      }
      this.resultGrace.dispose();

      // Deregister and leave group
      if (this.client) {
        try {
          await this.client.worker.deregister(this.config.workerId);
        } catch {
          // Best-effort
        }
        try {
          await this.client.stream.groupLeave(
            this.config.stream,
            this.config.group,
            this.config.consumer
          );
        } catch {
          // Best-effort
        }
      }

      // Close client if we own it
      if (this.ownedClient && this.client) {
        await this.client.close();
      }

      this.client = null;
      this.running = false;
      this.log("Stream worker stopped");
    }
  }

  /**
   * Join the consumer group and register this worker with the server. A
   * join the server refused without running it (overloaded or unavailable,
   * past the client's own retries) is tried again with the worker backoff;
   * any other error is thrown. Returns false if stop() came first.
   */
  private async joinGroup(): Promise<boolean> {
    const failed = newWorkerBackoff();
    for (;;) {
      if (this.stopRequested) return false;
      try {
        await this.client!.stream.groupJoin(this.config.stream, this.config.group, this.config.consumer);
        break;
      } catch (err) {
        if (!isRefusedUnrun(err)) throw err;
        const d = failed.next();
        this.log(`Group join refused: ${err}, retrying in ${d}ms...`);
        await pause(d, this.stopSignal.signal);
      }
    }
    this.log("Joined consumer group");

    const processName = `${this.config.stream}/${this.config.group}`;
    const processes: ProcessEntry[] = [
      { name: processName, kind: ProcessKind.StreamConsumer },
    ];
    const metadata = JSON.stringify({
      stream: this.config.stream,
      group: this.config.group,
      consumer: this.config.consumer,
    });
    await this.client!.worker.register(this.config.workerId, [], {
      workerType: WorkerType.Stream,
      maxConcurrency: this.config.concurrency,
      processes,
      metadata,
      machineId: this.config.machineId || undefined,
    });
    this.log("Registered in worker registry");
    return true;
  }

  /** Reconnect after a dropped connection, then join again. */
  private async recover(cause: unknown, connection: number): Promise<void> {
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
      await this.joinGroup();
    } catch (err) {
      // A connection error here surfaces again on the next read.
      this.logger.warn(`Re-join after reconnect failed: ${err}`);
    }
  }

  private async pollLoop(): Promise<void> {
    const backoff = new EmptyPollBackoff();
    const failed = newWorkerBackoff();
    let unproven = false;
    while (this.running && !this.stopRequested) {
      // The connection this iteration's poll goes on, to redial only it.
      const connection = this.client!.connection();
      try {
        // Check concurrency limit
        if (this.activeCount >= this.config.concurrency) {
          await this.sleep(100);
          continue;
        }

        if (this.stopRequested) break;

        // Read batch from consumer group
        const started = performance.now();
        const result = await this.client!.stream.groupRead(this.config.stream, {
          group: this.config.group,
          consumer: this.config.consumer,
          limit: this.config.batchSize,
          blockMs: this.config.blockMs,
        });
        failed.reset();
        unproven = false;

        const empty = !result || result.records.length === 0;
        const wait = backoff.next(empty, performance.now() - started, this.config.blockMs);
        if (empty) {
          if (wait > 0) await pause(wait, this.stopSignal.signal);
          continue;
        }

        // Process each record with concurrency control
        for (const record of result.records) {
          if (this.stopRequested) break;

          // Wait for a slot if at capacity
          while (this.activeCount >= this.config.concurrency && !this.stopRequested) {
            await this.sleep(50);
          }
          if (this.stopRequested) break;

          this.activeCount++;
          const taskPromise = this.processRecord(record).finally(() => {
            this.activeCount--;
            this.pendingTasks.delete(taskPromise);
          });
          this.pendingTasks.add(taskPromise);
        }
      } catch (err) {
        if (this.stopRequested) break;
        if (isConnectionError(err)) {
          // A server that takes the connection and drops it again is not
          // redialed at once each time: until a read succeeds after a
          // reconnect, each one waits first.
          if (unproven) await pause(failed.next(), this.stopSignal.signal);
          if (this.stopRequested) break;
          await this.recover(err, connection);
          unproven = true;
        } else {
          const d = failed.next();
          this.log(`GroupRead error: ${err}, retrying in ${d}ms...`);
          await pause(d, this.stopSignal.signal);
        }
      }
    }
  }

  private async processRecord(record: StreamRecord): Promise<void> {
    const client = this.client!;
    const ctx = new StreamContext(
      record,
      client.namespace(),
      this.config.stream,
      this.config.group,
      this.config.consumer
    );
    const id = `${record.id.timestampMs}-${record.id.sequence}`;
    const opts = { group: this.config.group, consumer: this.config.consumer };

    // The handler's outcome alone picks ack or nack; sending it is separate,
    // so a lost ack is resent, never turned into a nack.
    let acked = true;
    try {
      await this.withTimeout(this.handler(ctx), this.config.messageTimeoutMs);
    } catch (err) {
      this.log(`Record ${id} failed: ${err}`);
      acked = false;
    }

    const what = acked ? "ack" : "nack";
    await sendResult(client, this.resultGrace.signal, this.logger, this.clientCloses, `${what} for record ${id}`, () =>
      acked
        ? client.stream.groupAck(this.config.stream, [record.id], opts)
        : client.stream.groupNack(this.config.stream, [record.id], opts)
    );
  }

  /**
   * Signal the stream worker to stop.
   */
  stop(): void {
    this.log("Stopping stream worker...");
    this.running = false;
    this.stopRequested = true;
    this.stopSignal.abort();
  }

  /**
   * Stop and close the stream worker.
   */
  async close(): Promise<void> {
    this.stop();
    if (this.ownedClient && this.client) {
      await this.client.close();
    }
  }

  /**
   * Initiate graceful drain — no new records will be consumed.
   * In-flight records will continue to completion.
   */
  async drain(): Promise<void> {
    if (!this.client) return;
    await this.client.worker.drain(this.config.workerId);
    this.log("Stream worker drain requested");
  }

  private startHeartbeat(): void {
    if (this.config.heartbeatIntervalMs <= 0) return;

    this.heartbeatTimer = setInterval(async () => {
      if (!this.client || !this.running) return;
      try {
        const status = await this.client.worker.heartbeat(
          this.config.workerId,
          this.activeCount
        );
        if (status === WorkerStatus.Draining) {
          this.log("Worker is draining, stopping...");
          this.stop();
        }
      } catch {
        // Heartbeat failures are non-fatal
      }
    }, this.config.heartbeatIntervalMs);

    if (
      this.heartbeatTimer &&
      typeof this.heartbeatTimer === "object" &&
      "unref" in this.heartbeatTimer
    ) {
      this.heartbeatTimer.unref();
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private log(message: string, ...args: unknown[]): void {
    this.logger.debug(message, ...args);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
    let timeoutId: ReturnType<typeof setTimeout>;

    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`Stream record handler timed out after ${ms}ms`));
      }, ms);
    });

    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      clearTimeout(timeoutId!);
    }
  }
}

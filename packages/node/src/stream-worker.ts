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
  silentLogger,
  type ProcessEntry,
  type StreamID,
  type StreamRecord,
} from "@floruntime/core";
import { FloClient } from "./client.js";
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

  /** Block timeout for reads in milliseconds (default: 30000) */
  blockMs?: number;

  /** Maximum duration for a message handler in milliseconds (default: 300000 = 5 minutes) */
  messageTimeoutMs?: number;

  /** Heartbeat interval in milliseconds (default: 30000). Set to 0 to disable. */
  heartbeatIntervalMs?: number;

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
      blockMs: config.blockMs ?? 30000,
      messageTimeoutMs: config.messageTimeoutMs ?? 300000,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 30000,
    };

    this.handler = handler;

    if (typeof clientOrEndpoint === "string") {
      this.client = new FloClient(clientOrEndpoint, { namespace: "default" });
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

    try {
      // Join consumer group
      await this.client.stream.groupJoin(
        this.config.stream,
        this.config.group,
        this.config.consumer
      );
      this.log("Joined consumer group");

      // Register in worker registry
      const processName = `${this.config.stream}/${this.config.group}`;
      const processes: ProcessEntry[] = [
        { name: processName, kind: ProcessKind.StreamConsumer },
      ];
      const metadata = JSON.stringify({
        stream: this.config.stream,
        group: this.config.group,
        consumer: this.config.consumer,
      });

      await this.client.worker.register(this.config.workerId, [], {
        workerType: WorkerType.Stream,
        maxConcurrency: this.config.concurrency,
        processes,
        metadata,
        machineId: this.config.machineId || undefined,
      });
      this.log("Registered in worker registry");

      // Initialize state
      this.running = true;
      this.stopRequested = false;

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

  private async pollLoop(): Promise<void> {
    while (this.running && !this.stopRequested) {
      try {
        // Check concurrency limit
        if (this.activeCount >= this.config.concurrency) {
          await this.sleep(100);
          continue;
        }

        if (this.stopRequested) break;

        // Read batch from consumer group
        const result = await this.client!.stream.groupRead(this.config.stream, {
          group: this.config.group,
          consumer: this.config.consumer,
          limit: this.config.batchSize,
          blockMs: this.config.blockMs,
        });

        if (!result || result.records.length === 0) {
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
        if (!this.stopRequested) {
          this.log(`GroupRead error: ${err}, retrying...`);
          await this.sleep(1000);
        }
      }
    }
  }

  private async processRecord(record: StreamRecord): Promise<void> {
    const ctx = new StreamContext(
      record,
      this.client!.namespace(),
      this.config.stream,
      this.config.group,
      this.config.consumer
    );

    try {
      // Execute handler with timeout
      await this.withTimeout(this.handler(ctx), this.config.messageTimeoutMs);

      // Auto-ack on success
      await this.client!.stream.groupAck(this.config.stream, [record.id], {
        group: this.config.group,
        consumer: this.config.consumer,
      });
    } catch (err) {
      this.log(`Record ${record.id} failed: ${err}`);

      // Auto-nack on failure
      try {
        await this.client!.stream.groupNack(this.config.stream, [record.id], {
          group: this.config.group,
          consumer: this.config.consumer,
        });
      } catch (nackErr) {
        this.log(`Failed to nack record ${record.id}: ${nackErr}`);
      }
    }
  }

  /**
   * Signal the stream worker to stop.
   */
  stop(): void {
    this.log("Stopping stream worker...");
    this.running = false;
    this.stopRequested = true;
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

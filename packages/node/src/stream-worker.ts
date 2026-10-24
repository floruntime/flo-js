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
  ServerError,
  StatusCode,
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
import { EmptyPollBackoff, MAX_TIMER_MS, newWorkerBackoff, pause } from "./poll-backoff.js";
import {
  DRAIN_NOTICE_INTERVAL_MS,
  closedSince,
  isConnectionError,
  nextTaskOrTick,
  reconnect,
  sendDrainNotice,
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

  /**
   * Heartbeat interval in milliseconds (default: 30000); must be more than
   * 0. Keep it under the server's heartbeat timeout.
   */
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
  private readonly client: FloClient;
  private readonly ownedClient: boolean;
  private readonly handler: StreamRecordHandler;
  private running = false;
  // A run's stop and drain are cleared when it ends, not when it begins, so
  // one that lands before or during startup still ends that run.
  private stopRequested = false;
  private stopSignal = new AbortController();
  /** Aborted by stop() or a drain: the poll loop leaves, and its pauses and reconnects end. */
  private leave = new AbortController();
  /** client.closes() at start(): once it moves, the owner closed the client. */
  private clientCloses = 0;
  /** Handlers running: the load heartbeats report. */
  private activeCount = 0;
  /** Set by drain() or a Draining heartbeat. */
  private draining = false;
  /** The server is to be told of the drain: at its start, and after any re-registration. */
  private noticeDue = false;
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

    const heartbeatIntervalMs = config.heartbeatIntervalMs ?? 30000;
    // Node runs a longer setInterval every 1 ms.
    if (!(heartbeatIntervalMs > 0 && heartbeatIntervalMs <= MAX_TIMER_MS)) {
      throw new RangeError(`heartbeatIntervalMs must be more than 0 and at most ${MAX_TIMER_MS}, not ${heartbeatIntervalMs}`);
    }

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
      heartbeatIntervalMs,
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
   * Start consuming stream records until `stop()`, or a drain once its
   * running handlers finish and ack. Either one before or during startup
   * ends this run. It may be called again afterwards.
   */
  async start(): Promise<void> {
    this.log(
      `Starting stream worker (id=${this.config.workerId}, stream=${this.config.stream}, ` +
        `group=${this.config.group}, consumer=${this.config.consumer})`
    );

    this.running = true;
    let joined = false;
    try {
      // Connect if we own the client: on a later run, again after close().
      if (this.ownedClient && !this.client.isConnected()) {
        await this.client.connect();
      }
      this.clientCloses = this.client.closes();

      if (!this.stopRequested) {
        // Left and deregistered at the end even if this answer is lost.
        joined = true;
        if (await this.joinGroup()) {
          this.startHeartbeat();
          await this.pollLoop();
          if (this.draining && !this.stopRequested) await this.finishDrain();
        }
      }
    } finally {
      // Wait for in-flight tasks; heartbeats go on meanwhile.
      if (this.pendingTasks.size > 0) {
        this.log(`Waiting for ${this.pendingTasks.size} records to complete...`);
        await Promise.allSettled(this.pendingTasks);
      }
      this.stopHeartbeat();
      // Before deregistering: a heartbeat answering after it mustn't register again.
      this.running = false;

      if (joined) {
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
      if (this.ownedClient) {
        await this.client.close();
      }

      this.endRun();
      this.log("Stream worker stopped");
    }
  }

  /** Clear the run's stop and drain, for the next run. */
  private endRun(): void {
    this.stopRequested = false;
    this.draining = false;
    this.noticeDue = false;
    this.stopSignal = new AbortController();
    this.leave = new AbortController();
  }

  /** Whether a stop or drain has ended polling. */
  private leaving(): boolean {
    return this.stopRequested || this.draining;
  }

  /**
   * Join the consumer group and register this worker with the server. A
   * join the server refused without running it (overloaded or unavailable,
   * past the client's own retries) is tried again with the worker backoff;
   * any other error is thrown. Returns false if a stop or drain came first.
   */
  private async joinGroup(): Promise<boolean> {
    const failed = newWorkerBackoff();
    for (;;) {
      if (this.leaving()) return false;
      try {
        await this.client.stream.groupJoin(this.config.stream, this.config.group, this.config.consumer);
        break;
      } catch (err) {
        if (!isRefusedUnrun(err)) throw err;
        const d = failed.next();
        this.log(`Group join refused: ${err}, retrying in ${d}ms...`);
        await pause(d, this.leave.signal);
      }
    }
    this.log("Joined consumer group");
    await this.registerWorker();
    return true;
  }

  /** Register this worker, with the same options each time. */
  private async registerWorker(): Promise<void> {
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
  }

  /** Reconnect after a dropped connection, then join again. */
  private async recover(cause: unknown, connection: number): Promise<void> {
    this.logger.warn(`Connection lost: ${cause}; reconnecting...`);
    const seen = { connection, closes: this.clientCloses };
    if (!(await reconnect(this.client, this.leave.signal, this.logger, seen))) {
      if (closedSince(this.client, seen) && !this.stopRequested) {
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
    while (!this.leaving()) {
      // The connection this iteration's poll goes on, to redial only it.
      const connection = this.client.connection();
      try {
        // Check concurrency limit
        if (this.activeCount >= this.config.concurrency) {
          await this.sleep(100);
          continue;
        }

        if (this.leaving()) break;

        // Read batch from consumer group
        const started = performance.now();
        const result = await this.client.stream.groupRead(this.config.stream, {
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
          if (wait > 0) await pause(wait, this.leave.signal);
          continue;
        }

        // Process each record with concurrency control. A drain handles the
        // whole batch, already delivered here; a stop hands back the rest.
        let dispatched = 0;
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
          dispatched++;
        }
        if (dispatched < result.records.length) await this.handBack(result.records.slice(dispatched));
      } catch (err) {
        if (this.leaving()) break;
        if (isConnectionError(err)) {
          // A server that takes the connection and drops it again is not
          // redialed at once each time: until a read succeeds after a
          // reconnect, each one waits first.
          if (unproven) await pause(failed.next(), this.leave.signal);
          if (this.leaving()) break;
          await this.recover(err, connection);
          unproven = true;
        } else {
          const d = failed.next();
          this.log(`GroupRead error: ${err}, retrying in ${d}ms...`);
          await pause(d, this.leave.signal);
        }
      }
    }
  }

  /**
   * Nack records delivered here that a stop kept from starting, so the group
   * hands them out again now rather than after its ack timeout.
   */
  private async handBack(records: StreamRecord[]): Promise<void> {
    const opts = { group: this.config.group, consumer: this.config.consumer };
    await sendResult(this.client, this.leave.signal, this.logger, this.clientCloses, `nack of ${records.length} records not started`, () =>
      this.client.stream.groupNack(this.config.stream, records.map((r) => r.id), opts)
    );
  }

  private async processRecord(record: StreamRecord): Promise<void> {
    const client = this.client;
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
    await sendResult(client, this.leave.signal, this.logger, this.clientCloses, `${what} for record ${id}`, () =>
      acked
        ? client.stream.groupAck(this.config.stream, [record.id], opts)
        : client.stream.groupNack(this.config.stream, [record.id], opts)
    );
  }

  /**
   * Signal the stream worker to stop: polling ends, also mid-reconnect or
   * mid-drain, once an outstanding read returns (within blockMs), and
   * records of the last batch not yet started are handed back. Running
   * handlers finish and ack before `start()` returns. A stop before
   * `start()` ends that run at once.
   */
  stop(): void {
    this.log("Stopping stream worker...");
    this.stopRequested = true;
    this.stopSignal.abort();
    this.leave.abort();
  }

  /**
   * Stop and close the stream worker.
   */
  async close(): Promise<void> {
    this.stop();
    if (this.ownedClient) {
      await this.client.close();
    }
  }

  /**
   * Stop issuing reads and let the running handlers finish and ack;
   * `start()` then returns. An outstanding read can still wait up to
   * `blockMs`: the server doesn't wake a group read on a drain, and the
   * worker keeps its one connection. The server is told, so the worker list
   * shows it draining, and heartbeats go on meanwhile; a notice that can't
   * be sent now is tried again, and a failed one is logged, not thrown. A drain before `start()`
   * ends that run.
   */
  async drain(): Promise<void> {
    this.log("Draining stream worker...");
    this.beginDrain();
    if (this.running) await this.sendNotice();
  }

  private beginDrain(): void {
    if (!this.draining) this.noticeDue = true;
    this.draining = true;
    this.leave.abort();
  }

  /**
   * Wait for the running handlers, keeping the server told of the drain: a
   * notice that can't be sent is tried again every DRAIN_NOTICE_INTERVAL_MS
   * while handlers run, its connection redialed; an idle worker tries once.
   * A stop ends the wait; `start()` still waits for the handlers.
   */
  private async finishDrain(): Promise<void> {
    for (;;) {
      if (this.noticeDue) await this.sendNotice();
      if (this.pendingTasks.size === 0 || this.stopRequested) return;
      await nextTaskOrTick(this.pendingTasks, DRAIN_NOTICE_INTERVAL_MS, this.stopSignal.signal);
    }
  }

  private async sendNotice(): Promise<void> {
    if (await sendDrainNotice(this.client, this.config.workerId, this.logger, this.clientCloses, this.stopSignal.signal)) {
      this.noticeDue = false;
    }
  }

  /**
   * Send a heartbeat; drain on a Draining answer. A worker silent past the
   * server's removal time is removed, and its heartbeat answers not found:
   * it registers again, and a draining one tells the server again, since
   * registering marks it active.
   */
  private async heartbeat(): Promise<void> {
    let status: WorkerStatus;
    try {
      status = await this.client.worker.heartbeat(this.config.workerId, this.activeCount);
    } catch (err) {
      if (!(err instanceof ServerError && err.status === StatusCode.NotFound)) throw err;
      // A heartbeat out when the run ended answers after the worker
      // deregistered: registering then would leave one the server keeps till
      // it expires.
      if (!this.running) return;
      this.logger.warn("The server removed this worker; registering again");
      await this.registerWorker();
      if (this.draining) {
        this.noticeDue = true;
        await this.sendNotice();
      }
      return;
    }
    if (status === WorkerStatus.Draining && !this.draining) {
      this.log("The server drained this worker; finishing its running work");
      this.beginDrain();
    }
  }

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(async () => {
      if (!this.running) return;
      try {
        await this.heartbeat();
      } catch (err) {
        this.logger.warn(`Heartbeat failed: ${err}`);
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

/**
 * High-level StreamWorker API for Flo.
 *
 * Consumes stream records via a consumer group, which leases each record
 * to one consumer until it is acked, with:
 * - The group created at start with the lease and attempts given, if any
 * - Worker registry integration (register, heartbeat, deregister)
 * - Concurrency control
 * - Automatic ack/nack on handler success/failure, and touches that keep
 *   a record's lease while its handler runs
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
  isOutcomePending,
  isRefusedUnrun,
  silentLogger,
  type ProcessEntry,
  type RetryPolicy,
  type StreamGroupDelivery,
  type StreamGroupReadResult,
  type StreamGroupRecord,
  type StreamGroupReportResult,
  type StreamID,
  MAX_GROUP_BATCH,
  QueueRefusal,
  workerBlockMs,
} from "@floruntime/core";
import { FloClient } from "./client.js";
import { EmptyPollBackoff, HaltBackoff, LimitBackoff, MAX_TIMER_MS, NotReadyWarnings, newNotReadyBackoff, newWorkerBackoff, pause } from "./poll-backoff.js";
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

/** Touches never go more often than this, whatever the lease. */
const MIN_TOUCH_INTERVAL_MS = 100;
/** A group's lease bounds: the touch schedule assumes one within them. */
const MIN_LEASE_MS = 5000;
const MAX_LEASE_MS = 43_200_000;
/** Touch refusals that mean touching again can't help. */
const TOUCH_FINAL: StatusCode[] = [StatusCode.NotFound, StatusCode.BadRequest];

/** The delivery a report names: `records` of the read `read`. */
function deliveryOf(read: StreamGroupReadResult, records: StreamGroupRecord[]): StreamGroupDelivery {
  return { consumer: read.consumer, term: read.term, index: read.index, records };
}

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

  /**
   * The group's lease in ms, 5000 to 43200000. Given this or `maxDeliver`,
   * the worker creates the group at start with both (one unset is the
   * server's default); a group that exists with other settings makes
   * `start()` throw ConflictError, since a create can't change them. Given
   * neither, the first read creates the group at the server's defaults
   * (30000 ms, 5). A record not acked within the lease is delivered again;
   * the worker touches a record's lease while its handler runs.
   */
  ackTimeoutMs?: number;

  /** The group's attempts, 1 to 255: a record nacked or expired this many times is dead. See `ackTimeoutMs`. */
  maxDeliver?: number;

  /** Unique worker identifier (auto-generated if not provided) */
  workerId?: string;

  /** Machine ID for grouping workers on the same host (auto-detected if not provided) */
  machineId?: string;

  /** Maximum number of concurrent message handlers (default: 10) */
  concurrency?: number;

  /** Number of records to read per poll, 1 to 100 (default: 10) */
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
 * Throw an error to nack: the record is delivered again, and is dead once
 * nacked or expired the group's `maxDeliver` times.
 */
export type StreamRecordHandler = (ctx: StreamContext) => Promise<void>;

/**
 * Context passed to stream record handlers.
 *
 * Provides access to the stream record and metadata,
 * along with helpers for parsing payloads.
 */
export class StreamContext {
  /** The stream record being processed, with its deliveries (1 the first time) */
  readonly record: StreamGroupRecord;

  /** Namespace */
  readonly namespace: string;

  /** Stream name */
  readonly stream: string;

  /** Consumer group name */
  readonly group: string;

  /** Consumer name */
  readonly consumer: string;

  constructor(
    record: StreamGroupRecord,
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
    Omit<StreamWorkerConfig, "logger" | "ackTimeoutMs" | "maxDeliver">
  > & Pick<StreamWorkerConfig, "ackTimeoutMs" | "maxDeliver">;
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

    const batchSize = config.batchSize ?? 10;
    if (!(Number.isInteger(batchSize) && batchSize >= 1 && batchSize <= MAX_GROUP_BATCH)) {
      throw new RangeError(`batchSize must be an integer from 1 to ${MAX_GROUP_BATCH}, not ${batchSize}`);
    }

    this.config = {
      stream: config.stream,
      group: config.group ?? "default",
      consumer: config.consumer ?? workerId,
      ackTimeoutMs: config.ackTimeoutMs,
      maxDeliver: config.maxDeliver,
      workerId,
      machineId: config.machineId ?? (os.hostname() || ""),
      concurrency: config.concurrency ?? 10,
      batchSize,
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
   *
   * start() fails with the limit error; a limit hit while running is
   * logged and backed off.
   */
  async start(): Promise<void> {
    this.log(
      `Starting stream worker (id=${this.config.workerId}, stream=${this.config.stream}, ` +
        `group=${this.config.group}, consumer=${this.config.consumer})`
    );

    this.running = true;
    let registered = false;
    try {
      // Connect if we own the client: on a later run, again after close().
      if (this.ownedClient && !this.client.isConnected()) {
        await this.client.connect();
      }
      this.clientCloses = this.client.closes();

      if (!this.stopRequested) {
        // Deregistered at the end even if this answer is lost.
        registered = true;
        if (await this.setUp()) {
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

      if (registered) {
        try {
          await this.client.worker.deregister(this.config.workerId);
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
   * Create the consumer group, when given its settings, and register this
   * worker with the server. Either one refused by a halted shard, or
   * refused without being run (overloaded or unavailable, past the
   * client's own retries), is tried again with a backoff; any other
   * refusal, a limit included, is thrown as its typed error. Returns false
   * if a stop or drain came first.
   */
  private async setUp(): Promise<boolean> {
    const failed = newWorkerBackoff();
    const halted = new HaltBackoff(this.logger);
    const create = this.config.ackTimeoutMs !== undefined || this.config.maxDeliver !== undefined;
    while (create) {
      if (this.leaving()) return false;
      try {
        await this.client.stream.groupCreate(this.config.stream, this.config.group, {
          ackTimeoutMs: this.config.ackTimeoutMs,
          maxDeliver: this.config.maxDeliver,
        });
        break;
      } catch (err) {
        if (await halted.waitIf(err, "Group create", this.leave.signal)) continue;
        if (!isRefusedUnrun(err)) throw err;
        const d = failed.next();
        this.log(`Group create refused: ${err}, retrying in ${d}ms...`);
        await pause(d, this.leave.signal);
      }
    }
    halted.reset();
    failed.reset();
    for (;;) {
      if (this.leaving()) return false;
      try {
        await this.registerWorker();
        return true;
      } catch (err) {
        if (await halted.waitIf(err, "Register", this.leave.signal)) continue;
        if (!isRefusedUnrun(err)) throw err;
        const d = failed.next();
        this.log(`Register refused: ${err}, retrying in ${d}ms...`);
        await pause(d, this.leave.signal);
      }
    }
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

  /** Reconnect after a dropped connection, then set up again. */
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
    // A limit here leaves the worker unregistered, serving nothing: it
    // keeps trying, and says so at error.
    const limited = new LimitBackoff(this.logger, "error");
    for (;;) {
      try {
        await this.setUp();
        return;
      } catch (err) {
        // Committed but not applied: resending it would write it twice.
        if (isOutcomePending(err)) throw err;
        if (this.leaving()) return;
        if (await limited.waitIf(err, "Set-up after reconnect (the worker is not registered and serves nothing until it is)", this.leave.signal)) continue;
        // A connection error here surfaces again on the next read.
        this.logger.warn(`Set-up after reconnect failed: ${err}`);
        return;
      }
    }
  }

  private async pollLoop(): Promise<void> {
    const backoff = new EmptyPollBackoff();
    const failed = newWorkerBackoff();
    const halted = new HaltBackoff(this.logger);
    const limited = new LimitBackoff(this.logger);
    const notReady = newNotReadyBackoff();
    const notReadyWarnings = new NotReadyWarnings();
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
        const arrivedAt = performance.now();
        failed.reset();
        halted.reset();
        limited.reset();
        notReady.reset();
        notReadyWarnings.reset();
        unproven = false;

        const empty = result.records.length === 0;
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
          const taskPromise = this.processRecord(record, result, arrivedAt).finally(() => {
            this.activeCount--;
            this.pendingTasks.delete(taskPromise);
          });
          this.pendingTasks.add(taskPromise);
          dispatched++;
        }
        if (dispatched < result.records.length) await this.handBack(result, result.records.slice(dispatched));
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
        } else if (isOutcomePending(err)) {
          throw err;
        } else if (isRefusedUnrun(err)) {
          // Routine, not a failure: records being read back from disk, or a
          // leader not ready yet. The read leased nothing, so the same read
          // again is safe. A damaged or offline shard refuses
          // the same way and won't answer soon: the server's message says
          // which, so the first of a run is a warning, then one every 30 s.
          const d = notReady.next();
          const msg = `GroupRead on stream ${this.config.stream} not ready: ${(err as ServerError).serverMessage}; retrying in ${d}ms`;
          if (notReadyWarnings.due()) this.logger.warn(msg);
          else this.log(msg);
          await pause(d, this.leave.signal);
        } else if (await limited.waitIf(err, "GroupRead", this.leave.signal)) {
          // Read again once the wait is over.
        } else if (!(await halted.waitIf(err, "GroupRead", this.leave.signal))) {
          const d = failed.next();
          this.log(`GroupRead error: ${err}, retrying in ${d}ms...`);
          await pause(d, this.leave.signal);
        }
      }
    }
  }

  /**
   * Nack records delivered here that a stop kept from starting, so the group
   * hands them out again now rather than once their lease runs out. Like
   * the lease running out, the nack counts as one of their attempts.
   */
  private async handBack(read: StreamGroupReadResult, records: StreamGroupRecord[]): Promise<void> {
    const delivery = deliveryOf(read, records);
    await sendResult(this.client, this.leave.signal, this.logger, this.clientCloses, `nack of ${records.length} records not started`, async () =>
      this.noteRefused("nack", await this.client.stream.groupNack(this.config.stream, delivery, { group: this.config.group }))
    );
  }

  private async processRecord(record: StreamGroupRecord, read: StreamGroupReadResult, arrivedAt: number): Promise<void> {
    const client = this.client;
    const ctx = new StreamContext(
      record,
      client.namespace(),
      this.config.stream,
      this.config.group,
      this.config.consumer
    );
    const id = `${record.id.timestampMs}-${record.id.sequence}`;
    const delivery = deliveryOf(read, [record]);
    const opts = { group: this.config.group };

    // The handler's outcome alone picks ack or nack; sending it is separate,
    // so a lost ack is resent, never turned into a nack.
    let acked = true;
    const stopTouching = this.keepTouching(delivery, read.expiresAtMs, arrivedAt, id);
    try {
      await this.withTimeout(this.handler(ctx), this.config.messageTimeoutMs);
    } catch (err) {
      this.log(`Record ${id} failed: ${err}`);
      acked = false;
    } finally {
      stopTouching();
    }

    const what = acked ? "ack" : "nack";
    await sendResult(client, this.leave.signal, this.logger, this.clientCloses, `${what} for record ${id}`, async () =>
      this.noteRefused(
        what,
        acked ? await client.stream.groupAck(this.config.stream, delivery, opts) : await client.stream.groupNack(this.config.stream, delivery, opts)
      )
    );
  }

  /**
   * Log the records a report didn't apply to: its delivery was over (the
   * lease ran out, or the record went to another read). A resent ack whose
   * first applied finds its record gone, so gone after an ack is quiet.
   */
  private noteRefused(what: string, result: StreamGroupReportResult): void {
    for (const r of result.refused) {
      const msg = `${what} of record ${r.id.timestampMs}-${r.id.sequence} applied nothing: ${r.reason}`;
      if (what === "ack" && r.why === QueueRefusal.Gone) this.log(msg);
      else this.logger.warn(msg);
    }
  }

  /**
   * Touch a record's lease until the returned function is called, every
   * sixth of the lease (at least 100 ms apart), counted from when the read
   * arrived. The server extends a lease only with half of it or less left,
   * so the touch at a half extends it and the two after it can fail before
   * it runs out. The lease is the read's expiry less the time it arrived,
   * by this host's clock. A touch that applies to nothing (the delivery is
   * over), or is refused not found or bad request, stops touching; any
   * other failure is tried again at the next tick.
   */
  private keepTouching(delivery: StreamGroupDelivery, expiresAtMs: bigint, arrivedAt: number, id: string): () => void {
    const leaseMs = Math.min(Math.max(Number(expiresAtMs) - (Date.now() - (performance.now() - arrivedAt)), MIN_LEASE_MS), MAX_LEASE_MS);
    const intervalMs = Math.max(leaseMs / 6, MIN_TOUCH_INTERVAL_MS);
    let stopped = false;
    let ticks = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // The next tick on the schedule from arrival, skipping any a slow touch overran.
    const schedule = () => {
      const now = performance.now();
      ticks = Math.max(ticks + 1, Math.floor((now - arrivedAt) / intervalMs) + 1);
      timer = setTimeout(tick, Math.max(0, arrivedAt + ticks * intervalMs - now));
    };
    const tick = async (): Promise<void> => {
      if (stopped) return;
      try {
        const got = await this.client.stream.groupTouch(this.config.stream, delivery, { group: this.config.group });
        if (got.applied === 0) {
          this.logger.warn(`Touch for record ${id} applied nothing, stopping: ${got.refused[0]?.reason ?? "no answer"}`);
          return;
        }
      } catch (err) {
        if (err instanceof ServerError && TOUCH_FINAL.includes(err.status)) {
          this.logger.warn(`Touch for record ${id} was refused, stopping: ${err}`);
          return;
        }
        this.logger.warn(`Touch for record ${id} failed: ${err}; trying again`);
      }
      if (!stopped) schedule();
    };
    schedule();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
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
      // Only worker.register is sent again: the server keeps workers in
      // memory, so it can't halt a shard or answer outcome_pending.
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

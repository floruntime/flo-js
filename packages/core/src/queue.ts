/**
 * Queue operations for Flo SDK.
 */

import { createServerError, FloError, IncompleteResponseError } from "./errors.js";
import {
  decodeDequeueAnswer,
  decodeQueueDlqAnswer,
  decodeQueueDlqListAnswer,
  encodeQueueDlqBody,
  encodeQueueDlqListBody,
  decodePeekAnswer,
  decodeQueueReportAnswer,
  decodeQueueTouchAnswer,
  encodeDequeueBody,
  encodePeekBody,
  encodeQueueFailBody,
  encodeQueueReportBody,
  MAX_DELAY_MS,
} from "./kv_body.js";
import {
  type DequeueOptions,
  type DequeueResult,
  type DLQDeleteOptions,
  type DLQListOptions,
  type DLQListResult,
  type DLQSelectOptions,
  type QueueDLQResult,
  type EnqueueOptions,
  type PeekOptions,
  type PeekResult,
  type QueueDelivery,
  type QueueFailOptions,
  type QueueReportOptions,
  type QueueReportResult,
  type QueueTouchResult,
  OpCode,
  OptionTag,
  type RawResponse,
  StatusCode,
} from "./types.js";
import {
  OptionsBuilder,
  parseEnqueueResponse,
} from "./wire.js";

/**
 * Interface for sending requests (implemented by client).
 */
export interface RequestSender {
  sendRequest(
    opCode: OpCode,
    namespace: string,
    key: Uint8Array,
    value: Uint8Array,
    options: Uint8Array
  ): Promise<RawResponse>;

  getNamespace(override?: string): string;
}

const textEncoder = new TextEncoder();

/**
 * Queue client operations.
 */
export class QueueOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Enqueue adds a message to a queue, ready at once or after `delayMs`.
   * Returns the sequence number of the enqueued message.
   *
   * A queue past its cap (messages or held bytes, ready, leased and dead),
   * or a shard whose queues and groups together hold as many bytes as they
   * may, refuses it overloaded with reason QueueFull, not run. It drains as
   * messages are acked, so the client retries it as it retries any
   * overloaded refusal (about 350 ms by default), then throws
   * OverloadedError (`isQueueFull`): back off and send again, or consume
   * or purge the queue.
   */
  async enqueue(
    queue: string,
    payload: Uint8Array,
    opts?: EnqueueOptions
  ): Promise<bigint> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.priority !== undefined && opts.priority !== 0) {
      builder.addU8(OptionTag.Priority, opts.priority);
    }
    const delayMs = opts?.delayMs ?? 0;
    if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > MAX_DELAY_MS) {
      throw new FloError(`flo: delayMs must be a whole number of ms from 0 to ${MAX_DELAY_MS}, got ${delayMs}`);
    }
    if (delayMs > 0) builder.addU64(OptionTag.DelayMS, BigInt(delayMs));

    const resp = await this.sender.sendRequest(
      OpCode.QueueEnqueue,
      namespace,
      textEncoder.encode(queue),
      payload,
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseEnqueueResponse(resp.data);
  }

  /**
   * Take up to `count` (1 to 100) of the queue's next ready messages, leased
   * to `consumer` for `leaseMs` (the server's 30 s when unset). A dequeue
   * doesn't remove them: complete each within its lease, or fail it, or it
   * is delivered again once the lease runs out, with `deliveries` one
   * higher. Touch a lease to keep it. With `ack`, they are completed as
   * they're taken. Waits up to `blockMs` when nothing is ready; an empty
   * result has no messages.
   */
  async dequeue(queue: string, count = 1, opts?: DequeueOptions): Promise<DequeueResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueueDequeue, namespace, textEncoder.encode(queue), encodeDequeueBody(count, opts), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeDequeueAnswer(resp.data, opts?.consumer ?? "");
  }

  /**
   * Complete dequeued messages: they leave the queue. Pass the dequeue's
   * result, or it with `messages` narrowed. Messages the report didn't
   * apply to come back in `refused`, inside an ok answer; one refused
   * `QueueRefusal.Gone` was completed already (a resent complete), so it is
   * done.
   */
  async complete(queue: string, delivery: QueueDelivery, opts?: QueueReportOptions): Promise<QueueReportResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueueComplete, namespace, textEncoder.encode(queue), encodeQueueReportBody(delivery), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeQueueReportAnswer(resp.data);
  }

  /**
   * Fail dequeued messages: ready again after `delayMs`, or dead-lettered
   * once their attempts are spent, or at once with `dead`. Messages it
   * didn't apply to come back in `refused`.
   */
  async fail(queue: string, delivery: QueueDelivery, opts?: QueueFailOptions): Promise<QueueReportResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueueFail, namespace, textEncoder.encode(queue), encodeQueueFailBody(delivery, opts), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeQueueReportAnswer(resp.data);
  }

  /**
   * Keep dequeued messages: with half their lease or less left, it runs a
   * full lease from now, up to 12 h from their dequeue; with more left,
   * nothing changes. Touch at least every half lease.
   */
  async touch(queue: string, delivery: QueueDelivery, opts?: QueueReportOptions): Promise<QueueTouchResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueueTouch, namespace, textEncoder.encode(queue), encodeQueueReportBody(delivery), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeQueueTouchAnswer(resp.data);
  }

  /**
   * Remove every message from the queue, ready, leased and dead. Returns
   * how many it removed.
   */
  async purge(queue: string, opts?: QueueReportOptions): Promise<number> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueuePurge, namespace, textEncoder.encode(queue), new Uint8Array(0), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length !== 4) throw new IncompleteResponseError(`purge answer of ${resp.data.length} bytes`);
    return new DataView(resp.data.buffer, resp.data.byteOffset, 4).getUint32(0, true);
  }

  /**
   * A page of the queue's dead letters, oldest dead first: up to `limit`
   * (1 to 100, 20 by default), each with its failures, when it died, its
   * last error and up to 4 KiB of its payload. Read on the leader. Pass the
   * answer's `cursor` for the next page; it is null on the last.
   */
  async dlqList(queue: string, opts?: DLQListOptions): Promise<DLQListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueueDLQList, namespace, textEncoder.encode(queue), encodeQueueDlqListBody(opts), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeQueueDlqListAnswer(resp.data);
  }

  /**
   * Make dead letters ready again at once, at their priority, with their
   * failures reset to 0 (deliveries and last error stay): those named by
   * seq, or with `all` the oldest, up to `limit` per call, `more` saying
   * whether others remain. One named that isn't dead comes back in
   * `refused` as NotDead; one not in the queue as Gone.
   */
  async dlqRequeue(queue: string, seqs: readonly bigint[], opts?: DLQSelectOptions): Promise<QueueDLQResult> {
    return this.dead(OpCode.QueueDLQRequeue, queue, encodeQueueDlqBody(seqs, opts, false), opts?.namespace);
  }

  /**
   * Delete dead letters for good: those named by seq, or with `all` the
   * oldest, up to `limit` per call, `more` saying whether others remain.
   * With `dryRun`, `applied` is how many it would delete, and nothing is
   * deleted. One named that isn't dead comes back in `refused` as NotDead.
   */
  async dlqDelete(queue: string, seqs: readonly bigint[], opts?: DLQDeleteOptions): Promise<QueueDLQResult> {
    return this.dead(OpCode.QueueDLQDelete, queue, encodeQueueDlqBody(seqs, opts, true), opts?.namespace);
  }

  private async dead(op: OpCode, queue: string, body: Uint8Array, ns?: string): Promise<QueueDLQResult> {
    const namespace = this.sender.getNamespace(ns);
    const resp = await this.sender.sendRequest(op, namespace, textEncoder.encode(queue), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeQueueDlqAnswer(resp.data);
  }

  /**
   * The queue's next ready messages, in the order a dequeue takes them,
   * without taking them; each shows up to 4 KiB of its payload. Read on the
   * leader.
   *
   * @param queue - Queue name
   * @param limit - Most messages to show, 1 to 100 (default 20)
   * @param opts - Options
   */
  async peek(queue: string, limit = 20, opts?: PeekOptions): Promise<PeekResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.QueuePeek, namespace, textEncoder.encode(queue), encodePeekBody(limit), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodePeekAnswer(resp.data);
  }
}

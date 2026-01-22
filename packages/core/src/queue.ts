/**
 * Queue operations for Flo SDK.
 */

import { createServerError } from "./errors.js";
import {
  type AckOptions,
  type DequeueOptions,
  type DequeueResult,
  type DLQListOptions,
  type DLQRequeueOptions,
  type EnqueueOptions,
  type NackOptions,
  type PeekOptions,
  type TouchOptions,
  OpCode,
  OptionTag,
  type RawResponse,
  StatusCode,
} from "./types.js";
import {
  OptionsBuilder,
  parseDequeueResponse,
  parseEnqueueResponse,
  serializeSeqs,
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
   * Enqueue adds a message to a queue.
   * Returns the sequence number of the enqueued message.
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

    if (opts?.delayMs !== undefined) {
      builder.addU64(OptionTag.DelayMS, opts.delayMs);
    }

    if (opts?.dedupKey !== undefined && opts.dedupKey !== "") {
      builder.addBytes(OptionTag.DedupKey, textEncoder.encode(opts.dedupKey));
    }

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
   * Dequeue fetches messages from a queue.
   */
  async dequeue(
    queue: string,
    count: number,
    opts?: DequeueOptions
  ): Promise<DequeueResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();
    builder.addU32(OptionTag.Count, count);

    if (opts?.visibilityTimeoutMs !== undefined) {
      builder.addU32(OptionTag.VisibilityTimeoutMS, opts.visibilityTimeoutMs);
    }

    if (opts?.blockMs !== undefined) {
      builder.addU32(OptionTag.BlockMS, opts.blockMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.QueueDequeue,
      namespace,
      textEncoder.encode(queue),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseDequeueResponse(resp.data);
  }

  /**
   * Ack acknowledges messages as successfully processed.
   */
  async ack(queue: string, seqs: bigint[], opts?: AckOptions): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts?.namespace);
    const value = serializeSeqs(seqs);

    const resp = await this.sender.sendRequest(
      OpCode.QueueComplete,
      namespace,
      textEncoder.encode(queue),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Nack negative acknowledges messages (retry or send to DLQ).
   */
  async nack(queue: string, seqs: bigint[], opts?: NackOptions): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.toDlq) {
      builder.addU8(OptionTag.SendToDLQ, 1);
    }

    const value = serializeSeqs(seqs);

    const resp = await this.sender.sendRequest(
      OpCode.QueueFail,
      namespace,
      textEncoder.encode(queue),
      value,
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * DLQList lists messages in the Dead Letter Queue.
   */
  async dlqList(queue: string, opts?: DLQListOptions): Promise<DequeueResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const limit = opts?.limit ?? 100;

    const builder = new OptionsBuilder();
    builder.addU32(OptionTag.Limit, limit);

    const resp = await this.sender.sendRequest(
      OpCode.QueueDLQList,
      namespace,
      textEncoder.encode(queue),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseDequeueResponse(resp.data);
  }

  /**
   * DLQRequeue moves messages from DLQ back to the main queue.
   */
  async dlqRequeue(
    queue: string,
    seqs: bigint[],
    opts?: DLQRequeueOptions
  ): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts?.namespace);
    const value = serializeSeqs(seqs);

    const resp = await this.sender.sendRequest(
      OpCode.QueueDLQRequeue,
      namespace,
      textEncoder.encode(queue),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Peek reads messages from a queue without consuming them.
   * Unlike dequeue, peek does not create leases or affect message visibility.
   * Useful for inspecting queue contents or debugging.
   *
   * @param queue - Queue name
   * @param count - Maximum number of messages to peek
   * @param opts - Options
   */
  async peek(
    queue: string,
    count: number,
    opts?: PeekOptions
  ): Promise<DequeueResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();
    builder.addU32(OptionTag.Count, count);

    const resp = await this.sender.sendRequest(
      OpCode.QueuePeek,
      namespace,
      textEncoder.encode(queue),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseDequeueResponse(resp.data);
  }

  /**
   * Touch extends the visibility timeout (lease) for in-flight messages.
   * Call this to prevent messages from being redelivered while still processing.
   * Also known as "renew lease" or "extend lease".
   *
   * @param queue - Queue name
   * @param seqs - Sequence numbers of messages to touch
   * @param opts - Options
   */
  async touch(
    queue: string,
    seqs: bigint[],
    opts?: TouchOptions
  ): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts?.namespace);
    const value = serializeSeqs(seqs);

    const resp = await this.sender.sendRequest(
      OpCode.QueueTouch,
      namespace,
      textEncoder.encode(queue),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }
}

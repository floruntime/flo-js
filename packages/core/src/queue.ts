/**
 * Queue operations for Flo SDK.
 */

import { createServerError, FloError } from "./errors.js";
import { AnswerReader, decodePeekAnswer, encodePeekBody } from "./kv_body.js";
import {
  type AckOptions,
  type DequeueOptions,
  type DequeueResult,
  type DeadLetter,
  type DLQListOptions,
  type DLQListResult,
  type DLQRequeueOptions,
  type EnqueueOptions,
  type NackOptions,
  type PeekOptions,
  type PeekResult,
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
   * Nack negatively acknowledges messages. Queues are at-most-once today:
   * a dequeue consumes its messages, so a nack doesn't redeliver them
   * (tracked in floruntime/flo#179).
   */
  async nack(queue: string, seqs: bigint[], opts?: NackOptions): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts?.namespace);
    const value = serializeSeqs(seqs);

    const resp = await this.sender.sendRequest(
      OpCode.QueueFail,
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
   * A page of the queue's dead letters, oldest first: up to `limit` (100
   * at most), each with up to 4 KiB of its payload. For the next page, pass
   * the answer's `lastSeq` as `after` while `hasMore` is set.
   */
  async dlqList(queue: string, opts?: DLQListOptions): Promise<DLQListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.QueueDLQList,
      namespace,
      textEncoder.encode(queue),
      serializeDlqListValue(opts?.limit ?? MAX_DLQ_PAGE, opts?.after),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return decodeDlqListAnswer(resp.data);
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

/** The most dead letters one dlqList page holds. */
export const MAX_DLQ_PAGE = 100;

/** queue.dlq_list's value: [limit:u32][after_seq:u64], the seq left out for the first page. */
export function serializeDlqListValue(limit: number, after?: bigint): Uint8Array {
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_DLQ_PAGE) {
    throw new FloError(`flo: limit must be a whole number from 1 to ${MAX_DLQ_PAGE}, got ${limit}`);
  }
  if (after !== undefined && (after < 0n || after >= 1n << 64n)) throw new FloError(`flo: after must fit a u64, got ${after}`);
  const buf = new Uint8Array(after === undefined ? 4 : 12);
  const view = new DataView(buf.buffer);
  view.setUint32(0, limit, true);
  if (after !== undefined) view.setBigUint64(4, after, true);
  return buf;
}

/**
 * queue.dlq_list's answer: [count:u32]([seq:u64][size:u32][len:u32][payload][enqueued_at_ms:i64]
 * [attempts:u32][priority:u8][moved_at_ms:i64])*[total:u64][has_more:u8][last_seq:u64].
 */
export function decodeDlqListAnswer(data: Uint8Array): DLQListResult {
  const r = new AnswerReader(data);
  const messages: DeadLetter[] = [];
  for (let n = r.u32(); n > 0; n--) {
    const seq = r.u64();
    const size = r.u32();
    const payload = r.bytes32();
    const enqueuedAtMs = r.i64();
    const attempts = r.u32();
    const priority = r.u8();
    const movedAtMs = r.i64();
    messages.push({ seq, size, payload, enqueuedAtMs, attempts, priority, movedAtMs });
  }
  const total = r.u64();
  const hasMore = r.bool();
  const lastSeq = r.u64();
  r.end();
  return { messages, total, hasMore, lastSeq };
}

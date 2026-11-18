/**
 * Stream operations for Flo SDK.
 *
 * Append, read and consume streams through consumer groups.
 */

import { createReadError, createServerError, FloError, IncompleteResponseError } from "./errors.js";
import {
  OpCode,
  OptionTag,
  type RawResponse,
  type ScanResult,
  StatusCode,
  StorageTier,
  StreamID,
  type StreamAppendOptions,
  type StreamAppendResult,
  type StreamReadOptions,
  type StreamReadResult,
  type StreamRecord,
  type StreamInfoResult,
  type StreamGroupOptions,
  type StreamGroupCreateOptions,
  type StreamGroupDelivery,
  type StreamGroupNackOptions,
  type StreamGroupReadResult,
  type StreamGroupRecord,
  type StreamGroupReportOptions,
  type StreamGroupReportResult,
  type StreamGroupTouchResult,
  type StreamDeleteOptions,
  MAX_GROUP_BATCH,
  MAX_GROUP_NAME,
  QueueRefusal,
  type StreamGroupListResult,
  type StreamGroupMembersResult,
  type StreamGroupStealOptions,
  type StreamGroupStealResult,
  type StreamCreateOptions,
  type StreamAlterOptions,
  type DLQListOptions,
  type DLQSelectOptions,
  type DLQDeleteOptions,
  type GroupDLQListResult,
  type GroupDLQResult,
} from "./types.js";
import {
  decodeGroupDlqAnswer,
  decodeGroupDlqListAnswer,
  decodeGroupListAnswer,
  decodeGroupMembersAnswer,
  decodeGroupStealAnswer,
  encodeGroupDlqBody,
  encodeGroupDlqListBody,
  encodeGroupMembersBody,
  encodeGroupStealBody,
  MAX_CONSUMER,
  MAX_DELAY_MS,
} from "./kv_body.js";
import { kvGet, kvScan } from "./kv.js";
import { OptionsBuilder, serializeStreamAppendValue } from "./wire.js";

/**
 * Interface for sending requests (implemented by client).
 */
export interface StreamRequestSender {
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
const textDecoder = new TextDecoder();

const TIERS: readonly StorageTier[] = [StorageTier.Hot, StorageTier.Warm, StorageTier.Disk];

/** A record's tier byte; one this SDK doesn't know is StorageTier.Unknown, not an error. */
export function storageTier(code: number): StorageTier {
  return TIERS[code] ?? StorageTier.Unknown;
}

/**
 * Parse stream read response into records.
 *
 * Wire format: [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
 *              [key_present:u8][payload_len:u32][payload][header_count:u32])*
 */
export function parseStreamReadResponse(data: Uint8Array): StreamReadResult {
  if (data.length < 4) {
    return { records: [] };
  }
  const count = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(0, true);
  const records: StreamRecord[] = [];
  let offset = 4;
  for (let i = 0; i < count && offset < data.length; i++) {
    try {
      const { record, end } = parseRecordAt(data, offset);
      records.push(record);
      offset = end;
    } catch {
      break;
    }
  }
  return { records };
}

/**
 * One record of a stream read's answer at `offset`, and the offset past
 * it: [sequence:u64][timestamp_ms:i64][tier:u8][partition:u32][key_present:u8]
 * ([key_len:u32][key])?[payload_len:u32][payload][header_count:u32]
 * ([key_len:u32][key][val_len:u32][val])*. Throws on a truncated record.
 */
export function parseRecordAt(data: Uint8Array, offset: number): { record: StreamRecord; end: number } {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const need = (n: number) => {
    if (offset + n > data.length) throw new IncompleteResponseError("stream answer ends inside a record");
  };
  need(8 + 8 + 1 + 4 + 1);
  const sequence = view.getBigUint64(offset, true);
  const timestampMs = view.getBigInt64(offset + 8, true);
  const tier = storageTier(data[offset + 16]!);
  // partition (u32) skipped
  const keyPresent = data[offset + 21];
  offset += 22;
  let streamName = "";
  if (keyPresent !== 0) {
    need(4);
    const keyLen = view.getUint32(offset, true);
    offset += 4;
    need(keyLen);
    streamName = textDecoder.decode(data.subarray(offset, offset + keyLen));
    offset += keyLen;
  }
  need(4);
  const payloadLen = view.getUint32(offset, true);
  offset += 4;
  need(payloadLen);
  const payload = new Uint8Array(data.subarray(offset, offset + payloadLen));
  offset += payloadLen;
  need(4);
  const headerCount = view.getUint32(offset, true);
  offset += 4;
  let headers: Record<string, string> | null = null;
  if (headerCount > 0) {
    headers = {};
    for (let h = 0; h < headerCount; h++) {
      need(4);
      const kLen = view.getUint32(offset, true);
      offset += 4;
      need(kLen);
      const k = textDecoder.decode(data.subarray(offset, offset + kLen));
      offset += kLen;
      need(4);
      const vLen = view.getUint32(offset, true);
      offset += 4;
      need(vLen);
      headers[k] = textDecoder.decode(data.subarray(offset, offset + vLen));
      offset += vLen;
    }
  }
  return {
    record: { id: new StreamID(BigInt(timestampMs < 0n ? 0n : timestampMs), sequence), tier, stream: streamName, payload, headers },
    end: offset,
  };
}

/**
 * Parse stream append response.
 *
 * Wire format: [sequence:u64][timestamp_ms:i64]
 */
export function parseStreamAppendResponse(data: Uint8Array): StreamAppendResult {
  if (data.length < 16) {
    throw new Error("Invalid append response: too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const sequence = view.getBigUint64(0, true);
  const timestampMs = view.getBigInt64(8, true);

  return { id: new StreamID(BigInt(timestampMs < 0n ? 0n : timestampMs), sequence) };
}

/**
 * Parse stream info response.
 *
 * Wire format: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32]
 */
export function parseStreamInfoResponse(data: Uint8Array): StreamInfoResult {
  if (data.length !== 76) throw new IncompleteResponseError(`stream info answer of ${data.length} bytes, not 76`);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return {
    firstId: new StreamID(view.getBigUint64(0, true), view.getBigUint64(8, true)),
    lastId: new StreamID(view.getBigUint64(16, true), view.getBigUint64(24, true)),
    count: view.getBigUint64(32, true),
    bytes: view.getBigUint64(40, true),
    partitionCount: view.getUint32(48, true),
    retentionAgeS: view.getBigUint64(52, true),
    retentionCount: view.getBigUint64(60, true),
    // [retention_bytes:u64] follows: always 0, since retention by bytes isn't supported.
  };
}

/**
 * Stream operations for Flo SDK.
 *
 * Append to streams, read them, and consume them through consumer groups,
 * which lease each record to one consumer until it is acked.
 */
export class StreamOperations {
  constructor(private readonly sender: StreamRequestSender) {}

  /**
   * Append/publish a record to a stream.
   *
   * @param stream - Stream name
   * @param payload - Event payload
   * @param opts - Append options (partition key, partition, etc.)
   */
  async append(
    stream: string,
    payload: Uint8Array,
    opts?: StreamAppendOptions
  ): Promise<StreamAppendResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.partitionKey !== undefined) {
      builder.addString(OptionTag.PartitionKey, opts.partitionKey);
    }

    if (opts?.partition !== undefined) {
      builder.addU32(OptionTag.Partition, opts.partition);
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamAppend,
      namespace,
      textEncoder.encode(stream),
      serializeStreamAppendValue(payload, opts?.headers),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseStreamAppendResponse(resp.data);
  }

  /**
   * Read records from a stream. Without `blockMs`, a stream that isn't
   * there (never created, or deleted) throws NotFoundError, while one that
   * exists with nothing in the window answers no records; with `blockMs`,
   * a read of a stream not created yet waits for its first append. A
   * reader that may start before its stream exists catches NotFoundError
   * and reads again later. A StaleNotFoundError (a NotFoundError) comes
   * from a node that may be behind: the stream may exist but this node
   * hasn't seen it yet, so read again rather than treating it as gone.
   *
   * @param stream - Stream name
   * @param opts - Read options (start, end, tail, limit, blockMs)
   */
  async read(stream: string, opts?: StreamReadOptions): Promise<StreamReadResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    // Tail mode flag
    if (opts?.tail) {
      builder.addFlag(OptionTag.StreamTail);
    }

    // Start StreamID (16 bytes, big-endian)
    if (opts?.start !== undefined) {
      builder.addBytes(OptionTag.StreamStart, opts.start.toBytes());
    }

    // End StreamID (16 bytes, big-endian)
    if (opts?.end !== undefined) {
      builder.addBytes(OptionTag.StreamEnd, opts.end.toBytes());
    }

    // Explicit partition
    if (opts?.partition !== undefined) {
      builder.addU32(OptionTag.Partition, opts.partition);
    }

    if (opts?.limit !== undefined) {
      builder.addU32(OptionTag.Count, opts.limit);
    }

    if (opts?.blockMs !== undefined) {
      builder.addU32(OptionTag.BlockMS, opts.blockMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamRead,
      namespace,
      textEncoder.encode(stream),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createReadError(resp);
    }

    return parseStreamReadResponse(resp.data);
  }

  /**
   * Get stream metadata.
   *
   * @param stream - Stream name
   * @param opts - Optional namespace override
   */
  async info(stream: string, opts?: { namespace?: string }): Promise<StreamInfoResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.StreamInfo,
      namespace,
      textEncoder.encode(stream),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseStreamInfoResponse(resp.data);
  }

  /**
   * Create a stream with settings: `partitions` (1 by default) and
   * retention by age and count (none by default). An append also makes a
   * stream, at the defaults; a create of one an append made gives it these
   * settings. A create of one that exists with the same settings (a retry)
   * succeeds. One that a create or an alter gave other settings is refused
   * `ConflictError` ("the stream exists with other settings"): change them
   * with `alter`. A name whose 64-bit hash is another stream's is refused
   * `ConflictError` too: choose another name.
   */
  async create(stream: string, opts?: StreamCreateOptions): Promise<void> {
    const partitions = opts?.partitions ?? 1;
    if (!Number.isInteger(partitions) || partitions < 1 || partitions > 0xffffffff) {
      throw new FloError(`flo: partitions must be a whole number from 1, got ${partitions}`);
    }
    const value = new Uint8Array(4);
    new DataView(value.buffer).setUint32(0, partitions, true);
    await this.config(OpCode.StreamCreate, stream, value, opts);
  }

  /**
   * Change a stream's retention: each setting given replaces the stream's,
   * and one unset (or 0) is left as it is. It works on a stream an append
   * made as on one a create made. A stream that doesn't exist throws
   * `NotFoundError`. After an alter, a create must name the settings the
   * stream has, or it is refused `ConflictError`.
   */
  async alter(stream: string, opts: StreamAlterOptions): Promise<void> {
    await this.config(OpCode.StreamAlter, stream, new Uint8Array(0), opts);
  }

  private async config(op: OpCode, stream: string, value: Uint8Array, opts?: { namespace?: string; retentionAgeS?: bigint; retentionCount?: bigint }): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const builder = new OptionsBuilder();
    for (const [name, tag, v] of [
      ["retentionAgeS", OptionTag.RetentionAge, opts?.retentionAgeS],
      ["retentionCount", OptionTag.RetentionCount, opts?.retentionCount],
    ] as const) {
      if (v === undefined) continue;
      if (v < 0n || v >= 1n << 64n) throw new FloError(`flo: ${name} must fit a u64, got ${v}`);
      if (v > 0n) builder.addU64(tag, v);
    }
    const resp = await this.sender.sendRequest(op, namespace, textEncoder.encode(stream), value, builder.build());
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Delete a stream's records and metadata.
   *
   * A non-empty stream is refused with `ConflictError` unless `force` is set.
   * Deleting a stream that doesn't exist succeeds.
   *
   * @param stream - Stream name
   * @param opts - Optional namespace override; `force` to delete a non-empty stream
   */
  async delete(stream: string, opts?: StreamDeleteOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();
    if (opts?.force) {
      builder.addFlag(OptionTag.Force);
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamDelete,
      namespace,
      textEncoder.encode(stream),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Create a consumer group, with its lease (`ackTimeoutMs`) and attempts
   * (`maxDeliver`); unset, the server's 30 s and 5. Returns whether it made
   * the group: creating one that exists with the same settings changes
   * nothing and returns false; with other settings it throws ConflictError
   * naming the group's own, since a create can't change them. A group read
   * creates the group it names at the defaults, so this is needed only for
   * other settings.
   */
  async groupCreate(stream: string, group: string, opts?: StreamGroupCreateOptions): Promise<boolean> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const builder = new OptionsBuilder();
    const lease = opts?.ackTimeoutMs;
    if (lease !== undefined) {
      if (!Number.isInteger(lease) || lease < MIN_GROUP_LEASE_MS || lease > MAX_GROUP_LEASE_MS) {
        throw new FloError(`flo: ackTimeoutMs must be a whole number of ms from ${MIN_GROUP_LEASE_MS} to ${MAX_GROUP_LEASE_MS}, got ${lease}`);
      }
      builder.addU32(OptionTag.AckTimeoutMS, lease);
    }
    const attempts = opts?.maxDeliver;
    if (attempts !== undefined) {
      if (!Number.isInteger(attempts) || attempts < 1 || attempts > 255) {
        throw new FloError(`flo: maxDeliver must be a whole number from 1 to 255, got ${attempts}`);
      }
      builder.addU8(OptionTag.MaxDeliver, attempts);
    }
    const resp = await this.sender.sendRequest(OpCode.StreamGroupCreate, namespace, textEncoder.encode(stream), groupValue(group), builder.build());
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length !== 1) throw new IncompleteResponseError(`group create answer of ${resp.data.length} bytes`);
    return resp.data[0] === 1;
  }

  /**
   * Take up to `limit` (1 to 100) of the group's records, leased to
   * `consumer`: first records whose lease ran out or that were nacked, once
   * ready again, then the stream's next ones. A read doesn't consume them:
   * ack each within the lease (the group's `ackTimeoutMs`), or nack it, or
   * it is delivered again once the lease runs out, with `deliveries` one
   * higher. Touch to keep a lease. A record nacked or expired `maxDeliver`
   * times is dead. Delivery is at least once. Reading a group that doesn't
   * exist creates it at the server's defaults.
   */
  async groupRead(stream: string, opts: StreamGroupOptions): Promise<StreamGroupReadResult> {
    const namespace = this.sender.getNamespace(opts.namespace);

    const builder = new OptionsBuilder();
    if (opts.limit !== undefined) {
      if (!Number.isInteger(opts.limit) || opts.limit < 1 || opts.limit > MAX_GROUP_BATCH) {
        throw new FloError(`flo: limit must be a whole number from 1 to ${MAX_GROUP_BATCH}, got ${opts.limit}`);
      }
      builder.addU32(OptionTag.Count, opts.limit);
    }
    if (opts.blockMs !== undefined) {
      builder.addU32(OptionTag.BlockMS, opts.blockMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupRead,
      namespace,
      textEncoder.encode(stream),
      groupValue(opts.group, opts.consumer),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseGroupReadResponse(resp.data, opts.consumer);
  }

  /**
   * Ack records a group read leased: the group is done with them. Pass the
   * read's result, or it with `records` narrowed. Records it didn't apply
   * to come back in `refused`, inside an ok answer: a stale ack (the lease
   * ran out, or the record was delivered again since) applies nothing. A
   * group that isn't there throws NotFoundError.
   */
  async groupAck(stream: string, delivery: StreamGroupDelivery, opts: StreamGroupReportOptions): Promise<StreamGroupReportResult> {
    const namespace = this.sender.getNamespace(opts.namespace);
    const resp = await this.sender.sendRequest(OpCode.StreamGroupAck, namespace, textEncoder.encode(stream), groupReportValue(opts.group, delivery), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return parseGroupReportResponse(resp.data, false);
  }

  /**
   * Nack records a group read leased: ready again after `delayMs`, or dead
   * once nacked or expired `maxDeliver` times, or at once with `dead`. A
   * nack counts as a failed attempt. With `abandon` they go back unstarted
   * instead: ready at once, the delivery counted but no failure, and never
   * dead-lettered, so abandon only records never started (a stopping
   * consumer); nack the ones that failed. Records it didn't apply to come
   * back in `refused`. A group that isn't there throws NotFoundError.
   */
  async groupNack(stream: string, delivery: StreamGroupDelivery, opts: StreamGroupNackOptions): Promise<StreamGroupReportResult> {
    const namespace = this.sender.getNamespace(opts.namespace);
    const builder = new OptionsBuilder();
    const delayMs = opts.delayMs ?? 0;
    if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > MAX_DELAY_MS) {
      throw new FloError(`flo: delayMs must be a whole number of ms from 0 to ${MAX_DELAY_MS}, got ${delayMs}`);
    }
    const error = textEncoder.encode(opts.error ?? "");
    if (error.length > MAX_GROUP_NACK_ERROR) {
      throw new FloError(`flo: a nack's error is at most ${MAX_GROUP_NACK_ERROR} bytes, got ${error.length}`);
    }
    if (opts.abandon && (opts.dead || delayMs > 0 || error.length > 0)) {
      throw new FloError("flo: abandon hands records back unstarted, so it takes no dead, delayMs or error");
    }
    if (delayMs > 0) builder.addU64(OptionTag.DelayMS, BigInt(delayMs));
    if (opts.dead) builder.addFlag(OptionTag.Dead);
    if (opts.abandon) builder.addFlag(OptionTag.Abandon);
    const resp = await this.sender.sendRequest(OpCode.StreamGroupNack, namespace, textEncoder.encode(stream), groupNackValue(opts.group, delivery, error), builder.build());
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return parseGroupReportResponse(resp.data, false);
  }

  /**
   * Keep records a group read leased: with half the lease or less left, it
   * runs a full lease from now, up to 12 h from the read; with more left,
   * nothing changes. Touch at least every half lease.
   */
  async groupTouch(stream: string, delivery: StreamGroupDelivery, opts: StreamGroupReportOptions): Promise<StreamGroupTouchResult> {
    const namespace = this.sender.getNamespace(opts.namespace);
    const resp = await this.sender.sendRequest(OpCode.StreamGroupTouch, namespace, textEncoder.encode(stream), groupReportValue(opts.group, delivery), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return parseGroupReportResponse(resp.data, true) as StreamGroupTouchResult;
  }

  /**
   * The stream's consumer groups: members (the consumers holding records
   * now), pending records (ready or leased), settings and the last record
   * delivered.
   */
  async groupList(stream: string, opts?: { namespace?: string }): Promise<StreamGroupListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.StreamGroupList, namespace, textEncoder.encode(stream), new Uint8Array(0), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeGroupListAnswer(resp.data);
  }

  /**
   * A consumer group's members: the consumers holding its records now, each
   * with how many it holds. A group that doesn't exist throws NotFoundError.
   */
  async groupMembers(stream: string, group: string, opts?: { namespace?: string }): Promise<StreamGroupMembersResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.StreamGroupMembers, namespace, textEncoder.encode(stream), encodeGroupMembersBody(group), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeGroupMembersAnswer(resp.data);
  }

  /**
   * Take records another consumer holds and has left idle for at least
   * `minIdleMs`: each gets a new delivery leased to `consumer`, its
   * failures kept. Name each record with the deliveries its holder has (a
   * read's result, narrowed). Idle is the time since its holder last read,
   * stole or touched it, so a holder that touches more often is never
   * stolen from: such a record comes back in `refused` as NotIdle. An
   * operator's command: a read takes expired records by itself. A group
   * that doesn't exist throws NotFoundError.
   */
  async groupSteal(stream: string, records: StreamGroupDelivery["records"], opts: StreamGroupStealOptions): Promise<StreamGroupStealResult> {
    const body = encodeGroupStealBody(opts.group, opts.consumer, opts.minIdleMs, opts.leaseMs, records);
    const resp = await this.send(OpCode.StreamGroupSteal, stream, body, opts.namespace);
    return decodeGroupStealAnswer(resp, opts.consumer);
  }

  /**
   * A page of a group's dead letters, oldest dead first: up to `limit` (1
   * to 100, 20 by default), each with its deliveries, failures, when it
   * died and its last error. Read on the leader. Pass the answer's `cursor`
   * for the next page; it is null on the last.
   */
  async groupDlqList(stream: string, group: string, opts?: DLQListOptions): Promise<GroupDLQListResult> {
    return decodeGroupDlqListAnswer(await this.send(OpCode.StreamGroupDLQList, stream, encodeGroupDlqListBody(group, opts), opts?.namespace));
  }

  /**
   * Make a group's dead letters ready again at once, with their failures
   * reset to 0 (deliveries and last error stay): those named, or with
   * `all` the oldest, up to `limit` per call, `more` saying whether others
   * remain. One named that isn't dead comes back in `refused` as NotDead;
   * one trimmed from the stream since is Gone.
   */
  async groupDlqRequeue(stream: string, group: string, ids: readonly StreamID[], opts?: DLQSelectOptions): Promise<GroupDLQResult> {
    return decodeGroupDlqAnswer(await this.send(OpCode.StreamGroupDLQRequeue, stream, encodeGroupDlqBody(group, ids, opts, false), opts?.namespace));
  }

  /**
   * Delete a group's dead letters for good: those named, or with `all` the
   * oldest, up to `limit` per call, `more` saying whether others remain.
   * With `dryRun`, `applied` is how many it would delete, and nothing is
   * deleted. One named that isn't dead comes back in `refused` as NotDead.
   */
  async groupDlqDelete(stream: string, group: string, ids: readonly StreamID[], opts?: DLQDeleteOptions): Promise<GroupDLQResult> {
    return decodeGroupDlqAnswer(await this.send(OpCode.StreamGroupDLQDelete, stream, encodeGroupDlqBody(group, ids, opts, true), opts?.namespace));
  }

  /** Send a table op on `stream`, answering its body or throwing its refusal. */
  private async send(op: OpCode, stream: string, body: Uint8Array, ns?: string): Promise<Uint8Array> {
    const namespace = this.sender.getNamespace(ns);
    const resp = await this.sender.sendRequest(op, namespace, textEncoder.encode(stream), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return resp.data;
  }
}

/** A group's lease bounds, in ms. */
export const MIN_GROUP_LEASE_MS = 5000;
export const MAX_GROUP_LEASE_MS = 43_200_000;
/** The longest error a group nack carries, in bytes. */
export const MAX_GROUP_NACK_ERROR = 1024;

/** A group op's value: `[group_len:u16][group]`, then `[consumer_len:u16][consumer]` when given. */
function groupValue(group: string, consumer?: string): Uint8Array {
  const g = textEncoder.encode(group);
  if (g.length === 0 || g.length > MAX_GROUP_NAME) throw new FloError(`flo: a group name is 1 to ${MAX_GROUP_NAME} bytes, got ${g.length}`);
  const c = consumer === undefined ? undefined : textEncoder.encode(consumer);
  if (c !== undefined && c.length > MAX_CONSUMER) throw new FloError(`flo: consumer is ${c.length} bytes, at most ${MAX_CONSUMER}`);
  const out = new Uint8Array(2 + g.length + (c === undefined ? 0 : 2 + c.length));
  const view = new DataView(out.buffer);
  view.setUint16(0, g.length, true);
  out.set(g, 2);
  if (c !== undefined) {
    view.setUint16(2 + g.length, c.length, true);
    out.set(c, 4 + g.length);
  }
  return out;
}

/**
 * An ack's, nack's or touch's value: `[group][consumer][term:u64][index:u64]
 * [count:u32]([ms:u64][seq:u64][deliveries:u32])*`.
 */
export function groupReportValue(group: string, d: StreamGroupDelivery): Uint8Array {
  if (d.records.length === 0 || d.records.length > MAX_GROUP_BATCH) {
    throw new FloError(`flo: a report names 1 to ${MAX_GROUP_BATCH} records, got ${d.records.length}`);
  }
  const head = groupValue(group, d.consumer);
  const out = new Uint8Array(head.length + 20 + d.records.length * 20);
  out.set(head, 0);
  const view = new DataView(out.buffer);
  let o = head.length;
  view.setBigUint64(o, d.term, true);
  view.setBigUint64(o + 8, d.index, true);
  view.setUint32(o + 16, d.records.length, true);
  o += 20;
  for (const r of d.records) {
    if (!Number.isInteger(r.deliveries) || r.deliveries < 0 || r.deliveries > 0xffffffff) throw new FloError(`flo: deliveries must fit a u32, got ${r.deliveries}`);
    view.setBigUint64(o, r.id.timestampMs, true);
    view.setBigUint64(o + 8, r.id.sequence, true);
    view.setUint32(o + 16, r.deliveries, true);
    o += 20;
  }
  return out;
}

/** A nack's value: a report's, then `[error_len:u16][error]`, 0 for none. */
export function groupNackValue(group: string, d: StreamGroupDelivery, error: Uint8Array): Uint8Array {
  const report = groupReportValue(group, d);
  const out = new Uint8Array(report.length + 2 + error.length);
  out.set(report, 0);
  new DataView(out.buffer).setUint16(report.length, error.length, true);
  out.set(error, report.length + 2);
  return out;
}

/**
 * A group read's answer: `[term:u64][index:u64][expires_at_ms:i64][count:u32]`,
 * then each record as a stream read gives it, followed by `[deliveries:u32]`.
 */
export function parseGroupReadResponse(data: Uint8Array, consumer: string): StreamGroupReadResult {
  if (data.length < 28) throw new IncompleteResponseError(`group read answer of ${data.length} bytes`);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const term = view.getBigUint64(0, true);
  const index = view.getBigUint64(8, true);
  const expiresAtMs = view.getBigInt64(16, true);
  const count = view.getUint32(24, true);
  const records: StreamGroupRecord[] = [];
  let o = 28;
  for (let i = 0; i < count; i++) {
    const { record, end } = parseRecordAt(data, o);
    if (end + 4 > data.length) throw new IncompleteResponseError("group read answer ends inside a record");
    records.push({ ...record, deliveries: view.getUint32(end, true) });
    o = end + 4;
  }
  if (o !== data.length) throw new IncompleteResponseError(`${data.length - o} bytes left over in a group read answer`);
  return { consumer, term, index, expiresAtMs, records };
}

/** What a group report's refusal code means. */
const GROUP_REFUSAL_TEXT: Record<QueueRefusal, string> = {
  [QueueRefusal.Gone]: "gone: no such record in this group (acked already, or never delivered to it)",
  [QueueRefusal.DeliveredAgain]: "stale report: the record was delivered again since",
  [QueueRefusal.OtherHolder]: "stale report: another consumer holds this delivery",
  [QueueRefusal.Expired]: "stale report: the lease expired and the record moved on",
  [QueueRefusal.Released]: "stale report: the record was nacked",
  [QueueRefusal.NotHeld]: "stale report: the record isn't held",
  [QueueRefusal.NotDead]: "not dead: the record isn't a dead letter",
  [QueueRefusal.NotIdle]: "not idle: its holder kept its lease moving",
};

/**
 * An ack's or nack's answer: `[applied:u32][refused:u32]([ms:u64][seq:u64][why:u8])*`;
 * a touch's leads with `[expires_at_ms:i64]`.
 */
export function parseGroupReportResponse(data: Uint8Array, touch: boolean): StreamGroupReportResult | StreamGroupTouchResult {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let o = 0;
  const need = (n: number) => {
    if (o + n > data.length) throw new IncompleteResponseError("group report answer ends inside a field");
  };
  let expiresAtMs = 0n;
  if (touch) {
    need(8);
    expiresAtMs = view.getBigInt64(0, true);
    o = 8;
  }
  need(8);
  const applied = view.getUint32(o, true);
  const n = view.getUint32(o + 4, true);
  o += 8;
  const refused: StreamGroupReportResult["refused"] = [];
  for (let i = 0; i < n; i++) {
    need(17);
    const id = new StreamID(view.getBigUint64(o, true), view.getBigUint64(o + 8, true));
    const why = data[o + 16]!;
    if (!(why in GROUP_REFUSAL_TEXT)) throw new IncompleteResponseError(`group refusal ${why} is not one this client knows`);
    refused.push({ id, why: why as QueueRefusal, reason: GROUP_REFUSAL_TEXT[why as QueueRefusal] });
    o += 17;
  }
  if (o !== data.length) throw new IncompleteResponseError(`${data.length - o} bytes left over in a group report answer`);
  return touch ? { expiresAtMs, applied, refused } : { applied, refused };
}

/**
 * Read-only KV operations for web clients.
 * Only exposes get and scan - no mutations.
 */
export class KVReadOnlyOperations {
  constructor(private readonly sender: StreamRequestSender) {}

  /**
   * Get retrieves the value for a key.
   * Returns null if the key is not found.
   */
  async get(key: string, opts?: { namespace?: string }): Promise<Uint8Array | null> {
    const got = await kvGet(this.sender, this.sender.getNamespace(opts?.namespace), key);
    return got?.value ?? null;
  }

  /**
   * Scan returns one page of the key names that start with `prefix`, and
   * the cursor for the next page, null on the last.
   */
  async scan(prefix: string, opts?: { namespace?: string; cursor?: Uint8Array; limit?: number }): Promise<ScanResult> {
    return kvScan(this.sender, this.sender.getNamespace(opts?.namespace), prefix, opts);
  }
}

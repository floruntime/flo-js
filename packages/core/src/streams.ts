/**
 * Stream operations for Flo SDK.
 *
 * Append, read and consume streams through consumer groups.
 */

import { createServerError, FloError, IncompleteResponseError } from "./errors.js";
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
} from "./types.js";
import { decodeGroupListAnswer, decodeGroupMembersAnswer, encodeGroupMembersBody, MAX_CONSUMER, MAX_DELAY_MS, truncateUtf8 } from "./kv_body.js";
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
  if (data.length < 52) {
    throw new Error("Invalid stream info response: too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  return {
    firstId: new StreamID(view.getBigUint64(0, true), view.getBigUint64(8, true)),
    lastId: new StreamID(view.getBigUint64(16, true), view.getBigUint64(24, true)),
    count: view.getBigUint64(32, true),
    bytes: view.getBigUint64(40, true),
    partitionCount: view.getUint32(48, true),
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
   * Read records from a stream.
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
      throw createServerError(resp.status, resp.data);
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
   * ran out, or the record was delivered again since) applies nothing.
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
   * nack counts as a failed attempt. Records it didn't apply to come back
   * in `refused`.
   */
  async groupNack(stream: string, delivery: StreamGroupDelivery, opts: StreamGroupNackOptions): Promise<StreamGroupReportResult> {
    const namespace = this.sender.getNamespace(opts.namespace);
    const builder = new OptionsBuilder();
    const delayMs = opts.delayMs ?? 0;
    if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > MAX_DELAY_MS) {
      throw new FloError(`flo: delayMs must be a whole number of ms from 0 to ${MAX_DELAY_MS}, got ${delayMs}`);
    }
    if (delayMs > 0) builder.addU64(OptionTag.DelayMS, BigInt(delayMs));
    if (opts.dead) builder.addFlag(OptionTag.Dead);
    // An option's length is a u8, so the error goes cut to 255 bytes.
    if (opts.error) builder.addBytes(OptionTag.ErrorText, truncateUtf8(textEncoder.encode(opts.error), MAX_OPTION_BYTES));
    const resp = await this.sender.sendRequest(OpCode.StreamGroupNack, namespace, textEncoder.encode(stream), groupReportValue(opts.group, delivery), builder.build());
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
}

/** A group's lease bounds, in ms. */
export const MIN_GROUP_LEASE_MS = 5000;
export const MAX_GROUP_LEASE_MS = 43_200_000;
/** The longest option value: its length is a u8. */
const MAX_OPTION_BYTES = 255;

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

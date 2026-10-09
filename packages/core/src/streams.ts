/**
 * Stream operations for Flo SDK.
 *
 * Append, read and consume streams through consumer groups.
 */

import { createServerError } from "./errors.js";
import {
  OpCode,
  OptionTag,
  type RawResponse,
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
  type StreamAckOptions,
  type StreamNackOptions,
} from "./types.js";
import { OptionsBuilder, serializeListValue } from "./wire.js";

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

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  const count = view.getUint32(offset, true);
  offset += 4;

  const records: StreamRecord[] = [];

  for (let i = 0; i < count && offset < data.length; i++) {
    // sequence (u64 LE)
    if (offset + 8 > data.length) break;
    const sequence = view.getBigUint64(offset, true);
    offset += 8;

    // timestamp_ms (i64 LE)
    if (offset + 8 > data.length) break;
    const timestampMs = view.getBigInt64(offset, true);
    offset += 8;

    // tier (u8)
    if (offset + 1 > data.length) break;
    const tier = data[offset] as StorageTier;
    offset += 1;

    // partition (u32) — skip
    if (offset + 4 > data.length) break;
    offset += 4;

    // key_present (u8)
    if (offset + 1 > data.length) break;
    const keyPresent = data[offset];
    offset += 1;

    // read key (stream name) if present
    let streamName = '';
    if (keyPresent !== 0) {
      if (offset + 4 > data.length) break;
      const keyLen = view.getUint32(offset, true);
      offset += 4;
      if (offset + keyLen > data.length) break;
      streamName = new TextDecoder().decode(data.subarray(offset, offset + keyLen));
      offset += keyLen;
    }

    // payload_len (u32) + payload
    if (offset + 4 > data.length) break;
    const payloadLen = view.getUint32(offset, true);
    offset += 4;

    if (offset + payloadLen > data.length) break;
    const payload = new Uint8Array(data.subarray(offset, offset + payloadLen));
    offset += payloadLen;

    // headers: [header_count:u32]([key_len:u32][key][val_len:u32][val])*
    if (offset + 4 > data.length) break;
    const headerCount = view.getUint32(offset, true);
    offset += 4;

    let headers: Record<string, string> | null = null;
    if (headerCount > 0) {
      headers = {};
      const decoder = new TextDecoder();
      for (let h = 0; h < headerCount; h++) {
        if (offset + 4 > data.length) break;
        const hKeyLen = view.getUint32(offset, true);
        offset += 4;
        if (offset + hKeyLen > data.length) break;
        const hKey = decoder.decode(data.subarray(offset, offset + hKeyLen));
        offset += hKeyLen;
        if (offset + 4 > data.length) break;
        const hValLen = view.getUint32(offset, true);
        offset += 4;
        if (offset + hValLen > data.length) break;
        const hVal = decoder.decode(data.subarray(offset, offset + hValLen));
        offset += hValLen;
        headers[hKey] = hVal;
      }
    }

    records.push({
      id: new StreamID(BigInt(timestampMs < 0n ? 0n : timestampMs), sequence),
      tier,
      stream: streamName,
      payload,
      headers,
    });
  }

  return { records };
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
 * Provides real-time event streaming capabilities:
 * - Publish events to streams
 * - Read historical events
 * - Consumer group support for load balancing
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
      payload,
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
   * Read records as part of a consumer group.
   *
   * Consumer groups enable load balancing across multiple consumers.
   * Each record is delivered to only one consumer in the group.
   *
   * @param stream - Stream name
   * @param opts - Consumer group options (group, consumer, limit, blockMs)
   */
  async groupRead(stream: string, opts: StreamGroupOptions): Promise<StreamReadResult> {
    const namespace = this.sender.getNamespace(opts.namespace);

    const builder = new OptionsBuilder();

    if (opts.limit !== undefined) {
      builder.addU32(OptionTag.Count, opts.limit);
    }

    if (opts.blockMs !== undefined) {
      builder.addU32(OptionTag.BlockMS, opts.blockMs);
    }

    // Wire format for value: [group_len:u16][group][consumer_len:u16][consumer]
    const groupBytes = textEncoder.encode(opts.group);
    const consumerBytes = textEncoder.encode(opts.consumer);
    const value = new Uint8Array(2 + groupBytes.length + 2 + consumerBytes.length);
    const valueView = new DataView(value.buffer);

    let offset = 0;
    valueView.setUint16(offset, groupBytes.length, true);
    offset += 2;
    value.set(groupBytes, offset);
    offset += groupBytes.length;
    valueView.setUint16(offset, consumerBytes.length, true);
    offset += 2;
    value.set(consumerBytes, offset);

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupRead,
      namespace,
      textEncoder.encode(stream),
      value,
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseStreamReadResponse(resp.data);
  }

  /**
   * Join a consumer group.
   *
   * @param stream - Stream name
   * @param group - Consumer group name
   * @param consumer - Consumer ID (unique within the group)
   * @param opts - Optional namespace override
   */
  async groupJoin(
    stream: string,
    group: string,
    consumer: string,
    opts?: { namespace?: string }
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const groupBytes = textEncoder.encode(group);
    const consumerBytes = textEncoder.encode(consumer);
    const value = new Uint8Array(2 + groupBytes.length + 2 + consumerBytes.length);
    const valueView = new DataView(value.buffer);

    let offset = 0;
    valueView.setUint16(offset, groupBytes.length, true);
    offset += 2;
    value.set(groupBytes, offset);
    offset += groupBytes.length;
    valueView.setUint16(offset, consumerBytes.length, true);
    offset += 2;
    value.set(consumerBytes, offset);

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupJoin,
      namespace,
      textEncoder.encode(stream),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Leave a consumer group.
   *
   * @param stream - Stream name
   * @param group - Consumer group name
   * @param consumer - Consumer ID
   * @param opts - Optional namespace override
   */
  async groupLeave(
    stream: string,
    group: string,
    consumer: string,
    opts?: { namespace?: string }
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const groupBytes = textEncoder.encode(group);
    const consumerBytes = textEncoder.encode(consumer);
    const value = new Uint8Array(2 + groupBytes.length + 2 + consumerBytes.length);
    const valueView = new DataView(value.buffer);

    let offset = 0;
    valueView.setUint16(offset, groupBytes.length, true);
    offset += 2;
    value.set(groupBytes, offset);
    offset += groupBytes.length;
    valueView.setUint16(offset, consumerBytes.length, true);
    offset += 2;
    value.set(consumerBytes, offset);

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupLeave,
      namespace,
      textEncoder.encode(stream),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Acknowledge records in a consumer group.
   *
   * @param stream - Stream name
   * @param ids - StreamIDs to acknowledge
   * @param opts - Ack options (group, consumer)
   */
  async groupAck(stream: string, ids: StreamID[], opts: StreamAckOptions): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts.namespace);

    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
    const groupBytes = textEncoder.encode(opts.group);
    const consumerBytes = textEncoder.encode(opts.consumer);
    const value = new Uint8Array(
      2 + groupBytes.length + 2 + consumerBytes.length + 4 + ids.length * 16
    );
    const view = new DataView(value.buffer);

    let offset = 0;
    view.setUint16(offset, groupBytes.length, true);
    offset += 2;
    value.set(groupBytes, offset);
    offset += groupBytes.length;
    view.setUint16(offset, consumerBytes.length, true);
    offset += 2;
    value.set(consumerBytes, offset);
    offset += consumerBytes.length;
    view.setUint32(offset, ids.length, true);
    offset += 4;
    for (const id of ids) {
      view.setBigUint64(offset, id.timestampMs, true);
      offset += 8;
      view.setBigUint64(offset, id.sequence, true);
      offset += 8;
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupAck,
      namespace,
      textEncoder.encode(stream),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Negatively acknowledge records in a consumer group.
   * The records become available for redelivery.
   *
   * @param stream - Stream name
   * @param ids - StreamIDs to nack
   * @param opts - Nack options (group, consumer)
   */
  async groupNack(stream: string, ids: StreamID[], opts: StreamNackOptions): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts.namespace);

    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
    const groupBytes = textEncoder.encode(opts.group);
    const consumerBytes = textEncoder.encode(opts.consumer);
    const value = new Uint8Array(
      2 + groupBytes.length + 2 + consumerBytes.length + 4 + ids.length * 16
    );
    const view = new DataView(value.buffer);

    let offset = 0;
    view.setUint16(offset, groupBytes.length, true);
    offset += 2;
    value.set(groupBytes, offset);
    offset += groupBytes.length;
    view.setUint16(offset, consumerBytes.length, true);
    offset += 2;
    value.set(consumerBytes, offset);
    offset += consumerBytes.length;
    view.setUint32(offset, ids.length, true);
    offset += 4;
    for (const id of ids) {
      view.setBigUint64(offset, id.timestampMs, true);
      offset += 8;
      view.setBigUint64(offset, id.sequence, true);
      offset += 8;
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupNack,
      namespace,
      textEncoder.encode(stream),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }
}

/**
 * Read-only KV operations for web clients.
 * Only exposes get, scan, and history - no mutations.
 */
export class KVReadOnlyOperations {
  constructor(private readonly sender: StreamRequestSender) {}

  /**
   * Get retrieves the value for a key.
   * Returns null if the key is not found.
   */
  async get(key: string, opts?: { namespace?: string }): Promise<Uint8Array | null> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.KVGet,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status === StatusCode.NotFound) {
      return null;
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.data.length === 0) {
      return null;
    }

    return resp.data;
  }

  /**
   * Scan retrieves keys with a prefix.
   */
  async scan(
    prefix: string,
    opts?: { namespace?: string; cursor?: Uint8Array; limit?: number; keysOnly?: boolean }
  ): Promise<import("./types.js").ScanResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.keysOnly) {
      builder.addU8(OptionTag.KeysOnly, 1);
    }

    const value = serializeListValue(opts?.limit, opts?.cursor);

    const resp = await this.sender.sendRequest(
      OpCode.KVScan,
      namespace,
      textEncoder.encode(prefix),
      value,
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Import and use parseScanResponse from wire.ts
    const { parseScanResponse } = await import("./wire.js");
    return parseScanResponse(resp.data);
  }
}

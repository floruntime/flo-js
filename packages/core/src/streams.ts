/**
 * Stream operations for Flo SDK.
 *
 * Provides pub/sub style streaming for real-time applications.
 * Primary use case: browser clients receiving real-time events.
 */

import { createServerError } from "./errors.js";
import {
  OpCode,
  OptionTag,
  type RawResponse,
  StatusCode,
  StorageTier,
  type StreamAppendOptions,
  type StreamAppendResult,
  type StreamReadOptions,
  type StreamReadResult,
  type StreamRecord,
  type StreamInfoResult,
  type StreamSubscribeOptions,
  type StreamEventCallback,
  type StreamSubscription,
  type StreamGroupOptions,
  type StreamAckOptions,
  type StreamNackOptions,
} from "./types.js";
import { OptionsBuilder } from "./wire.js";

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

  /**
   * Register a callback for server-pushed stream events.
   * Used for real-time subscriptions.
   */
  onStreamEvent?(callback: (streamName: string, record: StreamRecord) => void): void;

  /**
   * Unregister stream event callback.
   */
  offStreamEvent?(callback: (streamName: string, record: StreamRecord) => void): void;
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

    // skip key if present
    if (keyPresent !== 0) {
      if (offset + 4 > data.length) break;
      const keyLen = view.getUint32(offset, true);
      offset += 4;
      if (offset + keyLen > data.length) break;
      offset += keyLen;
    }

    // payload_len (u32) + payload
    if (offset + 4 > data.length) break;
    const payloadLen = view.getUint32(offset, true);
    offset += 4;

    if (offset + payloadLen > data.length) break;
    const payload = new Uint8Array(data.subarray(offset, offset + payloadLen));
    offset += payloadLen;

    // header_count (u32) — skip for now
    if (offset + 4 > data.length) break;
    offset += 4;

    records.push({
      sequence,
      timestampMs,
      tier,
      payload,
      headers: null,
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

  return { sequence, timestampMs };
}

/**
 * Parse stream info response.
 *
 * Wire format: [first_seq:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32]
 */
export function parseStreamInfoResponse(data: Uint8Array): StreamInfoResult {
  if (data.length < 36) {
    throw new Error("Invalid stream info response: too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  return {
    firstSeq: view.getBigUint64(0, true),
    lastSeq: view.getBigUint64(8, true),
    count: view.getBigUint64(16, true),
    bytes: view.getBigUint64(24, true),
    partitionCount: view.getUint32(32, true),
  };
}

/**
 * Stream operations for Flo SDK.
 *
 * Provides real-time event streaming capabilities:
 * - Publish events to streams
 * - Subscribe to stream events (real-time push)
 * - Read historical events
 * - Consumer group support for load balancing
 */
export class StreamOperations {
  private subscriptions: Map<string, Set<StreamEventCallback>> = new Map();
  private subscriptionIds: Map<number, { streamKey: string; callback: StreamEventCallback }> = new Map();
  private nextSubscriptionId = 1;

  constructor(private readonly sender: StreamRequestSender) {
    // Register for server-pushed events if supported
    if (sender.onStreamEvent) {
      sender.onStreamEvent((streamName, record) => {
        const callbacks = this.subscriptions.get(streamName);
        if (callbacks) {
          for (const callback of callbacks) {
            try {
              callback(record);
            } catch (err) {
              console.error("[flo] Stream callback error:", err);
            }
          }
        }
      });
    }
  }

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
   * Subscribe to real-time events from a stream.
   *
   * This sends a StreamSubscribe (0x17) request and the server will
   * push StreamEvent (0x16) messages for new records until unsubscribed.
   * Requires WebSocket transport.
   *
   * @param stream - Stream name to subscribe to
   * @param callback - Function called for each event
   * @param opts - Subscribe options
   */
  async subscribe(
    stream: string,
    callback: StreamEventCallback,
    opts?: StreamSubscribeOptions
  ): Promise<StreamSubscription> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const streamKey = `${namespace}:${stream}`;

    // Generate subscription ID
    const subscriptionId = this.nextSubscriptionId++;

    // Register local callback
    if (!this.subscriptions.has(streamKey)) {
      this.subscriptions.set(streamKey, new Set());
    }
    this.subscriptions.get(streamKey)!.add(callback);

    // Track subscription ID for unsubscribe
    this.subscriptionIds.set(subscriptionId, { streamKey, callback });

    // Build subscription request
    const builder = new OptionsBuilder();

    // Tail mode flag (default for subscriptions is tail)
    if (opts?.tail !== false) {
      builder.addFlag(OptionTag.StreamTail);
    }

    // Start StreamID (16 bytes, big-endian)
    if (opts?.start !== undefined) {
      builder.addBytes(OptionTag.StreamStart, opts.start.toBytes());
    }

    // Explicit partition
    if (opts?.partition !== undefined) {
      builder.addU32(OptionTag.Partition, opts.partition);
    }

    // Subscription ID
    builder.addU64(OptionTag.SubscriptionID, BigInt(subscriptionId));

    // Send StreamSubscribe request
    const resp = await this.sender.sendRequest(
      OpCode.StreamSubscribe,
      namespace,
      textEncoder.encode(stream),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      // Clean up on failure
      this.subscriptions.get(streamKey)?.delete(callback);
      this.subscriptionIds.delete(subscriptionId);
      throw createServerError(resp.status, resp.data);
    }

    return {
      subscriptionId,
      unsubscribe: async () => {
        // Remove local callback
        const callbacks = this.subscriptions.get(streamKey);
        if (callbacks) {
          callbacks.delete(callback);
          if (callbacks.size === 0) {
            this.subscriptions.delete(streamKey);
          }
        }
        this.subscriptionIds.delete(subscriptionId);

        // Send StreamUnsubscribe request
        const unsubBuilder = new OptionsBuilder();
        unsubBuilder.addU64(OptionTag.SubscriptionID, BigInt(subscriptionId));

        try {
          await this.sender.sendRequest(
            OpCode.StreamUnsubscribe,
            namespace,
            textEncoder.encode(stream),
            new Uint8Array(0),
            unsubBuilder.build()
          );
        } catch (err) {
          // Log but don't throw - unsubscribe is best-effort
          console.warn("[flo] Unsubscribe failed:", err);
        }
      },
    };
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
   * @param seqs - Sequence numbers to acknowledge
   * @param opts - Ack options (group, consumer)
   */
  async groupAck(stream: string, seqs: bigint[], opts: StreamAckOptions): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts.namespace);

    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][seq:u64]*
    const groupBytes = textEncoder.encode(opts.group);
    const consumerBytes = textEncoder.encode(opts.consumer);
    const value = new Uint8Array(
      2 + groupBytes.length + 2 + consumerBytes.length + 4 + seqs.length * 8
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
    view.setUint32(offset, seqs.length, true);
    offset += 4;
    for (const seq of seqs) {
      view.setBigUint64(offset, seq, true);
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
   * Records will be redelivered after the redelivery delay.
   *
   * @param stream - Stream name
   * @param seqs - Sequence numbers to nack
   * @param opts - Nack options (group, consumer, redeliveryDelayMs)
   */
  async groupNack(stream: string, seqs: bigint[], opts: StreamNackOptions): Promise<void> {
    if (seqs.length === 0) {
      return;
    }

    const namespace = this.sender.getNamespace(opts.namespace);

    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][seq:u64]*
    const groupBytes = textEncoder.encode(opts.group);
    const consumerBytes = textEncoder.encode(opts.consumer);
    const value = new Uint8Array(
      2 + groupBytes.length + 2 + consumerBytes.length + 4 + seqs.length * 8
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
    view.setUint32(offset, seqs.length, true);
    offset += 4;
    for (const seq of seqs) {
      view.setBigUint64(offset, seq, true);
      offset += 8;
    }

    // Add redelivery delay via TLV options
    const builder = new OptionsBuilder();
    if (opts.redeliveryDelayMs !== undefined) {
      builder.addU32(OptionTag.RedeliveryDelayMS, opts.redeliveryDelayMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.StreamGroupNack,
      namespace,
      textEncoder.encode(stream),
      value,
      builder.build()
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

    if (opts?.limit !== undefined) {
      builder.addU32(OptionTag.Limit, opts.limit);
    }

    if (opts?.keysOnly) {
      builder.addU8(OptionTag.KeysOnly, 1);
    }

    const value = opts?.cursor ?? new Uint8Array(0);

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

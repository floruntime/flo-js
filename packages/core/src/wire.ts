/**
 * Flo wire protocol serialization and deserialization.
 */

import {
  BlockTooLongError,
  FloError,
  IncompleteResponseError,
  InvalidMagicError,
  KeyTooLargeError,
  NamespaceTooLargeError,
  TableMismatchError,
  ValueTooLargeError,
} from "./errors.js";
import {
  HEADER_SIZE,
  MAGIC,
  MAX_BLOCK_MS,
  MAX_KEY_SIZE,
  MAX_NAMESPACE_SIZE,
  MAX_VALUE_SIZE,
  OpCode,
  OptionTag,
  type RawResponse,
  RESPONSE_STALE,
  type StatusCode,
  VERSION,
  TABLE_HASH,
} from "./types.js";
import { isTableOp } from "./kv_body.js";

/** CRC32C (Castagnoli, reflected polynomial 0x82F63B78) lookup table. */
const CRC32C_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let crc = i;
  for (let j = 0; j < 8; j++) crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
  CRC32C_TABLE[i] = crc >>> 0;
}

/** CRC32C of `parts`, in order. */
export function crc32c(...parts: Uint8Array[]): number {
  let crc = 0xffffffff;
  for (const data of parts) {
    for (let i = 0; i < data.length; i++) {
      crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ data[i]!) & 0xff]!;
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * A frame's checksum: CRC32C over the header without its crc field
 * (bytes 16..20), then the payload.
 */
export function computeCRC32C(header: Uint8Array, payload: Uint8Array): number {
  return crc32c(header.subarray(0, 16), header.subarray(20, 32), payload);
}

/**
 * Refuse a blockMs the server would refuse (over 300000), or one the u32
 * encoding would silently change (negative, fractional, NaN).
 */
export function checkBlockMs(blockMs: number): void {
  if (!Number.isInteger(blockMs) || blockMs < 0) {
    throw new FloError(`flo: blockMs must be a whole number of ms from 0 to ${MAX_BLOCK_MS}, got ${blockMs}`);
  }
  if (blockMs > MAX_BLOCK_MS) {
    throw new BlockTooLongError(blockMs);
  }
}

/**
 * A worker's blockMs. A worker always long-polls, since 0 (don't wait) would
 * spin its poll loop against the server, so unset or 0 means 30000.
 */
export function workerBlockMs(blockMs: number | undefined): number {
  if (blockMs === undefined || blockMs === 0) return 30000;
  checkBlockMs(blockMs);
  return blockMs;
}

/**
 * How long the server may hold a serialized request before answering: a
 * kv.get's wait_ms, an action await's or queue dequeue's block_ms, or its
 * BlockMS option;
 * else 0.
 * Transports add this to their timeout so a long poll is not cut off
 * client-side while the server is still waiting.
 *
 * @internal
 */
export function requestBlockMs(request: Uint8Array): number {
  const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
  let off = HEADER_SIZE;
  if (request.length < off + 2) return 0;
  off += 2 + view.getUint16(off, true); // namespace
  if (request.length < off + 2) return 0;
  off += 2 + view.getUint16(off, true); // key
  const op = view.getUint16(20, true) as OpCode;
  if (isTableOp(op)) {
    if (op === OpCode.ActionAwait) {
      // [count:u8]([len:u16][action])*, then block_ms: [present:u8][u64], 30000 when absent.
      if (request.length < off + 1) return 0;
      for (let n = request[off++]!; n > 0; n--) {
        if (request.length < off + 2) return 0;
        off += 2 + view.getUint16(off, true);
      }
    } else if (op === OpCode.QueueDequeue) {
      // consumer [present:u8][len:u16][..], count [present:u8][u32], lease_ms [present:u8]([u64])?,
      // then block_ms [present:u8][u64], 0 when absent.
      if (request.length < off + 3 || request[off] !== 1) return 0;
      off += 3 + view.getUint16(off + 1, true) + 5;
      if (request.length < off + 1) return 0;
      off += request[off] === 1 ? 9 : 1;
      if (request.length < off + 9 || request[off] !== 1) return 0;
      return Number(view.getBigUint64(off + 1, true));
    } else if (op !== OpCode.KVGet) {
      return 0;
    }
    // kv.get's body opens with wait_ms: [present:u8][u64].
    if (request.length < off + 1) return 0;
    if (request[off] !== 1) return op === OpCode.ActionAwait ? 30000 : 0;
    if (request.length < off + 9) return 0;
    return Number(view.getBigUint64(off + 1, true));
  }
  if (request.length < off + 4) return 0;
  off += 4 + view.getUint32(off, true); // value
  if (request.length < off + 2) return 0;
  const end = Math.min(request.length, off + 2 + view.getUint16(off, true));
  off += 2;
  let blockMs: number | undefined;
  while (off + 2 <= end) {
    const tag = request[off]!;
    const len = request[off + 1]!;
    off += 2;
    if (off + len > end) break;
    if (tag === OptionTag.BlockMS && len === 4) {
      blockMs = view.getUint32(off, true);
    }
    off += len;
  }
  return blockMs ?? 0;
}

/**
 * Whether a serialized request takes items its reply carries (a dequeued
 * message, a group read's records, a claimed or awaited task). If its reply
 * is dropped, those items stay leased to nobody until their lease runs out.
 *
 * @internal
 */
export function requestTakesItems(request: Uint8Array): boolean {
  if (request.length < HEADER_SIZE) return false;
  const op = new DataView(request.buffer, request.byteOffset, request.byteLength).getUint16(20, true);
  return (
    op === OpCode.QueueDequeue ||
    op === OpCode.StreamGroupRead ||
    op === OpCode.StreamGroupClaim ||
    op === OpCode.ActionAwait
  );
}

/**
 * Builder for TLV-encoded options.
 */
export class OptionsBuilder {
  private buf: number[] = [];

  /**
   * Add a u8 option.
   */
  addU8(tag: OptionTag, value: number): this {
    this.buf.push(tag, 1, value & 0xff);
    return this;
  }

  /**
   * Add a u32 option (little-endian). Throws for a BlockMS the server would
   * refuse (see checkBlockMs).
   */
  addU32(tag: OptionTag, value: number): this {
    if (tag === OptionTag.BlockMS) checkBlockMs(value);
    this.buf.push(tag, 4);
    this.buf.push(value & 0xff);
    this.buf.push((value >>> 8) & 0xff);
    this.buf.push((value >>> 16) & 0xff);
    this.buf.push((value >>> 24) & 0xff);
    return this;
  }

  /**
   * Add a u64 option (little-endian).
   */
  addU64(tag: OptionTag, value: bigint): this {
    this.buf.push(tag, 8);
    for (let i = 0; i < 8; i++) {
      this.buf.push(Number((value >> BigInt(i * 8)) & 0xffn));
    }
    return this;
  }

  /**
   * Add an i64 option (little-endian, signed).
   */
  addI64(tag: OptionTag, value: bigint): this {
    this.buf.push(tag, 8);
    // Convert to unsigned representation for serialization
    const unsigned = value < 0n ? value + (1n << 64n) : value;
    for (let i = 0; i < 8; i++) {
      this.buf.push(Number((unsigned >> BigInt(i * 8)) & 0xffn));
    }
    return this;
  }

  /**
   * Add a string option (UTF-8 encoded).
   */
  addString(tag: OptionTag, value: string): this {
    const encoder = new TextEncoder();
    const bytes = encoder.encode(value);
    return this.addBytes(tag, bytes);
  }

  /**
   * Add a bytes option.
   */
  addBytes(tag: OptionTag, value: Uint8Array): this {
    if (value.length > 255) {
      throw new Error("flo: option value too large (max 255 bytes)");
    }
    this.buf.push(tag, value.length);
    for (let i = 0; i < value.length; i++) {
      this.buf.push(value[i]!);
    }
    return this;
  }

  /**
   * Add a flag option (presence indicates true).
   */
  addFlag(tag: OptionTag): this {
    this.buf.push(tag, 0);
    return this;
  }

  /**
   * Build and return the options as Uint8Array.
   */
  build(): Uint8Array {
    return new Uint8Array(this.buf);
  }
}

/**
 * Serialize a request into wire format.
 */
export function serializeRequest(
  requestId: bigint,
  opCode: OpCode,
  namespace: Uint8Array,
  key: Uint8Array,
  value: Uint8Array,
  options: Uint8Array
): Uint8Array {
  // Validate sizes
  if (namespace.length > MAX_NAMESPACE_SIZE) {
    throw new NamespaceTooLargeError();
  }
  if (key.length > MAX_KEY_SIZE) {
    throw new KeyTooLargeError();
  }
  if (value.length > MAX_VALUE_SIZE) {
    throw new ValueTooLargeError();
  }

  // A table op's payload is [namespace][key][body]: its body, coded by the
  // op's schema, is `value`, and it takes no options.
  const table = isTableOp(opCode);
  if (table && options.length > 0) throw new FloError(`flo: op ${opCode} takes a body, not options`);

  // Calculate payload size
  const payloadLen =
    2 +
    namespace.length + // namespace: [len:u16][data]
    2 +
    key.length + // key: [len:u16][data]
    value.length +
    (table ? 0 : 4 + 2 + options.length); // value length, options: [len:u16][data]

  // Allocate buffer for header + payload
  const buf = new Uint8Array(HEADER_SIZE + payloadLen);
  const view = new DataView(buf.buffer);

  // Build header (without its checksum)
  view.setUint32(0, MAGIC, true); // magic
  view.setUint32(4, payloadLen, true); // payload_length
  view.setBigUint64(8, requestId, true); // request_id
  // Checksum at [16:20] - filled later
  view.setUint16(20, opCode, true); // op_code (u16 LE)
  buf[22] = VERSION; // version
  buf[23] = 0; // flags
  view.setBigUint64(24, TABLE_HASH, true); // table_hash

  // Build payload
  let offset = HEADER_SIZE;

  // Namespace
  view.setUint16(offset, namespace.length, true);
  offset += 2;
  buf.set(namespace, offset);
  offset += namespace.length;

  // Key
  view.setUint16(offset, key.length, true);
  offset += 2;
  buf.set(key, offset);
  offset += key.length;

  if (table) {
    buf.set(value, offset);
  } else {
    // Value
    view.setUint32(offset, value.length, true);
    offset += 4;
    buf.set(value, offset);
    offset += value.length;

    // Options
    view.setUint16(offset, options.length, true);
    offset += 2;
    buf.set(options, offset);
  }

  // Compute and fill the checksum
  const crcValue = computeCRC32C(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE));
  view.setUint32(16, crcValue, true);

  return buf;
}

/**
 * Parse a response header.
 * Returns [status, dataLen, requestId, crc, flags].
 */
export function parseResponseHeader(
  header: Uint8Array
): [StatusCode, number, bigint, number, number] {
  if (header.length < HEADER_SIZE) {
    throw new IncompleteResponseError("header too short");
  }

  const view = new DataView(header.buffer, header.byteOffset, header.byteLength);

  const magic = view.getUint32(0, true);
  if (magic !== MAGIC) {
    throw new InvalidMagicError();
  }

  const dataLen = view.getUint32(4, true);
  const requestId = view.getBigUint64(8, true);
  const crcValue = view.getUint32(16, true);
  const version = header[20]!;
  // Not checked against StatusCode: a status this SDK doesn't know still
  // frames normally, so its body is consumed and surfaces as a ServerError.
  const status = header[21] as StatusCode;

  // Another build's body can't be read: the transport closes the
  // connection with it unread.
  const tableHash = view.getBigUint64(24, true);
  if (version !== VERSION || tableHash !== TABLE_HASH) {
    throw new TableMismatchError(version, tableHash);
  }

  return [status, dataLen, requestId, crcValue, header[22]!];
}

/**
 * Parse a raw response from header + data.
 */
export function parseRawResponse(
  header: Uint8Array,
  data: Uint8Array
): RawResponse {
  const [status, , requestId, , flags] = parseResponseHeader(header);
  // The table hash pins both sides, so a flag this client doesn't know
  // means the answer can't be read as this client reads it.
  if ((flags & ~RESPONSE_STALE) !== 0) {
    throw new IncompleteResponseError(`unknown response flags 0x${flags.toString(16).padStart(2, "0")}`);
  }
  return { status, data, requestId, stale: (flags & RESPONSE_STALE) !== 0 };
}

/**
 * Parse enqueue response data.
 */
export function parseEnqueueResponse(data: Uint8Array): bigint {
  if (data.length < 8) {
    throw new IncompleteResponseError("enqueue response too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return view.getBigUint64(0, true);
}

// ============================================================================
// ACTION/WORKER SERIALIZATION
// ============================================================================

const textEncoder = new TextEncoder();

/**
 * Serialize action register value.
 * Format: [action_type:u8][timeout_ms:u32][max_retries:u32][owner_len:u16][owner]
 */
export function serializeActionRegisterValue(
  actionType: number,
  timeoutMs: number,
  maxRetries: number,
  owner?: string
): Uint8Array {
  for (const [name, v] of [["timeoutMs", timeoutMs], ["maxRetries", maxRetries]] as const) {
    // Both go on the wire as u32.
    if (!(Number.isInteger(v) && v >= 0 && v < 2 ** 32)) {
      throw new RangeError(`${name} must be an integer in [0, 2^32), not ${v}`);
    }
  }
  const ownerBytes = textEncoder.encode(owner ?? "");
  if (ownerBytes.length > 0xffff) throw new RangeError("owner is longer than 65535 bytes");
  const buf = new Uint8Array(11 + ownerBytes.length);
  const view = new DataView(buf.buffer);
  buf[0] = actionType & 0xff;
  view.setUint32(1, timeoutMs, true);
  view.setUint32(5, maxRetries, true);
  view.setUint16(9, ownerBytes.length, true);
  buf.set(ownerBytes, 11);
  return buf;
}

/**
 * A stream append's value: a batch of one record,
 * [record_count:u32=1][payload_len:u32][payload][header_count:u16]([key_len:u16][key][value_len:u16][value])*.
 * The server refuses a payload that isn't framed this way.
 */
export function serializeStreamAppendValue(payload: Uint8Array, headers?: Record<string, string>): Uint8Array {
  const pairs = Object.entries(headers ?? {}).map(([k, v]) => [textEncoder.encode(k), textEncoder.encode(v)] as const);
  if (pairs.length > 0xffff) throw new RangeError("a record holds at most 65535 headers");
  let size = 4 + 4 + payload.length + 2;
  for (const [k, v] of pairs) {
    if (k.length > 0xffff || v.length > 0xffff) throw new RangeError("a header key or value is longer than 65535 bytes");
    size += 2 + k.length + 2 + v.length;
  }
  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let o = 0;
  view.setUint32(o, 1, true);
  o += 4;
  view.setUint32(o, payload.length, true);
  o += 4;
  buf.set(payload, o);
  o += payload.length;
  view.setUint16(o, pairs.length, true);
  o += 2;
  for (const [k, v] of pairs) {
    view.setUint16(o, k.length, true);
    o += 2;
    buf.set(k, o);
    o += k.length;
    view.setUint16(o, v.length, true);
    o += 2;
    buf.set(v, o);
    o += v.length;
  }
  return buf;
}

/**
 * Serialize action invoke value.
 * Format: [has_labels:u8]([labels_len:u16 LE][labels])?[input...]
 *
 * has_labels is 0 or 1. labels is a JSON object; only workers whose
 * registered labels contain every key/value receive the run.
 */
export function serializeActionInvokeValue(input: Uint8Array, labels?: string): Uint8Array {
  const labelBytes = labels ? textEncoder.encode(labels) : null;
  if (labelBytes && labelBytes.length > 0xffff) {
    throw new FloError(`flo: invoke labels are ${labelBytes.length} bytes; the limit is 65535`);
  }

  const buf = new Uint8Array(1 + (labelBytes ? 2 + labelBytes.length : 0) + input.length);
  let offset = 0;
  if (labelBytes) {
    buf[offset++] = 1;
    new DataView(buf.buffer).setUint16(offset, labelBytes.length, true);
    offset += 2;
    buf.set(labelBytes, offset);
    offset += labelBytes.length;
  } else {
    buf[offset++] = 0;
  }
  buf.set(input, offset);
  return buf;
}

/**
 * Serialize the value every list/scan op reads: [limit:u32][cursor...].
 * A limit of 0 asks for the server default; the cursor is the opaque bytes
 * the previous page returned (empty for the first page).
 */
export function serializeListValue(limit: number = 0, cursor?: Uint8Array | null): Uint8Array {
  const cursorLen = cursor?.length ?? 0;
  const buf = new Uint8Array(4 + cursorLen);
  new DataView(buf.buffer).setUint32(0, limit, true);
  if (cursor && cursorLen > 0) {
    buf.set(cursor, 4);
  }
  return buf;
}

/**
 * Serialize worker register value
 * Format: [type:u8][max_concurrency:u32][process_count:u16]
 *         ([name_len:u16][name][kind:u8])*
 *         [has_metadata:u8][metadata_len:u16][metadata]?
 *         [has_machine_id:u8][machine_id_len:u16][machine_id]?
 */
export function serializeWorkerRegisterValue(
  taskTypes: string[],
  opts?: {
    workerType?: number;
    maxConcurrency?: number;
    processes?: Array<{ name: string; kind: number }>;
    metadata?: string;
    machineId?: string;
  }
): Uint8Array {
  const workerType = opts?.workerType ?? 0; // default: action
  const maxConcurrency = opts?.maxConcurrency ?? 10;
  const metadata = opts?.metadata;
  const machineId = opts?.machineId;

  // Build process list: use explicit processes or legacy taskTypes
  let processes = opts?.processes;
  if (!processes || processes.length === 0) {
    processes = taskTypes.map((name) => ({ name, kind: 0 })); // kind=0 = action
  }

  // Calculate size
  let size = 1 + 4 + 2; // type + max_concurrency + process_count
  for (const p of processes) {
    size += 2 + textEncoder.encode(p.name).length + 1; // name_len + name + kind
  }
  size += 1; // has_metadata
  if (metadata) {
    size += 2 + textEncoder.encode(metadata).length;
  }
  size += 1; // has_machine_id
  if (machineId) {
    size += 2 + textEncoder.encode(machineId).length;
  }

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // type
  buf[offset++] = workerType;

  // max_concurrency
  view.setUint32(offset, maxConcurrency, true);
  offset += 4;

  // process list
  view.setUint16(offset, processes.length, true);
  offset += 2;
  for (const p of processes) {
    const nameBytes = textEncoder.encode(p.name);
    view.setUint16(offset, nameBytes.length, true);
    offset += 2;
    buf.set(nameBytes, offset);
    offset += nameBytes.length;
    buf[offset++] = p.kind;
  }

  // metadata
  if (metadata) {
    buf[offset++] = 1;
    const metaBytes = textEncoder.encode(metadata);
    view.setUint16(offset, metaBytes.length, true);
    offset += 2;
    buf.set(metaBytes, offset);
    offset += metaBytes.length;
  } else {
    buf[offset++] = 0;
  }

  // machine_id
  if (machineId) {
    buf[offset++] = 1;
    const machBytes = textEncoder.encode(machineId);
    view.setUint16(offset, machBytes.length, true);
    offset += 2;
    buf.set(machBytes, offset);
    offset += machBytes.length;
  } else {
    buf[offset++] = 0;
  }

  return buf.subarray(0, offset);
}

/**
 * Serialize worker heartbeat value.
 * Format: [current_load:u32]
 */
export function serializeWorkerHeartbeatValue(currentLoad: number): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, currentLoad, true);
  return buf;
}

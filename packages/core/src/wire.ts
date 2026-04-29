/**
 * Flo wire protocol serialization and deserialization.
 */

import {
  IncompleteResponseError,
  InvalidMagicError,
  KeyTooLargeError,
  NamespaceTooLargeError,
  UnsupportedVersionError,
  ValueTooLargeError,
} from "./errors.js";
import {
  DequeueResult,
  HEADER_SIZE,
  type KVEntry,
  MAGIC,
  MAX_KEY_SIZE,
  MAX_NAMESPACE_SIZE,
  MAX_VALUE_SIZE,
  type Message,
  type OpCode,
  OptionTag,
  type RawResponse,
  ScanResult,
  type StatusCode,
  VERSION,
  type VersionEntry,
} from "./types.js";

/**
 * CRC32 lookup table (IEEE polynomial).
 */
const CRC32_TABLE = new Uint32Array(256);

// Initialize CRC32 table
(function initCRC32Table() {
  const polynomial = 0xedb88320;
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >>> 1) ^ polynomial;
      } else {
        crc = crc >>> 1;
      }
    }
    CRC32_TABLE[i] = crc >>> 0;
  }
})();

/**
 * Compute CRC32 checksum for data.
 */
function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ data[i]!) & 0xff]!;
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Compute CRC32 for header (excluding crc32 field) + payload.
 */
export function computeCRC32(header: Uint8Array, payload: Uint8Array): number {
  // CRC32 is computed over: header[0..16] + header[20..32] + payload
  const totalLen = 16 + 12 + payload.length;
  const combined = new Uint8Array(totalLen);
  combined.set(header.subarray(0, 16), 0);
  combined.set(header.subarray(20, 32), 16);
  combined.set(payload, 28);
  return crc32(combined);
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
   * Add a u32 option (little-endian).
   */
  addU32(tag: OptionTag, value: number): this {
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

  // Calculate payload size
  const payloadLen =
    2 +
    namespace.length + // namespace: [len:u16][data]
    2 +
    key.length + // key: [len:u16][data]
    4 +
    value.length + // value: [len:u32][data]
    2 +
    options.length; // options: [len:u16][data]

  // Allocate buffer for header + payload
  const buf = new Uint8Array(HEADER_SIZE + payloadLen);
  const view = new DataView(buf.buffer);

  // Build header (without CRC32)
  view.setUint32(0, MAGIC, true); // magic
  view.setUint32(4, payloadLen, true); // payload_length
  view.setBigUint64(8, requestId, true); // request_id
  // CRC32 at [16:20] - filled later
  view.setUint16(20, opCode, true); // op_code (u16 LE)
  buf[22] = VERSION; // version
  buf[23] = 0; // flags
  // bytes 24-31 are reserved (already zero)

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

  // Value
  view.setUint32(offset, value.length, true);
  offset += 4;
  buf.set(value, offset);
  offset += value.length;

  // Options
  view.setUint16(offset, options.length, true);
  offset += 2;
  buf.set(options, offset);

  // Compute and fill CRC32
  const crcValue = computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE));
  view.setUint32(16, crcValue, true);

  return buf;
}

/**
 * Parse a response header.
 * Returns [status, dataLen, requestId, crc].
 */
export function parseResponseHeader(
  header: Uint8Array
): [StatusCode, number, bigint, number] {
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
  const status = header[21] as StatusCode;

  if (version !== VERSION) {
    throw new UnsupportedVersionError(version);
  }

  return [status, dataLen, requestId, crcValue];
}

/**
 * Parse a raw response from header + data.
 */
export function parseRawResponse(
  header: Uint8Array,
  data: Uint8Array
): RawResponse {
  const [status, , requestId] = parseResponseHeader(header);
  return { status, data, requestId };
}

/**
 * Parse scan response data.
 */
export function parseScanResponse(data: Uint8Array): ScanResult {
  if (data.length < 9) {
    throw new IncompleteResponseError("scan response too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // has_more
  const hasMore = data[offset]! !== 0;
  offset++;

  // cursor
  const cursorLen = view.getUint32(offset, true);
  offset += 4;

  if (data.length < offset + cursorLen) {
    throw new IncompleteResponseError("scan response truncated at cursor");
  }

  let cursor: Uint8Array | null = null;
  if (cursorLen > 0) {
    cursor = data.slice(offset, offset + cursorLen);
  }
  offset += cursorLen;

  // count
  if (data.length < offset + 4) {
    throw new IncompleteResponseError("scan response truncated at count");
  }

  const count = view.getUint32(offset, true);
  offset += 4;

  // entries
  const entries: KVEntry[] = [];
  for (let i = 0; i < count; i++) {
    // key
    if (data.length < offset + 2) {
      throw new IncompleteResponseError("scan response truncated at key length");
    }

    const keyLen = view.getUint16(offset, true);
    offset += 2;

    if (data.length < offset + keyLen) {
      throw new IncompleteResponseError("scan response truncated at key data");
    }

    const key = data.slice(offset, offset + keyLen);
    offset += keyLen;

    // value
    if (data.length < offset + 4) {
      throw new IncompleteResponseError(
        "scan response truncated at value length"
      );
    }

    const valueLen = view.getUint32(offset, true);
    offset += 4;

    let value: Uint8Array | null = null;
    if (valueLen > 0) {
      if (data.length < offset + valueLen) {
        throw new IncompleteResponseError(
          "scan response truncated at value data"
        );
      }
      value = data.slice(offset, offset + valueLen);
      offset += valueLen;
    }

    entries.push({ key, value });
  }

  return { entries, cursor, hasMore };
}

/**
 * Parse history response data.
 */
export function parseHistoryResponse(data: Uint8Array): VersionEntry[] {
  if (data.length < 4) {
    throw new IncompleteResponseError("history response too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  const count = view.getUint32(offset, true);
  offset += 4;

  const entries: VersionEntry[] = [];
  for (let i = 0; i < count; i++) {
    if (data.length < offset + 20) {
      throw new IncompleteResponseError("history response truncated at entry");
    }

    const version = view.getBigUint64(offset, true);
    offset += 8;

    const timestamp = view.getBigInt64(offset, true);
    offset += 8;

    const valueLen = view.getUint32(offset, true);
    offset += 4;

    if (data.length < offset + valueLen) {
      throw new IncompleteResponseError(
        "history response truncated at value data"
      );
    }

    const value = data.slice(offset, offset + valueLen);
    offset += valueLen;

    entries.push({ version, timestamp, value });
  }

  return entries;
}

/**
 * Parse dequeue response data.
 */
export function parseDequeueResponse(data: Uint8Array): DequeueResult {
  if (data.length < 4) {
    throw new IncompleteResponseError("dequeue response too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  const count = view.getUint32(offset, true);
  offset += 4;

  const messages: Message[] = [];
  for (let i = 0; i < count; i++) {
    if (data.length < offset + 12) {
      throw new IncompleteResponseError("dequeue response truncated at message");
    }

    const seq = view.getBigUint64(offset, true);
    offset += 8;

    const payloadLen = view.getUint32(offset, true);
    offset += 4;

    if (data.length < offset + payloadLen) {
      throw new IncompleteResponseError(
        "dequeue response truncated at payload"
      );
    }

    const payload = data.slice(offset, offset + payloadLen);
    offset += payloadLen;

    messages.push({ seq, payload });
  }

  return { messages };
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

/**
 * Serialize sequence numbers for ack/nack.
 */
export function serializeSeqs(seqs: bigint[]): Uint8Array {
  const buf = new Uint8Array(4 + seqs.length * 8);
  const view = new DataView(buf.buffer);

  view.setUint32(0, seqs.length, true);
  let offset = 4;
  for (const seq of seqs) {
    view.setBigUint64(offset, seq, true);
    offset += 8;
  }

  return buf;
}

// ============================================================================
// ACTION/WORKER SERIALIZATION
// ============================================================================

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Serialize action register value.
 * Format: [action_type:u8][timeout_ms:u32][max_retries:u32]
 *         [has_desc:u8][desc_len:u16]?[desc]?
 *         [has_wasm_module:u8]...(all optional fields as u8=0)
 */
export function serializeActionRegisterValue(
  actionType: number,
  timeoutMs: number,
  maxRetries: number,
  description?: string
): Uint8Array {
  const descBytes = description ? textEncoder.encode(description) : null;
  const size = 9 + (descBytes ? 3 + descBytes.length : 1) + 5; // 5 optional fields as u8=0

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // action_type
  buf[offset++] = actionType & 0xff;

  // timeout_ms
  view.setUint32(offset, timeoutMs, true);
  offset += 4;

  // max_retries
  view.setUint32(offset, maxRetries, true);
  offset += 4;

  // description (optional)
  if (descBytes) {
    buf[offset++] = 1; // has_desc
    view.setUint16(offset, descBytes.length, true);
    offset += 2;
    buf.set(descBytes, offset);
    offset += descBytes.length;
  } else {
    buf[offset++] = 0;
  }

  // Optional fields (all 0)
  buf[offset++] = 0; // wasm_module
  buf[offset++] = 0; // wasm_entrypoint
  buf[offset++] = 0; // wasm_memory_limit
  buf[offset++] = 0; // trigger_stream
  buf[offset++] = 0; // trigger_group

  return buf.subarray(0, offset);
}

/**
 * Serialize action invoke value.
 * Format: [priority:u8][delay_ms:i64][has_caller:u8]
 *         [has_idempotency_key:u8][key_len:u16]?[key]?[input...]
 */
export function serializeActionInvokeValue(
  input: Uint8Array,
  priority: number = 10,
  idempotencyKey?: string
): Uint8Array {
  const keyBytes = idempotencyKey ? textEncoder.encode(idempotencyKey) : null;
  const size = 1 + 8 + 1 + (keyBytes ? 1 + 2 + keyBytes.length : 1) + input.length;

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // priority
  buf[offset++] = priority & 0xff;

  // delay_ms (default 0)
  view.setBigInt64(offset, 0n, true);
  offset += 8;

  // caller_id (none)
  buf[offset++] = 0;

  // idempotency_key
  if (keyBytes) {
    buf[offset++] = 1;
    view.setUint16(offset, keyBytes.length, true);
    offset += 2;
    buf.set(keyBytes, offset);
    offset += keyBytes.length;
  } else {
    buf[offset++] = 0;
  }

  // input
  buf.set(input, offset);
  offset += input.length;

  return buf.subarray(0, offset);
}

/**
 * Serialize action list value.
 * Format: [limit:u32]
 */
export function serializeActionListValue(limit: number = 100): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, limit, true);
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
 * Serialize worker await value.
 * Format: [count:u32][task_type_len:u16][task_type]...
 */
export function serializeWorkerAwaitValue(taskTypes: string[]): Uint8Array {
  // Calculate size
  let size = 4; // count
  for (const tt of taskTypes) {
    size += 2 + textEncoder.encode(tt).length;
  }

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // count
  view.setUint32(offset, taskTypes.length, true);
  offset += 4;

  // task types
  for (const tt of taskTypes) {
    const ttBytes = textEncoder.encode(tt);
    view.setUint16(offset, ttBytes.length, true);
    offset += 2;
    buf.set(ttBytes, offset);
    offset += ttBytes.length;
  }

  return buf.subarray(0, offset);
}

/**
 * Serialize worker touch value.
 * Format: [action_name_len:u16][action_name][task_id_len:u16][task_id][extend_ms:u32]
 */
export function serializeWorkerTouchValue(
  actionName: string,
  taskId: string,
  extendMs: number = 30000
): Uint8Array {
  const actionNameBytes = textEncoder.encode(actionName);
  const taskIdBytes = textEncoder.encode(taskId);
  const buf = new Uint8Array(2 + actionNameBytes.length + 2 + taskIdBytes.length + 4);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // action_name
  view.setUint16(offset, actionNameBytes.length, true);
  offset += 2;
  buf.set(actionNameBytes, offset);
  offset += actionNameBytes.length;

  // task_id
  view.setUint16(offset, taskIdBytes.length, true);
  offset += 2;
  buf.set(taskIdBytes, offset);
  offset += taskIdBytes.length;

  // extend_ms
  view.setUint32(offset, extendMs, true);

  return buf;
}

/**
 * Serialize worker complete value.
 * Format: [action_name_len:u16][action_name][task_id_len:u16][task_id][outcome_len:u16][outcome][result_len:u16][result]
 */
export function serializeWorkerCompleteValue(
  actionName: string,
  taskId: string,
  result: Uint8Array,
  outcome: string = "success"
): Uint8Array {
  const actionNameBytes = textEncoder.encode(actionName);
  const taskIdBytes = textEncoder.encode(taskId);
  const outcomeBytes = textEncoder.encode(outcome);
  const buf = new Uint8Array(
    2 + actionNameBytes.length +
    2 + taskIdBytes.length +
    2 + outcomeBytes.length +
    2 + result.length
  );
  const view = new DataView(buf.buffer);
  let offset = 0;

  // action_name
  view.setUint16(offset, actionNameBytes.length, true);
  offset += 2;
  buf.set(actionNameBytes, offset);
  offset += actionNameBytes.length;

  // task_id
  view.setUint16(offset, taskIdBytes.length, true);
  offset += 2;
  buf.set(taskIdBytes, offset);
  offset += taskIdBytes.length;

  // outcome
  view.setUint16(offset, outcomeBytes.length, true);
  offset += 2;
  buf.set(outcomeBytes, offset);
  offset += outcomeBytes.length;

  // result
  view.setUint16(offset, result.length, true);
  offset += 2;
  buf.set(result, offset);

  return buf;
}

/**
 * Serialize worker fail value.
 * Format: [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
 */
export function serializeWorkerFailValue(
  actionName: string,
  taskId: string,
  errorMessage: string,
  retry: boolean = true
): Uint8Array {
  const actionNameBytes = textEncoder.encode(actionName);
  const taskIdBytes = textEncoder.encode(taskId);
  const errorBytes = textEncoder.encode(errorMessage);
  const buf = new Uint8Array(2 + actionNameBytes.length + 2 + taskIdBytes.length + 1 + errorBytes.length);
  const view = new DataView(buf.buffer);
  let offset = 0;

  // action_name
  view.setUint16(offset, actionNameBytes.length, true);
  offset += 2;
  buf.set(actionNameBytes, offset);
  offset += actionNameBytes.length;

  // task_id
  view.setUint16(offset, taskIdBytes.length, true);
  offset += 2;
  buf.set(taskIdBytes, offset);
  offset += taskIdBytes.length;

  // retry
  buf[offset++] = retry ? 1 : 0;

  // error_message
  buf.set(errorBytes, offset);

  return buf;
}

/**
 * Serialize worker list value.
 * Format: [limit:u32]
 */
export function serializeWorkerListValue(limit: number = 100): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, limit, true);
  return buf;
}

/**
 * Parse task assignment from server response.
 * Format: [task_id_len:u16][task_id][task_type_len:u16][task_type]
 *         [created_at:i64][attempt:u32][payload...]
 */
export function parseTaskAssignment(data: Uint8Array): import("./types.js").TaskAssignment {
  if (data.length < 10) {
    throw new IncompleteResponseError("task assignment too short");
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // task_id
  const taskIdLen = view.getUint16(offset, true);
  offset += 2;
  if (data.length < offset + taskIdLen) {
    throw new IncompleteResponseError("task assignment: missing task_id");
  }
  const taskId = textDecoder.decode(data.subarray(offset, offset + taskIdLen));
  offset += taskIdLen;

  // task_type
  if (data.length < offset + 2) {
    throw new IncompleteResponseError("task assignment: missing task_type length");
  }
  const taskTypeLen = view.getUint16(offset, true);
  offset += 2;
  if (data.length < offset + taskTypeLen) {
    throw new IncompleteResponseError("task assignment: missing task_type");
  }
  const taskType = textDecoder.decode(data.subarray(offset, offset + taskTypeLen));
  offset += taskTypeLen;

  // created_at
  if (data.length < offset + 8) {
    throw new IncompleteResponseError("task assignment: missing created_at");
  }
  const createdAt = view.getBigInt64(offset, true);
  offset += 8;

  // attempt
  if (data.length < offset + 4) {
    throw new IncompleteResponseError("task assignment: missing attempt");
  }
  const attempt = view.getUint32(offset, true);
  offset += 4;

  // payload (rest of data)
  const payload = data.subarray(offset);

  return {
    taskId,
    taskType,
    payload,
    createdAt,
    attempt,
  };
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

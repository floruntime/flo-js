/**
 * Every KV op is a table op: the request is [namespace][key][body], the body
 * coded by the op's schema, field by field in order. An optional field is a
 * presence byte then its value; a field with a default is sent present. A
 * byte string or list has a length prefix as wide as its bound needs. An
 * answer is the op's response fields in order, read whole.
 */

import { BlockTooLongError, FloError, IncompleteResponseError } from "./errors.js";
import {
  Condition,
  type GetOptions,
  type GetResult,
  type JsonGetResult,
  type KVCommitResult,
  type KVIncrResult,
  MAX_KV_VALUE_SIZE,
  MAX_WAIT_MS,
  OpCode,
  type PutOptions,
  type PutResult,
  type ScanResult,
  type VersionEntry,
  type HistoryResult,
  WaitFor,
  type WorkerInfo,
  type WorkerInfoResult,
  type WorkerProcess,
  type ProcessKind,
  type WorkerListResult,
  type WorkerStatus,
  type WorkerType,
  type ActionInfo,
  type PeekedMessage,
  type PeekResult,
  type StreamGroupInfo,
  type StreamGroupListResult,
  type StreamGroupMember,
  type StreamGroupMembersResult,
  type TaskAssignment,
  type TaskDelivery,
} from "./types.js";

/** The longest routing key; its length prefix is a u16. */
export const MAX_ROUTING_KEY = 4096;
/** The most keys one kv.mget takes. */
export const MAX_MGET_KEYS = 256;
/** The most names one kv.scan page holds. */
export const MAX_SCAN_LIMIT = 1024;
/** The most versions one kv.history answer holds. */
export const MAX_HISTORY_LIMIT = 65;
/** The longest TTL in ms: u64 max in nanoseconds. */
export const MAX_TTL_MS = 18_446_744_073_709n;
/** The longest JSON path. */
export const MAX_JSON_PATH = 1024;
/** The longest JSON document kv.json_set takes. */
export const MAX_JSON_DOCUMENT = 252_819;
/** The longest scan cursor: the shard (u16), then the longest key. */
const MAX_SCAN_CURSOR = 2 + 4096;
/** The most workers one worker.list page holds. */
export const MAX_WORKER_LIST_LIMIT = 1024;
/** The longest worker id, and so the longest worker.list cursor. */
const MAX_WORKER_ID = 4096;
/** The longest key in a kv.mget. */
export const MAX_MGET_KEY = 4096;
/**
 * The most bytes a kv.mget request's key elements (each u16 length + key)
 * may total: what fits one request frame after the count and the largest
 * routing key.
 */
export const MAX_MGET_KEYS_BYTES = 253_848;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

const TABLE_OPS: ReadonlySet<OpCode> = new Set([
  OpCode.KVPut,
  OpCode.KVGet,
  OpCode.KVMGet,
  OpCode.KVDelete,
  OpCode.KVScan,
  OpCode.KVHistory,
  OpCode.KVIncr,
  OpCode.KVJsonGet,
  OpCode.KVJsonSet,
  OpCode.KVJsonDel,
  OpCode.KVBeginTxn,
  OpCode.KVCommitTxn,
  OpCode.KVRollbackTxn,
  OpCode.KVExpire,
  OpCode.KVExists,
  OpCode.ActionStatus,
  OpCode.ActionAwait,
  OpCode.ActionComplete,
  OpCode.ActionFail,
  OpCode.ActionTouch,
  OpCode.ActionInfo,
  OpCode.WorkerList,
  OpCode.WorkerInfo,
  OpCode.QueuePeek,
  OpCode.StreamGroupList,
  OpCode.StreamGroupMembers,
  OpCode.ProcessingRemove,
]);

/** Whether `op`'s request carries a schema-coded body, not a value and options. */
export function isTableOp(op: OpCode): boolean {
  return TABLE_OPS.has(op);
}

class BodyWriter {
  private readonly parts: number[] = [];
  u8(v: number): void {
    this.parts.push(v & 0xff);
  }
  u16(v: number): void {
    this.parts.push(v & 0xff, (v >>> 8) & 0xff);
  }
  u32(v: number): void {
    this.u16(v & 0xffff);
    this.u16(v >>> 16);
  }
  u64(v: bigint): void {
    for (let i = 0n; i < 8n; i++) this.u8(Number((v >> (8n * i)) & 0xffn));
  }
  i64(v: bigint): void {
    this.u64(BigInt.asUintN(64, v));
  }
  bytes(b: Uint8Array): void {
    for (const x of b) this.parts.push(x);
  }
  /** A byte string with a u8 length prefix. */
  bytes8(b: Uint8Array): void {
    this.u8(b.length);
    this.bytes(b);
  }
  /** A byte string with a u16 length prefix. */
  bytes16(b: Uint8Array): void {
    this.u16(b.length);
    this.bytes(b);
  }
  /** A byte string with a u32 length prefix. */
  bytes32(b: Uint8Array): void {
    this.u32(b.length);
    this.bytes(b);
  }
  optU64(v: bigint | undefined): void {
    if (v === undefined) return this.u8(0);
    this.u8(1);
    this.u64(v);
  }
  routingKey(k: string | undefined): void {
    if (!k) return this.u8(0);
    const b = textEncoder.encode(k);
    if (b.length > MAX_ROUTING_KEY) throw new FloError(`flo: routing key is ${b.length} bytes, at most ${MAX_ROUTING_KEY}`);
    this.u8(1);
    this.bytes16(b);
  }
  /** A JSON path: defaulted, so always present. */
  jsonPath(path: string | undefined): void {
    const b = textEncoder.encode(path || "$");
    if (b.length > MAX_JSON_PATH) throw new FloError(`flo: JSON path is ${b.length} bytes, at most ${MAX_JSON_PATH}`);
    this.u8(1);
    this.bytes16(b);
  }
  build(): Uint8Array {
    return Uint8Array.from(this.parts);
  }
}

/** Reads an answer field by field; `end` refuses bytes left over. */
export class AnswerReader {
  private off = 0;
  private readonly view: DataView;
  constructor(private readonly data: Uint8Array) {
    this.view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }
  private take(n: number): number {
    if (this.off + n > this.data.length) throw new IncompleteResponseError("answer ends inside a field");
    const at = this.off;
    this.off += n;
    return at;
  }
  u8(): number {
    return this.data[this.take(1)]!;
  }
  u16(): number {
    return this.view.getUint16(this.take(2), true);
  }
  u32(): number {
    return this.view.getUint32(this.take(4), true);
  }
  u64(): bigint {
    return this.view.getBigUint64(this.take(8), true);
  }
  i64(): bigint {
    return this.view.getBigInt64(this.take(8), true);
  }
  bool(): boolean {
    const b = this.u8();
    if (b > 1) throw new IncompleteResponseError(`malformed bool ${b}`);
    return b === 1;
  }
  present(): boolean {
    return this.bool();
  }
  bytes(n: number): Uint8Array {
    const at = this.take(n);
    return this.data.slice(at, at + n);
  }
  bytes8(): Uint8Array {
    return this.bytes(this.u8());
  }
  bytes16(): Uint8Array {
    return this.bytes(this.u16());
  }
  bytes32(): Uint8Array {
    return this.bytes(this.u32());
  }
  string8(): string {
    return utf8(this.bytes8());
  }
  string16(): string {
    return utf8(this.bytes16());
  }
  end(): void {
    if (this.off !== this.data.length) {
      throw new IncompleteResponseError(`${this.data.length - this.off} bytes left over in the answer`);
    }
  }
}

function utf8(b: Uint8Array): string {
  try {
    return textDecoder.decode(b);
  } catch {
    throw new IncompleteResponseError("malformed UTF-8 string");
  }
}

const U64_MAX = (1n << 64n) - 1n;
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;

function checkU64(name: string, v: bigint | undefined): void {
  if (v !== undefined && (v < 0n || v > U64_MAX)) throw new FloError(`flo: ${name} must fit a u64, got ${v}`);
}

function checkLimit(name: string, v: number, max: number): void {
  if (!Number.isInteger(v) || v < 1 || v > max) throw new FloError(`flo: ${name} must be a whole number from 1 to ${max}, got ${v}`);
}

/** kv.get's request: wait_ms, wait_for, routing_key, txn. */
export function encodeGetBody(opts: GetOptions | undefined, txn?: bigint): Uint8Array {
  const waitMs = opts?.waitMs ?? 0;
  if (!Number.isInteger(waitMs) || waitMs < 0) throw new FloError(`flo: waitMs must be a whole number of ms from 0 to ${MAX_WAIT_MS}, got ${waitMs}`);
  if (waitMs > MAX_WAIT_MS) throw new BlockTooLongError(waitMs);
  const waitFor = opts?.waitFor ?? WaitFor.Exists;
  if (waitFor !== WaitFor.Exists && waitFor !== WaitFor.Change) throw new FloError(`flo: unknown waitFor ${waitFor}`);
  const w = new BodyWriter();
  w.u8(1);
  w.u64(BigInt(waitMs));
  w.u8(1);
  w.u8(waitFor);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.set's request: value, ttl_ms, condition, cas_version, routing_key, txn. */
export function encodeSetBody(value: Uint8Array, opts: PutOptions | undefined, txn?: bigint): Uint8Array {
  if (value.length > MAX_KV_VALUE_SIZE) throw new FloError(`flo: value is ${value.length} bytes, at most ${MAX_KV_VALUE_SIZE}`);
  const condition = opts?.condition ?? Condition.Always;
  if (condition !== Condition.Always && condition !== Condition.IfAbsent && condition !== Condition.IfPresent) {
    throw new FloError(`flo: unknown condition ${condition}`);
  }
  checkU64("ttlMs", opts?.ttlMs);
  checkU64("casVersion", opts?.casVersion);
  const w = new BodyWriter();
  w.bytes32(value);
  w.optU64(opts?.ttlMs === 0n ? undefined : opts?.ttlMs);
  w.u8(1);
  w.u8(condition);
  w.optU64(opts?.casVersion);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.delete's request: cas_version, routing_key, txn. */
export function encodeDeleteBody(opts: { casVersion?: bigint; routingKey?: string } | undefined, txn?: bigint): Uint8Array {
  checkU64("casVersion", opts?.casVersion);
  const w = new BodyWriter();
  w.optU64(opts?.casVersion);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.scan's request: limit (1 to 1024), cursor (empty for the first page). */
export function encodeScanBody(limit: number, cursor: Uint8Array | undefined): Uint8Array {
  checkLimit("limit", limit, MAX_SCAN_LIMIT);
  const c = cursor ?? new Uint8Array(0);
  if (c.length > MAX_SCAN_CURSOR) throw new FloError(`flo: scan cursor is ${c.length} bytes, at most ${MAX_SCAN_CURSOR}`);
  const w = new BodyWriter();
  w.u8(1);
  w.u32(limit);
  w.bytes16(c);
  return w.build();
}

/** kv.history's request: limit (1 to 65), before_version, routing_key. */
export function encodeHistoryBody(limit: number, opts?: { beforeVersion?: bigint; routingKey?: string }): Uint8Array {
  checkLimit("limit", limit, MAX_HISTORY_LIMIT);
  checkU64("beforeVersion", opts?.beforeVersion);
  const w = new BodyWriter();
  w.u8(1);
  w.u32(limit);
  w.optU64(opts?.beforeVersion);
  w.routingKey(opts?.routingKey);
  return w.build();
}

/**
 * Split mget's keys into runs whose bodies each fit one request frame;
 * returns each run's end index. Refuses a bad key list.
 */
export function splitMGetKeys(keys: readonly Uint8Array[], routingKey?: string): number[] {
  if (keys.length === 0 || keys.length > MAX_MGET_KEYS) throw new FloError(`flo: mget takes 1 to ${MAX_MGET_KEYS} keys, got ${keys.length}`);
  new BodyWriter().routingKey(routingKey); // refuses one too long
  const ends: number[] = [];
  let size = 0;
  keys.forEach((k, i) => {
    if (k.length > MAX_MGET_KEY) throw new FloError(`flo: mget key is ${k.length} bytes, at most ${MAX_MGET_KEY}`);
    if (size + 2 + k.length > MAX_MGET_KEYS_BYTES) {
      ends.push(i);
      size = 0;
    }
    size += 2 + k.length;
  });
  ends.push(keys.length);
  return ends;
}

/** kv.mget's request: the keys (1 to 256), routing_key for the whole batch. */
export function encodeMGetBody(keys: readonly Uint8Array[], routingKey?: string): Uint8Array {
  if (keys.length === 0 || keys.length > MAX_MGET_KEYS) throw new FloError(`flo: mget takes 1 to ${MAX_MGET_KEYS} keys, got ${keys.length}`);
  const w = new BodyWriter();
  w.u16(keys.length);
  for (const k of keys) {
    if (k.length > MAX_MGET_KEY) throw new FloError(`flo: mget key is ${k.length} bytes, at most ${MAX_MGET_KEY}`);
    w.bytes16(k);
  }
  w.routingKey(routingKey);
  return w.build();
}

/** kv.incr's request: delta (sent present, 1 by default), routing_key, txn. */
export function encodeIncrBody(opts: { delta?: bigint; routingKey?: string } | undefined, txn?: bigint): Uint8Array {
  const delta = opts?.delta ?? 1n;
  if (delta < I64_MIN || delta > I64_MAX) throw new FloError(`flo: delta must fit an i64, got ${delta}`);
  const w = new BodyWriter();
  w.u8(1);
  w.i64(delta);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.expire's request: ttl_ms (0 clears the TTL), cas_version, routing_key, txn. */
export function encodeExpireBody(
  ttlMs: bigint | number,
  opts: { casVersion?: bigint; routingKey?: string } | undefined,
  txn?: bigint
): Uint8Array {
  if (typeof ttlMs === "number" && (!Number.isInteger(ttlMs) || ttlMs < 0)) {
    throw new FloError(`flo: ttlMs must be a whole number of ms, got ${ttlMs}`);
  }
  const ttl = BigInt(ttlMs);
  if (ttl < 0n || ttl > MAX_TTL_MS) throw new FloError(`flo: ttlMs must be from 0 to ${MAX_TTL_MS}, got ${ttl}`);
  checkU64("casVersion", opts?.casVersion);
  const w = new BodyWriter();
  w.u64(ttl);
  w.optU64(opts?.casVersion);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.exists's request: routing_key, txn. */
export function encodeExistsBody(opts: { routingKey?: string } | undefined, txn?: bigint): Uint8Array {
  const w = new BodyWriter();
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.json_get's and kv.json_del's request: path ("$" by default), routing_key. */
export function encodeJsonPathBody(path: string | undefined, opts: { routingKey?: string } | undefined): Uint8Array {
  const w = new BodyWriter();
  w.jsonPath(path);
  w.routingKey(opts?.routingKey);
  return w.build();
}

/** kv.json_set's request: path ("$" by default), document, routing_key. */
export function encodeJsonSetBody(path: string | undefined, document: Uint8Array, opts: { routingKey?: string } | undefined): Uint8Array {
  if (document.length > MAX_JSON_DOCUMENT) throw new FloError(`flo: JSON document is ${document.length} bytes, at most ${MAX_JSON_DOCUMENT}`);
  const w = new BodyWriter();
  w.jsonPath(path);
  w.bytes32(document);
  w.routingKey(opts?.routingKey);
  return w.build();
}

/** kv.txn_commit's and kv.txn_rollback's request: the transaction id. */
export function encodeTxnBody(txn: bigint): Uint8Array {
  checkU64("txn", txn);
  const w = new BodyWriter();
  w.u64(txn);
  return w.build();
}

/** kv.get's answer: version, value, expires_at_ms (absent with no TTL). */
export function decodeGetAnswer(data: Uint8Array, stale = false): GetResult {
  const r = new AnswerReader(data);
  const version = r.u64();
  const value = r.bytes32();
  const expiresAtMs = r.present() ? r.i64() : null;
  r.end();
  return { version, value, expiresAtMs, stale };
}

/** kv.set's answer: its version, absent inside a transaction (0n then). */
export function decodeSetAnswer(data: Uint8Array): PutResult {
  const r = new AnswerReader(data);
  const version = r.present() ? r.u64() : 0n;
  r.end();
  return { version };
}

/** kv.delete's answer: whether the key existed, null inside a transaction. */
export function decodeDeleteAnswer(data: Uint8Array): boolean | null {
  const r = new AnswerReader(data);
  const existed = r.present() ? r.bool() : null;
  r.end();
  return existed;
}

/** kv.scan's answer: names, then the next page's cursor (null on the last page). */
export function decodeScanAnswer(data: Uint8Array, stale = false): ScanResult {
  const r = new AnswerReader(data);
  const keys: string[] = [];
  for (let n = r.u16(); n > 0; n--) keys.push(r.string16());
  const cursor = r.present() ? r.bytes16() : null;
  r.end();
  return { keys, cursor, stale };
}

/** kv.history's answer: versions, newest first, then whether older ones follow. */
export function decodeHistoryAnswer(data: Uint8Array, stale = false): HistoryResult {
  const r = new AnswerReader(data);
  const out: VersionEntry[] = [];
  for (let n = r.u8(); n > 0; n--) {
    const version = r.u64();
    const atMs = r.i64();
    const deleted = r.bool();
    const value = r.bytes32();
    out.push({ version, atMs, deleted, value });
  }
  const more = r.bool();
  r.end();
  return { versions: out, more, stale };
}

/** One kv.mget answer: the entries it holds, and the index of the first key it didn't answer. */
export interface MGetPage {
  entries: { found: boolean; version: bigint; value: Uint8Array }[];
  next: number | null;
}

/** kv.mget's answer: entries in key order, then next when the answer filled up. */
export function decodeMGetAnswer(data: Uint8Array): MGetPage {
  const r = new AnswerReader(data);
  const entries: MGetPage["entries"] = [];
  for (let n = r.u16(); n > 0; n--) {
    const found = r.bool();
    const version = r.u64();
    const value = r.bytes32();
    entries.push({ found, version, value });
  }
  const next = r.present() ? r.u16() : null;
  r.end();
  return { entries, next };
}

/** kv.incr's answer: version and value, both null inside a transaction. */
export function decodeIncrAnswer(data: Uint8Array): { version: bigint | null; value: bigint | null } {
  const r = new AnswerReader(data);
  const version = r.present() ? r.u64() : null;
  const value = r.present() ? r.i64() : null;
  r.end();
  return { version, value };
}

/** kv.incr's answer outside a transaction, where both fields are present. */
export function decodeIncrResult(data: Uint8Array): KVIncrResult {
  const { version, value } = decodeIncrAnswer(data);
  if (version === null || value === null) throw new IncompleteResponseError("kv.incr answered no value");
  return { version, value };
}

/** kv.exists's answer: exists, version (0 when absent or buffered in a transaction). */
export function decodeExistsAnswer(data: Uint8Array): { exists: boolean; version: bigint } {
  const r = new AnswerReader(data);
  const exists = r.bool();
  const version = r.u64();
  r.end();
  return { exists, version };
}

/** kv.json_get's answer: version, document. */
export function decodeJsonGetAnswer(data: Uint8Array, stale = false): JsonGetResult {
  const r = new AnswerReader(data);
  const version = r.u64();
  const value = r.bytes32();
  r.end();
  return { version, value, stale };
}

/** kv.json_set's answer: version. */
export function decodeJsonSetAnswer(data: Uint8Array): PutResult {
  const r = new AnswerReader(data);
  const version = r.u64();
  r.end();
  return { version };
}

/** kv.json_del's answer: version, null when the path was "$" and the key went. */
export function decodeJsonDelAnswer(data: Uint8Array): bigint | null {
  const r = new AnswerReader(data);
  const version = r.present() ? r.u64() : null;
  r.end();
  return version;
}

/** kv.txn_begin's answer: txn, pinned. */
export function decodeTxnBeginAnswer(data: Uint8Array): { txn: bigint; pinned: bigint } {
  const r = new AnswerReader(data);
  const txn = r.u64();
  const pinned = r.u64();
  r.end();
  return { txn, pinned };
}

/** kv.txn_commit's answer: commit_index, ops. */
export function decodeTxnCommitAnswer(data: Uint8Array): KVCommitResult {
  const r = new AnswerReader(data);
  const commitIndex = r.u64();
  const opCount = r.u16();
  r.end();
  return { commitIndex, opCount };
}

/** An answer with no fields: kv.expire's, kv.txn_rollback's. */
export function decodeEmptyAnswer(data: Uint8Array): void {
  new AnswerReader(data).end();
}

/** action.status's status values, by their wire number. */
const RUN_STATUSES = ["pending", "running", "completed", "failed", "cancelled", "timed_out"] as const;

/**
 * action.status's answer: [status:u8][created_at_ms:i64][started_at_ms?][completed_at_ms?]
 * [attempt:u32][output?: u16 length][error_message?: u16 length][worker_id?: u16 length]
 * [outcome?: u16 length], each ? a present byte first.
 */
export function decodeActionStatusAnswer(runId: string, data: Uint8Array): import("./types.js").ActionRunStatus {
  const r = new AnswerReader(data);
  const code = r.u8();
  const status = RUN_STATUSES[code];
  if (status === undefined) throw new IncompleteResponseError(`action status ${code} is not one this client knows`);
  const createdAtMs = r.i64();
  const startedAtMs = r.present() ? r.i64() : undefined;
  const completedAtMs = r.present() ? r.i64() : undefined;
  const attempt = r.u32();
  const output = r.present() ? r.bytes16() : undefined;
  const errorMessage = r.present() ? new TextDecoder().decode(r.bytes16()) : undefined;
  const workerId = r.present() ? new TextDecoder().decode(r.bytes16()) : undefined;
  const outcome = r.present() ? r.string16() : undefined;
  r.end();
  return { runId, status, createdAtMs, startedAtMs, completedAtMs, attempt, output, errorMessage, workerId, outcome };
}

/** worker.list's request: limit (1 to 1024), cursor (empty for the first page), action (empty for every worker). */
export function encodeWorkerListBody(limit: number, cursor: Uint8Array | null | undefined, action?: string): Uint8Array {
  checkLimit("limit", limit, MAX_WORKER_LIST_LIMIT);
  const c = cursor ?? new Uint8Array(0);
  if (c.length > MAX_WORKER_ID) throw new FloError(`flo: worker list cursor is ${c.length} bytes, at most ${MAX_WORKER_ID}`);
  const w = new BodyWriter();
  w.u8(1);
  w.u32(limit);
  w.bytes16(c);
  w.bytes16(textEncoder.encode(action ?? ""));
  return w.build();
}

const WORKER_TYPES = 2; // action, stream
const WORKER_STATUSES = 4; // active, idle, draining, unhealthy
const PROCESS_KINDS = 2; // action, stream_consumer

/** One worker row, as worker.list and worker.info answer it. */
function readWorkerRow(r: AnswerReader): WorkerInfo {
  const id = r.string16();
  const type = r.u8();
  if (type >= WORKER_TYPES) throw new IncompleteResponseError(`worker type ${type} is not one this client knows`);
  const status = r.u8();
  if (status >= WORKER_STATUSES) throw new IncompleteResponseError(`worker status ${status} is not one this client knows`);
  const currentLoad = r.u32();
  const maxConcurrency = r.u32();
  const tasksCompleted = r.u64();
  const tasksFailed = r.u64();
  const registeredAtMs = r.i64();
  const lastHeartbeatMs = r.i64();
  const machineId = r.present() ? new TextDecoder().decode(r.bytes8()) : undefined;
  return {
    id,
    type: type as WorkerType,
    status: status as WorkerStatus,
    currentLoad,
    maxConcurrency,
    tasksCompleted,
    tasksFailed,
    registeredAtMs,
    lastHeartbeatMs,
    machineId,
  };
}

/** worker.list's answer: the page's workers in id order, then the next page's cursor (null on the last page). */
export function decodeWorkerListAnswer(data: Uint8Array): WorkerListResult {
  const r = new AnswerReader(data);
  const workers: WorkerInfo[] = [];
  for (let n = r.u16(); n > 0; n--) workers.push(readWorkerRow(r));
  const cursor = r.present() ? r.bytes16() : null;
  r.end();
  return { workers, cursor };
}

/** worker.info's answer: the worker's row, its metadata, then its processes. */
export function decodeWorkerInfoAnswer(data: Uint8Array): WorkerInfoResult {
  const r = new AnswerReader(data);
  const worker = readWorkerRow(r);
  const metadata = r.present() ? r.bytes16() : null;
  const processes: WorkerProcess[] = [];
  for (let n = r.u16(); n > 0; n--) {
    const name = r.string16();
    const kind = r.u8();
    if (kind >= PROCESS_KINDS) throw new IncompleteResponseError(`process kind ${kind} is not one this client knows`);
    const runCount = r.u64();
    const failCount = r.u64();
    const lastRunAtMs = r.i64();
    processes.push({ name, kind: kind as ProcessKind, runCount, failCount, lastRunAtMs });
  }
  r.end();
  return { worker, metadata, processes };
}

// ── Action runs ─────────────────────────────────────────────────────────

/** The longest action name. */
export const MAX_ACTION_NAME = 256;
/** The most actions one await names. */
export const MAX_AWAIT_ACTIONS = 64;
/** The longest await request key. */
export const MAX_REQUEST_KEY = 64;
/** The longest outcome a complete names. */
export const MAX_OUTCOME = 256;
/** The longest result a complete carries, and error a fail carries. */
export const MAX_RESULT = 65_535;
/** The longest delay a fail asks for: a day. */
export const MAX_DELAY_MS = 86_400_000;

function checkLength(name: string, b: Uint8Array, max: number): void {
  if (b.length > max) throw new FloError(`flo: ${name} is ${b.length} bytes, at most ${max}`);
}

/** action.await's request: actions (tried in order), block_ms, request_key. */
export function encodeAwaitBody(actions: readonly string[], blockMs: number, requestKey?: string): Uint8Array {
  if (actions.length === 0 || actions.length > MAX_AWAIT_ACTIONS) {
    throw new FloError(`flo: an await names 1 to ${MAX_AWAIT_ACTIONS} actions, got ${actions.length}`);
  }
  if (!Number.isInteger(blockMs) || blockMs < 0) throw new FloError(`flo: blockMs must be a whole number of ms from 0 to ${MAX_WAIT_MS}, got ${blockMs}`);
  if (blockMs > MAX_WAIT_MS) throw new BlockTooLongError(blockMs);
  const w = new BodyWriter();
  w.u8(actions.length);
  for (const a of actions) {
    const b = textEncoder.encode(a);
    checkLength("action name", b, MAX_ACTION_NAME);
    w.bytes16(b);
  }
  w.u8(1);
  w.u64(BigInt(blockMs));
  if (requestKey === undefined) {
    w.u8(0);
  } else {
    const k = textEncoder.encode(requestKey);
    checkLength("request key", k, MAX_REQUEST_KEY);
    w.u8(1);
    w.bytes8(k);
  }
  return w.build();
}

/**
 * The task's id as the CLI prints and parses it: `run:deliveries:term:index:action`.
 * A run id holds no colon and the action comes last, so the action may.
 */
export function formatTaskId(t: TaskDelivery): string {
  return `${t.runId}:${t.deliveries}:${t.term}:${t.index}:${t.action}`;
}

/** A task id as formatTaskId writes it; null for one that isn't. The namespace isn't in it. */
export function parseTaskId(id: string): Omit<TaskDelivery, "namespace"> | null {
  const m = /^([^:]+):(\d+):(\d+):(\d+):(.+)$/.exec(id);
  if (!m) return null;
  const deliveries = Number(m[2]);
  const term = BigInt(m[3]!);
  const index = BigInt(m[4]!);
  if (deliveries > 0xffffffff || term > U64_MAX || index > U64_MAX) return null;
  return { runId: m[1]!, deliveries, term, index, action: m[5]! };
}

/** action.await's answer: the task, or null if no run was ready in time. */
export function decodeAwaitAnswer(data: Uint8Array): TaskAssignment | null {
  const r = new AnswerReader(data);
  if (!r.present()) {
    r.end();
    return null;
  }
  const runId = r.string16();
  const namespace = r.string8();
  const deliveries = r.u32();
  const term = r.u64();
  const index = r.u64();
  const action = r.string16();
  const createdAtMs = r.i64();
  const attempt = r.u32();
  const leaseMs = r.u64();
  const callerRunId = r.present() ? r.string16() : undefined;
  const callerWorkflowName = r.present() ? r.string16() : undefined;
  const input = r.bytes32();
  r.end();
  const delivery = { runId, namespace, deliveries, term, index, action };
  return { ...delivery, taskId: formatTaskId(delivery), createdAtMs, attempt, leaseMs, callerRunId, callerWorkflowName, input };
}

/** The fields every report starts with: action, worker, deliveries, term, index. */
function writeDelivery(w: BodyWriter, worker: string, t: TaskDelivery): void {
  const action = textEncoder.encode(t.action);
  checkLength("action name", action, MAX_ACTION_NAME);
  const id = textEncoder.encode(worker);
  checkLength("worker id", id, MAX_WORKER_ID);
  if (!Number.isInteger(t.deliveries) || t.deliveries < 0 || t.deliveries > 0xffffffff) throw new FloError(`flo: deliveries must fit a u32, got ${t.deliveries}`);
  checkU64("term", t.term);
  checkU64("index", t.index);
  w.bytes16(action);
  w.bytes16(id);
  w.u32(t.deliveries);
  w.u64(t.term);
  w.u64(t.index);
}

/** action.complete's request: the delivery, outcome ("" is success), result. */
export function encodeCompleteBody(worker: string, t: TaskDelivery, result: Uint8Array, outcome = ""): Uint8Array {
  const o = textEncoder.encode(outcome);
  checkLength("outcome", o, MAX_OUTCOME);
  checkLength("result", result, MAX_RESULT);
  const w = new BodyWriter();
  writeDelivery(w, worker, t);
  w.u8(1);
  w.bytes16(o);
  w.u8(1);
  w.bytes16(result);
  return w.build();
}

/** action.fail's request: the delivery, retry, abandoned, delay_ms, error. */
export function encodeFailBody(
  worker: string,
  t: TaskDelivery,
  error: string,
  opts?: { retry?: boolean; abandoned?: boolean; delayMs?: number }
): Uint8Array {
  const delayMs = opts?.delayMs ?? 0;
  if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > MAX_DELAY_MS) {
    throw new FloError(`flo: delayMs must be a whole number of ms from 0 to ${MAX_DELAY_MS}, got ${delayMs}`);
  }
  const e = textEncoder.encode(error);
  checkLength("error", e, MAX_RESULT);
  const w = new BodyWriter();
  writeDelivery(w, worker, t);
  w.u8(1);
  w.u8((opts?.retry ?? true) ? 1 : 0);
  w.u8(1);
  w.u8(opts?.abandoned ? 1 : 0);
  w.u8(1);
  w.u64(BigInt(delayMs));
  w.u8(1);
  w.bytes16(e);
  return w.build();
}

/** action.touch's request: the delivery alone. */
export function encodeTouchBody(worker: string, t: TaskDelivery): Uint8Array {
  const w = new BodyWriter();
  writeDelivery(w, worker, t);
  return w.build();
}

/** action.touch's answer: when the lease runs out without another touch. */
export function decodeTouchAnswer(data: Uint8Array): bigint {
  const r = new AnswerReader(data);
  const expiresAtMs = r.i64();
  r.end();
  return expiresAtMs;
}

/** action.info's answer. */
export function decodeActionInfoAnswer(data: Uint8Array): ActionInfo {
  const r = new AnswerReader(data);
  const opt = () => (r.present() ? r.u64() : undefined);
  const info: ActionInfo = {
    owner: r.string16(),
    version: r.u32(),
    timeoutMs: r.u64(),
    maxAttempts: r.u32(),
    createdAtMs: r.i64(),
    ready: r.u64(),
    readyCapped: r.bool(),
    tooManyRuns: r.bool(),
  };
  info.running = opt();
  info.completed = opt();
  info.failed = opt();
  info.timedOut = opt();
  info.latencyCount = opt();
  info.latencyAvgMs = opt();
  info.latencyP50Ms = opt();
  info.latencyP95Ms = opt();
  info.latencyMaxMs = opt();
  r.end();
  return info;
}

// ── Queue and stream reads on the leader ────────────────────────────────

/** The most messages one queue.peek shows. */
export const MAX_PEEK_LIMIT = 100;

/** queue.peek's request: limit (1 to 100). */
export function encodePeekBody(limit: number): Uint8Array {
  checkLimit("limit", limit, MAX_PEEK_LIMIT);
  const w = new BodyWriter();
  w.u8(1);
  w.u32(limit);
  return w.build();
}

/** queue.peek's answer: the next ready messages, then whether more may follow. */
export function decodePeekAnswer(data: Uint8Array): PeekResult {
  const r = new AnswerReader(data);
  const messages: PeekedMessage[] = [];
  for (let n = r.u8(); n > 0; n--) {
    const seq = r.u64();
    const priority = r.u32();
    const attempts = r.u32();
    const enqueuedAtMs = r.i64();
    const size = r.u32();
    const payload = r.bytes16();
    messages.push({ seq, priority, attempts, enqueuedAtMs, size, payload });
  }
  const more = r.bool();
  r.end();
  return { messages, more };
}

/** stream.group.list's answer: the stream's groups, then whether it has more. */
export function decodeGroupListAnswer(data: Uint8Array): StreamGroupListResult {
  const r = new AnswerReader(data);
  const groups: StreamGroupInfo[] = [];
  for (let n = r.u16(); n > 0; n--) {
    groups.push({
      name: r.string16(),
      members: r.u32(),
      pending: r.u64(),
      createdAtMs: r.i64(),
      ackTimeoutMs: r.u32(),
      maxDeliver: r.u8(),
      lastDeliveredMs: r.u64(),
      lastDeliveredSeq: r.u64(),
    });
  }
  const more = r.bool();
  r.end();
  return { groups, more };
}

/** The longest consumer group name. */
const MAX_GROUP_NAME = 4096;

/** stream.group.members's request: the group. */
export function encodeGroupMembersBody(group: string): Uint8Array {
  const g = textEncoder.encode(group);
  checkLength("group name", g, MAX_GROUP_NAME);
  const w = new BodyWriter();
  w.bytes16(g);
  return w.build();
}

const MEMBER_STATES = ["active", "leaving"] as const;

/** stream.group.members's answer: the group's members, then whether it has more. */
export function decodeGroupMembersAnswer(data: Uint8Array): StreamGroupMembersResult {
  const r = new AnswerReader(data);
  const members: StreamGroupMember[] = [];
  for (let n = r.u16(); n > 0; n--) {
    const id = r.string16();
    const code = r.u8();
    const state = MEMBER_STATES[code];
    if (state === undefined) throw new IncompleteResponseError(`member state ${code} is not one this client knows`);
    const pending = r.u32();
    const joinedAtMs = r.i64();
    const lastActiveMs = r.present() ? r.i64() : undefined;
    members.push({ id, state, pending, joinedAtMs, lastActiveMs });
  }
  const more = r.bool();
  r.end();
  return { members, more };
}

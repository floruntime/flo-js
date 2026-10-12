/**
 * kv.get and kv.set are table ops: the request is [namespace][key][body],
 * the body coded by the op's schema, field by field in order. An optional
 * field is a presence byte then its value; a field with a default is sent
 * present. A byte string has a length prefix as wide as its bound needs.
 */

import { BlockTooLongError, FloError, IncompleteResponseError } from "./errors.js";
import {
  Condition,
  type GetOptions,
  type GetResult,
  MAX_KV_VALUE_SIZE,
  MAX_WAIT_MS,
  OpCode,
  type PutOptions,
  type PutResult,
  WaitFor,
} from "./types.js";

/** The longest routing key; its length prefix is a u16. */
const MAX_ROUTING_KEY = 4096;

const textEncoder = new TextEncoder();

/** Whether `op`'s request carries a schema-coded body, not a value and options. */
export function isTableOp(op: OpCode): boolean {
  return op === OpCode.KVGet || op === OpCode.KVPut;
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
  bytes(b: Uint8Array): void {
    for (const x of b) this.parts.push(x);
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
    this.u16(b.length);
    this.bytes(b);
  }
  build(): Uint8Array {
    return Uint8Array.from(this.parts);
  }
}

const U64_MAX = (1n << 64n) - 1n;

function checkU64(name: string, v: bigint | undefined): void {
  if (v !== undefined && (v < 0n || v > U64_MAX)) throw new FloError(`flo: ${name} must fit a u64, got ${v}`);
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
  w.u32(value.length);
  w.bytes(value);
  w.optU64(opts?.ttlMs === 0n ? undefined : opts?.ttlMs);
  w.u8(1);
  w.u8(condition);
  w.optU64(opts?.casVersion);
  w.routingKey(opts?.routingKey);
  w.optU64(txn);
  return w.build();
}

/** kv.get's answer: [version:u64][value_len:u32][value], whole. */
export function decodeGetAnswer(data: Uint8Array): GetResult {
  if (data.length < 12) throw new IncompleteResponseError();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const n = view.getUint32(8, true);
  if (data.length !== 12 + n) throw new IncompleteResponseError();
  return { version: view.getBigUint64(0, true), value: data.slice(12) };
}

/** kv.set's answer: its version, absent inside a transaction (0n then). */
export function decodeSetAnswer(data: Uint8Array): PutResult {
  if (data.length === 1 && data[0] === 0) return { version: 0n };
  if (data.length === 9 && data[0] === 1) {
    return { version: new DataView(data.buffer, data.byteOffset, data.byteLength).getBigUint64(1, true) };
  }
  throw new IncompleteResponseError();
}

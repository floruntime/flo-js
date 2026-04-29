/**
 * Flo KV Per-Shard Transactions
 *
 * Transactions are pinned to a single partition (chosen by the routing key
 * passed to `begin`). Every key written or read inside the transaction must
 * hash to the same partition; otherwise the server returns a
 * "kv_txn_cross_shard" error.
 *
 * Server-enforced caps:
 *   - 256 ops per transaction
 *   - 1 MiB total payload across buffered writes
 *
 * The following operations are NOT supported inside a transaction and throw
 * `TxnUnsupportedOpError` without a server round-trip: `scan`, `mget`,
 * `jsonGet`, `jsonSet`, `jsonDel`, `history`.
 */

import { createServerError } from "./errors.js";
import {
  type GetResult,
  type KVCommitResult,
  OpCode,
  OptionTag,
  type PutOptions,
  type PutResult,
  type RawResponse,
  StatusCode,
} from "./types.js";
import { OptionsBuilder } from "./wire.js";

const textEncoder = new TextEncoder();

export interface TxnRequestSender {
  sendRequest(
    opCode: OpCode,
    namespace: string,
    key: Uint8Array,
    value: Uint8Array,
    options: Uint8Array
  ): Promise<RawResponse>;
}

export class TxnUnsupportedOpError extends Error {
  constructor(op: string) {
    super(`${op} is not supported inside a KV transaction`);
    this.name = "TxnUnsupportedOpError";
  }
}

export class TxnFinishedError extends Error {
  constructor() {
    super("transaction already committed or rolled back");
    this.name = "TxnFinishedError";
  }
}

/**
 * Per-shard KV transaction handle. Operations are buffered on the server's
 * pinned shard until {@link Transaction.commit} or {@link Transaction.rollback}
 * is called. Use with `try/finally` for safe cleanup:
 *
 * ```ts
 * const txn = await client.kv.begin("user:123");
 * try {
 *   await txn.put("user:123:name", new TextEncoder().encode("Jane"));
 *   await txn.incr("user:123:visits", 1n);
 *   await txn.commit();
 * } catch (err) {
 *   await txn.rollback();
 *   throw err;
 * }
 * ```
 */
export class Transaction {
  private done = false;

  constructor(
    private readonly sender: TxnRequestSender,
    private readonly namespace: string,
    private readonly routingKey: string,
    /** Server-assigned transaction id. */
    public readonly id: bigint,
    /** Partition hash this transaction is bound to. */
    public readonly pinnedHash: bigint
  ) {}

  private txnOptions(): OptionsBuilder {
    const b = new OptionsBuilder();
    if (this.routingKey) {
      b.addBytes(OptionTag.RoutingKey, textEncoder.encode(this.routingKey));
    }
    b.addU64(OptionTag.TxnID, this.id);
    return b;
  }

  private checkAlive(): void {
    if (this.done) throw new TxnFinishedError();
  }

  /** Buffer a put inside the transaction. */
  async put(
    key: string,
    value: Uint8Array,
    opts?: PutOptions
  ): Promise<PutResult> {
    this.checkAlive();
    const builder = this.txnOptions();
    if (opts?.ttlSeconds !== undefined) {
      builder.addU64(OptionTag.TTLSeconds, opts.ttlSeconds);
    }
    if (opts?.casVersion !== undefined) {
      builder.addU64(OptionTag.CASVersion, opts.casVersion);
    }
    if (opts?.ifNotExists) builder.addFlag(OptionTag.IfNotExists);
    if (opts?.ifExists) builder.addFlag(OptionTag.IfExists);
    const resp = await this.sender.sendRequest(
      OpCode.KVPut,
      this.namespace,
      textEncoder.encode(key),
      value,
      builder.build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    let version = 0n;
    if (resp.data.length >= 8) {
      const view = new DataView(
        resp.data.buffer,
        resp.data.byteOffset,
        resp.data.byteLength
      );
      version = view.getBigUint64(0, true);
    }
    return { version };
  }

  /** Read a key inside the transaction (sees buffered writes). */
  async get(key: string): Promise<GetResult | null> {
    this.checkAlive();
    const resp = await this.sender.sendRequest(
      OpCode.KVGet,
      this.namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      this.txnOptions().build()
    );
    if (resp.status === StatusCode.NotFound) return null;
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length < 8) return { value: new Uint8Array(0), version: 0n };
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    return { value: resp.data.subarray(8), version: view.getBigUint64(0, true) };
  }

  /** Buffer a delete inside the transaction. */
  async delete(key: string): Promise<void> {
    this.checkAlive();
    const resp = await this.sender.sendRequest(
      OpCode.KVDelete,
      this.namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      this.txnOptions().build()
    );
    if (resp.status !== StatusCode.OK && resp.status !== StatusCode.NotFound) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /** Buffer an atomic counter increment inside the transaction. */
  async incr(key: string, delta: bigint = 1n): Promise<bigint> {
    this.checkAlive();
    const value = new Uint8Array(8);
    new DataView(value.buffer).setBigInt64(0, delta, true);
    const resp = await this.sender.sendRequest(
      OpCode.KVIncr,
      this.namespace,
      textEncoder.encode(key),
      value,
      this.txnOptions().build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length < 8) return 0n;
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    return view.getBigInt64(0, true);
  }

  /** Update the TTL on an existing key inside the transaction. */
  async touch(key: string, ttlSeconds: bigint): Promise<void> {
    this.checkAlive();
    const value = new Uint8Array(8);
    new DataView(value.buffer).setBigUint64(0, ttlSeconds, true);
    const resp = await this.sender.sendRequest(
      OpCode.KVTouch,
      this.namespace,
      textEncoder.encode(key),
      value,
      this.txnOptions().build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /** Remove the TTL on a key inside the transaction. */
  async persist(key: string): Promise<void> {
    this.checkAlive();
    const resp = await this.sender.sendRequest(
      OpCode.KVPersist,
      this.namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      this.txnOptions().build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /** Check key existence inside the transaction. */
  async exists(key: string): Promise<boolean> {
    this.checkAlive();
    const resp = await this.sender.sendRequest(
      OpCode.KVExists,
      this.namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      this.txnOptions().build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    // Wire body: [version:u64 LE][1 byte 0/1]
    if (resp.data.length < 9) return false;
    return resp.data[8] === 1;
  }

  // ── Disallowed inside a transaction ───────────────────────────────

  async scan(): Promise<never> {
    throw new TxnUnsupportedOpError("scan");
  }

  async mget(): Promise<never> {
    throw new TxnUnsupportedOpError("mget");
  }

  async jsonGet(): Promise<never> {
    throw new TxnUnsupportedOpError("jsonGet");
  }

  async jsonSet(): Promise<never> {
    throw new TxnUnsupportedOpError("jsonSet");
  }

  async jsonDel(): Promise<never> {
    throw new TxnUnsupportedOpError("jsonDel");
  }

  async history(): Promise<never> {
    throw new TxnUnsupportedOpError("history");
  }

  // ── Lifecycle ─────────────────────────────────────────────────────

  /**
   * Atomically apply all buffered operations. After commit returns the
   * transaction is closed; further operations throw {@link TxnFinishedError}.
   */
  async commit(): Promise<KVCommitResult> {
    this.checkAlive();
    this.done = true;
    const builder = new OptionsBuilder();
    if (this.routingKey) {
      builder.addBytes(OptionTag.RoutingKey, textEncoder.encode(this.routingKey));
    }
    builder.addU64(OptionTag.TxnID, this.id);
    const resp = await this.sender.sendRequest(
      OpCode.KVCommitTxn,
      this.namespace,
      textEncoder.encode(this.routingKey),
      new Uint8Array(0),
      builder.build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    // Wire body: [variant:u8=1][commit_index:u64 LE][op_count:u16 LE]
    if (resp.data.length < 11) {
      throw new Error(`flo: short KV commit reply (${resp.data.length} bytes)`);
    }
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    return {
      commitIndex: view.getBigUint64(1, true),
      opCount: view.getUint16(9, true),
    };
  }

  /**
   * Discard the buffered operations without committing. Idempotent: calling
   * rollback after commit (or vice versa) is a no-op.
   */
  async rollback(): Promise<void> {
    if (this.done) return;
    this.done = true;
    const builder = new OptionsBuilder();
    if (this.routingKey) {
      builder.addBytes(OptionTag.RoutingKey, textEncoder.encode(this.routingKey));
    }
    builder.addU64(OptionTag.TxnID, this.id);
    const resp = await this.sender.sendRequest(
      OpCode.KVRollbackTxn,
      this.namespace,
      textEncoder.encode(this.routingKey),
      new Uint8Array(0),
      builder.build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }
}

/** Open a new per-shard KV transaction. */
export async function beginTxn(
  sender: TxnRequestSender,
  namespace: string,
  routingKey: string
): Promise<Transaction> {
  const builder = new OptionsBuilder();
  if (routingKey) {
    builder.addBytes(OptionTag.RoutingKey, textEncoder.encode(routingKey));
  }
  const resp = await sender.sendRequest(
    OpCode.KVBeginTxn,
    namespace,
    textEncoder.encode(routingKey),
    new Uint8Array(0),
    builder.build()
  );
  if (resp.status !== StatusCode.OK) {
    throw createServerError(resp.status, resp.data);
  }
  // Wire body: [variant:u8=0][txn_id:u64 LE][pinned_hash:u64 LE]
  if (resp.data.length < 17) {
    throw new Error(`flo: short KV begin reply (${resp.data.length} bytes)`);
  }
  const view = new DataView(
    resp.data.buffer,
    resp.data.byteOffset,
    resp.data.byteLength
  );
  return new Transaction(
    sender,
    namespace,
    routingKey,
    view.getBigUint64(1, true),
    view.getBigUint64(9, true)
  );
}

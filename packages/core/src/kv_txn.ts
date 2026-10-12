/**
 * Flo KV Per-Shard Transactions
 *
 * A transaction is pinned to the shard of the routing key passed to `begin`.
 * Every op inside it carries that routing key and the transaction's id, so
 * its keys live on that shard.
 *
 * Server-enforced caps:
 *   - 256 ops per transaction
 *   - 1 MiB total payload across buffered writes
 *
 * The following operations are NOT supported inside a transaction and throw
 * `TxnUnsupportedOpError` without a server round-trip: `scan`, `mget`,
 * `jsonGet`, `jsonSet`, `jsonDel`, `history`.
 */

import { createServerError, FloError } from "./errors.js";
import {
  decodeDeleteAnswer,
  decodeEmptyAnswer,
  decodeExistsAnswer,
  decodeIncrAnswer,
  decodeTxnBeginAnswer,
  decodeTxnCommitAnswer,
  encodeDeleteBody,
  encodeExistsBody,
  encodeExpireBody,
  encodeIncrBody,
  encodeTxnBody,
  MAX_ROUTING_KEY,
} from "./kv_body.js";
import {
  type GetResult,
  type KVCommitResult,
  OpCode,
  type PutOptions,
  type PutResult,
  type RawResponse,
  StatusCode,
} from "./types.js";
import { kvGet, kvSet } from "./kv.js";

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

/** Send a table op and throw on any status but OK. */
async function send(sender: TxnRequestSender, op: OpCode, namespace: string, key: string, body: Uint8Array): Promise<RawResponse> {
  const resp = await sender.sendRequest(op, namespace, textEncoder.encode(key), body, new Uint8Array(0));
  if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
  return resp;
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

  private checkAlive(): void {
    if (this.done) throw new TxnFinishedError();
  }

  /** Buffer a put inside the transaction; it has no version until commit (0n). */
  async put(key: string, value: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    this.checkAlive();
    return kvSet(this.sender, this.namespace, key, value, { ...opts, routingKey: this.routingKey }, this.id);
  }

  /** Read a key inside the transaction (sees buffered writes). */
  async get(key: string): Promise<GetResult | null> {
    this.checkAlive();
    return kvGet(this.sender, this.namespace, key, { routingKey: this.routingKey }, this.id);
  }

  /**
   * Buffer a delete inside the transaction. Whether the key existed isn't
   * known until commit.
   */
  async delete(key: string, opts?: { casVersion?: bigint }): Promise<void> {
    this.checkAlive();
    const body = encodeDeleteBody({ casVersion: opts?.casVersion, routingKey: this.routingKey }, this.id);
    decodeDeleteAnswer((await send(this.sender, OpCode.KVDelete, this.namespace, key, body)).data);
  }

  /**
   * Buffer an atomic counter increment inside the transaction. The new value
   * isn't known until commit.
   */
  async incr(key: string, delta: bigint = 1n): Promise<void> {
    this.checkAlive();
    const body = encodeIncrBody({ delta, routingKey: this.routingKey }, this.id);
    decodeIncrAnswer((await send(this.sender, OpCode.KVIncr, this.namespace, key, body)).data);
  }

  /**
   * Set the TTL of a key inside the transaction, in milliseconds;
   * `ttlMs = 0` clears it.
   */
  async expire(key: string, ttlMs: bigint | number, opts?: { casVersion?: bigint }): Promise<void> {
    this.checkAlive();
    const body = encodeExpireBody(ttlMs, { casVersion: opts?.casVersion, routingKey: this.routingKey }, this.id);
    decodeEmptyAnswer((await send(this.sender, OpCode.KVExpire, this.namespace, key, body)).data);
  }

  /** Check key existence inside the transaction (sees buffered writes). */
  async exists(key: string): Promise<boolean> {
    this.checkAlive();
    const body = encodeExistsBody({ routingKey: this.routingKey }, this.id);
    return decodeExistsAnswer((await send(this.sender, OpCode.KVExists, this.namespace, key, body)).data).exists;
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
    const resp = await send(this.sender, OpCode.KVCommitTxn, this.namespace, "", encodeTxnBody(this.id));
    return decodeTxnCommitAnswer(resp.data);
  }

  /**
   * Discard the buffered operations without committing. Idempotent: calling
   * rollback after commit (or vice versa) is a no-op.
   */
  async rollback(): Promise<void> {
    if (this.done) return;
    this.done = true;
    const resp = await send(this.sender, OpCode.KVRollbackTxn, this.namespace, "", encodeTxnBody(this.id));
    decodeEmptyAnswer(resp.data);
  }
}

/** Open a new per-shard KV transaction; the routing key is the request's key. */
export async function beginTxn(
  sender: TxnRequestSender,
  namespace: string,
  routingKey: string
): Promise<Transaction> {
  const n = textEncoder.encode(routingKey).length;
  if (n === 0) throw new FloError("flo: a transaction needs a routing key");
  if (n > MAX_ROUTING_KEY) throw new FloError(`flo: routing key is ${n} bytes, at most ${MAX_ROUTING_KEY}`);
  const resp = await send(sender, OpCode.KVBeginTxn, namespace, routingKey, new Uint8Array(0));
  const { txn, pinned } = decodeTxnBeginAnswer(resp.data);
  return new Transaction(sender, namespace, routingKey, txn, pinned);
}

/**
 * Flo KV Per-Shard Transactions
 *
 * A transaction is pinned to the `{tag}` of the key passed to `begin`, or
 * to the whole key when it has none. Every key inside it must route with
 * that key, so give them all the same `{tag}` (`{user:1}:balance`,
 * `{user:1}:visits`); one that routes elsewhere is refused with reason
 * KeysNotColocated, not run.
 *
 * The server caps a transaction's ops and bytes, by default at 64 ops and
 * 256 KiB; both are operator settings. An op past them is refused
 * (reason TxnTooLarge).
 *
 * A condition, casVersion or incr conflict inside a transaction is refused
 * at commit, not at the op: the server decides them as the batch applies.
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
  MAX_MGET_KEY,
} from "./kv_body.js";
import {
  type GetResult,
  type KVExistsResult,
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
 * const txn = await client.kv.begin("{user:123}");
 * try {
 *   await txn.put("{user:123}:name", new TextEncoder().encode("Jane"));
 *   await txn.incr("{user:123}:visits", 1n);
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
    /** Server-assigned transaction id. */
    public readonly id: bigint,
    /** The routing hash this transaction is pinned to. */
    public readonly pinnedHash: bigint
  ) {}

  private checkAlive(): void {
    if (this.done) throw new TxnFinishedError();
  }

  /** Buffer a put inside the transaction; it has no version until commit (0n). */
  async put(key: string, value: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    this.checkAlive();
    return kvSet(this.sender, this.namespace, key, value, opts, this.id);
  }

  /** Read a key inside the transaction (sees buffered writes). */
  async get(key: string): Promise<GetResult | null> {
    this.checkAlive();
    return kvGet(this.sender, this.namespace, key, undefined, this.id);
  }

  /**
   * Buffer a delete inside the transaction. Whether the key existed isn't
   * known until commit.
   */
  async delete(key: string, opts?: { casVersion?: bigint }): Promise<void> {
    this.checkAlive();
    const body = encodeDeleteBody({ casVersion: opts?.casVersion }, this.id);
    decodeDeleteAnswer((await send(this.sender, OpCode.KVDelete, this.namespace, key, body)).data);
  }

  /**
   * Buffer an atomic counter increment inside the transaction. The new value
   * isn't known until commit.
   */
  async incr(key: string, delta: bigint = 1n): Promise<void> {
    this.checkAlive();
    const body = encodeIncrBody({ delta }, this.id);
    decodeIncrAnswer((await send(this.sender, OpCode.KVIncr, this.namespace, key, body)).data);
  }

  /**
   * Set the TTL of a key inside the transaction, in milliseconds;
   * `ttlMs = 0` clears it.
   */
  async expire(key: string, ttlMs: bigint | number, opts?: { casVersion?: bigint }): Promise<void> {
    this.checkAlive();
    const body = encodeExpireBody(ttlMs, { casVersion: opts?.casVersion }, this.id);
    decodeEmptyAnswer((await send(this.sender, OpCode.KVExpire, this.namespace, key, body)).data);
  }

  /** Check key existence inside the transaction (sees buffered writes; version 0 for one). */
  async exists(key: string): Promise<KVExistsResult> {
    this.checkAlive();
    const body = encodeExistsBody(this.id);
    const resp = await send(this.sender, OpCode.KVExists, this.namespace, key, body);
    return { ...decodeExistsAnswer(resp.data), stale: resp.stale === true };
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

/** Open a new per-shard KV transaction, pinned to `key`'s {tag} (the request's key). */
export async function beginTxn(sender: TxnRequestSender, namespace: string, key: string): Promise<Transaction> {
  const n = textEncoder.encode(key).length;
  if (n === 0) throw new FloError("flo: a transaction is begun with a key");
  if (n > MAX_MGET_KEY) throw new FloError(`flo: key is ${n} bytes, at most ${MAX_MGET_KEY}`);
  const resp = await send(sender, OpCode.KVBeginTxn, namespace, key, new Uint8Array(0));
  const { txn, pinned } = decodeTxnBeginAnswer(resp.data);
  return new Transaction(sender, namespace, txn, pinned);
}

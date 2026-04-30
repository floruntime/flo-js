/**
 * KV operations for Flo SDK.
 */

import { createServerError } from "./errors.js";
import { beginTxn, Transaction } from "./kv_txn.js";
import {
  type DeleteOptions,
  type GetOptions,
  type GetResult,
  type HistoryOptions,
  type KVExistsOptions,
  type KVIncrOptions,
  type KVJsonOptions,
  type KVMGetOptions,
  type KVTouchOptions,
  type MGetEntry,
  OpCode,
  OptionTag,
  type PutOptions,
  type PutResult,
  type RawResponse,
  type ScanOptions,
  type ScanResult,
  StatusCode,
  type VersionEntry,
} from "./types.js";
import {
  OptionsBuilder,
  parseHistoryResponse,
  parseScanResponse,
} from "./wire.js";

/**
 * Interface for sending requests (implemented by client).
 */
export interface RequestSender {
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
 * KV client operations.
 */
export class KVOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Get retrieves the value and version for a key.
   * Returns null if the key is not found.
   *
   * @param key - The key to retrieve
   * @param opts - Options including optional blocking behavior
   * @param opts.blockMs - If set, block for up to this many milliseconds waiting for the key.
   *                       Use 0 to block forever, or a positive number for a timeout.
   */
  async get(key: string, opts?: GetOptions): Promise<GetResult | null> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.blockMs !== undefined) {
      builder.addU32(OptionTag.BlockMS, opts.blockMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.KVGet,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status === StatusCode.NotFound) {
      return null;
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Wire body: [version:u64 LE][value bytes]
    if (resp.data.length < 8) {
      return { value: new Uint8Array(0), version: 0n };
    }
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    const version = view.getBigUint64(0, true);
    const value = resp.data.subarray(8);
    return { value, version };
  }

  /**
   * Put sets a key-value pair and returns the new version.
   *
   * The returned `version` may be passed to {@link PutOptions.casVersion} on
   * the next write to enforce optimistic concurrency.
   */
  async put(
    key: string,
    value: Uint8Array,
    opts?: PutOptions
  ): Promise<PutResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.ttlSeconds !== undefined) {
      builder.addU64(OptionTag.TTLSeconds, opts.ttlSeconds);
    }

    if (opts?.casVersion !== undefined) {
      builder.addU64(OptionTag.CASVersion, opts.casVersion);
    }

    if (opts?.ifNotExists) {
      builder.addFlag(OptionTag.IfNotExists);
    }

    if (opts?.ifExists) {
      builder.addFlag(OptionTag.IfExists);
    }

    const resp = await this.sender.sendRequest(
      OpCode.KVPut,
      namespace,
      textEncoder.encode(key),
      value,
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.data.length < 8) {
      return { version: 0n };
    }
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    return { version: view.getBigUint64(0, true) };
  }

  /**
   * MGet looks up many keys in a single round trip. Keys may live on different
   * shards — the server gathers results in parallel and returns one entry per
   * requested key in the same order. The `found` flag distinguishes missing
   * keys from keys whose value is empty.
   *
   * Limited to 256 keys per call.
   */
  async mget(keys: string[], opts?: KVMGetOptions): Promise<MGetEntry[]> {
    if (keys.length === 0) return [];
    if (keys.length > 256) throw new Error("mget: too many keys (max 256)");
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Pack request: [count:u16 LE]([key_len:u16 LE][key])*
    const encoded = keys.map((k) => textEncoder.encode(k));
    let size = 2;
    for (const k of encoded) {
      if (k.length > 0xffff) throw new Error("mget: key too long");
      size += 2 + k.length;
    }
    const value = new Uint8Array(size);
    const w = new DataView(value.buffer);
    w.setUint16(0, encoded.length, true);
    let off = 2;
    for (const k of encoded) {
      w.setUint16(off, k.length, true);
      off += 2;
      value.set(k, off);
      off += k.length;
    }

    const resp = await this.sender.sendRequest(
      OpCode.KVMGet,
      namespace,
      new Uint8Array(0),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.data.length < 4) return [];
    const r = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    const count = r.getUint32(0, true);
    const decoder = new TextDecoder();
    const out: MGetEntry[] = [];
    let p = 4;
    for (let i = 0; i < count; i++) {
      if (p + 1 + 2 > resp.data.length) break;
      const status = resp.data[p];
      p += 1;
      const klen = r.getUint16(p, true);
      p += 2;
      if (p + klen + 8 + 4 > resp.data.length) break;
      const key = decoder.decode(resp.data.subarray(p, p + klen));
      p += klen;
      const version = r.getBigUint64(p, true);
      p += 8;
      const vlen = r.getUint32(p, true);
      p += 4;
      if (p + vlen > resp.data.length) break;
      const val =
        vlen > 0 ? resp.data.slice(p, p + vlen) : new Uint8Array(0);
      p += vlen;
      out.push({ key, value: val, version, found: status === 0 });
    }
    return out;
  }

  /**
   * Delete removes a key.
   * This operation succeeds even if the key doesn't exist (unless `ifMatch`
   * is set, in which case a missing key is treated as a CAS mismatch).
   */
  async delete(key: string, opts?: DeleteOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();
    if (opts?.ifMatch !== undefined) {
      builder.addU64(OptionTag.CASVersion, opts.ifMatch);
    }

    const resp = await this.sender.sendRequest(
      OpCode.KVDelete,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      builder.build()
    );

    // Delete succeeds for both OK and NOT_FOUND when no CAS guard is set.
    const allowNotFound = opts?.ifMatch === undefined;
    if (resp.status !== StatusCode.OK && !(allowNotFound && resp.status === StatusCode.NotFound)) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Scan retrieves keys with a prefix.
   */
  async scan(prefix: string, opts?: ScanOptions): Promise<ScanResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.keysOnly) {
      builder.addU8(OptionTag.KeysOnly, 1);
    }

    // Value: [limit:u32][cursor...]
    const limit = opts?.limit ?? 0; // 0 = server default
    const cursor = opts?.cursor ?? new Uint8Array(0);
    const value = new Uint8Array(4 + cursor.length);
    const view = new DataView(value.buffer);
    view.setUint32(0, limit, true);
    if (cursor.length > 0) {
      value.set(cursor, 4);
    }

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

    return parseScanResponse(resp.data);
  }

  /**
   * History retrieves the version history for a key.
   */
  async history(key: string, opts?: HistoryOptions): Promise<VersionEntry[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.limit !== undefined) {
      builder.addU32(OptionTag.Limit, opts.limit);
    }

    const resp = await this.sender.sendRequest(
      OpCode.KVHistory,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      builder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return parseHistoryResponse(resp.data);
  }

  // ─── Extended ops: counters, TTL lifecycle, exists, JSON paths ─────

  /**
   * Atomically add `delta` (default +1) to the i64 counter at `key`.
   *
   * The first incr on a missing key creates it at the delta value. Throws if
   * the key already holds a non-counter value.
   *
   * @returns The new counter value.
   */
  async incr(key: string, opts?: KVIncrOptions): Promise<bigint> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    let value = new Uint8Array(0);
    if (opts?.delta !== undefined) {
      value = new Uint8Array(8);
      new DataView(value.buffer).setBigInt64(0, opts.delta, true);
    }
    const resp = await this.sender.sendRequest(
      OpCode.KVIncr,
      namespace,
      textEncoder.encode(key),
      value,
      new Uint8Array(0)
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length < 16) {
      throw new Error("incr: short response");
    }
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    return view.getBigInt64(8, true);
  }

  /**
   * Update the TTL on an existing key. `ttlSeconds = 0` clears the TTL.
   *
   * When `opts.ifMatch` is set, the touch only succeeds if the current key
   * version equals it — enabling race-free lease renewal.
   */
  async touch(
    key: string,
    ttlSeconds: bigint | number,
    opts?: KVTouchOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const ttl = typeof ttlSeconds === "bigint" ? ttlSeconds : BigInt(ttlSeconds);
    const value = new Uint8Array(8);
    new DataView(value.buffer).setBigUint64(0, ttl, true);
    const builder = new OptionsBuilder();
    if (opts?.ifMatch !== undefined) {
      builder.addU64(OptionTag.CASVersion, opts.ifMatch);
    }
    const resp = await this.sender.sendRequest(
      OpCode.KVTouch,
      namespace,
      textEncoder.encode(key),
      value,
      builder.build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Clear the TTL on an existing key, making it permanent.
   *
   * When `opts.ifMatch` is set, the persist only succeeds if the current key
   * version equals it.
   */
  async persist(key: string, opts?: KVTouchOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const builder = new OptionsBuilder();
    if (opts?.ifMatch !== undefined) {
      builder.addU64(OptionTag.CASVersion, opts.ifMatch);
    }
    const resp = await this.sender.sendRequest(
      OpCode.KVPersist,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      builder.build()
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Returns true if `key` is present without transferring its value.
   */
  async exists(key: string, opts?: KVExistsOptions): Promise<boolean> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(
      OpCode.KVExists,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      new Uint8Array(0)
    );
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    // Wire body: [version:u64][1 byte 0/1]
    return resp.data.length >= 9 && resp.data[8] === 1;
  }

  /**
   * Extract the value at `path` from the JSON document at `key`.
   *
   * Returns the JSON-encoded value as bytes, or `null` if the key or path
   * is missing.
   */
  /**
   * Extract the value at `path` from the JSON document at `key`.
   *
   * Returns a {@link GetResult} carrying the JSON-encoded bytes and the
   * document's current version, or `null` if the key or path is missing.
   */
  async jsonGet(
    key: string,
    path: string = "$",
    opts?: KVJsonOptions
  ): Promise<GetResult | null> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(
      OpCode.KVJsonGet,
      namespace,
      textEncoder.encode(key),
      textEncoder.encode(path),
      new Uint8Array(0)
    );
    if (resp.status === StatusCode.NotFound) {
      return null;
    }
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    if (resp.data.length < 8) {
      return null;
    }
    const view = new DataView(
      resp.data.buffer,
      resp.data.byteOffset,
      resp.data.byteLength
    );
    const version = view.getBigUint64(0, true);
    return { value: resp.data.subarray(8), version };
  }

  /**
   * Set the JSON value at `path` inside the document at `key`.
   *
   * Path `"$"` replaces the whole document (and creates the key if missing).
   * Sub-paths require the key to already exist. Returns a {@link PutResult}
   * with the new document version.
   */
  async jsonSet(
    key: string,
    path: string,
    jsonValue: Uint8Array,
    opts?: KVJsonOptions
  ): Promise<PutResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const pathBytes = textEncoder.encode(path || "$");
    if (pathBytes.length > 0xffff) {
      throw new Error("jsonSet: path too long");
    }
    const value = new Uint8Array(2 + pathBytes.length + jsonValue.length);
    new DataView(value.buffer).setUint16(0, pathBytes.length, true);
    value.set(pathBytes, 2);
    value.set(jsonValue, 2 + pathBytes.length);
    const resp = await this.sender.sendRequest(
      OpCode.KVJsonSet,
      namespace,
      textEncoder.encode(key),
      value,
      new Uint8Array(0)
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

  /**
   * Remove the value at `path` from the JSON document at `key`.
   *
   * Sub-paths return a {@link PutResult} with the new document version. For
   * `"$"` (whole document delete) the version is `0n` since the key is gone.
   */
  async jsonDel(
    key: string,
    path: string = "$",
    opts?: KVJsonOptions
  ): Promise<PutResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(
      OpCode.KVJsonDel,
      namespace,
      textEncoder.encode(key),
      textEncoder.encode(path || "$"),
      new Uint8Array(0)
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

  /**
   * Open a per-shard KV transaction pinned to `routingKey`'s partition.
   *
   * Every key written or read inside the transaction must hash to the same
   * partition; otherwise the server returns a "kv_txn_cross_shard" error.
   *
   * Returns a {@link Transaction} handle. Use `try/finally` for safe cleanup:
   *
   * ```ts
   * const txn = await client.kv.begin("user:123");
   * try {
   *   await txn.put("user:123:name", new TextEncoder().encode("Jane"));
   *   await txn.commit();
   * } catch (err) {
   *   await txn.rollback();
   *   throw err;
   * }
   * ```
   */
  async begin(routingKey: string, opts?: { namespace?: string }): Promise<Transaction> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    return beginTxn(this.sender, namespace, routingKey);
  }
}

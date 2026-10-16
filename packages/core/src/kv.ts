/**
 * KV operations for Flo SDK.
 */

import { createServerError, IncompleteResponseError, ServerError } from "./errors.js";
import {
  decodeDeleteAnswer,
  decodeEmptyAnswer,
  decodeExistsAnswer,
  decodeGetAnswer,
  decodeHistoryAnswer,
  decodeIncrResult,
  decodeJsonDelAnswer,
  decodeJsonGetAnswer,
  decodeJsonSetAnswer,
  decodeMGetAnswer,
  decodeScanAnswer,
  decodeSetAnswer,
  encodeDeleteBody,
  encodeExistsBody,
  encodeExpireBody,
  encodeGetBody,
  encodeHistoryBody,
  encodeIncrBody,
  encodeJsonPathBody,
  encodeJsonSetBody,
  encodeMGetBody,
  encodeScanBody,
  splitMGetKeys,
  encodeSetBody,
  MAX_SCAN_LIMIT,
} from "./kv_body.js";
import { beginTxn, Transaction } from "./kv_txn.js";
import {
  type DeleteOptions,
  type GetOptions,
  type GetResult,
  type HistoryOptions,
  type KVExistsOptions,
  type KVExpireOptions,
  type KVIncrOptions,
  type KVIncrResult,
  type KVJsonOptions,
  type KVMGetOptions,
  type MGetEntry,
  OpCode,
  type PutOptions,
  type PutResult,
  type RawResponse,
  Reason,
  type ScanOptions,
  type ScanResult,
  StatusCode,
  type VersionEntry,
} from "./types.js";

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

/** Null for a not-found whose reason is not_found; any other (no such transaction) throws. */
function absent(resp: RawResponse): null {
  const err = createServerError(resp.status, resp.data);
  if (err instanceof ServerError && err.reason === Reason.NotFound) return null;
  throw err;
}

/** kv.get, inside transaction `txn` when given. */
export async function kvGet(sender: Pick<RequestSender, "sendRequest">, namespace: string, key: string, opts?: GetOptions, txn?: bigint): Promise<GetResult | null> {
  const resp = await sender.sendRequest(OpCode.KVGet, namespace, textEncoder.encode(key), encodeGetBody(opts, txn), new Uint8Array(0));
  if (resp.status === StatusCode.NotFound) return absent(resp);
  if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
  return decodeGetAnswer(resp.data);
}

/** kv.set, inside transaction `txn` when given. */
export async function kvSet(sender: Pick<RequestSender, "sendRequest">, namespace: string, key: string, value: Uint8Array, opts?: PutOptions, txn?: bigint): Promise<PutResult> {
  const resp = await sender.sendRequest(OpCode.KVPut, namespace, textEncoder.encode(key), encodeSetBody(value, opts, txn), new Uint8Array(0));
  if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
  return decodeSetAnswer(resp.data);
}

/** kv.scan: one page of names under `prefix`. */
export async function kvScan(sender: Pick<RequestSender, "sendRequest">, namespace: string, prefix: string, opts?: { cursor?: Uint8Array; limit?: number }): Promise<ScanResult> {
  const body = encodeScanBody(opts?.limit ?? MAX_SCAN_LIMIT, opts?.cursor);
  const resp = await sender.sendRequest(OpCode.KVScan, namespace, textEncoder.encode(prefix), body, new Uint8Array(0));
  if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
  return decodeScanAnswer(resp.data);
}

/**
 * KV client operations.
 */
export class KVOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Get retrieves the value and version for a key. Returns null if the key
   * is not found, also when a wait (`waitMs`) ends without it.
   */
  async get(key: string, opts?: GetOptions): Promise<GetResult | null> {
    return kvGet(this.sender, this.sender.getNamespace(opts?.namespace), key, opts);
  }

  /**
   * Put sets a key-value pair and returns the new version. A `casVersion`
   * that isn't the key's, or a `condition` that doesn't hold, throws a
   * conflict with reason CasMismatch or ConditionFailed.
   */
  async put(key: string, value: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    return kvSet(this.sender, this.sender.getNamespace(opts?.namespace), key, value, opts);
  }

    /**
   * MGet looks up many keys and returns one entry per requested key, in the
   * same order. The `found` flag tells a missing key from one whose value is
   * empty. When the server's answer fills up before every key is answered,
   * mget asks again from the first key it didn't answer; keys too many bytes
   * for one request go in several.
   *
   * Limited to 256 keys per call, each at most 4096 bytes.
   */
  async mget(keys: string[], opts?: KVMGetOptions): Promise<MGetEntry[]> {
    if (keys.length === 0) return [];
    const namespace = this.sender.getNamespace(opts?.namespace);
    const encoded = keys.map((k) => textEncoder.encode(k));
    const out: MGetEntry[] = [];
    // Each run of keys fits one request frame; within a run, follow next.
    for (const end of splitMGetKeys(encoded, opts?.routingKey)) {
      while (out.length < end) {
        const from = out.length;
        const resp = await this.sender.sendRequest(OpCode.KVMGet, namespace, new Uint8Array(0), encodeMGetBody(encoded.slice(from, end), opts?.routingKey), new Uint8Array(0));
        if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
        const page = decodeMGetAnswer(resp.data);
        const asked = end - from;
        // Without next the answer covers every key asked; with it, exactly the
        // keys before next, and at least one, or the loop would not end.
        const answered = page.next ?? asked;
        if (page.entries.length !== answered || answered === 0 || answered > asked || (page.next !== null && answered === asked)) {
          throw new IncompleteResponseError(`kv.mget answered ${page.entries.length} entries, next ${page.next}, for ${asked} keys`);
        }
        for (const e of page.entries) out.push({ key: keys[out.length]!, ...e });
      }
    }
    return out;
  }

  /**
   * Delete removes a key and says whether it existed; deleting a missing key
   * is not an error. A `casVersion` that isn't the key's throws a conflict
   * with reason CasMismatch.
   */
  async delete(key: string, opts?: DeleteOptions): Promise<boolean> {
    const resp = await this.send(OpCode.KVDelete, opts?.namespace, key, encodeDeleteBody(opts));
    const existed = decodeDeleteAnswer(resp.data);
    if (existed === null) throw new IncompleteResponseError("kv.delete answered no existed");
    return existed;
  }

  /**
   * Scan returns one page of the key names that start with `prefix` ("" for
   * all), and the cursor for the next page, null on the last.
   */
  async scan(prefix: string, opts?: ScanOptions): Promise<ScanResult> {
    return kvScan(this.sender, this.sender.getNamespace(opts?.namespace), prefix, opts);
  }

  /**
   * History returns a key's versions, newest first, each with its write
   * time; a missing key has none.
   */
  async history(key: string, opts?: HistoryOptions): Promise<VersionEntry[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.KVHistory, namespace, textEncoder.encode(key), encodeHistoryBody(opts?.limit ?? 10, opts), new Uint8Array(0));
    if (resp.status === StatusCode.NotFound) return absent(resp) ?? [];
    if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
    return decodeHistoryAnswer(resp.data);
  }

  // ─── Counters, TTLs, exists, JSON paths ─────

  /**
   * Atomically add `delta` (default +1) to the i64 counter at `key`, and
   * return its new value and version.
   *
   * The first incr on a missing key creates it at the delta value. A key
   * that holds something other than a counter throws reason WrongType.
   */
  async incr(key: string, opts?: KVIncrOptions): Promise<KVIncrResult> {
    const resp = await this.send(OpCode.KVIncr, opts?.namespace, key, encodeIncrBody(opts));
    return decodeIncrResult(resp.data);
  }

  /**
   * Set the TTL of an existing key, in milliseconds; `ttlMs = 0` clears it,
   * making the key permanent.
   *
   * When `opts.casVersion` is set, the expire only succeeds if the key's
   * version equals it, for race-free lease renewal.
   */
  async expire(key: string, ttlMs: bigint | number, opts?: KVExpireOptions): Promise<void> {
    const resp = await this.send(OpCode.KVExpire, opts?.namespace, key, encodeExpireBody(ttlMs, opts));
    decodeEmptyAnswer(resp.data);
  }

  /**
   * Returns true if `key` is present, without transferring its value.
   */
  async exists(key: string, opts?: KVExistsOptions): Promise<boolean> {
    const resp = await this.send(OpCode.KVExists, opts?.namespace, key, encodeExistsBody(opts));
    return decodeExistsAnswer(resp.data).exists;
  }

  /**
   * Extract the value at `path` (default "$", the whole document) from the
   * JSON document at `key`.
   *
   * Returns a {@link GetResult} carrying the JSON-encoded bytes and the
   * document's current version, or `null` if the key or path is missing.
   */
  async jsonGet(key: string, path: string = "$", opts?: KVJsonOptions): Promise<GetResult | null> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.KVJsonGet, namespace, textEncoder.encode(key), encodeJsonPathBody(path, opts), new Uint8Array(0));
    if (resp.status === StatusCode.NotFound) return absent(resp);
    if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
    return decodeJsonGetAnswer(resp.data);
  }

  /**
   * Set the JSON value at `path` inside the document at `key`.
   *
   * Path `"$"` replaces the whole document (and creates the key if missing).
   * Sub-paths require the key to already exist. Returns a {@link PutResult}
   * with the new document version.
   */
  async jsonSet(key: string, path: string, jsonValue: Uint8Array, opts?: KVJsonOptions): Promise<PutResult> {
    const resp = await this.send(OpCode.KVJsonSet, opts?.namespace, key, encodeJsonSetBody(path, jsonValue, opts));
    return decodeJsonSetAnswer(resp.data);
  }

  /**
   * Remove the value at `path` (default "$") from the JSON document at `key`.
   *
   * Returns the document's new version, or null for `"$"`, which deletes
   * the key.
   */
  async jsonDel(key: string, path: string = "$", opts?: KVJsonOptions): Promise<bigint | null> {
    const resp = await this.send(OpCode.KVJsonDel, opts?.namespace, key, encodeJsonPathBody(path, opts));
    return decodeJsonDelAnswer(resp.data);
  }

  /** Send a table op and throw on any status but OK. */
  private async send(op: OpCode, namespace: string | undefined, key: string, body: Uint8Array): Promise<RawResponse> {
    const resp = await this.sender.sendRequest(op, this.sender.getNamespace(namespace), textEncoder.encode(key), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
    return resp;
  }

  /**
   * Open a per-shard KV transaction pinned to `routingKey`'s shard. Every
   * op inside it is routed by `routingKey`, so its keys share that shard.
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

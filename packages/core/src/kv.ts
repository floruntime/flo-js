/**
 * KV operations for Flo SDK.
 */

import { createServerError } from "./errors.js";
import {
  type DeleteOptions,
  type GetOptions,
  type HistoryOptions,
  OpCode,
  OptionTag,
  type PutOptions,
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
   * Get retrieves the value for a key.
   * Returns null if the key is not found.
   *
   * @param key - The key to retrieve
   * @param opts - Options including optional blocking behavior
   * @param opts.blockMs - If set, block for up to this many milliseconds waiting for the key.
   *                       Use 0 to block forever, or a positive number for a timeout.
   */
  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
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

    if (resp.data.length === 0) {
      return null;
    }

    return resp.data;
  }

  /**
   * Put sets a key-value pair.
   */
  async put(key: string, value: Uint8Array, opts?: PutOptions): Promise<void> {
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
  }

  /**
   * Delete removes a key.
   * This operation succeeds even if the key doesn't exist.
   */
  async delete(key: string, opts?: DeleteOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.KVDelete,
      namespace,
      textEncoder.encode(key),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    // Delete succeeds for both OK and NOT_FOUND
    if (resp.status !== StatusCode.OK && resp.status !== StatusCode.NotFound) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Scan retrieves keys with a prefix.
   */
  async scan(prefix: string, opts?: ScanOptions): Promise<ScanResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const builder = new OptionsBuilder();

    if (opts?.limit !== undefined) {
      builder.addU32(OptionTag.Limit, opts.limit);
    }

    if (opts?.keysOnly) {
      builder.addU8(OptionTag.KeysOnly, 1);
    }

    // Cursor goes in value field
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
}

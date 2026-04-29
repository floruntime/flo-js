/**
 * Processing (stream processing) operations for Flo SDK.
 *
 * Stream processing jobs are continuous data pipelines (Flink-inspired)
 * submitted as YAML definitions. This module provides submit/status/list/
 * stop/cancel/savepoint/restore/rescale operations that map to the
 * processing opcodes (0xC0–0xD1).
 */

import { createServerError } from "./errors.js";
import {
  OpCode,
  type RawResponse,
  StatusCode,
  type ProcessingSubmitOptions,
  type ProcessingStatusOptions,
  type ProcessingStatusResult,
  type ProcessingListOptions,
  type ProcessingListEntry,
  type ProcessingStopOptions,
  type ProcessingCancelOptions,
  type ProcessingSavepointOptions,
  type ProcessingRestoreOptions,
  type ProcessingRescaleOptions,
  type ProcessingSyncOptions,
  type ProcessingSyncResult,
  type ProcessingSyncDirFn,
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
const textDecoder = new TextDecoder();

/**
 * Processing operations for the Flo client.
 */
export class ProcessingOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Submit a processing job from a YAML definition.
   * Returns the server-assigned job ID.
   *
   * @param yaml - YAML job definition (bytes or string)
   * @param opts - Options (namespace)
   */
  async submit(
    yaml: Uint8Array | string,
    opts?: ProcessingSubmitOptions
  ): Promise<string> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const yamlBytes =
      typeof yaml === "string" ? textEncoder.encode(yaml) : yaml;

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingSubmit,
      namespace,
      new Uint8Array(0),
      yamlBytes,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return textDecoder.decode(resp.data);
  }

  /**
   * Get the status of a processing job.
   *
   * @param jobId - Job ID returned from submit()
   * @param opts - Options (namespace)
   */
  async status(
    jobId: string,
    opts?: ProcessingStatusOptions
  ): Promise<ProcessingStatusResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingStatus,
      namespace,
      textEncoder.encode(jobId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status === StatusCode.NotFound) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Binary wire format:
    // [job_id_len:u16][job_id][name_len:u16][name][status:u8]
    // [parallelism:u32][batch_size:u32][records_processed:u64][created_at:i64]
    return parseWireStatus(resp.data);
  }

  /**
   * List processing jobs.
   *
   * @param opts - Options (namespace, limit)
   */
  async list(opts?: ProcessingListOptions): Promise<ProcessingListEntry[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Value: [limit:u32][cursor...]
    const limit = opts?.limit ?? 100;
    const cursor = opts?.cursor ?? new Uint8Array(0);
    const value = new Uint8Array(4 + cursor.length);
    const view = new DataView(value.buffer);
    view.setUint32(0, limit, true);
    if (cursor.length > 0) {
      value.set(cursor, 4);
    }

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingList,
      namespace,
      new Uint8Array(0),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Binary wire format:
    // [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
    //  [status_len:u16][status][parallelism:u32][created_at:i64])*
    // [has_more:u8][cursor_len:u16]
    return parseWireList(resp.data);
  }

  /**
   * Gracefully stop a processing job.
   *
   * @param jobId - Job ID
   * @param opts - Options (namespace)
   */
  async stop(jobId: string, opts?: ProcessingStopOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingStop,
      namespace,
      textEncoder.encode(jobId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Force cancel a processing job.
   *
   * @param jobId - Job ID
   * @param opts - Options (namespace)
   */
  async cancel(jobId: string, opts?: ProcessingCancelOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingCancel,
      namespace,
      textEncoder.encode(jobId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Trigger a savepoint for a processing job.
   * Returns the savepoint ID.
   *
   * @param jobId - Job ID
   * @param opts - Options (namespace)
   */
  async savepoint(
    jobId: string,
    opts?: ProcessingSavepointOptions
  ): Promise<string> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingSavepoint,
      namespace,
      textEncoder.encode(jobId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return textDecoder.decode(resp.data);
  }

  /**
   * Restore a processing job from a savepoint.
   *
   * @param jobId - Job ID
   * @param savepointId - Savepoint ID to restore from
   * @param opts - Options (namespace)
   */
  async restore(
    jobId: string,
    savepointId: string,
    opts?: ProcessingRestoreOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingRestore,
      namespace,
      textEncoder.encode(jobId),
      textEncoder.encode(savepointId),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Rescale a processing job's parallelism.
   *
   * @param jobId - Job ID
   * @param parallelism - New parallelism level
   * @param opts - Options (namespace)
   */
  async rescale(
    jobId: string,
    parallelism: number,
    opts?: ProcessingRescaleOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Value: [parallelism:u32]
    const value = new Uint8Array(4);
    const view = new DataView(value.buffer);
    view.setUint32(0, parallelism, true);

    const resp = await this.sender.sendRequest(
      OpCode.ProcessingRescale,
      namespace,
      textEncoder.encode(jobId),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  // ===========================================================================
  // Declarative Sync
  // ===========================================================================

  /**
   * Declarative sync of a processing job YAML string.
   *
   * Extracts the job name from the YAML, submits it to the server,
   * and returns the result with the server-assigned job ID.
   *
   * Unlike workflow sync, processing jobs don't have version-based
   * change guards — each sync submits a new job instance.
   *
   * @param yaml - YAML job definition string
   * @param opts - Options (namespace)
   */
  async sync(
    yaml: string,
    opts?: ProcessingSyncOptions
  ): Promise<ProcessingSyncResult> {
    const name = extractProcessingMeta(yaml);
    const jobId = await this.submit(yaml, opts);
    return { name, job_id: jobId, action: "submitted" };
  }

  /**
   * Sync raw YAML bytes.
   *
   * @param yaml - YAML job definition bytes
   * @param opts - Options (namespace)
   */
  async syncBytes(
    yaml: Uint8Array,
    opts?: ProcessingSyncOptions
  ): Promise<ProcessingSyncResult> {
    return this.sync(textDecoder.decode(yaml), opts);
  }

  /**
   * Sync all YAML files in a directory.
   *
   * The `readDir` function must be injected by the caller — this keeps the
   * core package free of Node.js dependencies.
   *
   * @param readDir - Function that returns an array of `{ name, content }` for all .yaml/.yml files in a directory
   * @param dir - Directory path
   * @param opts - Options (namespace)
   */
  async syncDir(
    readDir: ProcessingSyncDirFn,
    dir: string,
    opts?: ProcessingSyncOptions
  ): Promise<ProcessingSyncResult[]> {
    const files = await readDir(dir);
    const results: ProcessingSyncResult[] = [];
    for (const file of files) {
      const result = await this.sync(file.content, opts);
      results.push(result);
    }
    return results;
  }
}

// =============================================================================
// YAML Metadata Extraction
// =============================================================================

/**
 * Extract the job name from processing YAML.
 */
export function extractProcessingMeta(yaml: string): string {
  const name = extractYAMLField(yaml, "name");
  if (!name) {
    throw new Error("flo: processing YAML missing required 'name' field");
  }
  return name;
}

/**
 * Lightweight extraction of a top-level scalar field from YAML or JSON.
 */
function extractYAMLField(yaml: string, field: string): string | undefined {
  const lines = yaml.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;

    if (trimmed.startsWith(field + ":")) {
      const val = trimmed.slice(field.length + 1).trim();
      return unquote(val);
    }

    const jsonKey = `"${field}"`;
    if (trimmed.startsWith(jsonKey)) {
      const rest = trimmed.slice(jsonKey.length).trim();
      if (rest.startsWith(":")) {
        let val = rest.slice(1).trim();
        if (val.endsWith(",")) val = val.slice(0, -1);
        return unquote(val);
      }
    }
  }
  return undefined;
}

function unquote(s: string): string {
  if (s.length >= 2) {
    if (
      (s[0] === '"' && s[s.length - 1] === '"') ||
      (s[0] === "'" && s[s.length - 1] === "'")
    ) {
      return s.slice(1, -1);
    }
  }
  return s;
}

// =============================================================================
// Wire Format Parsers
// =============================================================================

const statusNames = [
  "running",
  "stopped",
  "cancelled",
  "failed",
  "completed",
];

/**
 * Parse binary wire format for processing job status.
 *
 * Wire format: [job_id_len:u16][job_id][name_len:u16][name][status:u8]
 *              [parallelism:u32][batch_size:u32][records_processed:u64][created_at:i64]
 */
function parseWireStatus(data: Uint8Array): ProcessingStatusResult {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let pos = 0;

  const jobIdLen = view.getUint16(pos, true);
  pos += 2;
  const job_id = textDecoder.decode(data.subarray(pos, pos + jobIdLen));
  pos += jobIdLen;

  const nameLen = view.getUint16(pos, true);
  pos += 2;
  const name = textDecoder.decode(data.subarray(pos, pos + nameLen));
  pos += nameLen;

  const statusByte = data[pos++]!;
  const status = statusNames[statusByte] ?? `unknown(${statusByte})`;

  const parallelism = view.getUint32(pos, true);
  pos += 4;

  const batch_size = view.getUint32(pos, true);
  pos += 4;

  const records_processed = Number(view.getBigUint64(pos, true));
  pos += 8;

  const created_at = Number(view.getBigInt64(pos, true));

  return { job_id, name, status, parallelism, batch_size, records_processed, created_at };
}

/**
 * Parse binary wire format for processing job list.
 *
 * Wire format: [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
 *              [status_len:u16][status][parallelism:u32][created_at:i64])*
 *              [has_more:u8][cursor_len:u16]
 */
function parseWireList(data: Uint8Array): ProcessingListEntry[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);
  const results: ProcessingListEntry[] = [];
  let pos = 4;

  for (let i = 0; i < count; i++) {
    const nameLen = view.getUint16(pos, true);
    pos += 2;
    const name = textDecoder.decode(data.subarray(pos, pos + nameLen));
    pos += nameLen;

    const jobIdLen = view.getUint16(pos, true);
    pos += 2;
    const job_id = textDecoder.decode(data.subarray(pos, pos + jobIdLen));
    pos += jobIdLen;

    const statusLen = view.getUint16(pos, true);
    pos += 2;
    const status = textDecoder.decode(data.subarray(pos, pos + statusLen));
    pos += statusLen;

    const parallelism = view.getUint32(pos, true);
    pos += 4;

    const created_at = Number(view.getBigInt64(pos, true));
    pos += 8;

    results.push({ name, job_id, status, parallelism, created_at });
  }

  return results;
}

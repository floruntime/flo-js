/**
 * Workflow operations for Flo SDK.
 *
 * Workflows are durable, multi-step orchestration built on
 * Flo's primitives (actions, KV, streams, queues). This module
 * provides create/start/signal/cancel/status/history/list/sync
 * operations that map to the workflow opcodes (0x80–0x93).
 */

import { createServerError } from "./errors.js";
import {
  OpCode,
  type RawResponse,
  StatusCode,
  type WorkflowCreateOptions,
  type WorkflowStartOptions,
  type WorkflowSignalOptions,
  type WorkflowCancelOptions,
  type WorkflowStatusOptions,
  type WorkflowStatusResult,
  type WorkflowHistoryOptions,
  type WorkflowHistoryEvent,
  type WorkflowListRunsOptions,
  type WorkflowListRunEntry,
  type WorkflowGetDefinitionOptions,
  type WorkflowDisableOptions,
  type WorkflowEnableOptions,
  type WorkflowListDefinitionsOptions,
  type WorkflowDefinitionEntry,
  type WorkflowSyncOptions,
  type WorkflowSyncResult,
  type WorkflowSyncDirFn,
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
 * Workflow operations for the Flo client.
 */
export class WorkflowOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Create (or replace) a workflow from a YAML definition.
   * The server parses the YAML, validates the definition, and stores it.
   * This is an upsert — calling with the same name overwrites the previous version.
   *
   * @param name - Workflow name (must match the `name` field in YAML)
   * @param yaml - YAML definition bytes
   * @param opts - Options (namespace)
   */
  async create(
    name: string,
    yaml: Uint8Array | string,
    opts?: WorkflowCreateOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const yamlBytes =
      typeof yaml === "string" ? textEncoder.encode(yaml) : yaml;

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowCreate,
      namespace,
      textEncoder.encode(name),
      yamlBytes,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Get the YAML definition of a workflow by name.
   * Returns the raw YAML string, or null if not found.
   *
   * @param name - Workflow name
   * @param opts - Options (namespace, version)
   */
  async getDefinition(
    name: string,
    opts?: WorkflowGetDefinitionOptions
  ): Promise<string | null> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = opts?.version
      ? textEncoder.encode(opts.version)
      : new Uint8Array(0);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowGetDefinition,
      namespace,
      textEncoder.encode(name),
      value,
      new Uint8Array(0)
    );

    if (resp.status === StatusCode.NotFound) {
      return null;
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return textDecoder.decode(resp.data);
  }

  /**
   * Start a workflow run with optional input data.
   * Returns the server-assigned run ID.
   *
   * @param name - Workflow name
   * @param input - Input data (JSON bytes or string, optional)
   * @param opts - Options (namespace, idempotencyKey, runId)
   */
  async start(
    name: string,
    input?: Uint8Array | string | Record<string, any>,
    opts?: WorkflowStartOptions
  ): Promise<string> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Wire format: [has_idem:u8][idem_len:u16]?[idem]?[has_rid:u8][rid_len:u16]?[rid]?[input...]
    const inputBytes = input
      ? input instanceof Uint8Array
        ? input
        : typeof input === "string"
          ? textEncoder.encode(input)
          : textEncoder.encode(JSON.stringify(input))
      : new Uint8Array(0);

    const parts: number[] = [];

    // Version (u16 length prefix + version string)
    if (opts?.version) {
      const verBytes = textEncoder.encode(opts.version);
      parts.push(verBytes.length & 0xff, (verBytes.length >> 8) & 0xff);
      for (const b of verBytes) parts.push(b);
    } else {
      parts.push(0, 0); // zero-length version → "latest"
    }

    // Idempotency key
    if (opts?.idempotencyKey) {
      const idemBytes = textEncoder.encode(opts.idempotencyKey);
      parts.push(1);
      parts.push(idemBytes.length & 0xff, (idemBytes.length >> 8) & 0xff);
      for (const b of idemBytes) parts.push(b);
    } else {
      parts.push(0);
    }

    // Explicit run ID
    if (opts?.runId) {
      const ridBytes = textEncoder.encode(opts.runId);
      parts.push(1);
      parts.push(ridBytes.length & 0xff, (ridBytes.length >> 8) & 0xff);
      for (const b of ridBytes) parts.push(b);
    } else {
      parts.push(0);
    }

    // Input payload
    for (const b of inputBytes) parts.push(b);

    const value = new Uint8Array(parts);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowStart,
      namespace,
      textEncoder.encode(name),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return textDecoder.decode(resp.data);
  }

  /**
   * Get the status of a workflow run.
   *
   * @param runId - Run ID returned from start()
   * @param opts - Options (namespace)
   */
  async status(
    runId: string,
    opts?: WorkflowStatusOptions
  ): Promise<WorkflowStatusResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowStatus,
      namespace,
      textEncoder.encode(runId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status === StatusCode.NotFound) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Server returns binary wire format:
    // [run_id_len:u16][run_id][workflow_len:u16][workflow]
    // [version_len:u16][version][status:u8]
    // [current_step_len:u16][current_step][input_len:u32][input]
    // [created_at:i64][has_started:u8][started_at?:i64]
    // [has_completed:u8][completed_at?:i64]
    // [has_wait_signal:u8][wait_signal_len:u16][wait_signal]?
    const data = resp.data;
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    let pos = 0;

    const statusNames = [
      "pending",
      "running",
      "waiting",
      "completed",
      "failed",
      "cancelled",
      "timed_out",
    ];

    const readU16 = () => {
      const v = view.getUint16(pos, true);
      pos += 2;
      return v;
    };
    const readStr = (len: number) => {
      const s = textDecoder.decode(data.subarray(pos, pos + len));
      pos += len;
      return s;
    };

    const runIdLen = readU16();
    const parsedRunId = readStr(runIdLen);

    const workflowLen = readU16();
    const workflow = readStr(workflowLen);

    const versionLen = readU16();
    const version = readStr(versionLen);

    const statusByte = data[pos++]!;
    const statusStr = statusNames[statusByte] ?? `unknown(${statusByte})`;

    const stepLen = readU16();
    const currentStep = readStr(stepLen);

    const inputLen = view.getUint32(pos, true);
    pos += 4;
    const input = data.slice(pos, pos + inputLen);
    pos += inputLen;

    const createdAt = view.getBigInt64(pos, true);
    pos += 8;

    let startedAt: bigint | undefined;
    const hasStarted = data[pos++];
    if (hasStarted === 1) {
      startedAt = view.getBigInt64(pos, true);
      pos += 8;
    }

    let completedAt: bigint | undefined;
    const hasCompleted = data[pos++];
    if (hasCompleted === 1) {
      completedAt = view.getBigInt64(pos, true);
      pos += 8;
    }

    let waitSignal: string | undefined;
    const hasWaitSignal = data[pos++];
    if (hasWaitSignal === 1) {
      const wsLen = readU16();
      waitSignal = readStr(wsLen);
    }

    const result: WorkflowStatusResult = {
      run_id: parsedRunId,
      workflow,
      version,
      status: statusStr,
      current_step: currentStep,
      input,
      created_at: createdAt,
    };
    if (startedAt !== undefined) result.started_at = startedAt;
    if (completedAt !== undefined) result.completed_at = completedAt;
    if (waitSignal !== undefined) result.wait_signal = waitSignal;

    return result;
  }

  /**
   * Get the execution history of a workflow run.
   *
   * @param runId - Run ID
   * @param opts - Options (namespace, limit)
   */
  async history(
    runId: string,
    opts?: WorkflowHistoryOptions
  ): Promise<WorkflowHistoryEvent[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Value: [limit:u32] (little-endian)
    const limit = opts?.limit ?? 100;
    const value = new Uint8Array(4);
    const view = new DataView(value.buffer);
    view.setUint32(0, limit, true);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowHistory,
      namespace,
      textEncoder.encode(runId),
      value,
      new Uint8Array(0)
    );

    if (resp.status === StatusCode.NotFound) {
      throw createServerError(resp.status, resp.data);
    }

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Server returns binary wire format:
    // [count:u32]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
    // [has_more:u8][cursor_len:u16]
    return parseWireHistory(resp.data);
  }

  /**
   * List workflow runs, optionally filtered by workflow name and status.
   *
   * @param opts - Options (namespace, workflowName, statusFilter, limit)
   */
  async listRuns(
    opts?: WorkflowListRunsOptions
  ): Promise<WorkflowListRunEntry[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const key = opts?.workflowName
      ? textEncoder.encode(opts.workflowName)
      : new Uint8Array(0);

    // Value: [limit:u32][status_len:u16][status]?
    const limit = opts?.limit ?? 100;
    const statusBytes = opts?.statusFilter
      ? textEncoder.encode(opts.statusFilter)
      : new Uint8Array(0);

    const value = new Uint8Array(4 + 2 + statusBytes.length);
    const view = new DataView(value.buffer);
    view.setUint32(0, limit, true);
    view.setUint16(4, statusBytes.length, true);
    value.set(statusBytes, 6);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowListRuns,
      namespace,
      key,
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Server returns binary wire format:
    // [count:u32]([run_id_len:u16][run_id][workflow_len:u16][workflow]
    //  [status_len:u16][status][created_at:i64])*
    // [has_more:u8][cursor_len:u16]
    return parseWireListRuns(resp.data);
  }

  /**
   * Send a signal to a running workflow.
   *
   * @param runId - Run ID of the target workflow
   * @param signalName - Signal type name
   * @param data - Signal payload (optional)
   * @param opts - Options (namespace)
   */
  async signal(
    runId: string,
    signalName: string,
    data?: Uint8Array | string | Record<string, any>,
    opts?: WorkflowSignalOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Value format: [signal_name_len:u16 LE][signal_name][data...]
    const sigBytes = textEncoder.encode(signalName);
    const dataBytes = data
      ? data instanceof Uint8Array
        ? data
        : typeof data === "string"
          ? textEncoder.encode(data)
          : textEncoder.encode(JSON.stringify(data))
      : new Uint8Array(0);

    const value = new Uint8Array(2 + sigBytes.length + dataBytes.length);
    const view = new DataView(value.buffer);
    view.setUint16(0, sigBytes.length, true);
    value.set(sigBytes, 2);
    value.set(dataBytes, 2 + sigBytes.length);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowSignal,
      namespace,
      textEncoder.encode(runId),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Cancel a running workflow.
   *
   * @param runId - Run ID to cancel
   * @param reason - Cancellation reason (optional)
   * @param opts - Options (namespace)
   */
  async cancel(
    runId: string,
    reason?: string,
    opts?: WorkflowCancelOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = reason
      ? textEncoder.encode(reason)
      : new Uint8Array(0);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowCancel,
      namespace,
      textEncoder.encode(runId),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Disable a workflow definition (prevents new runs from starting).
   *
   * @param name - Workflow name
   * @param opts - Options (namespace)
   */
  async disable(name: string, opts?: WorkflowDisableOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowDisable,
      namespace,
      textEncoder.encode(name),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Re-enable a previously disabled workflow definition.
   *
   * @param name - Workflow name
   * @param opts - Options (namespace)
   */
  async enable(name: string, opts?: WorkflowEnableOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowEnable,
      namespace,
      textEncoder.encode(name),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * List all workflow definitions.
   *
   * @param opts - Options (namespace)
   */
  async listDefinitions(
    opts?: WorkflowListDefinitionsOptions
  ): Promise<WorkflowDefinitionEntry[]> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkflowListDefinitions,
      namespace,
      new Uint8Array(0),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Server returns binary wire format:
    // [count:u32]([name_len:u16][name][version_len:u16][version][created_at:i64])*
    // [has_more:u8][cursor_len:u16]
    return parseWireDefinitions(resp.data);
  }

  // ===========================================================================
  // Declarative Sync
  // ===========================================================================

  /**
   * Declarative, idempotent sync of a workflow YAML string.
   *
   * On every call (e.g. worker boot), sync:
   *  1. Parses the YAML to extract name and version
   *  2. Fetches the existing definition from the server
   *  3. Compares versions:
   *     - Not found → creates the workflow
   *     - Same version → no-op (returns "unchanged")
   *     - Different version → updates the workflow (upsert)
   *
   * Safe to call on every startup. The version field acts as a change guard.
   *
   * @param yaml - YAML definition string
   * @param opts - Options (namespace)
   */
  async sync(
    yaml: string,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult> {
    const { name, version, description } = extractWorkflowMeta(yaml);
    const namespace = this.sender.getNamespace(opts?.namespace);

    // Check if workflow already exists
    const existing = await this.getDefinition(name, { namespace });

    if (existing !== null) {
      const existingVersion = extractYAMLField(existing, "version");
      if (existingVersion === version) {
        return { name, version, description, action: "unchanged" };
      }
    }

    // Create or update (server upserts)
    await this.create(name, yaml, { namespace });

    return {
      name,
      version,
      description,
      action: existing !== null ? "updated" : "created",
    };
  }

  /**
   * Sync raw YAML bytes.
   *
   * @param yaml - YAML definition bytes
   * @param opts - Options (namespace)
   */
  async syncBytes(
    yaml: Uint8Array,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult> {
    return this.sync(textDecoder.decode(yaml), opts);
  }

  /**
   * Sync all YAML files in a directory.
   *
   * This is a Node.js-only operation (requires `fs`). In the browser, use
   * `sync()` or `syncBytes()` directly.
   *
   * The `readDir` function must be injected by the caller — this keeps the
   * core package free of Node.js dependencies.
   *
   * @param readDir - Function that returns an array of `{ name, content }` for all .yaml/.yml files in a directory
   * @param opts - Options (namespace)
   *
   * @example
   * ```ts
   * import { readdir, readFile } from "node:fs/promises";
   * import { join } from "node:path";
   *
   * const results = await client.workflow.syncDir(async (dir) => {
   *   const entries = await readdir(dir, { withFileTypes: true });
   *   const files = entries.filter(e => !e.isDirectory() && /\.ya?ml$/.test(e.name));
   *   return Promise.all(files.map(async f => ({
   *     name: f.name,
   *     content: await readFile(join(dir, f.name), "utf-8"),
   *   })));
   * }, "./workflows", opts);
   * ```
   */
  async syncDir(
    readDir: WorkflowSyncDirFn,
    dir: string,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult[]> {
    const files = await readDir(dir);
    const results: WorkflowSyncResult[] = [];
    for (const file of files) {
      const result = await this.sync(file.content, opts);
      results.push(result);
    }
    return results;
  }
}

// =============================================================================
// YAML Metadata Extraction (lightweight — no full parser needed)
// =============================================================================

/**
 * Extract name and version from workflow YAML.
 * Works with both YAML and JSON formats.
 */
export function extractWorkflowMeta(yaml: string): {
  name: string;
  version: string;
  description: string;
} {
  const name = extractYAMLField(yaml, "name");
  const version = extractYAMLField(yaml, "version");
  const description = extractYAMLField(yaml, "description") ?? "";

  if (!name) {
    throw new Error("flo: workflow YAML missing required 'name' field");
  }
  if (!version) {
    throw new Error("flo: workflow YAML missing required 'version' field");
  }

  return { name, version, description };
}

/**
 * Lightweight extraction of a top-level scalar field from YAML or JSON.
 * Handles: `field: value`, `field: "value"`, `"field": "value"`.
 */
export function extractYAMLField(
  yaml: string,
  field: string
): string | undefined {
  const lines = yaml.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();

    // Skip comments and empty lines
    if (trimmed === "" || trimmed.startsWith("#")) continue;

    // YAML: `field: value` or `field: "value"`
    if (trimmed.startsWith(field + ":")) {
      const val = trimmed.slice(field.length + 1).trim();
      return unquote(val);
    }

    // JSON: `"field": "value"` or `"field": value`
    const jsonKey = `"${field}"`;
    if (trimmed.startsWith(jsonKey)) {
      const rest = trimmed.slice(jsonKey.length).trim();
      if (rest.startsWith(":")) {
        let val = rest.slice(1).trim();
        // Remove trailing comma (JSON)
        if (val.endsWith(",")) val = val.slice(0, -1);
        return unquote(val);
      }
    }
  }

  return undefined;
}

/**
 * Parse binary wire format for workflow definitions.
 *
 * Wire format: [count:u32]([name_len:u16][name][version_len:u16][version][created_at:i64])*
 *              [has_more:u8][cursor_len:u16]
 */
function parseWireDefinitions(data: Uint8Array): WorkflowDefinitionEntry[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);
  const results: WorkflowDefinitionEntry[] = [];
  let pos = 4;

  for (let i = 0; i < count; i++) {
    const nameLen = view.getUint16(pos, true);
    pos += 2;
    const name = textDecoder.decode(data.subarray(pos, pos + nameLen));
    pos += nameLen;

    const versionLen = view.getUint16(pos, true);
    pos += 2;
    const version = textDecoder.decode(data.subarray(pos, pos + versionLen));
    pos += versionLen;

    const createdAt = Number(view.getBigInt64(pos, true));
    pos += 8;

    results.push({ name, version, created_at: createdAt });
  }

  return results;
}

/**
 * Parse binary wire format for workflow history events.
 *
 * Wire format: [count:u32]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
 *              [has_more:u8][cursor_len:u16]
 */
function parseWireHistory(data: Uint8Array): WorkflowHistoryEvent[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);
  const results: WorkflowHistoryEvent[] = [];
  let pos = 4;

  for (let i = 0; i < count; i++) {
    const typeLen = view.getUint16(pos, true);
    pos += 2;
    const type = textDecoder.decode(data.subarray(pos, pos + typeLen));
    pos += typeLen;

    const detailLen = view.getUint16(pos, true);
    pos += 2;
    const detail = textDecoder.decode(data.subarray(pos, pos + detailLen));
    pos += detailLen;

    const timestamp = Number(view.getBigInt64(pos, true));
    pos += 8;

    results.push({ type, detail, timestamp });
  }

  return results;
}

/**
 * Parse binary wire format for workflow run listing.
 *
 * Wire format: [count:u32]([run_id_len:u16][run_id][workflow_len:u16][workflow]
 *              [status_len:u16][status][created_at:i64])*
 *              [has_more:u8][cursor_len:u16]
 */
function parseWireListRuns(data: Uint8Array): WorkflowListRunEntry[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);
  const results: WorkflowListRunEntry[] = [];
  let pos = 4;

  for (let i = 0; i < count; i++) {
    const runIdLen = view.getUint16(pos, true);
    pos += 2;
    const run_id = textDecoder.decode(data.subarray(pos, pos + runIdLen));
    pos += runIdLen;

    const workflowLen = view.getUint16(pos, true);
    pos += 2;
    const workflow = textDecoder.decode(data.subarray(pos, pos + workflowLen));
    pos += workflowLen;

    const statusLen = view.getUint16(pos, true);
    pos += 2;
    const status = textDecoder.decode(data.subarray(pos, pos + statusLen));
    pos += statusLen;

    const created_at = Number(view.getBigInt64(pos, true));
    pos += 8;

    results.push({ run_id, workflow, status, created_at });
  }

  return results;
}

/**
 * Remove surrounding quotes from a string value.
 */
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

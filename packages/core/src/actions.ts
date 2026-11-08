/**
 * Action and Worker operations for Flo SDK.
 */

import { createServerError, IncompleteResponseError } from "./errors.js";
import {
  AnswerReader,
  decodeActionInfoAnswer,
  decodeActionStatusAnswer,
  decodeAwaitAnswer,
  decodeEmptyAnswer,
  decodeTouchAnswer,
  decodeWorkerInfoAnswer,
  decodeWorkerListAnswer,
  encodeAwaitBody,
  encodeCompleteBody,
  encodeFailBody,
  encodeTouchBody,
  encodeWorkerListBody,
  MAX_WORKER_LIST_LIMIT,
} from "./kv_body.js";
import {
  type ActionDeleteOptions,
  type ActionInfo,
  type ActionInfoOptions,
  type ActionInvokeOptions,
  type ActionInvokeResult,
  type ActionListEntry,
  type ActionListOptions,
  type ActionListResult,
  type ActionRegisterOptions,
  type ActionRunStatus,
  type ActionStatusOptions,
  ActionType,
  OpCode,
  type RawResponse,
  StatusCode,
  type TaskDelivery,
  type WorkerAwaitOptions,
  type WorkerAwaitResult,
  type WorkerCompleteOptions,
  type WorkerDeregisterOptions,
  type WorkerDrainOptions,
  type WorkerFailOptions,
  type WorkerHeartbeatOptions,
  type WorkerInfoOptions,
  type WorkerInfoResult,
  type WorkerListOptions,
  type WorkerListResult,
  type WorkerRegisterOptions,
  type WorkerStatus,
} from "./types.js";
import {
  serializeActionInvokeValue,
  serializeListValue,
  serializeActionRegisterValue,
  serializeWorkerHeartbeatValue,
  serializeWorkerRegisterValue,
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
const textDecoder = new TextDecoder();

/**
 * Action operations for the Flo client.
 */
export class ActionOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Register an action.
   *
   * @param name - Action name
   * @param actionType - Type of action (User or Wasm)
   * @param opts - Registration options
   */
  async register(
    name: string,
    actionType: ActionType = ActionType.User,
    opts?: ActionRegisterOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeActionRegisterValue(
      actionType,
      opts?.timeoutMs ?? 30000,
      opts?.maxRetries ?? 3,
      opts?.owner
    );

    const resp = await this.sender.sendRequest(
      OpCode.ActionRegister,
      namespace,
      textEncoder.encode(name),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Invoke an action.
   *
   * @param name - Action name to invoke
   * @param input - Input data for the action
   * @param opts - Invoke options
   * @returns ActionInvokeResult with run_id
   */
  async invoke(
    name: string,
    input: Uint8Array,
    opts?: ActionInvokeOptions
  ): Promise<ActionInvokeResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeActionInvokeValue(input, opts?.labels);

    const resp = await this.sender.sendRequest(
      OpCode.ActionInvoke,
      namespace,
      textEncoder.encode(name),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return { runId: parseActionInvokeAnswer(resp.data) };
  }

  /**
   * Get action run status.
   *
   * @param runId - The run ID to check
   * @param opts - Status options
   * @returns ActionRunStatus with current status
   */
  async status(runId: string, opts?: ActionStatusOptions): Promise<ActionRunStatus> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ActionStatus,
      namespace,
      textEncoder.encode(runId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return decodeActionStatusAnswer(runId, resp.data);
  }

  /**
   * An action's definition, its runs by status and how long completed runs
   * took. An action that isn't registered throws NotFoundError.
   */
  async info(name: string, opts?: ActionInfoOptions): Promise<ActionInfo> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.ActionInfo, namespace, textEncoder.encode(name), new Uint8Array(0), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeActionInfoAnswer(resp.data);
  }

  /**
   * List a page of registered actions, with their settings. Pass the
   * result's `cursor` to get the next page.
   *
   * @param opts - List options
   */
  async list(opts?: ActionListOptions): Promise<ActionListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeListValue(opts?.limit ?? 100, opts?.cursor);

    const resp = await this.sender.sendRequest(
      OpCode.ActionList,
      namespace,
      textEncoder.encode(opts?.prefix ?? ""),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    return decodeActionListAnswer(resp.data);
  }

  /**
   * Delete an action.
   *
   * @param name - Action name to delete
   * @param opts - Delete options
   */
  async delete(name: string, opts?: ActionDeleteOptions): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.ActionDelete,
      namespace,
      textEncoder.encode(name),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }
}

/**
 * Worker operations for the Flo client.
 * Handles both worker registry (register/heartbeat/deregister/drain)
 * and task dispatch (await/touch/complete/fail).
 */
export class WorkerOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Register a worker in the worker registry.
   *
   * @param workerId - Unique worker identifier
   * @param taskTypes - List of task types this worker can handle
   * @param opts - Registration options (workerType, maxConcurrency, processes, metadata, machineId)
   */
  async register(
    workerId: string,
    taskTypes: string[],
    opts?: WorkerRegisterOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerRegisterValue(taskTypes, {
      workerType: opts?.workerType,
      maxConcurrency: opts?.maxConcurrency,
      processes: opts?.processes,
      metadata: opts?.metadata,
      machineId: opts?.machineId,
    });

    const resp = await this.sender.sendRequest(
      OpCode.WorkerRegister,
      namespace,
      textEncoder.encode(workerId),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Send a heartbeat to the worker registry.
   * Returns the worker's current status (e.g. draining).
   *
   * @param workerId - Worker identifier
   * @param currentLoad - Current number of active tasks
   * @param opts - Heartbeat options
   * @returns WorkerStatus from the server
   */
  async heartbeat(
    workerId: string,
    currentLoad: number,
    opts?: WorkerHeartbeatOptions
  ): Promise<WorkerStatus> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerHeartbeatValue(currentLoad);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerHeartbeat,
      namespace,
      textEncoder.encode(workerId),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Server responds with [status:u8]
    if (resp.data.length >= 1) {
      return resp.data[0] as WorkerStatus;
    }
    return 0 as WorkerStatus; // Active
  }

  /**
   * Remove the worker from the registry.
   *
   * @param workerId - Worker identifier
   * @param opts - Deregister options
   */
  async deregister(
    workerId: string,
    opts?: WorkerDeregisterOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerDeregister,
      namespace,
      textEncoder.encode(workerId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Mark the worker as draining — no new tasks will be assigned.
   * In-flight tasks continue to completion.
   *
   * @param workerId - Worker identifier
   * @param opts - Drain options
   */
  async drain(
    workerId: string,
    opts?: WorkerDrainOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerDrain,
      namespace,
      textEncoder.encode(workerId),
      new Uint8Array(0),
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
  }

  /**
   * Wait for a task of one of `actions` (tried in order) and take it: the
   * worker holds it for the task's `leaseMs`, unless it touches it. Null if
   * no run was ready within `blockMs`.
   *
   * The await is a write. If its answer is lost, send it again with the
   * same `requestKey` to get the task the first one took, rather than leave
   * that one held until its lease runs out.
   *
   * @param workerId - Worker identifier
   * @param actions - The actions to take a task of, 1 to 64
   * @param opts - Await options (blockMs, requestKey)
   */
  async awaitTask(
    workerId: string,
    actions: string[],
    opts?: WorkerAwaitOptions
  ): Promise<WorkerAwaitResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const body = encodeAwaitBody(actions, opts?.blockMs ?? 30000, opts?.requestKey);
    const resp = await this.sender.sendRequest(OpCode.ActionAwait, namespace, textEncoder.encode(workerId), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return { task: decodeAwaitAnswer(resp.data) };
  }

  /**
   * Keep a task: with half its lease or less left, the lease runs a full
   * lease from now (up to 12 h from its delivery); with more left, nothing
   * changes. Touch at least every half lease. Returns when the lease runs
   * out without another touch, in ms since the Unix epoch. A delivery the
   * run has moved on from throws StaleReportError.
   */
  async touch(workerId: string, task: TaskDelivery): Promise<bigint> {
    const resp = await this.sender.sendRequest(OpCode.ActionTouch, task.namespace, textEncoder.encode(task.runId), encodeTouchBody(workerId, task), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeTouchAnswer(resp.data);
  }

  /**
   * Complete a task: its run is done, with `result`. Sending it again after
   * it applied answers ok; a delivery the run has moved on from, or a
   * second complete with another result, throws StaleReportError.
   */
  async complete(workerId: string, task: TaskDelivery, result: Uint8Array, opts?: WorkerCompleteOptions): Promise<void> {
    const body = encodeCompleteBody(workerId, task, result, opts?.outcome ?? "");
    const resp = await this.sender.sendRequest(OpCode.ActionComplete, task.namespace, textEncoder.encode(task.runId), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    decodeEmptyAnswer(resp.data);
  }

  /**
   * Fail a task: its run is ready again (after `delayMs`) while attempts
   * are left, unless `retry` is false. `abandoned` hands it back untried,
   * counting no failure. Sending it again after it applied answers ok; a
   * delivery the run has moved on from throws StaleReportError.
   */
  async fail(workerId: string, task: TaskDelivery, errorMessage: string, opts?: WorkerFailOptions): Promise<void> {
    const body = encodeFailBody(workerId, task, errorMessage, opts);
    const resp = await this.sender.sendRequest(OpCode.ActionFail, task.namespace, textEncoder.encode(task.runId), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    decodeEmptyAnswer(resp.data);
  }

  /**
   * List a page of the namespace's workers, in id order, or only those that
   * run `action`. Pass the answer's cursor to the next call for the page
   * after; it is null on the last page.
   */
  async list(opts?: WorkerListOptions): Promise<WorkerListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const body = encodeWorkerListBody(opts?.limit ?? MAX_WORKER_LIST_LIMIT, opts?.cursor, opts?.action);
    const resp = await this.sender.sendRequest(OpCode.WorkerList, namespace, new Uint8Array(0), body, new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeWorkerListAnswer(resp.data);
  }

  /**
   * A worker's record: its row, the metadata it registered with, and its
   * processes with their counts. A worker that isn't registered throws
   * NotFoundError.
   */
  async info(workerId: string, opts?: WorkerInfoOptions): Promise<WorkerInfoResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);
    const resp = await this.sender.sendRequest(OpCode.WorkerInfo, namespace, textEncoder.encode(workerId), new Uint8Array(0), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }
    return decodeWorkerInfoAnswer(resp.data);
  }
}

/** An invoke's answer: [run_id_len:u16][run_id][has_output:u8]; the server never sets output. */
export function parseActionInvokeAnswer(data: Uint8Array): string {
  if (data.length < 2) throw new IncompleteResponseError("invoke answer: missing run id length");
  const len = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(0, true);
  if (len === 0 || data.length !== 2 + len + 1 || data[2 + len] !== 0) {
    throw new IncompleteResponseError(`invoke answer: run id length ${len} in ${data.length} bytes`);
  }
  return textDecoder.decode(data.subarray(2, 2 + len));
}

/**
 * action.list's answer: [count:u32]([name_len:u16][name][value_len:u32][value])*
 * [has_more:u8][cursor_len:u16][cursor], each value
 * [version:u32][owner_len:u16][owner][timeout_ms:u64][max_attempts:u32][created_at_ms:i64].
 */
export function decodeActionListAnswer(data: Uint8Array): ActionListResult {
  const r = new AnswerReader(data);
  const actions: ActionListEntry[] = [];
  for (let n = r.u32(); n > 0; n--) {
    const name = r.string16();
    const v = new AnswerReader(r.bytes32());
    actions.push({ name, version: v.u32(), owner: v.string16(), timeoutMs: v.u64(), maxAttempts: v.u32(), createdAtMs: v.i64() });
    v.end();
  }
  const hasMore = r.bool();
  const cursor = r.bytes16();
  r.end();
  if (hasMore !== cursor.length > 0) {
    throw new IncompleteResponseError(`action list answer: has_more ${hasMore} with a cursor of ${cursor.length} bytes`);
  }
  return { actions, hasMore, cursor: hasMore ? cursor : null };
}

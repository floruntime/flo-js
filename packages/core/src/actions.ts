/**
 * Action and Worker operations for Flo SDK.
 */

import { createServerError } from "./errors.js";
import {
  type ActionDeleteOptions,
  type ActionInvokeOptions,
  type ActionInvokeResult,
  type ActionListOptions,
  type ActionListResult,
  type ActionRegisterOptions,
  type ActionRunStatus,
  type ActionStatusOptions,
  ActionType,
  OpCode,
  OptionTag,
  type RawResponse,
  StatusCode,
  type WorkerAwaitOptions,
  type WorkerAwaitResult,
  type WorkerCompleteOptions,
  type WorkerFailOptions,
  type WorkerListOptions,
  type WorkerListResult,
  type WorkerRegisterOptions,
  type WorkerTouchOptions,
} from "./types.js";
import {
  OptionsBuilder,
  parseTaskAssignment,
  serializeActionInvokeValue,
  serializeActionListValue,
  serializeActionRegisterValue,
  serializeWorkerAwaitValue,
  serializeWorkerCompleteValue,
  serializeWorkerFailValue,
  serializeWorkerListValue,
  serializeWorkerRegisterValue,
  serializeWorkerTouchValue,
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
      opts?.description
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

    const value = serializeActionInvokeValue(
      input,
      opts?.priority ?? 10,
      opts?.idempotencyKey
    );

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

    // Response contains run_id as string
    const runId = resp.data.length > 0 ? textDecoder.decode(resp.data) : "";
    return { runId };
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

    // Parse status response - format TBD based on server implementation
    return {
      runId,
      status: "unknown",
      result: resp.data.length > 0 ? resp.data : undefined,
    };
  }

  /**
   * List registered actions.
   *
   * @param opts - List options
   * @returns ActionListResult with list of actions
   */
  async list(opts?: ActionListOptions): Promise<ActionListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeActionListValue(opts?.limit ?? 100);

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

    // Parse list response - format TBD based on server implementation
    return { actions: [] };
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
 */
export class WorkerOperations {
  constructor(private readonly sender: RequestSender) {}

  /**
   * Register a worker.
   *
   * @param workerId - Unique worker identifier
   * @param taskTypes - List of task types this worker can handle
   * @param opts - Registration options
   */
  async register(
    workerId: string,
    taskTypes: string[],
    opts?: WorkerRegisterOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerRegisterValue(taskTypes);

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
   * Wait for a task assignment.
   *
   * @param workerId - Worker identifier
   * @param taskTypes - Task types to listen for
   * @param opts - Await options (blockMs, timeoutMs)
   * @returns WorkerAwaitResult with task if available
   */
  async awaitTask(
    workerId: string,
    taskTypes: string[],
    opts?: WorkerAwaitOptions
  ): Promise<WorkerAwaitResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerAwaitValue(taskTypes);

    // Build options for block_ms and timeout_ms
    const optionsBuilder = new OptionsBuilder();
    if (opts?.blockMs !== undefined) {
      optionsBuilder.addU32(OptionTag.BlockMS, opts.blockMs);
    }
    if (opts?.timeoutMs !== undefined) {
      optionsBuilder.addU32(OptionTag.TimeoutMS, opts.timeoutMs);
    }

    const resp = await this.sender.sendRequest(
      OpCode.WorkerAwait,
      namespace,
      textEncoder.encode(workerId),
      value,
      optionsBuilder.build()
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Parse task assignment response - if empty, no task available
    if (resp.data.length === 0) {
      return { task: null };
    }

    const task = parseTaskAssignment(resp.data);
    return { task };
  }

  /**
   * Extend task lease (heartbeat).
   *
   * @param workerId - Worker identifier
   * @param taskId - Task identifier to extend
   * @param opts - Touch options (extendMs)
   */
  async touch(
    workerId: string,
    taskId: string,
    opts?: WorkerTouchOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerTouchValue(taskId, opts?.extendMs ?? 30000);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerTouch,
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
   * Complete a task successfully.
   *
   * @param workerId - Worker identifier
   * @param taskId - Task identifier to complete
   * @param result - Result data from the task
   * @param opts - Complete options
   */
  async complete(
    workerId: string,
    taskId: string,
    result: Uint8Array,
    opts?: WorkerCompleteOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerCompleteValue(taskId, result);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerComplete,
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
   * Fail a task.
   *
   * @param workerId - Worker identifier
   * @param taskId - Task identifier that failed
   * @param errorMessage - Error message describing the failure
   * @param opts - Fail options (retry flag)
   */
  async fail(
    workerId: string,
    taskId: string,
    errorMessage: string,
    opts?: WorkerFailOptions
  ): Promise<void> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerFailValue(taskId, errorMessage, opts?.retry ?? true);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerFail,
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
   * List registered workers.
   *
   * @param opts - List options
   * @returns WorkerListResult with list of workers
   */
  async list(opts?: WorkerListOptions): Promise<WorkerListResult> {
    const namespace = this.sender.getNamespace(opts?.namespace);

    const value = serializeWorkerListValue(opts?.limit ?? 100);

    const resp = await this.sender.sendRequest(
      OpCode.WorkerList,
      namespace,
      new Uint8Array(0),
      value,
      new Uint8Array(0)
    );

    if (resp.status !== StatusCode.OK) {
      throw createServerError(resp.status, resp.data);
    }

    // Parse list response - format TBD based on server implementation
    return { workers: [] };
  }
}

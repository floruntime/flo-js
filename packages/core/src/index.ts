/**
 * @floruntime/core - Flo SDK core package
 *
 * This package provides the wire protocol, types, and transport-agnostic operations
 * for the Flo SDK. It is used by the Node.js and Web packages.
 */

// Types
export {
  // Protocol constants
  MAGIC,
  VERSION,
  HEADER_SIZE,
  MAX_NAMESPACE_SIZE,
  MAX_KEY_SIZE,
  MAX_VALUE_SIZE,
  // Enums
  OpCode,
  StatusCode,
  statusCodeToString,
  OptionTag,
  // KV Result types
  type KVEntry,
  type ScanResult,
  type VersionEntry,
  type PutResult,
  type GetResult,
  type MGetEntry,
  type KVBeginResult,
  type KVCommitResult,
  // Queue types (for Node.js backend use)
  type Message,
  type DequeueResult,
  // Stream types (for real-time applications)
  StorageTier,
  StreamID,
  type StreamRecord,
  type StreamAppendResult,
  type StreamReadResult,
  type StreamInfoResult,
  type StreamAppendOptions,
  type StreamReadOptions,
  type StreamSubscribeOptions,
  type StreamEventCallback,
  type StreamSubscription,
  type StreamGroupOptions,
  type StreamAckOptions,
  type StreamNackOptions,
  // KV Option types
  type GetOptions,
  type PutOptions,
  type DeleteOptions,
  type ScanOptions,
  type HistoryOptions,
  type KVIncrOptions,
  type KVTouchOptions,
  type KVExistsOptions,
  type KVJsonOptions,
  type KVMGetOptions,
  // Queue Option types (for Node.js backend use)
  type EnqueueOptions,
  type DequeueOptions,
  type AckOptions,
  type NackOptions,
  type DLQListOptions,
  type DLQRequeueOptions,
  type PeekOptions,
  type TouchOptions,
  // Action types
  ActionType,
  type ActionInfo,
  type ActionRunStatus,
  type ActionInvokeResult,
  type ActionListResult,
  type ActionRegisterOptions,
  type ActionInvokeOptions,
  type ActionStatusOptions,
  type ActionListOptions,
  type ActionDeleteOptions,
  // Worker types
  WorkerType,
  WorkerStatus,
  ProcessKind,
  type ProcessEntry,
  type TaskAssignment,
  type WorkerAwaitResult,
  type WorkerInfo,
  type WorkerListResult,
  type WorkerRegisterOptions,
  type WorkerAwaitOptions,
  type WorkerTouchOptions,
  type WorkerCompleteOptions,
  type WorkerFailOptions,
  type WorkerListOptions,
  type WorkerHeartbeatOptions,
  type WorkerDeregisterOptions,
  type WorkerDrainOptions,
  // Workflow types
  type WorkflowCreateOptions,
  type WorkflowGetDefinitionOptions,
  type WorkflowStartOptions,
  type WorkflowStatusOptions,
  type WorkflowStatusResult,
  type WorkflowHistoryOptions,
  type WorkflowHistoryEvent,
  type WorkflowListRunsOptions,
  type WorkflowListRunEntry,
  type WorkflowSignalOptions,
  type WorkflowCancelOptions,
  type WorkflowDisableOptions,
  type WorkflowEnableOptions,
  type WorkflowListDefinitionsOptions,
  type WorkflowDefinitionEntry,
  type WorkflowSyncOptions,
  type WorkflowSyncResult,
  type WorkflowSyncDirFile,
  type WorkflowSyncDirFn,
  // Processing types
  type ProcessingSubmitOptions,
  type ProcessingStatusOptions,
  type ProcessingListOptions,
  type ProcessingStopOptions,
  type ProcessingCancelOptions,
  type ProcessingSavepointOptions,
  type ProcessingRestoreOptions,
  type ProcessingRescaleOptions,
  type ProcessingStatusResult,
  type ProcessingListEntry,
  type ProcessingSyncOptions,
  type ProcessingSyncResult,
  type ProcessingSyncDirFile,
  type ProcessingSyncDirFn,
  // Internal types
  type RawResponse,
  type Transport,
  type ClientOptions,
  type WebClientOptions,
  // Logger
  type Logger,
  consoleLogger,
  silentLogger,
} from "./types.js";

// Errors
export {
  FloError,
  NotConnectedError,
  ConnectionError,
  InvalidEndpointError,
  UnexpectedEOFError,
  InvalidMagicError,
  UnsupportedVersionError,
  InvalidChecksumError,
  IncompleteResponseError,
  NamespaceTooLargeError,
  KeyTooLargeError,
  ValueTooLargeError,
  TimeoutError,
  ServerError,
  NotFoundError,
  BadRequestError,
  ConflictError,
  UnauthorizedError,
  OverloadedError,
  InternalError,
  createServerError,
  // Error type guards
  isNotFound,
  isConflict,
  isBadRequest,
  isUnauthorized,
  isOverloaded,
  isInternal,
} from "./errors.js";

// Wire protocol
export {
  OptionsBuilder,
  computeCRC32,
  serializeRequest,
  parseResponseHeader,
  parseRawResponse,
  parseScanResponse,
  parseHistoryResponse,
  parseDequeueResponse,
  parseEnqueueResponse,
  serializeSeqs,
} from "./wire.js";

// Operations
export { KVOperations, type RequestSender as KVRequestSender } from "./kv.js";
export {
  Transaction,
  TxnFinishedError,
  TxnUnsupportedOpError,
  beginTxn,
} from "./kv_txn.js";
export { QueueOperations, type RequestSender as QueueRequestSender } from "./queue.js";
export {
  StreamOperations,
  KVReadOnlyOperations,
  parseStreamReadResponse,
  parseStreamAppendResponse,
  parseStreamInfoResponse,
  type StreamRequestSender,
} from "./streams.js";
export {
  ActionOperations,
  WorkerOperations,
  type RequestSender as ActionRequestSender,
} from "./actions.js";
export {
  WorkflowOperations,
  extractWorkflowMeta,
  extractYAMLField,
  type RequestSender as WorkflowRequestSender,
} from "./workflows.js";
export {
  ProcessingOperations,
  extractProcessingMeta,
  type RequestSender as ProcessingRequestSender,
} from "./processing.js";

// Wire protocol - Action/Worker serialization
export {
  serializeActionRegisterValue,
  serializeActionInvokeValue,
  serializeActionListValue,
  serializeWorkerRegisterValue,
  serializeWorkerAwaitValue,
  serializeWorkerTouchValue,
  serializeWorkerCompleteValue,
  serializeWorkerFailValue,
  serializeWorkerListValue,
  parseTaskAssignment,
} from "./wire.js";

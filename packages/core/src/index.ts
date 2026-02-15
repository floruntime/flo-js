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
  // Queue types (for Node.js backend use)
  type Message,
  type DequeueResult,
  // Stream types (for real-time applications)
  StorageTier,
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

/**
 * Flo protocol constants and types.
 */

// Protocol constants
export const MAGIC = 0x004f4c46; // "FLO\0" in little-endian
export const VERSION = 0x01;
export const HEADER_SIZE = 24;

// Size limits (for client-side validation)
export const MAX_NAMESPACE_SIZE = 255;
export const MAX_KEY_SIZE = 64 * 1024; // 64 KB
export const MAX_VALUE_SIZE = 16 * 1024 * 1024; // 16 MB practical limit

/**
 * Operation codes for Flo protocol requests.
 */
export const OpCode = {
  // System Operations (0x00 - 0x0F)
  Ping: 0x00,
  Pong: 0x01,
  ErrorResponse: 0x02,
  Auth: 0x03,
  SetDurability: 0x04,
  OK: 0x05,

  // Streams (0x10 - 0x1F)
  StreamAppend: 0x10,
  StreamRead: 0x11,
  StreamTrim: 0x12,
  StreamInfo: 0x13,
  StreamAppendResponse: 0x14,
  StreamReadResponse: 0x15,
  StreamEvent: 0x16, // Server-push for subscriptions
  StreamSubscribe: 0x17, // Subscribe to stream (WebSocket continuous push)
  StreamUnsubscribe: 0x18, // Unsubscribe from stream
  StreamSubscribed: 0x19, // Response: subscription confirmed
  StreamUnsubscribed: 0x1a, // Response: unsubscription confirmed
  StreamList: 0x1b, // List all streams in namespace
  StreamListResponse: 0x1c,
  StreamCreate: 0x1d, // Create stream with partition count
  StreamCreateResponse: 0x1e,
  StreamAlter: 0x1f, // Alter stream configuration (retention policy)

  // Stream Consumer Groups (0x20 - 0x2F)
  StreamGroupCreate: 0x20, // Create consumer group with configuration
  StreamGroupJoin: 0x21,
  StreamGroupLeave: 0x22,
  StreamGroupRead: 0x23,
  StreamGroupAck: 0x24,
  StreamGroupClaim: 0x25,
  StreamGroupPending: 0x26,
  StreamGroupConfigureSweeper: 0x27,
  StreamGroupReadResponse: 0x28,
  StreamGroupNack: 0x29,
  StreamGroupTouch: 0x2a, // Extend ack deadline for pending messages
  StreamGroupInfo: 0x2b, // Get consumer group info (config + consumers)
  StreamGroupDelete: 0x2c, // Delete consumer group

  // KV Operations (0x30 - 0x3F)
  KVPut: 0x30,
  KVGet: 0x31,
  KVDelete: 0x32,
  KVScan: 0x33,
  KVHistory: 0x34,
  KVGetResponse: 0x35,
  KVPutResponse: 0x36,
  KVScanResponse: 0x37,
  KVHistoryResponse: 0x38,

  // Transactions (0x39 - 0x3B)
  KVBeginTxn: 0x39,
  KVCommitTxn: 0x3a,
  KVRollbackTxn: 0x3b,

  // Snapshots (0x3C - 0x3F)
  KVSnapshotCreate: 0x3c,
  KVSnapshotGet: 0x3d,
  KVSnapshotRelease: 0x3e,
  KVSnapshotCreateResponse: 0x3f,

  // Queues (0x40 - 0x5F)
  QueueEnqueue: 0x40,
  QueueDequeue: 0x41,
  QueueComplete: 0x42,
  QueueExtendLease: 0x43,
  QueueFail: 0x44,
  QueueFailAuto: 0x45,
  QueueDLQList: 0x46,
  QueueDLQDelete: 0x47,
  QueueDLQRequeue: 0x48,
  QueueDLQStats: 0x49,
  QueuePromoteDue: 0x4a,
  QueueStats: 0x4b,
  QueuePeek: 0x4c,
  QueueTouch: 0x4d,
  QueueBatchEnqueue: 0x4e,
  QueuePurge: 0x4f,

  // Queue responses (0x50 - 0x5F)
  QueueEnqueueResponse: 0x50,
  QueueDequeueResponse: 0x51,
  QueueDLQListResponse: 0x52,
  QueueStatsResponse: 0x53,
  QueuePeekResponse: 0x54,
  QueueTouchResponse: 0x55,
  QueueBatchEnqueueResponse: 0x56,
  QueuePurgeResponse: 0x57,
  QueueList: 0x58, // List all queues in namespace
  QueueListResponse: 0x59,

  // Actions (0x60 - 0x6D)
  ActionRegister: 0x60,
  ActionInvoke: 0x61,
  ActionStatus: 0x62,
  ActionList: 0x63,
  ActionDelete: 0x64,
  ActionAwait: 0x65,
  ActionComplete: 0x66,
  ActionFail: 0x67,
  ActionTouch: 0x68,
  ActionRegisterResponse: 0x69,
  ActionInvokeResponse: 0x6a,
  ActionStatusResponse: 0x6b,
  ActionListResponse: 0x6c,
  ActionTaskAssignment: 0x6d,

  // Workers (0x70 - 0x78)
  WorkerRegister: 0x70,
  WorkerHeartbeat: 0x71,
  WorkerDeregister: 0x72,
  WorkerList: 0x73,
  WorkerInfo: 0x74,
  WorkerRegisterResponse: 0x75,
  WorkerListResponse: 0x76,
  WorkerInfoResponse: 0x77,
  WorkerDrain: 0x78,

  // Workflows (0x80 - 0x93)
  WorkflowCreate: 0x80, // Create workflow from YAML definition
  WorkflowStart: 0x81, // Start a workflow run
  WorkflowSignal: 0x82, // Send signal to running workflow
  WorkflowCancel: 0x83, // Cancel a workflow run
  WorkflowStatus: 0x84, // Get workflow run status
  WorkflowHistory: 0x85, // Get workflow run history
  WorkflowListRuns: 0x86, // List workflow runs
  WorkflowGetDefinition: 0x87, // Get workflow definition
  WorkflowCreateResponse: 0x88,
  WorkflowStartResponse: 0x89,
  WorkflowStatusResponse: 0x8a,
  WorkflowHistoryResponse: 0x8b,
  WorkflowListRunsResponse: 0x8c,
  WorkflowGetDefinitionResponse: 0x8d,
  WorkflowDisable: 0x8e,
  WorkflowEnable: 0x8f,
  WorkflowDisableResponse: 0x90,
  WorkflowEnableResponse: 0x91,
  WorkflowListDefinitions: 0x92,
  WorkflowListDefinitionsResponse: 0x93,

  // Cluster Management (0xA0 - 0xAF)
  ClusterStatus: 0xa0, // Get cluster status (leader, term, health)
  ClusterMembers: 0xa1, // List cluster members
  ClusterJoin: 0xa2, // Request to join cluster
  ClusterLeave: 0xa3, // Request to leave cluster gracefully
  ClusterTransferLeader: 0xa4, // Transfer leadership to another node
  ClusterAddNode: 0xa5, // Admin: add node to cluster (leader only)
  ClusterRemoveNode: 0xa6, // Admin: remove node from cluster (leader only)
  ClusterStatusResponse: 0xa8,
  ClusterMembersResponse: 0xa9,
  ClusterJoinResponse: 0xaa,

  // Namespace Management (0xB0 - 0xBF)
  NamespaceCreate: 0xb0, // Create a new namespace
  NamespaceDelete: 0xb1, // Delete an existing namespace
  NamespaceList: 0xb2, // List all namespaces
  NamespaceInfo: 0xb3, // Get namespace info/config
  NamespaceCreateResponse: 0xb4,
  NamespaceDeleteResponse: 0xb5,
  NamespaceListResponse: 0xb6,
  NamespaceInfoResponse: 0xb7,
  NamespaceConfigSet: 0xb8,
  NamespaceConfigGet: 0xb9,
  NamespaceConfigSetResponse: 0xba,
  NamespaceConfigGetResponse: 0xbb,

  // Processing / Stream Processing (0xC0 - 0xD1)
  ProcessingSubmit: 0xc0, // Submit a processing job
  ProcessingStop: 0xc1, // Gracefully stop a processing job
  ProcessingCancel: 0xc2, // Force cancel a processing job
  ProcessingStatus: 0xc3, // Get processing job status
  ProcessingList: 0xc4, // List processing jobs
  ProcessingSavepoint: 0xc6, // Trigger a savepoint
  ProcessingRestore: 0xc7, // Restore from a savepoint
  ProcessingRescale: 0xc8, // Rescale job parallelism
  ProcessingSubmitResponse: 0xc9,
  ProcessingStopResponse: 0xca,
  ProcessingCancelResponse: 0xcb,
  ProcessingStatusResponse: 0xcc,
  ProcessingListResponse: 0xcd,
  ProcessingSavepointResponse: 0xcf,
  ProcessingRestoreResponse: 0xd0,
  ProcessingRescaleResponse: 0xd1,

  // Time-Series Operations (0xE0 - 0xED)
  TSWrite: 0xe0, // Write data point(s) to a time-series
  TSRead: 0xe1, // Read raw data points from a time-series
  TSQuery: 0xe2, // Aggregated query over a time range
  TSFloQL: 0xe3, // FloQL query string
  TSList: 0xe4, // List measurements or series
  TSDelete: 0xe5, // Delete a series and its metadata
  TSRetention: 0xe6, // Configure retention / downsampling policy
  TSWriteResponse: 0xe7,
  TSReadResponse: 0xe8,
  TSQueryResponse: 0xe9,
  TSFloQLResponse: 0xea,
  TSListResponse: 0xeb,
  TSDeleteResponse: 0xec,
  TSRetentionResponse: 0xed,
} as const;

export type OpCode = (typeof OpCode)[keyof typeof OpCode];

/**
 * Status codes for Flo protocol responses.
 */
export const StatusCode = {
  OK: 0,
  ErrorGeneric: 1,
  NotFound: 2,
  BadRequest: 3,
  CrossCoreTransaction: 4,
  NoActiveTransaction: 5,
  GroupLocked: 6,
  Unauthorized: 7,
  Conflict: 8,
  InternalError: 9,
  Overloaded: 10,
  RateLimited: 11, // Request rate limit exceeded (WebSocket)
} as const;

export type StatusCode = (typeof StatusCode)[keyof typeof StatusCode];

/**
 * Returns a human-readable message for a status code.
 */
export function statusCodeToString(status: StatusCode): string {
  switch (status) {
    case StatusCode.OK:
      return "OK";
    case StatusCode.ErrorGeneric:
      return "Generic error";
    case StatusCode.NotFound:
      return "Not found";
    case StatusCode.BadRequest:
      return "Bad request";
    case StatusCode.CrossCoreTransaction:
      return "Cross-core transaction not supported";
    case StatusCode.NoActiveTransaction:
      return "No active transaction";
    case StatusCode.GroupLocked:
      return "Consumer group is locked";
    case StatusCode.Unauthorized:
      return "Unauthorized";
    case StatusCode.Conflict:
      return "Conflict";
    case StatusCode.InternalError:
      return "Internal server error";
    case StatusCode.Overloaded:
      return "Server overloaded";
    case StatusCode.RateLimited:
      return "Request rate limit exceeded";
    default:
      return "Unknown error";
  }
}

/**
 * Option tags for TLV-encoded operation parameters.
 */
export const OptionTag = {
  // KV Options (0x01 - 0x0F)
  TTLSeconds: 0x01, // u64: Time-to-live in seconds (0 = no expiration)
  CASVersion: 0x02, // u64: Expected version for compare-and-swap
  IfNotExists: 0x03, // void: Only set if key doesn't exist (NX)
  IfExists: 0x04, // void: Only set if key exists (XX)
  Limit: 0x05, // u32: Maximum number of results for scan/list operations
  KeysOnly: 0x06, // u8: Skip values in scan response (0/1)
  Cursor: 0x07, // bytes: Pagination cursor (ShardWalker format)
  RoutingKey: 0x08, // string: Explicit routing key for shard co-location

  // Queue Options (0x10 - 0x1F)
  Priority: 0x10, // u8: Message priority (0-255, higher = more urgent)
  DelayMS: 0x11, // u64: Delay before message becomes visible
  VisibilityTimeoutMS: 0x12, // u32: How long message is invisible after dequeue
  DedupKey: 0x13, // string: Deduplication key
  MaxRetries: 0x14, // u8: Maximum retry attempts before DLQ
  Count: 0x15, // u32: Number of messages to dequeue
  SendToDLQ: 0x16, // u8: Whether to send failed messages to DLQ (0/1)
  BlockMS: 0x17, // u32: Blocking timeout for dequeue (0 = infinite, default = 0)
  WaitMS: 0x18, // u32: Watch timeout - wait for NEXT version change (0=forever)

  // Stream Options (0x20 - 0x2F) - StreamID-native ONLY
  // 0x20 reserved
  StreamStart: 0x21, // [16]u8: Start StreamID for reads (inclusive)
  StreamEnd: 0x22, // [16]u8: End StreamID for reads (inclusive)
  StreamTail: 0x23, // void: Flag indicating tail read (start from end of stream)
  Partition: 0x24, // u32: Explicit partition index
  PartitionKey: 0x25, // string: Key for partition routing
  MaxAgeSeconds: 0x26, // u64: Maximum age in seconds for retention
  MaxBytes: 0x27, // u64: Maximum size in bytes for retention
  DryRun: 0x28, // void: Flag to preview what would be deleted without deleting
  RetentionCount: 0x29, // u64: Retention policy - max event count
  RetentionAge: 0x2a, // u64: Retention policy - max age in seconds
  RetentionBytes: 0x2b, // u64: Retention policy - max bytes

  // Consumer Group Options (0x30 - 0x3F)
  AckTimeoutMS: 0x30, // u32: Time before unacked message auto-redelivers (overrides default)
  MaxDeliver: 0x31, // u8: Max delivery attempts before DLQ (default: 10, 0=unlimited)
  SubscriptionMode: 0x32, // u8: 0=shared, 1=exclusive, 2=key_shared
  RedeliveryDelayMS: 0x33, // u32: Delay before NACK'd message becomes visible again
  ConsumerTimeoutMS: 0x34, // u32: Remove consumer from group if no activity (session timeout)
  NoAck: 0x35, // void: Auto-ack on delivery (at-most-once semantics)
  IdleTimeoutMS: 0x36, // u64: Min idle time for claiming stuck messages (XCLAIM-style)
  MaxAckPending: 0x37, // u32: Max unacked messages per consumer (backpressure)
  ExtendAckMS: 0x38, // u32: Amount of time to extend ack deadline (for touch)
  MaxStandbys: 0x39, // u16: Max standby consumers in exclusive mode (0=singleton, null=unlimited)
  NumSlots: 0x3a, // u16: Number of hash slots for key_shared mode (default: 256)

  // Worker/Action Options (0x40 - 0x4F)
  WorkerID: 0x40, // string: Worker identifier
  ExtendMS: 0x41, // u32: Lease extension time in milliseconds
  MaxTasks: 0x42, // u32: Maximum tasks to return in batch
  Retry: 0x43, // u8: Whether to retry on failure (0/1)

  // Workflow Options (0x50 - 0x5F)
  TimeoutMS: 0x50, // u64: Workflow/activity timeout
  RetryPolicy: 0x51, // bytes: Serialized retry policy
  CorrelationID: 0x52, // string: Correlation ID for tracing
  SubscriptionID: 0x53, // u64: Subscription ID for stream subscriptions

  // Time-Series Options (0x60 - 0x6F)
  TSFromMS: 0x60, // i64: Start of time range (inclusive, unix ms)
  TSToMS: 0x61, // i64: End of time range (inclusive, 0 = now)
  TSWindowMS: 0x62, // i64: Aggregation window size (ms)
  TSAggregation: 0x63, // string: Aggregation function name (avg, sum, count, min, max)
  TSField: 0x64, // string: Field name filter (empty = "value")
  TSTags: 0x65, // string: Comma-separated tag filters "key=val,key2=val2"
  TSPrecision: 0x66, // u8: Timestamp precision (0=ns, 1=us, 2=ms, 3=s)
  TSTimestamp: 0x67, // i64: Explicit timestamp for write (0 = server-assigned)
  TSRawTTL: 0x68, // string: Raw data TTL (e.g., "7d")
  TSDownsample: 0x69, // string: Downsample rule (e.g., "1m:avg:30d")
  TSBatch: 0x6a, // void: Flag indicating batch/line-protocol mode
} as const;

export type OptionTag = (typeof OptionTag)[keyof typeof OptionTag];

/**
 * KV entry from scan results.
 */
export interface KVEntry {
  key: Uint8Array;
  value: Uint8Array | null; // null if keys_only=true
}

/**
 * Result of a KV scan operation.
 */
export interface ScanResult {
  entries: KVEntry[];
  cursor: Uint8Array | null; // null if no more pages
  hasMore: boolean;
}

/**
 * KV version entry from history.
 */
export interface VersionEntry {
  version: bigint;
  timestamp: bigint;
  value: Uint8Array;
}

/**
 * Queue message.
 */
export interface Message {
  seq: bigint;
  payload: Uint8Array;
}

/**
 * Result of a queue dequeue operation.
 */
export interface DequeueResult {
  messages: Message[];
}

// ============================================================================
// STREAM TYPES
// ============================================================================

/**
 * Storage tier of a stream record.
 */
export const StorageTier = {
  Hot: 0,
  Pending: 1,
  Warm: 2,
  Cold: 3,
} as const;

export type StorageTier = (typeof StorageTier)[keyof typeof StorageTier];

/**
 * A single stream record/event.
 */
export interface StreamRecord {
  /** Full StreamID (timestamp_ms + sequence) */
  id: StreamID;
  /** Storage tier (hot, pending, warm, cold) */
  tier: StorageTier;
  /** Event payload */
  payload: Uint8Array;
  /** Optional headers (key-value pairs) */
  headers: Record<string, string> | null;
}

/**
 * Result of appending a record to a stream.
 */
export interface StreamAppendResult {
  /** StreamID assigned to the record */
  id: StreamID;
}

/**
 * Result of reading from a stream.
 */
export interface StreamReadResult {
  /** Records read from the stream */
  records: StreamRecord[];
}

/**
 * Result of a stream info query.
 */
export interface StreamInfoResult {
  firstId: StreamID;
  lastId: StreamID;
  count: bigint;
  bytes: bigint;
  partitionCount: number;
}

/**
 * Options for stream append operations.
 */
export interface StreamAppendOptions {
  namespace?: string;
  /** Partition key for routing (optional - uses round-robin if not provided) */
  partitionKey?: string;
  /** Explicit partition index (optional - uses partitionKey hash if not provided) */
  partition?: number;
}

/**
 * 128-bit stream identifier composed of timestamp_ms and sequence.
 * Binary layout (big-endian for lexicographic sorting):
 * [timestamp_ms: u64 BE][sequence: u64 BE]
 */
export class StreamID {
  constructor(
    /** Unix timestamp in milliseconds */
    public readonly timestampMs: bigint,
    /** Sequence number within the millisecond */
    public readonly sequence: bigint
  ) {}

  /** Create a StreamID from timestamp only (sequence = 0). */
  static fromTimestamp(timestampMs: bigint): StreamID {
    return new StreamID(timestampMs, 0n);
  }

  /** Parse a 16-byte big-endian binary StreamID. */
  static parse(data: Uint8Array): StreamID {
    if (data.length < 16) {
      throw new Error(`StreamID requires 16 bytes, got ${data.length}`);
    }
    const view = new DataView(data.buffer, data.byteOffset);
    const timestampMs = view.getBigUint64(0, false); // big-endian
    const sequence = view.getBigUint64(8, false); // big-endian
    return new StreamID(timestampMs, sequence);
  }

  /** Return the 16-byte big-endian binary representation. */
  toBytes(): Uint8Array {
    const buf = new Uint8Array(16);
    const view = new DataView(buf.buffer);
    view.setBigUint64(0, this.timestampMs, false); // big-endian
    view.setBigUint64(8, this.sequence, false); // big-endian
    return buf;
  }
}

/**
 * Options for stream read operations.
 */
export interface StreamReadOptions {
  namespace?: string;
  /** Start position (inclusive). If undefined and tail is false, reads from beginning. */
  start?: StreamID;
  /** End position (inclusive). If undefined, reads to end of stream. */
  end?: StreamID;
  /** If true, start from end of stream (tail mode). */
  tail?: boolean;
  /** Maximum number of records to return */
  limit?: number;
  /** Explicit partition index to read from. */
  partition?: number;
  /**
   * Block for up to this many milliseconds waiting for new records.
   * - undefined/not set: no blocking (immediate return)
   * - 0: block forever until records available
   * - >0: block for at most N milliseconds
   */
  blockMs?: number;
}

/**
 * Options for subscribing to a stream.
 */
export interface StreamSubscribeOptions {
  namespace?: string;
  /** Start position (inclusive). If undefined and tail is false, starts from beginning. */
  start?: StreamID;
  /** If true, start from end of stream (receive new records only). */
  tail?: boolean;
  /** Specific partition to subscribe to (optional). */
  partition?: number;
}

/**
 * Callback for stream subscription events.
 */
export type StreamEventCallback = (record: StreamRecord) => void;

/**
 * Stream subscription handle.
 */
export interface StreamSubscription {
  /** Subscription ID (used for tracking/debugging) */
  subscriptionId: number;
  /** Unsubscribe from the stream */
  unsubscribe(): Promise<void>;
}

/**
 * Options for stream consumer group operations.
 */
export interface StreamGroupOptions {
  namespace?: string;
  /** Consumer group name */
  group: string;
  /** Consumer ID within the group */
  consumer: string;
  /** Maximum number of records to fetch */
  limit?: number;
  /**
   * Block for up to this many milliseconds waiting for new records.
   * - undefined/not set: no blocking (immediate return)
   * - 0: block forever until records available
   * - >0: block for at most N milliseconds
   */
  blockMs?: number;
}

/**
 * Options for acknowledging stream records in a consumer group.
 */
export interface StreamAckOptions {
  namespace?: string;
  /** Consumer group name */
  group: string;
  /** Consumer ID (required for correct ack matching in multi-consumer groups) */
  consumer: string;
}

/**
 * Options for negatively acknowledging stream records in a consumer group.
 */
export interface StreamNackOptions {
  namespace?: string;
  /** Consumer group name */
  group: string;
  /** Consumer ID (required for correct nack matching in multi-consumer groups) */
  consumer: string;
  /** Delay in milliseconds before message becomes visible again */
  redeliveryDelayMs?: number;
}

/**
 * Options for KV get operations.
 */
export interface GetOptions {
  namespace?: string;
  /**
   * Block for up to this many milliseconds if the key doesn't exist.
   * Useful for waiting on a key to be set by another process.
   * - undefined/not set: no blocking (immediate return)
   * - 0: block forever until key exists
   * - >0: block for at most N milliseconds
   */
  blockMs?: number;
}

/**
 * Options for KV put operations.
 */
export interface PutOptions {
  namespace?: string;
  ttlSeconds?: bigint;
  casVersion?: bigint;
  ifNotExists?: boolean;
  ifExists?: boolean;
}

/**
 * Options for KV delete operations.
 */
export interface DeleteOptions {
  namespace?: string;
}

/**
 * Options for KV scan operations.
 */
export interface ScanOptions {
  namespace?: string;
  cursor?: Uint8Array;
  limit?: number;
  keysOnly?: boolean;
}

/**
 * Options for KV history operations.
 */
export interface HistoryOptions {
  namespace?: string;
  limit?: number;
}

/**
 * Options for queue enqueue operations.
 */
export interface EnqueueOptions {
  namespace?: string;
  priority?: number;
  delayMs?: bigint;
  dedupKey?: string;
}

/**
 * Options for queue dequeue operations.
 */
export interface DequeueOptions {
  namespace?: string;
  visibilityTimeoutMs?: number;
  blockMs?: number;
}

/**
 * Options for queue ack operations.
 */
export interface AckOptions {
  namespace?: string;
}

/**
 * Options for queue nack operations.
 */
export interface NackOptions {
  namespace?: string;
  toDlq?: boolean;
}

/**
 * Options for DLQ list operations.
 */
export interface DLQListOptions {
  namespace?: string;
  limit?: number;
}

/**
 * Options for DLQ requeue operations.
 */
export interface DLQRequeueOptions {
  namespace?: string;
}

/**
 * Options for queue peek operations.
 * Peek reads messages without creating leases (non-consuming read).
 */
export interface PeekOptions {
  namespace?: string;
}

/**
 * Options for queue touch operations.
 * Touch extends the visibility timeout (lease) for in-flight messages.
 */
export interface TouchOptions {
  namespace?: string;
}

/**
 * Raw response from the server.
 */
export interface RawResponse {
  status: StatusCode;
  data: Uint8Array;
  requestId: bigint;
}

/**
 * Transport interface for sending and receiving data.
 * Implemented by TCP (Node.js) and WebSocket (browser) transports.
 */
export interface Transport {
  /** Connect to the server */
  connect(): Promise<void>;

  /** Close the connection */
  close(): Promise<void>;

  /** Check if connected */
  isConnected(): boolean;

  /** Send data and receive response */
  sendAndReceive(data: Uint8Array): Promise<Uint8Array>;
}

/**
 * Logger interface for Flo SDK.
 * Implement this to integrate with your logging system.
 */
export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Console-based logger implementation.
 */
export const consoleLogger: Logger = {
  debug: (message, ...args) => console.log(`[flo] ${message}`, ...args),
  warn: (message, ...args) => console.warn(`[flo] ${message}`, ...args),
  error: (message, ...args) => console.error(`[flo] ${message}`, ...args),
};

/**
 * Silent logger (no-op).
 */
export const silentLogger: Logger = {
  debug: () => {},
  warn: () => {},
  error: () => {},
};

/**
 * Client options for Flo client.
 */
export interface ClientOptions {
  /** Default namespace for operations */
  namespace?: string;

  /** Connection and operation timeout in milliseconds */
  timeoutMs?: number;

  /**
   * Logger for SDK messages.
   * - `true`: use console logger
   * - `false` or omitted: silent (no logging)
   * - Logger object: use custom logger implementation
   */
  logger?: boolean | Logger;
}

/**
 * Client options for Flo web client (browser).
 * Extends base options with authentication support.
 */
export interface WebClientOptions extends ClientOptions {
  /**
   * Authentication token (JWT or API key) for the connection.
   * Sent as query parameter during WebSocket upgrade handshake.
   * Server validates token before establishing connection.
   */
  authToken?: string;

  /**
   * Callback invoked when authentication fails.
   * Should return a new auth token for retry.
   * If not provided, auth failures will throw an error.
   */
  onAuthRequired?: () => Promise<string>;
}

// ============================================================================
// ACTION TYPES
// ============================================================================

/**
 * Action type enumeration.
 */
export const ActionType = {
  /** External worker-based action */
  User: 0,
  /** WebAssembly action */
  Wasm: 1,
} as const;

export type ActionType = (typeof ActionType)[keyof typeof ActionType];

/**
 * Information about a registered action.
 */
export interface ActionInfo {
  name: string;
  actionType: ActionType;
  timeoutMs: number;
  maxRetries: number;
  description?: string;
}

/**
 * Status of an action run.
 */
export interface ActionRunStatus {
  runId: string;
  /** Status: "pending" | "running" | "completed" | "failed" */
  status: string;
  result?: Uint8Array;
  error?: string;
}

/**
 * Result of invoking an action.
 */
export interface ActionInvokeResult {
  runId: string;
}

/**
 * Result of listing actions.
 */
export interface ActionListResult {
  actions: ActionInfo[];
  cursor?: Uint8Array;
}

/**
 * Options for registering an action.
 */
export interface ActionRegisterOptions {
  namespace?: string;
  timeoutMs?: number;
  maxRetries?: number;
  description?: string;
}

/**
 * Options for invoking an action.
 */
export interface ActionInvokeOptions {
  namespace?: string;
  priority?: number;
  idempotencyKey?: string;
}

/**
 * Options for getting action status.
 */
export interface ActionStatusOptions {
  namespace?: string;
}

/**
 * Options for listing actions.
 */
export interface ActionListOptions {
  namespace?: string;
  limit?: number;
  prefix?: string;
}

/**
 * Options for deleting an action.
 */
export interface ActionDeleteOptions {
  namespace?: string;
}

// ============================================================================
// WORKER TYPES
// ============================================================================

/**
 * Worker type identifies the kind of worker.
 */
export const WorkerType = {
  /** Processes action tasks */
  Action: 0,
  /** Processes stream records */
  Stream: 1,
} as const;

export type WorkerType = (typeof WorkerType)[keyof typeof WorkerType];

/**
 * Worker health status.
 */
export const WorkerStatus = {
  Active: 0,
  Idle: 1,
  Draining: 2,
  Unhealthy: 3,
} as const;

export type WorkerStatus = (typeof WorkerStatus)[keyof typeof WorkerStatus];

/**
 * Process kind identifies what a registered process does.
 */
export const ProcessKind = {
  /** Handles an action */
  Action: 0,
  /** Consumes a stream */
  StreamConsumer: 1,
} as const;

export type ProcessKind = (typeof ProcessKind)[keyof typeof ProcessKind];

/**
 * A process entry describing what a worker handles.
 */
export interface ProcessEntry {
  name: string;
  kind: ProcessKind;
}

/**
 * A task assigned to a worker.
 */
export interface TaskAssignment {
  taskId: string;
  taskType: string;
  payload: Uint8Array;
  createdAt: bigint;
  attempt: number;
}

/**
 * Result of awaiting a task.
 */
export interface WorkerAwaitResult {
  /** Task if available, null if no task */
  task: TaskAssignment | null;
}

/**
 * Information about a registered worker.
 */
export interface WorkerInfo {
  id: string;
  type: WorkerType;
  status: WorkerStatus;
  metadata?: string;
  machineId?: string;
  tasksCompleted: number;
  tasksFailed: number;
  currentLoad: number;
  maxConcurrency: number;
  registeredAtMs: bigint;
  lastHeartbeat: bigint;
}

/**
 * Result of listing workers.
 */
export interface WorkerListResult {
  workers: WorkerInfo[];
}

/**
 * Options for registering a worker in the worker registry.
 */
export interface WorkerRegisterOptions {
  namespace?: string;
  /** Worker type (action or stream) */
  workerType?: WorkerType;
  /** Maximum concurrent tasks (default: 10) */
  maxConcurrency?: number;
  /** Actions/streams this worker handles */
  processes?: ProcessEntry[];
  /** Optional JSON metadata */
  metadata?: string;
  /** Machine/host identifier */
  machineId?: string;
}

/**
 * Options for awaiting a task.
 */
export interface WorkerAwaitOptions {
  namespace?: string;
  /** Block waiting for task. 0 = infinite, >0 = timeout in ms */
  blockMs?: number;
  timeoutMs?: number;
}

/**
 * Options for extending task lease.
 */
export interface WorkerTouchOptions {
  namespace?: string;
  extendMs?: number;
}

/**
 * Options for completing a task.
 */
export interface WorkerCompleteOptions {
  namespace?: string;
  /** Named outcome for the action (default: "success"). Used by workflow transitions. */
  outcome?: string;
}

/**
 * Options for failing a task.
 */
export interface WorkerFailOptions {
  namespace?: string;
  /** Whether to retry the task (default: true) */
  retry?: boolean;
}

/**
 * Options for listing workers.
 */
export interface WorkerListOptions {
  namespace?: string;
  limit?: number;
}

/**
 * Options for worker heartbeat.
 */
export interface WorkerHeartbeatOptions {
  namespace?: string;
}

/**
 * Options for worker deregistration.
 */
export interface WorkerDeregisterOptions {
  namespace?: string;
}

/**
 * Options for draining a worker.
 */
export interface WorkerDrainOptions {
  namespace?: string;
}

// ============================================================================
// WORKFLOW TYPES
// ============================================================================

/**
 * Options for creating a workflow.
 */
export interface WorkflowCreateOptions {
  namespace?: string;
}

/**
 * Options for getting a workflow definition.
 */
export interface WorkflowGetDefinitionOptions {
  namespace?: string;
  /** Specific version to retrieve (optional — latest if omitted) */
  version?: string;
}

/**
 * Options for starting a workflow run.
 */
export interface WorkflowStartOptions {
  namespace?: string;
  /** Specific version to run (default: "latest") */
  version?: string;
  /** Idempotency key — same key returns existing run ID without creating a new one */
  idempotencyKey?: string;
  /** Explicit run ID (optional — server generates one if omitted) */
  runId?: string;
}

/**
 * Options for getting workflow run status.
 */
export interface WorkflowStatusOptions {
  namespace?: string;
}

/**
 * Result of a workflow status query.
 */
export interface WorkflowStatusResult {
  run_id: string;
  workflow: string;
  version: string;
  status: string;
  current_step: string;
  input: Uint8Array;
  created_at: bigint;
  started_at?: bigint;
  completed_at?: bigint;
  wait_signal?: string;
}

/**
 * Options for getting workflow run history.
 */
export interface WorkflowHistoryOptions {
  namespace?: string;
  /** Maximum number of history events to return (default: 100) */
  limit?: number;
}

/**
 * A single workflow history event.
 */
export interface WorkflowHistoryEvent {
  type: string;
  detail: string;
  timestamp: number;
}

/**
 * Options for listing workflow runs.
 */
export interface WorkflowListRunsOptions {
  namespace?: string;
  /** Filter by workflow name (optional) */
  workflowName?: string;
  /** Filter by status: "pending" | "running" | "waiting" | "completed" | "failed" | "cancelled" | "timed_out" */
  statusFilter?: string;
  /** Maximum number of runs to return (default: 100) */
  limit?: number;
}

/**
 * A single entry in the workflow runs list.
 */
export interface WorkflowListRunEntry {
  run_id: string;
  workflow: string;
  status: string;
  created_at: number;
}

/**
 * Options for sending a signal to a workflow.
 */
export interface WorkflowSignalOptions {
  namespace?: string;
}

/**
 * Options for cancelling a workflow run.
 */
export interface WorkflowCancelOptions {
  namespace?: string;
}

/**
 * Options for disabling a workflow.
 */
export interface WorkflowDisableOptions {
  namespace?: string;
}

/**
 * Options for enabling a workflow.
 */
export interface WorkflowEnableOptions {
  namespace?: string;
}

/**
 * Options for listing workflow definitions.
 */
export interface WorkflowListDefinitionsOptions {
  namespace?: string;
}

/**
 * A single entry in the workflow definitions list.
 */
export interface WorkflowDefinitionEntry {
  name: string;
  version: string;
  created_at: number;
}

/**
 * Options for syncing workflows.
 */
export interface WorkflowSyncOptions {
  namespace?: string;
}

/**
 * Result of a workflow sync operation.
 */
export interface WorkflowSyncResult {
  name: string;
  version: string;
  description: string;
  action: "created" | "updated" | "unchanged";
}

/**
 * A file entry returned by the directory reader for syncDir.
 */
export interface WorkflowSyncDirFile {
  name: string;
  content: string;
}

/**
 * Function that reads all .yaml/.yml files from a directory.
 * Injected by the caller to keep core free of Node.js fs dependencies.
 */
export type WorkflowSyncDirFn = (
  dir: string
) => Promise<WorkflowSyncDirFile[]>;

// =============================================================================
// Processing Types
// =============================================================================

/**
 * Options for submitting a processing job.
 */
export interface ProcessingSubmitOptions {
  namespace?: string;
}

/**
 * Options for getting processing job status.
 */
export interface ProcessingStatusOptions {
  namespace?: string;
}

/**
 * Options for listing processing jobs.
 */
export interface ProcessingListOptions {
  namespace?: string;
  limit?: number;
}

/**
 * Options for stopping a processing job.
 */
export interface ProcessingStopOptions {
  namespace?: string;
}

/**
 * Options for cancelling a processing job.
 */
export interface ProcessingCancelOptions {
  namespace?: string;
}

/**
 * Options for triggering a processing savepoint.
 */
export interface ProcessingSavepointOptions {
  namespace?: string;
}

/**
 * Options for restoring a processing job from a savepoint.
 */
export interface ProcessingRestoreOptions {
  namespace?: string;
}

/**
 * Options for rescaling a processing job's parallelism.
 */
export interface ProcessingRescaleOptions {
  namespace?: string;
}

/**
 * Status of a processing job.
 */
export interface ProcessingStatusResult {
  job_id: string;
  name: string;
  status: string;
  parallelism: number;
  batch_size: number;
  records_processed: number;
  created_at: number;
}

/**
 * A processing job entry returned by list.
 */
export interface ProcessingListEntry {
  name: string;
  job_id: string;
  status: string;
  parallelism: number;
  created_at: number;
}

/**
 * Options for syncing processing jobs.
 */
export interface ProcessingSyncOptions {
  namespace?: string;
}

/**
 * Result of a processing sync operation.
 */
export interface ProcessingSyncResult {
  name: string;
  job_id: string;
  action: "submitted";
}

/**
 * A file entry returned by the directory reader for syncDir.
 */
export interface ProcessingSyncDirFile {
  name: string;
  content: string;
}

/**
 * Function that reads all .yaml/.yml files from a directory.
 * Injected by the caller to keep core free of Node.js fs dependencies.
 */
export type ProcessingSyncDirFn = (
  dir: string
) => Promise<ProcessingSyncDirFile[]>;

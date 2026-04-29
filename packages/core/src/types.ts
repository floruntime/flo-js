/**
 * Flo protocol constants and types.
 */

// Protocol constants
export const MAGIC = 0x004f4c46; // "FLO\0" in little-endian
export const VERSION = 0x01;
export const HEADER_SIZE = 32;

// Size limits (for client-side validation)
export const MAX_NAMESPACE_SIZE = 255;
export const MAX_KEY_SIZE = 64 * 1024; // 64 KB
export const MAX_VALUE_SIZE = 16 * 1024 * 1024; // 16 MB practical limit

/**
 * Operation codes for Flo protocol requests.
 * Three-layer layout: Infra(0x000–0x0FF), Data(0x100–0x2FF), Compute(0x300–0x3FF)
 */
export const OpCode = {
  // ── System (0x000 – 0x00F) ──
  Ping: 0x000,
  Pong: 0x001,
  ErrorResponse: 0x002,
  Auth: 0x003,
  SetDurability: 0x004,
  OK: 0x005,

  // ── Namespace (0x010 – 0x02F) ──
  NamespaceCreate: 0x010,
  NamespaceDelete: 0x011,
  NamespaceList: 0x012,
  NamespaceInfo: 0x013,
  NamespaceConfigSet: 0x014,
  NamespaceConfigGet: 0x015,
  NamespaceCreateResponse: 0x020,
  NamespaceDeleteResponse: 0x021,
  NamespaceListResponse: 0x022,
  NamespaceInfoResponse: 0x023,
  NamespaceConfigSetResponse: 0x024,
  NamespaceConfigGetResponse: 0x025,

  // ── Cluster (0x030 – 0x04F) ──
  ClusterStatus: 0x030,
  ClusterMembers: 0x031,
  ClusterJoin: 0x032,
  ClusterLeave: 0x033,
  ClusterTransferLeader: 0x034,
  ClusterAddNode: 0x035,
  ClusterRemoveNode: 0x036,
  ClusterStatusResponse: 0x040,
  ClusterMembersResponse: 0x041,
  ClusterJoinResponse: 0x042,

  // ── KV + Transactions + Snapshots (0x100 – 0x12F) ──
  KVPut: 0x100,
  KVGet: 0x101,
  KVMGet: 0x102,
  KVDelete: 0x103,
  KVScan: 0x104,
  KVHistory: 0x105,
  KVGetResponse: 0x106,
  KVMGetResponse: 0x107,
  KVPutResponse: 0x108,
  KVScanResponse: 0x109,
  KVHistoryResponse: 0x10a,
  // KV extended (atomic counters, JSON ops)
  KVIncr: 0x10b,
  KVJsonGet: 0x10c,
  KVJsonSet: 0x10d,
  KVJsonDel: 0x10e,
  // KV per-shard transactions
  KVBeginTxn: 0x110,
  KVCommitTxn: 0x111,
  KVRollbackTxn: 0x112,
  // KV extended (TTL lifecycle, exists)
  KVTouch: 0x113,
  KVPersist: 0x114,
  KVExists: 0x115,
  KVIncrResponse: 0x116,
  KVJsonResponse: 0x117,
  KVExistsResponse: 0x118,
  KVTxnResponse: 0x119,

  // ── Streams (0x130 – 0x14F) ──
  StreamAppend: 0x130,
  StreamRead: 0x131,
  StreamTrim: 0x132,
  StreamInfo: 0x133,
  StreamAppendResponse: 0x134,
  StreamReadResponse: 0x135,
  StreamEvent: 0x136,
  StreamSubscribe: 0x137,
  StreamUnsubscribe: 0x138,
  StreamSubscribed: 0x139,
  StreamUnsubscribed: 0x13a,
  StreamList: 0x13b,
  StreamListResponse: 0x13c,
  StreamCreate: 0x13d,
  StreamCreateResponse: 0x13e,
  StreamAlter: 0x13f,

  // ── Stream Consumer Groups (0x150 – 0x16F) ──
  StreamGroupCreate: 0x150,
  StreamGroupJoin: 0x151,
  StreamGroupLeave: 0x152,
  StreamGroupRead: 0x153,
  StreamGroupAck: 0x154,
  StreamGroupClaim: 0x155,
  StreamGroupPending: 0x156,
  StreamGroupConfigureSweeper: 0x157,
  StreamGroupReadResponse: 0x158,
  StreamGroupNack: 0x159,
  StreamGroupTouch: 0x15a,
  StreamGroupInfo: 0x15b,
  StreamGroupDelete: 0x15c,

  // ── Queues (0x170 – 0x19F) ──
  QueueEnqueue: 0x170,
  QueueDequeue: 0x171,
  QueueComplete: 0x172,
  QueueExtendLease: 0x173,
  QueueFail: 0x174,
  QueueFailAuto: 0x175,
  QueueDLQList: 0x176,
  QueueDLQDelete: 0x177,
  QueueDLQRequeue: 0x178,
  QueueDLQStats: 0x179,
  QueuePromoteDue: 0x17a,
  QueueStats: 0x17b,
  QueuePeek: 0x17c,
  QueueTouch: 0x17d,
  QueueBatchEnqueue: 0x17e,
  QueuePurge: 0x17f,
  QueueEnqueueResponse: 0x190,
  QueueDequeueResponse: 0x191,
  QueueDLQListResponse: 0x192,
  QueueStatsResponse: 0x193,
  QueuePeekResponse: 0x194,
  QueueTouchResponse: 0x195,
  QueueBatchEnqueueResponse: 0x196,
  QueuePurgeResponse: 0x197,
  QueueList: 0x198,
  QueueListResponse: 0x199,

  // ── Time-Series (0x1A0 – 0x1BF) ──
  TSWrite: 0x1a0,
  TSRead: 0x1a1,
  TSQuery: 0x1a2,
  TSFloQL: 0x1a3,
  TSList: 0x1a4,
  TSDelete: 0x1a5,
  TSRetention: 0x1a6,
  TSWriteResponse: 0x1a7,
  TSReadResponse: 0x1a8,
  TSQueryResponse: 0x1a9,
  TSFloQLResponse: 0x1aa,
  TSListResponse: 0x1ab,
  TSDeleteResponse: 0x1ac,
  TSRetentionResponse: 0x1ad,

  // ── Actions (0x300 – 0x31F) ──
  ActionRegister: 0x300,
  ActionInvoke: 0x301,
  ActionStatus: 0x302,
  ActionList: 0x303,
  ActionListRuns: 0x304,
  ActionDelete: 0x305,
  ActionAwait: 0x306,
  ActionComplete: 0x307,
  ActionFail: 0x308,
  ActionTouch: 0x309,
  ActionRegisterResponse: 0x310,
  ActionInvokeResponse: 0x311,
  ActionStatusResponse: 0x312,
  ActionListResponse: 0x313,
  ActionListRunsResponse: 0x314,
  ActionTaskAssignment: 0x315,

  // ── Workers (0x320 – 0x33F) ──
  WorkerRegister: 0x320,
  WorkerHeartbeat: 0x321,
  WorkerDeregister: 0x322,
  WorkerList: 0x323,
  WorkerInfo: 0x324,
  WorkerDrain: 0x325,
  WorkerRegisterResponse: 0x330,
  WorkerListResponse: 0x331,
  WorkerInfoResponse: 0x332,
  WorkerDrainResponse: 0x333,

  // ── Workflows (0x340 – 0x35F) ──
  WorkflowCreate: 0x340,
  WorkflowStart: 0x341,
  WorkflowSignal: 0x342,
  WorkflowCancel: 0x343,
  WorkflowStatus: 0x344,
  WorkflowHistory: 0x345,
  WorkflowListRuns: 0x346,
  WorkflowGetDefinition: 0x347,
  WorkflowDisable: 0x348,
  WorkflowEnable: 0x349,
  WorkflowListDefinitions: 0x34a,
  WorkflowCreateResponse: 0x350,
  WorkflowStartResponse: 0x351,
  WorkflowStatusResponse: 0x352,
  WorkflowHistoryResponse: 0x353,
  WorkflowListRunsResponse: 0x354,
  WorkflowGetDefinitionResponse: 0x355,
  WorkflowDisableResponse: 0x356,
  WorkflowEnableResponse: 0x357,
  WorkflowListDefinitionsResponse: 0x358,

  // ── Processing (0x360 – 0x37F) ──
  ProcessingSubmit: 0x360,
  ProcessingStop: 0x361,
  ProcessingCancel: 0x362,
  ProcessingStatus: 0x363,
  ProcessingList: 0x364,
  ProcessingSavepoint: 0x365,
  ProcessingRestore: 0x366,
  ProcessingRescale: 0x367,
  ProcessingSubmitResponse: 0x370,
  ProcessingStopResponse: 0x371,
  ProcessingCancelResponse: 0x372,
  ProcessingStatusResponse: 0x373,
  ProcessingListResponse: 0x374,
  ProcessingSavepointResponse: 0x375,
  ProcessingRestoreResponse: 0x376,
  ProcessingRescaleResponse: 0x377,
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
  TxnID: 0x09, // u64: Transaction ID for per-shard transactions

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
 * Result of a successful KV put.
 *
 * The `version` is the new version assigned by the server, suitable for CAS
 * on the next write via {@link PutOptions.casVersion}.
 */
export interface PutResult {
  version: bigint;
}

/**
 * Result of a successful KV transaction begin.
 *
 * `txnId` is the server-assigned transaction handle. `pinnedHash` is the
 * partition hash this transaction is bound to — every key written or read
 * inside the transaction must hash to the same partition.
 */
export interface KVBeginResult {
  txnId: bigint;
  pinnedHash: bigint;
}

/**
 * Result of a successful KV transaction commit.
 *
 * `commitIndex` is the Raft log index of the committed batch and `opCount`
 * is the number of buffered operations applied atomically.
 */
export interface KVCommitResult {
  commitIndex: bigint;
  opCount: number;
}

/**
 * Result of a KV get that found a key.
 *
 * `kv.get` returns `null` when the key is missing; check before dereferencing.
 */
export interface GetResult {
  value: Uint8Array;
  version: bigint;
}

/**
 * One entry in a {@link KV.mget} response. `found` is false when the key
 * did not exist; in that case `value` is empty and `version` is `0n`.
 */
export interface MGetEntry {
  key: string;
  value: Uint8Array;
  version: bigint;
  found: boolean;
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
  /** Stream name (identifies which stream the record came from) */
  stream: string;
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
 * Options for KV incr operations.
 */
export interface KVIncrOptions {
  namespace?: string;
  /** Defaults to +1 when omitted. Negative values decrement. */
  delta?: bigint;
}

/**
 * Options for KV touch / persist operations.
 */
export interface KVTouchOptions {
  namespace?: string;
}

/**
 * Options for KV exists operations.
 */
export interface KVExistsOptions {
  namespace?: string;
}

/**
 * Options for KV JSON.* operations.
 */
export interface KVJsonOptions {
  namespace?: string;
}

/**
 * Options for KV mget operations.
 */
export interface KVMGetOptions {
  namespace?: string;
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
  limit?: number;
  cursor?: Uint8Array;
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
  cursor?: Uint8Array;
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

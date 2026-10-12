/**
 * Flo protocol constants and types.
 */

import type { RetryPolicy } from "./retry.js";

// Protocol constants
export const MAGIC = 0x004f4c46; // "FLO\0" in little-endian
export const VERSION = 0x02;
/**
 * The hash of the op table this SDK was built for, written into every
 * request header. A server built from another table refuses the request,
 * and its answers carry its own hash, which this SDK refuses.
 */
export const TABLE_HASH = 0xc8a2c3c65e95d11dn;

/**
 * Why the server refused a request. Mirrors the server's list; the table
 * hash covers it, so a server can't send one this SDK doesn't know.
 */
export enum Reason {
  UnknownOp = 1,
  Malformed = 2,
  FieldTooLong = 3,
  OutOfRange = 4,
  LeaderUnknown = 5,
  ShardBusy = 6,
  TableMismatch = 7,
  UnknownFlag = 8,
  Internal = 9,
  CommittedNotApplied = 10,
  AnswerLost = 11,
  /** A refusal from an op family the server hasn't classified yet. */
  Unclassified = 12,
  /** What the request names doesn't exist. */
  NotFound = 13,
  /** The key is in a prefix the server keeps for itself. */
  ReservedKey = 14,
  /** The write's casVersion isn't the key's version. */
  CasMismatch = 15,
  /** The write's condition didn't hold. */
  ConditionFailed = 16,
  /** No open transaction has this id here. */
  NoSuchTransaction = 17,
  /** The routing key (the key, when none is given) isn't the one the transaction was begun with. */
  WrongRoutingKey = 18,
  /** The transaction holds as many ops or bytes as it may. */
  TxnTooLarge = 19,
  /** The shard holds as many waiting reads as it may. */
  TooManyWaiters = 20,
  /** Two fields given together that don't go together. */
  ConflictingFields = 21,
  /** The shard stopped taking writes until the node restarts. */
  WritesStopped = 22,
  /** Too many writes wait for commit. */
  TooManyProposals = 23,
  /** The key's value isn't the kind the op works on (a counter, a JSON document). */
  WrongType = 24,
  /** The shard holds as many open transactions as it may. */
  TooManyTransactions = 25,
  /** An action complete or fail for a run that has moved on (another worker holds it, or it finished). It was dropped; resending it is refused the same way. */
  StaleReport = 26,
}

/** Whether a refused request took effect: retry logic reads this, not the message. */
export enum Ran {
  No = 0,
  /** It took effect; don't resend it. */
  Yes = 1,
  /** It may still take effect; check before resending. */
  Unknown = 2,
}

const REASON_NAMES: Record<Reason, string> = {
  [Reason.UnknownOp]: "unknown_op",
  [Reason.Malformed]: "malformed",
  [Reason.FieldTooLong]: "field_too_long",
  [Reason.OutOfRange]: "out_of_range",
  [Reason.LeaderUnknown]: "leader_unknown",
  [Reason.ShardBusy]: "shard_busy",
  [Reason.TableMismatch]: "table_mismatch",
  [Reason.UnknownFlag]: "unknown_flag",
  [Reason.Internal]: "internal",
  [Reason.CommittedNotApplied]: "committed_not_applied",
  [Reason.AnswerLost]: "answer_lost",
  [Reason.Unclassified]: "unclassified",
  [Reason.NotFound]: "not_found",
  [Reason.ReservedKey]: "reserved_key",
  [Reason.CasMismatch]: "cas_mismatch",
  [Reason.ConditionFailed]: "condition_failed",
  [Reason.NoSuchTransaction]: "no_such_transaction",
  [Reason.WrongRoutingKey]: "wrong_routing_key",
  [Reason.TxnTooLarge]: "txn_too_large",
  [Reason.TooManyWaiters]: "too_many_waiters",
  [Reason.ConflictingFields]: "conflicting_fields",
  [Reason.WritesStopped]: "writes_stopped",
  [Reason.TooManyProposals]: "too_many_proposals",
  [Reason.WrongType]: "wrong_type",
  [Reason.TooManyTransactions]: "too_many_transactions",
  [Reason.StaleReport]: "stale_report",
};

/** The server's name for a reason, as it appears in messages. */
export function reasonName(r: Reason): string {
  return REASON_NAMES[r];
}
export const HEADER_SIZE = 32;

// Size limits (for client-side validation)
export const MAX_NAMESPACE_SIZE = 255;
export const MAX_KEY_SIZE = 64 * 1024; // 64 KB
export const MAX_VALUE_SIZE = 16 * 1024 * 1024; // 16 MB practical limit
/** Longest blocking wait (blockMs) the server accepts: 5 minutes. */
export const MAX_BLOCK_MS = 300_000;
/** Longest a KV get may wait (waitMs). */
export const MAX_WAIT_MS = 300_000;
/** Longest value a KV put takes. */
export const MAX_KV_VALUE_SIZE = 253_817;

/** What a KV get with waitMs waits for. */
export enum WaitFor {
  /** The key to exist; one that exists answers at once. */
  Exists = 0,
  /** The key's next version. */
  Change = 1,
}

/** When a KV put may write. */
export enum Condition {
  Always = 0,
  IfAbsent = 1,
  IfPresent = 2,
}

/**
 * Operation codes for Flo protocol requests.
 * Three-layer layout: Infra(0x000–0x0FF), Data(0x100–0x2FF), Compute(0x300–0x3FF)
 */
export const OpCode = {
  // ── System (0x000 – 0x00F) ──
  Ping: 0x000,

  // ── Namespace (0x010 – 0x02F) ──
  NamespaceCreate: 0x010,
  NamespaceDelete: 0x011,
  NamespaceList: 0x012,
  NamespaceInfo: 0x013,
  NamespaceConfigSet: 0x014,
  NamespaceConfigGet: 0x015,

  // ── Cluster (0x030 – 0x04F) ──
  ClusterStatus: 0x030,
  ClusterMembers: 0x031,
  ClusterJoin: 0x032,
  ClusterLeave: 0x033,
  ClusterTransferLeader: 0x034,
  ClusterAddNode: 0x035,
  ClusterRemoveNode: 0x036,

  // ── KV + Transactions + Snapshots (0x100 – 0x12F) ──
  KVPut: 0x100,
  KVGet: 0x101,
  KVMGet: 0x102,
  KVDelete: 0x103,
  KVScan: 0x104,
  KVHistory: 0x105,
  // KV extended (atomic counters, JSON ops)
  KVIncr: 0x10b,
  KVJsonGet: 0x10c,
  KVJsonSet: 0x10d,
  KVJsonDel: 0x10e,
  // KV per-shard transactions
  KVBeginTxn: 0x110,
  KVCommitTxn: 0x111,
  KVRollbackTxn: 0x112,
  // KV extended (TTL, exists)
  KVExpire: 0x113,
  KVExists: 0x115,

  // ── Streams (0x130 – 0x14F) ──
  StreamAppend: 0x130,
  StreamRead: 0x131,
  StreamTrim: 0x132,
  StreamInfo: 0x133,
  StreamList: 0x13b,
  StreamCreate: 0x13d,
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
  StreamGroupNack: 0x159,
  StreamGroupTouch: 0x15a,
  StreamGroupInfo: 0x15b,
  StreamGroupDelete: 0x15c,

  // ── Queues (0x170 – 0x19F) ──
  QueueEnqueue: 0x170,
  QueueDequeue: 0x171,
  QueueComplete: 0x172,
  QueueFail: 0x174,
  QueueDLQList: 0x176,
  QueueDLQDelete: 0x177,
  QueueDLQRequeue: 0x178,
  QueueStats: 0x17b,
  QueuePeek: 0x17c,
  QueuePurge: 0x17f,
  QueueList: 0x198,

  // ── Time-Series (0x1A0 – 0x1BF) ──
  TSWrite: 0x1a0,
  TSRead: 0x1a1,
  TSQuery: 0x1a2,
  TSFloQL: 0x1a3,
  TSList: 0x1a4,
  TSDelete: 0x1a5,
  TSRetention: 0x1a6,

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

  // ── Workers (0x320 – 0x33F) ──
  WorkerRegister: 0x320,
  WorkerHeartbeat: 0x321,
  WorkerDeregister: 0x322,
  WorkerList: 0x323,
  WorkerInfo: 0x324,
  WorkerDrain: 0x325,

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

  // ── Processing (0x360 – 0x37F) ──
  ProcessingSubmit: 0x360,
  ProcessingStop: 0x361,
  ProcessingCancel: 0x362,
  ProcessingStatus: 0x363,
  ProcessingList: 0x364,
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
  Unavailable: 12, // No leader, or the shard isn't taking writes; retryable
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
    case StatusCode.Unavailable:
      return "Unavailable: no leader or the shard isn't taking writes; retry";
    default:
      // A newer server can send a status this SDK predates.
      return `Unknown status ${status as number}`;
  }
}

/**
 * Option tags for TLV-encoded operation parameters.
 */
export const OptionTag = {
  Limit: 0x05, // u32: Maximum results for TS read; list/scan ops take theirs in the value

  // Queue Options (0x10 - 0x1F)
  Priority: 0x10, // u8: Message priority (0-255, lower is taken first; unset is 0)
  Count: 0x15, // u32: Number of messages to dequeue
  BlockMS: 0x17, // u32: Blocking timeout (0 = don't wait, max 300000)

  // Stream Options (0x20 - 0x2F) - StreamID-native ONLY
  // 0x20 reserved
  StreamStart: 0x21, // [16]u8: Start StreamID for reads (exclusive)
  StreamEnd: 0x22, // [16]u8: End StreamID for reads (inclusive)
  StreamTail: 0x23, // void: Flag indicating tail read (start from end of stream)
  Partition: 0x24, // u32: Explicit partition index
  PartitionKey: 0x25, // string: Key for partition routing
  MaxAgeSeconds: 0x26, // u64: Maximum age in seconds for retention
  DryRun: 0x28, // void: Flag to preview what would be deleted without deleting

  // Consumer Group Options (0x30 - 0x3F)
  AckTimeoutMS: 0x30, // u32: Time before unacked message auto-redelivers (overrides default)
  MaxDeliver: 0x31, // u8: Max delivery attempts before DLQ (default: 10, 0=unlimited)

  // Time-Series Options (0x60 - 0x6F)
  TSFromMS: 0x60, // i64: Start of time range (inclusive, unix ms)
  TSToMS: 0x61, // i64: End of time range (inclusive, 0 = now)
  TSWindowMS: 0x62, // i64: Aggregation window size (ms)
  TSAggregation: 0x63, // string: Aggregation function name (avg, sum, count, min, max)
  TSField: 0x64, // string: Field name filter (empty = "value")
  TSTags: 0x65, // string: Comma-separated tag filters "key=val,key2=val2"
  TSTimestamp: 0x67, // i64: Explicit timestamp for write (0 = server-assigned)
  TSRawTTL: 0x68, // string: Raw data TTL (e.g., "7d")
  TSDownsample: 0x69, // string: Downsample rule (e.g., "1m:avg:30d")
} as const;

export type OptionTag = (typeof OptionTag)[keyof typeof OptionTag];

/**
 * One page of a KV scan: the key names, and the cursor for the next page
 * (null on the last page).
 */
export interface ScanResult {
  keys: string[];
  cursor: Uint8Array | null;
}

/**
 * One version of a key, from KV history.
 */
export interface VersionEntry {
  version: bigint;
  /** When the version was written, in ms since the Unix epoch. */
  atMs: bigint;
  value: Uint8Array;
}

/**
 * Result of a KV incr: the counter's new value and the key's new version.
 */
export interface KVIncrResult {
  value: bigint;
  version: bigint;
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
  enqueuedAtMs: bigint;
  deliveryCount: number;
  priority: number;
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
  /** Served from the in-memory log. */
  Hot: 0,
  /** Served from the warm store after leaving the in-memory log. */
  Warm: 1,
} as const;

export type StorageTier = (typeof StorageTier)[keyof typeof StorageTier];

/**
 * A single stream record/event.
 */
export interface StreamRecord {
  /** Full StreamID (timestamp_ms + sequence) */
  id: StreamID;
  /** Storage tier the record was served from */
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
  /** Read records after this position (exclusive): pass the last id seen to continue. If undefined and tail is false, reads from beginning. */
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
   * - undefined/not set or 0: don't wait (immediate return)
   * - up to 300000 (5 minutes); more throws BlockTooLongError
   */
  blockMs?: number;
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
   * - undefined/not set or 0: don't wait (immediate return)
   * - up to 300000 (5 minutes); more throws BlockTooLongError
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
  /** Consumer ID. Sent, but the server doesn't check it: any consumer can ack an entry. */
  consumer: string;
}

/**
 * Options for negatively acknowledging stream records in a consumer group.
 */
export interface StreamNackOptions {
  namespace?: string;
  /** Consumer group name */
  group: string;
  /** Consumer ID. Sent, but the server doesn't check it: any consumer can nack an entry. */
  consumer: string;
}

/**
 * Options for KV get operations.
 */
export interface GetOptions {
  namespace?: string;
  /**
   * Wait up to this many milliseconds for what `waitFor` names: 0 or unset
   * doesn't wait; at most 300000 (5 minutes), more throws BlockTooLongError.
   */
  waitMs?: number;
  /** What a wait waits for: the key to exist (the default) or its next version. */
  waitFor?: WaitFor;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV put operations.
 */
export interface PutOptions {
  namespace?: string;
  /** Time-to-live in milliseconds (0 = no expiration). */
  ttlMs?: bigint;
  /** When the write may happen; Always when unset. */
  condition?: Condition;
  /** The key's version the write requires; 0n means it doesn't exist. */
  casVersion?: bigint;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV delete operations.
 */
export interface DeleteOptions {
  namespace?: string;
  /**
   * The key's version the delete requires; a mismatch throws a conflict with
   * reason CasMismatch. For race-free "only the owner deletes" (locks, leases).
   */
  casVersion?: bigint;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV scan operations.
 */
export interface ScanOptions {
  namespace?: string;
  /** The cursor the previous page returned; unset for the first page. */
  cursor?: Uint8Array;
  /** Most names to return, 1 to 1024; 1024 when unset. */
  limit?: number;
}

/**
 * Options for KV history operations.
 */
export interface HistoryOptions {
  namespace?: string;
  /** Most versions to return, 1 to 65; 10 when unset. */
  limit?: number;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV incr operations.
 */
export interface KVIncrOptions {
  namespace?: string;
  /** Defaults to +1 when omitted. Negative values decrement. */
  delta?: bigint;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV expire operations.
 */
export interface KVExpireOptions {
  namespace?: string;
  /**
   * The key's version the expire requires; a mismatch throws a conflict with
   * reason CasMismatch. For race-free lease renewal ("only the owner extends
   * the TTL").
   */
  casVersion?: bigint;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV exists operations.
 */
export interface KVExistsOptions {
  namespace?: string;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV JSON operations.
 */
export interface KVJsonOptions {
  namespace?: string;
  /** Routes the request in place of the key. */
  routingKey?: string;
}

/**
 * Options for KV mget operations.
 */
export interface KVMGetOptions {
  namespace?: string;
  /**
   * The routing key every key was written with: all are looked up on its
   * shard. Unset, each key is looked up on its own shard.
   */
  routingKey?: string;
}

/**
 * Options for queue enqueue operations.
 */
export interface EnqueueOptions {
  namespace?: string;
  /** 0-255; lower is taken first. Unset is 0, so it is taken before any explicit priority. */
  priority?: number;
}

/**
 * Options for queue dequeue operations.
 */
export interface DequeueOptions {
  namespace?: string;
  /** Wait for messages if the queue is empty, in ms. 0 = don't wait, max 300000. */
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
}

/**
 * Options for DLQ list operations.
 */
export interface DLQListOptions {
  namespace?: string;
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

  /**
   * How requests the server refused without running them (overloaded or
   * unavailable, ran = no) are sent again. Unset fields take the defaults:
   * `{ maxAttempts: 4, initialBackoffMs: 50, maxBackoffMs: 1000 }`.
   * `{ maxAttempts: 1 }` turns retrying off.
   */
  retry?: Partial<RetryPolicy>;
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
  /**
   * Required worker labels as a JSON object string, e.g. '{"gpu":true}'.
   * Only workers whose registered labels contain every key/value receive the run.
   */
  labels?: string;
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
  /** Block waiting for task, in ms. Unset = 30000, 0 = don't wait, max 300000. */
  blockMs?: number;
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
 * Status of a processing job.
 */
export interface ProcessingStatusResult {
  job_id: string;
  name: string;
  status: string;
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

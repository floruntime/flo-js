# Changelog

All notable changes to the Flo JavaScript SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `ActionWorker` and `StreamWorker` reconnect after the connection drops, or after a poll times out on a connection that went quiet, then register again (`ActionWorker`) or rejoin the group and re-register (`StreamWorker`) before polling. They used to retry on the dead connection forever. Redials wait as a failed poll does (jittered, from 100 ms doubling to 5 s) until `stop()`, which ends a wait at once; a server that takes each new connection and drops it is redialed at that pace too, not at once. A worker given a client its owner then closes stops rather than reopening it, and stays stopped even if the owner connects the client again. Workers sharing a client redial a dropped connection once between them. The lost connection and each failed attempt are logged at warn.
- `FloClient.connect()` called again while a dial is in progress waits on that dial instead of opening a second socket. Sockets use TCP keepalive.

### Changed

- A request refused as overloaded or unavailable with `ran === Ran.No` is sent again as a new request, up to 4 tries with a jittered wait doubling from 50 ms to 1 s (`retry` client option; `{ maxAttempts: 1 }` turns it off). Workers back off from 100 ms to 5 s after a failed poll (was a flat 1 s that `stop()` couldn't cut short), and a `StreamWorker` retries a group join refused this way instead of failing `start()`.
- Every refusal's body is `[reason:u16][ran:u8][message]`. `ServerError` gains `.reason` (`Reason`) and `.ran` (`Ran`): whether the request took effect, so retry logic reads a field, not the message. Its message names the reason when the server gave one. A body that isn't a refusal is an `IncompleteResponseError`.
- Requests carry the op table hash (`TABLE_HASH`) under protocol version 2. An answer from a server built from another protocol version or table throws `TableMismatchError`, naming both tables, before its body is read, and the connection is closed. `UnsupportedVersionError` is gone.
- Workers back off (50 ms, doubling to 1 s) when blocking polls keep coming back empty at once.
- `blockMs: 0` now means don't wait, as on the server. An `ActionWorker` or `StreamWorker` given 0 or nothing uses 30000.
- A `blockMs` over 300000 (5 minutes) throws `BlockTooLongError` before anything is sent. A negative or fractional `blockMs` throws `FloError`.
- A blocking call waits `timeoutMs + blockMs` before timing out, so a long poll is not cut off by the plain request timeout. An action await with no `blockMs` counts as 30000, the server's default.
- Replies are routed to their request by request id, so concurrent requests on one connection are safe.
- When the server cannot parse a request, every request in flight on that connection fails with the server's error.
- KV TTLs are in milliseconds, as the server reads them: `PutOptions.ttlSeconds` is now `ttlMs`, `OptionTag.TTLSeconds` is now `OptionTag.TTLMs`, and `kv.touch` and a transaction's `touch` take milliseconds (0 clears the TTL).
- Every KV op sends its schema-coded body (`[namespace][key][body]`, no options) and reads its op's response schema whole. `kv.delete` returns whether the key existed and takes `casVersion` (was `ifMatch`) and `routingKey`; `kv.scan` returns `{ keys, cursor }` (names only, cursor null on the last page, limit 1–1024, default 1024); `VersionEntry.timestamp` is now `atMs`; `kv.mget` refuses keys over 4096 bytes, splits keys across requests that fit the frame, and follows the server's `next` until every key is answered; `kv.incr` returns `{ value, version }`; `kv.jsonDel` returns the new version or null; `history`, `incr`, `exists` and the JSON ops take `routingKey`; `history` of a missing key is empty. `kv.expire(key, ttlMs, { casVersion?, routingKey? })` replaces `touch` and `persist` (0 clears the TTL), and `OpCode.KVTouch` is now `KVExpire`. Inside a transaction, `delete` and `incr` return nothing. New reasons `WrongType` and `TooManyTransactions`; the table hash moves with the server.
- List and scan requests carry their limit and cursor in the value as `[limit:u32][cursor]`; the web client's `kv.scan` sent them as an option and a bare cursor.
- `kv.scan` reads the server's reply layout, `[count][entries][has_more:u8][cursor_len:u16][cursor]`; it parsed a header-first layout the server never sent.

### Removed

- `@floruntime/web`, the browser client: the server has no WebSocket endpoint, so it could not connect.

Opcodes and options the server never serves:

- `queue.touch` and `TouchOptions`; `EnqueueOptions.delayMs` and `dedupKey`; `DequeueOptions.visibilityTimeoutMs`; `NackOptions.toDlq`.
- `StreamNackOptions.redeliveryDelayMs`; `WorkerAwaitOptions.timeoutMs`.
- The web client's `authenticate()` and `AuthResult`, `streams.subscribe()` and its types (`StreamSubscribeOptions`, `StreamEventCallback`, `StreamSubscription`, `StreamEventHandler`), and the transport's push handling.
- Every `*Response` opcode (a reply header has no opcode), the unhandled opcodes (`Pong`, `Auth`, `SetDurability`, `OK`, stream subscribe/event, `QueueExtendLease`, `QueueFailAuto`, `QueueDLQStats`, `QueuePromoteDue`, `QueueTouch`, `QueueBatchEnqueue`, `ActionTaskAssignment`), and the option tags the server does not read.
- `serializeActionListValue` and `serializeWorkerListValue`, replaced by `serializeListValue(limit, cursor)`.
- `ActionInvokeOptions.priority` and `idempotencyKey`, which the server ignores; invoke now sends `[has_labels:u8]([labels_len:u16][labels])?[input]` and takes a `labels` option (a JSON object string naming required worker labels). Invoke priority, delay and idempotency are deferred: https://github.com/floruntime/flo/issues/181.
- The deprecated `Worker` and `WorkerConfig` aliases; use `ActionWorker` and `ActionWorkerConfig`.
- `kv.touch`, `kv.persist`, `KVTouchOptions`, `OpCode.KVPersist`, `KVEntry`, `KVBeginResult`, `parseScanResponse`, `parseHistoryResponse`, and the KV option tags (`TTLMs`, `CASVersion`, `IfNotExists`, `IfExists`, `RoutingKey`, `TxnID`).

## [0.1.0-dev.4] - 2026-05-04

### Changed

- Full feature parity with Go SDK confirmed across all modules: KV, Queue, Stream, Actions, Workers, Workflows, and Processing
- JS SDK reviewed and validated against Go SDK wire protocol and API surface

## [0.1.0] - 2026-01-22

### Added

- Initial release of Flo JavaScript SDK
- **@floruntime/core**: Core wire protocol, types, and transport-agnostic operations
  - Binary protocol serialization/deserialization
  - CRC32 checksum validation
  - StreamID support for stream positioning
  - KV, Queue, Stream, and Action/Worker operation types
- **@floruntime/node**: Node.js client with TCP transport
  - Full KV operations (get, put, delete, scan, history)
  - Full Queue operations (enqueue, dequeue, ack, nack, DLQ)
  - Full Stream operations (append, read, subscribe, consumer groups)
  - Action and Worker support for distributed task execution
- **@floruntime/web**: Browser client with WebSocket transport
  - Stream operations for real-time pub/sub
  - Read-only KV access for config and feature flags
  - Designed for browser-to-server real-time communication

### Notes

- WebSocket authentication (authToken) is defined but server-side support is pending
- Requires Flo server v0.1.0 or later

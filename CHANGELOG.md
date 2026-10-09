# Changelog

All notable changes to the Flo JavaScript SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `ActionWorker` and `StreamWorker` reconnect after the connection drops (retrying 1 s apart, doubling to 30 s, until `stop()`), then register again (`ActionWorker`) or rejoin the group (`StreamWorker`) before polling. They used to retry on the dead connection forever. The lost connection and each failed attempt are logged at warn.

### Changed

- Workers back off (50 ms, doubling to 1 s) when blocking polls keep coming back empty at once.
- `blockMs: 0` now means don't wait, as on the server. An `ActionWorker` or `StreamWorker` given 0 or nothing uses 30000.
- A `blockMs` over 300000 (5 minutes) throws `BlockTooLongError` before anything is sent. A negative or fractional `blockMs` throws `FloError`.
- A blocking call waits `timeoutMs + blockMs` before timing out, so a long poll is not cut off by the plain request timeout. An action await with no `blockMs` counts as 30000, the server's default.
- Replies are routed to their request by request id, so concurrent requests on one connection are safe.
- When the server cannot parse a request, every request in flight on that connection fails with the server's error.
- KV TTLs are in milliseconds, as the server reads them: `PutOptions.ttlSeconds` is now `ttlMs`, `OptionTag.TTLSeconds` is now `OptionTag.TTLMs`, and `kv.touch` and a transaction's `touch` take milliseconds (0 clears the TTL).

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

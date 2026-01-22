# Changelog

All notable changes to the Flo JavaScript SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

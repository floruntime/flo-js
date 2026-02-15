/**
 * @floruntime/web - Flo SDK for browser environments
 *
 * This package provides a Flo client optimized for real-time browser applications:
 * - **Streams**: Subscribe to and publish real-time events (pub/sub)
 * - **KV (read-only)**: Access configuration, feature flags, user preferences
 * - **Authentication**: JWT/API key auth during WebSocket upgrade
 *
 * For full KV mutations (put, delete) and queue operations, use @floruntime/node
 * on the backend.
 *
 * @example
 * ```typescript
 * import { FloClient } from "@floruntime/web";
 *
 * const client = new FloClient("wss://flo.example.com/ws", {
 *   namespace: "myapp",
 *   authToken: "user-jwt-token",
 *   onAuthRequired: async () => await refreshToken(),
 * });
 *
 * await client.connect();
 *
 * // Real-time subscriptions
 * await client.streams.subscribe("chat:room-123", (record) => {
 *   console.log("New message:", new TextDecoder().decode(record.payload));
 * });
 *
 * // Read config/feature flags
 * const flags = await client.kv.get("config:feature-flags");
 * ```
 */

// Re-export core types needed for web usage
export {
  // Protocol constants (for advanced usage)
  MAGIC,
  VERSION,
  HEADER_SIZE,
  // Status codes and errors
  StatusCode,
  statusCodeToString,
  FloError,
  NotConnectedError,
  ConnectionError,
  TimeoutError,
  NotFoundError,
  UnauthorizedError,
  // Stream types
  type StreamRecord,
  type StreamAppendResult,
  type StreamReadResult,
  type StreamAppendOptions,
  type StreamReadOptions,
  type StreamSubscribeOptions,
  type StreamEventCallback,
  type StreamSubscription,
  type StreamGroupOptions,
  type StreamAckOptions,
  // KV types (read-only subset)
  type KVEntry,
  type ScanResult,
  // Client options
  type WebClientOptions,
  // Logger
  type Logger,
  consoleLogger,
  silentLogger,
  // Error type guards
  isNotFound,
  isUnauthorized,
} from "@floruntime/core";

// Web-specific exports
export { FloClient, type AuthResult } from "./client.js";
export { WebSocketTransport, AuthenticationError, type WebSocketTransportOptions, type StreamEventHandler } from "./transport.js";

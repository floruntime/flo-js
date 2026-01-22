/**
 * Flo client for browser environments.
 *
 * Designed for real-time web applications with:
 * - Stream operations (pub/sub for real-time events)
 * - Read-only KV access (for config, feature flags, preferences)
 * - Authentication support (TODO: server-side not yet implemented)
 *
 * Note: Queue operations and KV mutations are not exposed in the web client.
 * Use the Node.js client (@floruntime/node) for backend operations.
 */

import {
  type WebClientOptions,
  HEADER_SIZE,
  type OpCode,
  type RawResponse,
  type StreamRecord,
  StreamOperations,
  KVReadOnlyOperations,
  parseRawResponse,
  serializeRequest,
} from "@floruntime/core";
import { WebSocketTransport, type WebSocketTransportOptions } from "./transport.js";

/**
 * Flo client for browser environments using WebSocket transport.
 *
 * This client is optimized for real-time browser applications:
 * - **Streams**: Subscribe to and publish real-time events
 * - **KV (read-only)**: Access configuration, feature flags, user preferences
 *
 * For full KV mutations and queue operations, use @floruntime/node on the backend.
 *
 * @example
 * ```typescript
 * import { FloWebClient } from "@floruntime/web";
 *
 * const client = new FloWebClient("wss://flo.example.com/ws", {
 *   namespace: "myapp",
 *   authToken: "user-jwt-token", // TODO: auth not yet implemented
 * });
 *
 * await client.connect();
 *
 * // Subscribe to real-time events
 * const sub = await client.streams.subscribe("chat:room-123", (record) => {
 *   console.log("New message:", new TextDecoder().decode(record.payload));
 * });
 *
 * // Publish an event
 * await client.streams.append("chat:room-123", encoder.encode("Hello!"), {
 *   partitionKey: "user-456",
 * });
 *
 * // Read configuration (read-only)
 * const config = await client.kv.get("config:feature-flags");
 * ```
 */
export class FloWebClient {
  private readonly transport: WebSocketTransport;
  private readonly defaultNamespace: string;
  private readonly debug: boolean;
  private requestId: bigint = 0n;

  // Stream event handlers for forwarding to StreamOperations
  private streamEventCallbacks: Set<(streamName: string, record: StreamRecord) => void> = new Set();

  /** Stream operations (pub/sub for real-time events) */
  readonly streams: StreamOperations;

  /** KV operations (read-only - get and scan only) */
  readonly kv: KVReadOnlyOperations;

  /**
   * Create a new Flo client for browser environments.
   *
   * @param url - WebSocket URL in the format "ws://host:port/path" or "wss://host:port/path"
   * @param options - Client options including auth token
   */
  constructor(url: string, options?: WebClientOptions) {
    this.defaultNamespace = options?.namespace ?? "default";
    this.debug = options?.debug ?? false;

    const transportOpts: WebSocketTransportOptions = {
      connectTimeoutMs: options?.timeoutMs ?? 5000,
      timeoutMs: options?.timeoutMs ?? 5000,
      debug: this.debug,
      authToken: options?.authToken,
      onAuthRequired: options?.onAuthRequired,
    };

    this.transport = new WebSocketTransport(url, transportOpts);

    // Set up stream event forwarding from transport
    this.transport.onStreamEvent((streamName, record) => {
      for (const callback of this.streamEventCallbacks) {
        callback(streamName, record);
      }
    });

    // Initialize sub-clients with request sender interface
    const sender = {
      sendRequest: this.sendRequest.bind(this),
      getNamespace: this.getNamespace.bind(this),
      onStreamEvent: (callback: (streamName: string, record: StreamRecord) => void) => {
        this.streamEventCallbacks.add(callback);
      },
      offStreamEvent: (callback: (streamName: string, record: StreamRecord) => void) => {
        this.streamEventCallbacks.delete(callback);
      },
    };

    this.streams = new StreamOperations(sender);
    this.kv = new KVReadOnlyOperations(sender);
  }

  /**
   * Connect to the server.
   *
   * If an auth token was provided, authentication will be performed
   * after the WebSocket connection is established.
   * TODO: Server-side auth not yet implemented.
   */
  async connect(): Promise<void> {
    await this.transport.connect();
  }

  /**
   * Close the connection.
   */
  async close(): Promise<void> {
    await this.transport.close();
  }

  /**
   * Check if connected to the server.
   */
  isConnected(): boolean {
    return this.transport.isConnected();
  }

  /**
   * Get the default namespace.
   */
  namespace(): string {
    return this.defaultNamespace;
  }

  /**
   * Get the effective namespace (override or default).
   */
  getNamespace(override?: string): string {
    return override ?? this.defaultNamespace;
  }

  /**
   * Send a request to the server.
   * Used internally by Stream and KV operations.
   */
  async sendRequest(
    opCode: OpCode,
    namespace: string,
    key: Uint8Array,
    value: Uint8Array,
    options: Uint8Array
  ): Promise<RawResponse> {
    this.requestId += 1n;
    const requestId = this.requestId;

    const textEncoder = new TextEncoder();
    const namespaceBytes = textEncoder.encode(namespace);

    if (this.debug) {
      const textDecoder = new TextDecoder();
      console.log(
        `[flo] -> ${opCode} ns=${namespace} key=${textDecoder.decode(key)}`
      );
    }

    const request = serializeRequest(
      requestId,
      opCode,
      namespaceBytes,
      key,
      value,
      options
    );

    const response = await this.transport.sendAndReceive(request);

    const header = response.subarray(0, HEADER_SIZE);
    const data = response.subarray(HEADER_SIZE);

    const rawResponse = parseRawResponse(header, data);

    if (this.debug) {
      console.log(`[flo] <- ${rawResponse.status} ${data.length} bytes`);
    }

    return rawResponse;
  }
}

// Re-export the old name for backwards compatibility, but mark as deprecated
/**
 * @deprecated Use FloWebClient instead. FloClient will be removed in a future version.
 */
export const FloClient = FloWebClient;

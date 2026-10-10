/**
 * Flo client for browser environments.
 *
 * Designed for real-time web applications with:
 * - Stream operations (append, read, consumer groups)
 * - Read-only KV access (for config, feature flags, preferences)
 * - Authentication support (JWT/API key via WebSocket upgrade)
 *
 * Note: Queue operations and KV mutations are not exposed in the web client.
 * Use the Node.js client (@floruntime/node) for backend operations.
 */

import {
  type WebClientOptions,
  type Logger,
  consoleLogger,
  silentLogger,
  HEADER_SIZE,
  type OpCode,
  type RawResponse,
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
 * - **Streams**: Append, read and consume via consumer groups
 * - **KV (read-only)**: Access configuration, feature flags, user preferences
 *
 * For full KV mutations and queue operations, use @floruntime/node on the backend.
 *
 * @example
 * ```typescript
 * import { FloClient } from "@floruntime/web";
 *
 * const client = new FloClient("wss://flo.example.com/ws", {
 *   namespace: "myapp",
 *   authToken: "user-jwt-token",
 *   onAuthRequired: async () => {
 *     // Called if auth fails - return a fresh token
 *     return await refreshAuthToken();
 *   },
 * });
 *
 * await client.connect();
 *
 * // Read new records
 * const { records } = await client.streams.read("chat:room-123", { blockMs: 30000 });
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
export class FloClient {
  private readonly transport: WebSocketTransport;
  private readonly defaultNamespace: string;
  private readonly logger: Logger;
  private requestId: bigint = 0n;

  // Disconnect handlers
  private disconnectCallbacks: Set<(reason?: string) => void> = new Set();

  /** Stream operations */
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
    
    // Resolve logger: explicit logger option > debug flag > silent
    if (options?.logger === true) {
      this.logger = consoleLogger;
    } else if (options?.logger && typeof options.logger === "object") {
      this.logger = options.logger;
    } else {
      this.logger = silentLogger;
    }

    const transportOpts: WebSocketTransportOptions = {
      connectTimeoutMs: options?.timeoutMs ?? 5000,
      timeoutMs: options?.timeoutMs ?? 5000,
      logger: this.logger,
      authToken: options?.authToken,
      onAuthRequired: options?.onAuthRequired,
    };

    this.transport = new WebSocketTransport(url, transportOpts);

    // Set up disconnect forwarding from transport
    this.transport.onDisconnect((reason) => {
      for (const callback of this.disconnectCallbacks) {
        callback(reason);
      }
    });

    // Initialize sub-clients with request sender interface
    const sender = {
      sendRequest: this.sendRequest.bind(this),
      getNamespace: this.getNamespace.bind(this),
    };

    this.streams = new StreamOperations(sender);
    this.kv = new KVReadOnlyOperations(sender);
  }

  /**
   * Connect to the server.
   *
   * If an auth token was provided, it will be sent during the WebSocket
   * upgrade handshake. The server validates the token before establishing
   * the connection. If auth fails and onAuthRequired was provided, it will
   * be called to obtain a new token for retry.
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
   * Register a callback for disconnect events.
   * Called when the connection is closed (by server or network).
   * Returns a cleanup function to unregister the callback.
   */
  onDisconnect(callback: (reason?: string) => void): () => void {
    this.disconnectCallbacks.add(callback);
    return () => this.disconnectCallbacks.delete(callback);
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

    const textDecoder = new TextDecoder();
    this.logger.debug(`-> ${opCode} ns=${namespace} key=${textDecoder.decode(key)}`);

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

    this.logger.debug(`<- ${rawResponse.status} ${data.length} bytes`);

    return rawResponse;
  }
}

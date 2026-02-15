/**
 * Flo client for browser environments.
 *
 * Designed for real-time web applications with:
 * - Stream operations (pub/sub for real-time events)
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
  OpCode,
  StatusCode,
  type RawResponse,
  StreamOperations,
  KVReadOnlyOperations,
  parseRawResponse,
  serializeRequest,
  UnauthorizedError,
} from "@floruntime/core";
import { WebSocketTransport, type WebSocketTransportOptions } from "./transport.js";

/**
 * Authentication result from OpCode.Auth request.
 */
export interface AuthResult {
  /** User ID from the validated token (if present) */
  userId: string | null;
  /** Locked namespace from the token (if present) */
  namespace: string | null;
}

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
export class FloClient {
  private readonly transport: WebSocketTransport;
  private readonly defaultNamespace: string;
  private readonly logger: Logger;
  private requestId: bigint = 0n;

  // Subscription push handlers for forwarding to StreamOperations
  private subscriptionPushCallbacks: Set<(subscriptionId: number, data: Uint8Array) => void> = new Set();

  // Disconnect handlers
  private disconnectCallbacks: Set<(reason?: string) => void> = new Set();

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

    // Set up subscription push forwarding from transport
    this.transport.onPushMessage((subscriptionId, data) => {
      for (const callback of this.subscriptionPushCallbacks) {
        callback(subscriptionId, data);
      }
    });

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
      onSubscriptionPush: (callback: (subscriptionId: number, data: Uint8Array) => void) => {
        this.subscriptionPushCallbacks.add(callback);
      },
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

  /**
   * Authenticate with the server using a token (JWT or API key).
   *
   * This method sends an OpCode.Auth request to the server to validate
   * the token and update the connection's authentication state. Use this
   * for token refresh scenarios where you need to update auth without
   * reconnecting.
   *
   * Note: Initial auth is typically handled during WebSocket upgrade via
   * the `authToken` option. This method is for post-connection auth updates.
   *
   * @param token - The authentication token (JWT or API key)
   * @returns Authentication result with user ID and namespace (if present)
   * @throws {UnauthorizedError} If the token is invalid
   *
   * @example
   * ```typescript
   * // Refresh auth token after expiry
   * const newToken = await refreshTokenFromAuthServer();
   * const authResult = await client.authenticate(newToken);
   * console.log("Authenticated as:", authResult.userId);
   * ```
   */
  async authenticate(token: string): Promise<AuthResult> {
    const textEncoder = new TextEncoder();
    const tokenBytes = textEncoder.encode(token);

    const response = await this.sendRequest(
      OpCode.Auth,
      "", // No namespace for auth
      tokenBytes, // Token goes in key field
      new Uint8Array(0), // No value
      new Uint8Array(0) // No options
    );

    if (response.status !== StatusCode.OK) {
      throw new UnauthorizedError("Authentication failed: invalid token");
    }

    // Parse auth response: [has_user_id:u8] [user_id_len:u32 user_id:bytes]? [has_namespace:u8] [namespace_len:u32 namespace:bytes]?
    const data = response.data;
    const textDecoder = new TextDecoder();
    let offset = 0;

    let userId: string | null = null;
    let namespace: string | null = null;

    if (data.length > offset) {
      const hasUserId = data[offset];
      offset += 1;
      if (hasUserId === 1 && data.length >= offset + 4) {
        const view = new DataView(data.buffer, data.byteOffset + offset, 4);
        const userIdLen = view.getUint32(0, true);
        offset += 4;
        if (data.length >= offset + userIdLen) {
          userId = textDecoder.decode(data.subarray(offset, offset + userIdLen));
          offset += userIdLen;
        }
      }
    }

    if (data.length > offset) {
      const hasNamespace = data[offset];
      offset += 1;
      if (hasNamespace === 1 && data.length >= offset + 4) {
        const view = new DataView(data.buffer, data.byteOffset + offset, 4);
        const namespaceLen = view.getUint32(0, true);
        offset += 4;
        if (data.length >= offset + namespaceLen) {
          namespace = textDecoder.decode(data.subarray(offset, offset + namespaceLen));
          offset += namespaceLen;
        }
      }
    }

    // Update transport's auth token for future reconnections
    this.transport.setAuthToken(token);

    this.logger.debug(`Authenticated: userId=${userId}, namespace=${namespace}`);

    return { userId, namespace };
  }
}

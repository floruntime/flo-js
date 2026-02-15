/**
 * WebSocket transport for browser environments.
 */

import {
  ConnectionError,
  HEADER_SIZE,
  InvalidChecksumError,
  NotConnectedError,
  TimeoutError,
  type Transport,
  UnexpectedEOFError,
  computeCRC32,
  parseResponseHeader,
  type Logger,
  silentLogger,
} from "@floruntime/core";

import type { StreamRecord } from "@floruntime/core";

/**
 * Callback for server-pushed stream events.
 */
export type StreamEventHandler = (streamName: string, record: StreamRecord) => void;

/**
 * Callback for server-pushed messages (subscriptions).
 */
export type PushMessageHandler = (subscriptionId: number, data: Uint8Array) => void;

/**
 * Callback for disconnect events.
 */
export type DisconnectHandler = (reason?: string) => void;

/**
 * WebSocket transport options.
 */
export interface WebSocketTransportOptions {
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;

  /** Request timeout in milliseconds */
  timeoutMs?: number;

  /** Logger for debug/warning/error messages */
  logger?: Logger;

  /**
   * Authentication token (JWT or API key).
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

/**
 * WebSocket transport implementation for browsers.
 *
 * Note: This transport requires WebSocket server support on the Flo server.
 * The WebSocket connection sends and receives the same binary protocol
 * messages as the TCP transport, just wrapped in WebSocket binary frames.
 */
/**
 * Authentication error thrown when server rejects credentials.
 */
export class AuthenticationError extends Error {
  constructor(message: string = "Authentication failed") {
    super(message);
    this.name = "AuthenticationError";
  }
}

export class WebSocketTransport implements Transport {
  private ws: WebSocket | null = null;
  private readonly baseUrl: string;
  private readonly connectTimeoutMs: number;
  private readonly timeoutMs: number;
  private readonly logger: Logger;
  private authToken?: string;
  private readonly onAuthRequired?: () => Promise<string>;

  // Pending requests waiting for responses
  private pendingRequests: Map<
    bigint,
    { resolve: (data: Uint8Array) => void; reject: (err: Error) => void }
  > = new Map();

  // Buffer for partial messages
  private receiveBuffer: Uint8Array = new Uint8Array(0);

  // Stream event handlers for server-push notifications
  private streamEventHandlers: Set<StreamEventHandler> = new Set();

  // Push message handler for subscription notifications
  private pushMessageHandler: PushMessageHandler | null = null;

  // Disconnect handler
  private disconnectHandler: DisconnectHandler | null = null;

  constructor(url: string, options?: WebSocketTransportOptions) {
    // Validate URL format
    if (!url.startsWith("ws://") && !url.startsWith("wss://")) {
      throw new Error(
        "WebSocket URL must start with ws:// or wss://"
      );
    }
    this.baseUrl = url;
    this.connectTimeoutMs = options?.connectTimeoutMs ?? 5000;
    this.timeoutMs = options?.timeoutMs ?? 5000;
    this.logger = options?.logger ?? silentLogger;
    this.authToken = options?.authToken;
    this.onAuthRequired = options?.onAuthRequired;
  }

  /**
   * Build the WebSocket URL with authentication token if provided.
   * Token is passed as query parameter for browser compatibility.
   */
  private buildUrl(): string {
    if (!this.authToken) {
      return this.baseUrl;
    }
    const separator = this.baseUrl.includes("?") ? "&" : "?";
    return `${this.baseUrl}${separator}token=${encodeURIComponent(this.authToken)}`;
  }

  /**
   * Update the authentication token.
   * Call this after onAuthRequired returns a new token.
   */
  setAuthToken(token: string): void {
    this.authToken = token;
  }

  /**
   * Register a handler for server-pushed stream events.
   */
  onStreamEvent(handler: StreamEventHandler): void {
    this.streamEventHandlers.add(handler);
  }

  /**
   * Unregister a stream event handler.
   */
  offStreamEvent(handler: StreamEventHandler): void {
    this.streamEventHandlers.delete(handler);
  }

  /**
   * Register a handler for push messages (subscription notifications).
   * The handler receives the subscription ID and raw data payload.
   */
  onPushMessage(handler: PushMessageHandler): void {
    this.pushMessageHandler = handler;
  }

  /**
   * Register a handler for disconnect events.
   */
  onDisconnect(handler: DisconnectHandler): void {
    this.disconnectHandler = handler;
  }

  async connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    return this.attemptConnect();
  }

  /**
   * Attempt to connect, handling auth failures with retry via onAuthRequired.
   */
  private async attemptConnect(isRetry: boolean = false): Promise<void> {
    const url = this.buildUrl();

    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(url, "flo-proto");
      ws.binaryType = "arraybuffer";

      const timeoutId = setTimeout(() => {
        ws.close();
        reject(new TimeoutError(this.connectTimeoutMs));
      }, this.connectTimeoutMs);

      ws.onopen = async () => {
        clearTimeout(timeoutId);
        this.ws = ws;
        this.setupMessageHandler();

        this.logger.debug(`Connected to ${this.baseUrl}`);
        if (this.authToken) {
          this.logger.debug(`Authenticated successfully`);
        }
        resolve();
      };

      ws.onerror = () => {
        clearTimeout(timeoutId);
        reject(new ConnectionError(this.baseUrl, new Error("WebSocket error")));
      };

      ws.onclose = async (event) => {
        clearTimeout(timeoutId);

        // HTTP 401 Unauthorized - server rejected auth during upgrade
        // WebSocket close code 1002 (protocol error) or immediate close may indicate auth failure
        if (event.code === 1002 || (event.code === 1006 && this.authToken)) {
          if (!isRetry && this.onAuthRequired) {
            try {
              this.logger.debug(`Auth failed, requesting new token...`);
              const newToken = await this.onAuthRequired();
              this.setAuthToken(newToken);
              // Retry connection with new token
              resolve(this.attemptConnect(true));
              return;
            } catch (authErr) {
              reject(new AuthenticationError("Failed to obtain new auth token"));
              return;
            }
          }
          reject(new AuthenticationError("Server rejected authentication"));
          return;
        }

        if (!this.ws) {
          reject(new ConnectionError(this.baseUrl, new Error("Connection closed")));
        }
      };
    });
  }

  private setupMessageHandler(): void {
    if (!this.ws) return;

    this.ws.onmessage = (event) => {
      if (!(event.data instanceof ArrayBuffer)) {
        this.logger.warn("Received non-binary message:", event.data);
        return;
      }

      const data = new Uint8Array(event.data);

      this.logger.debug(`WebSocket frame received: ${data.length} bytes, first bytes:`, Array.from(data.slice(0, 32)).map(b => b.toString(16).padStart(2, '0')).join(' '));

      // Append to receive buffer
      const newBuffer = new Uint8Array(this.receiveBuffer.length + data.length);
      newBuffer.set(this.receiveBuffer, 0);
      newBuffer.set(data, this.receiveBuffer.length);
      this.receiveBuffer = newBuffer;

      // Try to process complete messages
      this.processReceiveBuffer();
    };

    this.ws.onclose = (event) => {
      // Reject all pending requests
      for (const [, { reject }] of this.pendingRequests) {
        reject(new UnexpectedEOFError("connection closed"));
      }
      this.pendingRequests.clear();
      this.ws = null;

      this.logger.debug("Disconnected");

      // Notify disconnect handler
      if (this.disconnectHandler) {
        const reason = event.reason || (event.code === 1000 ? "normal closure" : `code ${event.code}`);
        this.disconnectHandler(reason);
      }
    };

    this.ws.onerror = () => {
      // Reject all pending requests
      for (const [, { reject }] of this.pendingRequests) {
        reject(new UnexpectedEOFError("WebSocket error"));
      }
      this.pendingRequests.clear();
    };
  }

  private processReceiveBuffer(): void {
    while (this.receiveBuffer.length >= HEADER_SIZE) {
      // Parse header to get data length
      const header = this.receiveBuffer.subarray(0, HEADER_SIZE);

      let dataLen: number;
      let requestId: bigint;
      let expectedCRC: number;

      try {
        const [, len, reqId, crc] = parseResponseHeader(header);
        dataLen = len;
        requestId = reqId;
        expectedCRC = crc;
      } catch (err) {
        // Invalid header - reject all pending requests
        for (const [, { reject }] of this.pendingRequests) {
          reject(err as Error);
        }
        this.pendingRequests.clear();
        this.receiveBuffer = new Uint8Array(0);
        return;
      }

      const totalLen = HEADER_SIZE + dataLen;
      if (this.receiveBuffer.length < totalLen) {
        // Not enough data yet
        return;
      }

      // Extract complete message
      const message = this.receiveBuffer.subarray(0, totalLen);
      this.receiveBuffer = this.receiveBuffer.subarray(totalLen);

      // Verify CRC32
      const payload = message.subarray(HEADER_SIZE);
      const computedCRC = computeCRC32(header, payload);

      // Header byte 21 is status code for responses
      const status = header[21];

      this.logger.debug(`Received message: status=0x${status?.toString(16)} reqId=${requestId} dataLen=${dataLen}`);

      const pending = this.pendingRequests.get(requestId);
      if (pending) {
        // This is a response to our request
        this.pendingRequests.delete(requestId);

        if (expectedCRC !== computedCRC) {
          pending.reject(new InvalidChecksumError(expectedCRC, computedCRC));
        } else {
          // Make a copy of the message data
          const result = new Uint8Array(message.length);
          result.set(message);
          pending.resolve(result);
        }
      } else if (this.pushMessageHandler) {
        // No pending request - this is a server-pushed message (subscription)
        // The request_id field contains the subscription_id
        this.logger.debug(`Push received for subscription ${requestId}, crcMatch=${expectedCRC === computedCRC}`);
        if (expectedCRC === computedCRC) {
          const subscriptionId = Number(requestId);
          // Make a copy of the payload
          const payloadCopy = new Uint8Array(payload.length);
          payloadCopy.set(payload);
          this.pushMessageHandler(subscriptionId, payloadCopy);
        } else {
          this.logger.warn(`Push CRC mismatch for subscription ${requestId}`);
        }
      }
    }
  }

  async close(): Promise<void> {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  async sendAndReceive(data: Uint8Array): Promise<Uint8Array> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new NotConnectedError();
    }

    // Extract request ID from the data (at offset 8, little-endian u64)
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const requestId = view.getBigUint64(8, true);

    return new Promise<Uint8Array>((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(new TimeoutError(this.timeoutMs));
      }, this.timeoutMs);

      this.pendingRequests.set(requestId, {
        resolve: (result) => {
          clearTimeout(timeoutId);
          resolve(result);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err);
        },
      });

      // Send data
      this.ws!.send(data);
    });
  }
}

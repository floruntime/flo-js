/**
 * WebSocket transport for browser environments.
 */

import {
  ConnectionError,
  HEADER_SIZE,
  InvalidChecksumError,
  NotConnectedError,
  OpCode,
  TimeoutError,
  type Transport,
  UnexpectedEOFError,
  computeCRC32,
  parseResponseHeader,
  parseStreamReadResponse,
} from "@floruntime/core";

import type { StreamRecord } from "@floruntime/core";

/**
 * Callback for server-pushed stream events.
 */
export type StreamEventHandler = (streamName: string, record: StreamRecord) => void;

/**
 * WebSocket transport options.
 */
export interface WebSocketTransportOptions {
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;

  /** Request timeout in milliseconds */
  timeoutMs?: number;

  /** Enable debug logging */
  debug?: boolean;

  /**
   * Authentication token to send after connection.
   * TODO: Server-side auth not yet implemented - this is a placeholder.
   */
  authToken?: string;

  /**
   * Callback invoked when authentication is required or fails.
   * Should return a new auth token.
   * TODO: Server-side auth not yet implemented.
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
export class WebSocketTransport implements Transport {
  private ws: WebSocket | null = null;
  private readonly url: string;
  private readonly connectTimeoutMs: number;
  private readonly timeoutMs: number;
  private readonly debug: boolean;
  private readonly authToken?: string;

  // Pending requests waiting for responses
  private pendingRequests: Map<
    bigint,
    { resolve: (data: Uint8Array) => void; reject: (err: Error) => void }
  > = new Map();

  // Buffer for partial messages
  private receiveBuffer: Uint8Array = new Uint8Array(0);

  // Stream event handlers for server-push notifications
  private streamEventHandlers: Set<StreamEventHandler> = new Set();

  constructor(url: string, options?: WebSocketTransportOptions) {
    // Validate URL format
    if (!url.startsWith("ws://") && !url.startsWith("wss://")) {
      throw new Error(
        "WebSocket URL must start with ws:// or wss://"
      );
    }
    this.url = url;
    this.connectTimeoutMs = options?.connectTimeoutMs ?? 5000;
    this.timeoutMs = options?.timeoutMs ?? 5000;
    this.debug = options?.debug ?? false;
    this.authToken = options?.authToken;
    // TODO: options?.onAuthRequired will be used when server-side auth is implemented
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

  async connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.url);
      ws.binaryType = "arraybuffer";

      const timeoutId = setTimeout(() => {
        ws.close();
        reject(new TimeoutError(this.connectTimeoutMs));
      }, this.connectTimeoutMs);

      ws.onopen = async () => {
        clearTimeout(timeoutId);
        this.ws = ws;
        this.setupMessageHandler();

        // TODO: Send auth token after connection when server-side auth is implemented
        // if (this.authToken) {
        //   await this.sendAuthRequest(this.authToken);
        // }
        // The auth flow would use OpCode.Auth (0x03) to send the token
        // and handle Unauthorized responses by calling onAuthRequired

        if (this.debug) {
          console.log(`[flo] Connected to ${this.url}`);
          if (this.authToken) {
            console.log(`[flo] Auth token provided (auth not yet implemented on server)`);
          }
        }
        resolve();
      };

      ws.onerror = () => {
        clearTimeout(timeoutId);
        reject(new ConnectionError(this.url, new Error("WebSocket error")));
      };

      ws.onclose = () => {
        clearTimeout(timeoutId);
        if (!this.ws) {
          reject(new ConnectionError(this.url, new Error("Connection closed")));
        }
      };
    });
  }

  private setupMessageHandler(): void {
    if (!this.ws) return;

    this.ws.onmessage = (event) => {
      if (!(event.data instanceof ArrayBuffer)) {
        return;
      }

      const data = new Uint8Array(event.data);

      // Append to receive buffer
      const newBuffer = new Uint8Array(this.receiveBuffer.length + data.length);
      newBuffer.set(this.receiveBuffer, 0);
      newBuffer.set(data, this.receiveBuffer.length);
      this.receiveBuffer = newBuffer;

      // Try to process complete messages
      this.processReceiveBuffer();
    };

    this.ws.onclose = () => {
      // Reject all pending requests
      for (const [, { reject }] of this.pendingRequests) {
        reject(new UnexpectedEOFError("connection closed"));
      }
      this.pendingRequests.clear();
      this.ws = null;

      if (this.debug) {
        console.log("[flo] Disconnected");
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
      let opCode: number;

      try {
        const [op, len, reqId, crc] = parseResponseHeader(header);
        opCode = op;
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

      if (expectedCRC !== computedCRC) {
        // CRC mismatch - if it was a pending request, reject it
        const pending = this.pendingRequests.get(requestId);
        if (pending) {
          this.pendingRequests.delete(requestId);
          pending.reject(new InvalidChecksumError(expectedCRC, computedCRC));
        }
        continue;
      }

      // Check if this is a server-push stream event (OpCode 0x16)
      if (opCode === OpCode.StreamEvent) {
        this.handleStreamEvent(payload);
        continue;
      }

      // Handle as regular request/response
      const pending = this.pendingRequests.get(requestId);
      if (pending) {
        this.pendingRequests.delete(requestId);
        // Make a copy of the message data
        const result = new Uint8Array(message.length);
        result.set(message);
        pending.resolve(result);
      }
    }
  }

  /**
   * Handle server-push stream events.
   * 
   * Stream event payload format:
   * [stream_name_len: u16][stream_name: bytes][record_data...]
   * 
   * The record data is parsed using parseStreamReadResponse.
   */
  private handleStreamEvent(payload: Uint8Array): void {
    if (payload.length < 2) {
      if (this.debug) {
        console.warn("[flo] Invalid stream event: payload too short");
      }
      return;
    }

    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    const streamNameLen = view.getUint16(0, true);
    
    if (payload.length < 2 + streamNameLen) {
      if (this.debug) {
        console.warn("[flo] Invalid stream event: stream name truncated");
      }
      return;
    }

    const textDecoder = new TextDecoder();
    const streamName = textDecoder.decode(payload.subarray(2, 2 + streamNameLen));
    const recordData = payload.subarray(2 + streamNameLen);

    // Parse the record data (same format as stream read response)
    try {
      const result = parseStreamReadResponse(recordData);
      for (const record of result.records) {
        for (const handler of this.streamEventHandlers) {
          try {
            handler(streamName, record);
          } catch (err) {
            if (this.debug) {
              console.error("[flo] Stream event handler error:", err);
            }
          }
        }
      }
    } catch (err) {
      if (this.debug) {
        console.warn("[flo] Failed to parse stream event:", err);
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

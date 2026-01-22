/**
 * TCP transport for Node.js.
 */

import * as net from "node:net";
import {
  ConnectionError,
  HEADER_SIZE,
  InvalidChecksumError,
  InvalidEndpointError,
  NotConnectedError,
  TimeoutError,
  type Transport,
  UnexpectedEOFError,
  computeCRC32,
  parseResponseHeader,
} from "@floruntime/core";

/**
 * TCP transport options.
 */
export interface TcpTransportOptions {
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;

  /** Read/write timeout in milliseconds */
  timeoutMs?: number;

  /** Enable debug logging */
  debug?: boolean;
}

/**
 * Parse endpoint string into host and port.
 */
export function parseEndpoint(endpoint: string): { host: string; port: number } {
  // Handle IPv6 addresses in brackets
  if (endpoint.startsWith("[")) {
    const idx = endpoint.lastIndexOf("]:");
    if (idx === -1) {
      throw new InvalidEndpointError(endpoint, "expected [host]:port");
    }
    const host = endpoint.substring(1, idx);
    const portStr = endpoint.substring(idx + 2);
    const port = parseInt(portStr, 10);
    if (isNaN(port) || port < 1 || port > 65535) {
      throw new InvalidEndpointError(endpoint, `invalid port: ${portStr}`);
    }
    return { host, port };
  }

  // Handle IPv4 or hostname
  const idx = endpoint.lastIndexOf(":");
  if (idx === -1) {
    throw new InvalidEndpointError(endpoint, "expected host:port");
  }

  const host = endpoint.substring(0, idx);
  const portStr = endpoint.substring(idx + 1);
  const port = parseInt(portStr, 10);

  if (isNaN(port) || port < 1 || port > 65535) {
    throw new InvalidEndpointError(endpoint, `invalid port: ${portStr}`);
  }

  return { host, port };
}

/**
 * TCP transport implementation.
 */
export class TcpTransport implements Transport {
  private socket: net.Socket | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly connectTimeoutMs: number;
  private readonly timeoutMs: number;
  private readonly debug: boolean;

  constructor(endpoint: string, options?: TcpTransportOptions) {
    const { host, port } = parseEndpoint(endpoint);
    this.host = host;
    this.port = port;
    this.connectTimeoutMs = options?.connectTimeoutMs ?? 5000;
    this.timeoutMs = options?.timeoutMs ?? 5000;
    this.debug = options?.debug ?? false;
  }

  async connect(): Promise<void> {
    if (this.socket) {
      return;
    }

    return new Promise<void>((resolve, reject) => {
      const socket = new net.Socket();
      let connected = false;

      const timeoutId = setTimeout(() => {
        if (!connected) {
          socket.destroy();
          reject(new TimeoutError(this.connectTimeoutMs));
        }
      }, this.connectTimeoutMs);

      socket.once("connect", () => {
        connected = true;
        clearTimeout(timeoutId);
        this.socket = socket;
        if (this.debug) {
          console.log(`[flo] Connected to ${this.host}:${this.port}`);
        }
        resolve();
      });

      socket.once("error", (err) => {
        clearTimeout(timeoutId);
        if (!connected) {
          reject(
            new ConnectionError(`${this.host}:${this.port}`, err as Error)
          );
        }
      });

      socket.connect(this.port, this.host);
    });
  }

  async close(): Promise<void> {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      if (this.debug) {
        console.log("[flo] Disconnected");
      }
    }
  }

  isConnected(): boolean {
    return this.socket !== null && !this.socket.destroyed;
  }

  async sendAndReceive(data: Uint8Array): Promise<Uint8Array> {
    if (!this.socket || this.socket.destroyed) {
      throw new NotConnectedError();
    }

    return new Promise<Uint8Array>((resolve, reject) => {
      const socket = this.socket!;
      const chunks: Buffer[] = [];
      let totalLength = 0;
      let headerParsed = false;
      let expectedDataLen = 0;
      let resolved = false;

      const cleanup = () => {
        socket.removeListener("data", onData);
        socket.removeListener("error", onError);
        socket.removeListener("close", onClose);
        clearTimeout(timeoutId);
      };

      const timeoutId = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          cleanup();
          reject(new TimeoutError(this.timeoutMs));
        }
      }, this.timeoutMs);

      const onData = (chunk: Buffer) => {
        chunks.push(chunk);
        totalLength += chunk.length;

        // Check if we have enough for header
        if (!headerParsed && totalLength >= HEADER_SIZE) {
          const combined = Buffer.concat(chunks);
          const header = new Uint8Array(
            combined.buffer,
            combined.byteOffset,
            HEADER_SIZE
          );

          try {
            const [, dataLen] = parseResponseHeader(header);
            expectedDataLen = dataLen;
            headerParsed = true;
          } catch (err) {
            resolved = true;
            cleanup();
            reject(err);
            return;
          }
        }

        // Check if we have complete response
        if (headerParsed && totalLength >= HEADER_SIZE + expectedDataLen) {
          resolved = true;
          cleanup();

          const combined = Buffer.concat(chunks);
          const result = new Uint8Array(HEADER_SIZE + expectedDataLen);
          result.set(
            new Uint8Array(
              combined.buffer,
              combined.byteOffset,
              HEADER_SIZE + expectedDataLen
            )
          );

          // Verify CRC32
          const header = result.subarray(0, HEADER_SIZE);
          const payload = result.subarray(HEADER_SIZE);
          const view = new DataView(header.buffer, header.byteOffset, HEADER_SIZE);
          const expectedCRC = view.getUint32(16, true);
          const computedCRC = computeCRC32(header, payload);

          if (expectedCRC !== computedCRC) {
            reject(new InvalidChecksumError(expectedCRC, computedCRC));
            return;
          }

          resolve(result);
        }
      };

      const onError = (err: Error) => {
        if (!resolved) {
          resolved = true;
          cleanup();
          reject(new UnexpectedEOFError(err.message));
        }
      };

      const onClose = () => {
        if (!resolved) {
          resolved = true;
          cleanup();
          reject(new UnexpectedEOFError("connection closed"));
        }
      };

      socket.on("data", onData);
      socket.once("error", onError);
      socket.once("close", onClose);

      // Send data
      socket.write(Buffer.from(data));
    });
  }
}

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
  requestBlockMs,
  type Logger,
  silentLogger,
} from "@floruntime/core";

/**
 * TCP transport options.
 */
export interface TcpTransportOptions {
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;

  /** Read/write timeout in milliseconds */
  timeoutMs?: number;

  /** Logger for debug/warning/error messages */
  logger?: Logger;
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
 *
 * Requests may be in flight together (a worker completes tasks while its
 * long poll is parked), and the server can answer them out of order, so
 * replies are routed to their request by request id.
 */
export class TcpTransport implements Transport {
  private socket: net.Socket | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly connectTimeoutMs: number;
  private readonly timeoutMs: number;
  private readonly logger: Logger;
  private readonly pending = new Map<
    bigint,
    { resolve: (data: Uint8Array) => void; reject: (err: Error) => void }
  >();
  private receiveBuffer: Buffer = Buffer.alloc(0);

  constructor(endpoint: string, options?: TcpTransportOptions) {
    const { host, port } = parseEndpoint(endpoint);
    this.host = host;
    this.port = port;
    this.connectTimeoutMs = options?.connectTimeoutMs ?? 5000;
    this.timeoutMs = options?.timeoutMs ?? 5000;
    this.logger = options?.logger ?? silentLogger;
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
        this.receiveBuffer = Buffer.alloc(0);
        socket.on("data", (chunk: Buffer) => this.onData(chunk));
        socket.on("error", (err) => this.failAll(socket, new UnexpectedEOFError(err.message)));
        socket.on("close", () => this.failAll(socket, new UnexpectedEOFError("connection closed")));
        this.logger.debug(`Connected to ${this.host}:${this.port}`);
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
      const socket = this.socket;
      this.failAll(socket, new UnexpectedEOFError("connection closed"));
      socket.destroy();
      this.logger.debug("Disconnected");
    }
  }

  isConnected(): boolean {
    return this.socket !== null && !this.socket.destroyed;
  }

  async sendAndReceive(data: Uint8Array): Promise<Uint8Array> {
    if (!this.socket || this.socket.destroyed) {
      throw new NotConnectedError();
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const requestId = view.getBigUint64(8, true);
    const deadlineMs = this.timeoutMs + requestBlockMs(data);

    return new Promise<Uint8Array>((resolve, reject) => {
      // Once timed out the id is forgotten, so a late reply is dropped
      // rather than taken as the answer to a later request.
      const timeoutId = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new TimeoutError(deadlineMs));
      }, deadlineMs);

      this.pending.set(requestId, {
        resolve: (result) => {
          clearTimeout(timeoutId);
          resolve(result);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err);
        },
      });

      this.socket!.write(Buffer.from(data));
    });
  }

  private onData(chunk: Buffer): void {
    this.receiveBuffer =
      this.receiveBuffer.length === 0 ? chunk : Buffer.concat([this.receiveBuffer, chunk]);

    while (this.receiveBuffer.length >= HEADER_SIZE) {
      const buf = this.receiveBuffer;
      const header = new Uint8Array(buf.buffer, buf.byteOffset, HEADER_SIZE);

      let dataLen: number;
      let requestId: bigint;
      let expectedCRC: number;
      try {
        [, dataLen, requestId, expectedCRC] = parseResponseHeader(header);
      } catch (err) {
        // The stream can't be re-framed past a bad header.
        const socket = this.socket;
        if (socket) {
          this.failAll(socket, err as Error);
          socket.destroy();
        }
        return;
      }

      const totalLen = HEADER_SIZE + dataLen;
      if (buf.length < totalLen) return;

      const result = new Uint8Array(totalLen);
      result.set(new Uint8Array(buf.buffer, buf.byteOffset, totalLen));
      this.receiveBuffer = buf.subarray(totalLen);

      const pending = this.pending.get(requestId);
      if (!pending) {
        this.logger.debug(`Dropping reply to request ${requestId}: no longer waited for`);
        continue;
      }
      this.pending.delete(requestId);

      const computedCRC = computeCRC32(result.subarray(0, HEADER_SIZE), result.subarray(HEADER_SIZE));
      if (expectedCRC !== computedCRC) {
        pending.reject(new InvalidChecksumError(expectedCRC, computedCRC));
      } else {
        pending.resolve(result);
      }
    }
  }

  private failAll(socket: net.Socket, err: Error): void {
    if (this.socket !== socket) return;
    this.socket = null;
    this.receiveBuffer = Buffer.alloc(0);
    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const p of pending) p.reject(err);
  }
}

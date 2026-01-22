/**
 * Flo client for Node.js.
 */

import {
  ActionOperations,
  type ClientOptions,
  HEADER_SIZE,
  KVOperations,
  type OpCode,
  QueueOperations,
  type RawResponse,
  StreamOperations,
  type Transport,
  WorkerOperations,
  parseRawResponse,
  serializeRequest,
} from "@floruntime/core";
import { TcpTransport, type TcpTransportOptions } from "./transport.js";

/**
 * Flo client for Node.js using TCP transport.
 */
export class FloClient {
  private readonly transport: Transport;
  private readonly defaultNamespace: string;
  private readonly debug: boolean;
  private requestId: bigint = 0n;

  /** KV operations */
  readonly kv: KVOperations;

  /** Queue operations */
  readonly queue: QueueOperations;

  /** Stream operations */
  readonly stream: StreamOperations;

  /** Action operations */
  readonly action: ActionOperations;

  /** Worker operations */
  readonly worker: WorkerOperations;

  /**
   * Create a new Flo client.
   *
   * @param endpoint - Server endpoint in the format "host:port"
   * @param options - Client options
   */
  constructor(endpoint: string, options?: ClientOptions) {
    this.defaultNamespace = options?.namespace ?? "default";
    this.debug = options?.debug ?? false;

    const transportOpts: TcpTransportOptions = {
      connectTimeoutMs: options?.timeoutMs ?? 5000,
      timeoutMs: options?.timeoutMs ?? 5000,
      debug: this.debug,
    };

    this.transport = new TcpTransport(endpoint, transportOpts);

    // Initialize sub-clients
    this.kv = new KVOperations(this);
    this.queue = new QueueOperations(this);
    this.stream = new StreamOperations(this);
    this.action = new ActionOperations(this);
    this.worker = new WorkerOperations(this);
  }

  /**
   * Connect to the server.
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
   * Get the effective namespace.
   */
  getNamespace(override?: string): string {
    return override ?? this.defaultNamespace;
  }

  /**
   * Send a request to the server.
   * Used internally by KV and Queue operations.
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

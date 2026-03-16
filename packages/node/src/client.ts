/**
 * Flo client for Node.js.
 */

import {
  ActionOperations,
  type ClientOptions,
  type Logger,
  consoleLogger,
  silentLogger,
  HEADER_SIZE,
  KVOperations,
  type OpCode,
  QueueOperations,
  type RawResponse,
  StreamOperations,
  type Transport,
  WorkerOperations,
  WorkflowOperations,
  type WorkflowSyncOptions,
  type WorkflowSyncResult,
  type WorkflowSyncDirFile,
  parseRawResponse,
  serializeRequest,
} from "@floruntime/core";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { TcpTransport, type TcpTransportOptions } from "./transport.js";

/**
 * Flo client for Node.js using TCP transport.
 */
export class FloClient {
  private readonly transport: Transport;
  private readonly defaultNamespace: string;
  private readonly logger: Logger;
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

  /** Workflow operations */
  readonly workflow: NodeWorkflowOperations;

  /**
   * Create a new Flo client.
   *
   * @param endpoint - Server endpoint in the format "host:port"
   * @param options - Client options
   */
  constructor(endpoint: string, options?: ClientOptions) {
    this.defaultNamespace = options?.namespace ?? "default";
    
    // Resolve logger: explicit logger option > debug flag > silent
    if (options?.logger === true) {
      this.logger = consoleLogger;
    } else if (options?.logger && typeof options.logger === "object") {
      this.logger = options.logger;
    } else {
      this.logger = silentLogger;
    }

    const transportOpts: TcpTransportOptions = {
      connectTimeoutMs: options?.timeoutMs ?? 5000,
      timeoutMs: options?.timeoutMs ?? 5000,
      logger: this.logger,
    };

    this.transport = new TcpTransport(endpoint, transportOpts);

    // Initialize sub-clients
    this.kv = new KVOperations(this);
    this.queue = new QueueOperations(this);
    this.stream = new StreamOperations(this);
    this.action = new ActionOperations(this);
    this.worker = new WorkerOperations(this);
    this.workflow = new NodeWorkflowOperations(this);
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

/**
 * Node.js workflow operations with built-in syncDir support.
 * Extends the core WorkflowOperations with filesystem access.
 */
class NodeWorkflowOperations extends WorkflowOperations {
  /**
   * Sync all .yaml/.yml workflow files in a directory.
   *
   * Reads every YAML file, extracts the workflow name and version,
   * compares with the server, and creates/updates as needed.
   * Safe to call on every boot.
   *
   * @param dir - Path to directory containing workflow YAML files
   * @param opts - Options (namespace)
   */
  async syncDir(
    dir: string,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult[]>;
  /**
   * Sync all YAML files using a custom directory reader.
   *
   * @param readDirFn - Custom function that returns file entries
   * @param dir - Directory path passed to readDirFn
   * @param opts - Options (namespace)
   */
  async syncDir(
    readDirFn: (dir: string) => Promise<WorkflowSyncDirFile[]>,
    dir: string,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult[]>;
  async syncDir(
    dirOrFn: string | ((dir: string) => Promise<WorkflowSyncDirFile[]>),
    dirOrOpts?: string | WorkflowSyncOptions,
    opts?: WorkflowSyncOptions
  ): Promise<WorkflowSyncResult[]> {
    if (typeof dirOrFn === "string") {
      // syncDir(dir, opts?) — use built-in fs
      return super.syncDir(readYamlDir, dirOrFn, dirOrOpts as WorkflowSyncOptions | undefined);
    }
    // syncDir(readDirFn, dir, opts?) — custom reader
    return super.syncDir(dirOrFn, dirOrOpts as string, opts);
  }
}

/**
 * Read all .yaml/.yml files from a directory using Node.js fs.
 */
async function readYamlDir(dir: string): Promise<WorkflowSyncDirFile[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: WorkflowSyncDirFile[] = [];
  for (const entry of entries) {
    if (entry.isDirectory()) continue;
    if (/\.ya?ml$/.test(entry.name)) {
      const content = await readFile(join(dir, entry.name), "utf-8");
      files.push({ name: entry.name, content });
    }
  }
  return files;
}

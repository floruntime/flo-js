# Flo JavaScript SDK

JavaScript/TypeScript SDK for the Flo distributed systems platform. This monorepo contains packages for both Node.js and browser environments.

## Packages

| Package | Description | Transport |
|---------|-------------|-----------|
| `@floruntime/core` | Core wire protocol, types, and transport-agnostic operations | - |
| `@floruntime/node` | Node.js client with TCP transport | TCP |
| `@floruntime/web` | Browser client with WebSocket transport | WebSocket |

## Installation

### Node.js

```bash
npm install @floruntime/node
# or
yarn install @floruntime/node
```

### Browser

```bash
npm install @floruntime/web
# or
yarn add @floruntime/web
```

> **Note:** The `@floruntime/web` package requires WebSocket server support on the Flo server. See the Flo documentation for WebSocket endpoint configuration.

## Quick Start

### Node.js

```typescript
import { FloClient } from "@floruntime/node";

const client = new FloClient("localhost:9000", {
  namespace: "myapp",
  timeoutMs: 5000,
});

await client.connect();

// KV operations
const encoder = new TextEncoder();
const decoder = new TextDecoder();

await client.kv.put("greeting", encoder.encode("Hello, Flo!"));
const value = await client.kv.get("greeting");
console.log(decoder.decode(value!)); // "Hello, Flo!"

// Queue operations
const seq = await client.queue.enqueue(
  "tasks",
  encoder.encode(JSON.stringify({ task: "process" }))
);

const result = await client.queue.dequeue("tasks", 10);
for (const msg of result.messages) {
  console.log(decoder.decode(msg.payload));
  await client.queue.ack("tasks", [msg.seq]);
}

await client.close();
```

### Browser

```typescript
import { FloClient } from "@floruntime/web";

const client = new FloClient("wss://flo.example.com/ws", {
  namespace: "myapp",
});

await client.connect();

// Same API as Node.js
await client.kv.put("key", new TextEncoder().encode("value"));
const value = await client.kv.get("key");

await client.close();
```

## API Reference

### Client Options

```typescript
interface ClientOptions {
  namespace?: string;           // Default namespace for operations (default: "default")
  timeoutMs?: number;           // Connection and operation timeout in ms (default: 5000)
  logger?: boolean | Logger;    // true = console logging, false = silent, or custom Logger
}
```

### KV Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `get(key, opts?) → Promise<Uint8Array \| null>` | Get value, null if not found |
| `put` | `put(key, value, opts?) → Promise<void>` | Set key-value |
| `delete` | `delete(key, opts?) → Promise<void>` | Delete key |
| `scan` | `scan(prefix, opts?) → Promise<ScanResult>` | Prefix scan with pagination |
| `history` | `history(key, opts?) → Promise<VersionEntry[]>` | Get version history |

#### Put Options

```typescript
interface PutOptions {
  namespace?: string;      // Override default namespace
  ttlSeconds?: bigint;     // Time-to-live in seconds
  casVersion?: bigint;     // CAS version for optimistic locking
  ifNotExists?: boolean;   // Only put if key doesn't exist
  ifExists?: boolean;      // Only put if key exists
}
```

#### Scan Options

```typescript
interface ScanOptions {
  namespace?: string;
  cursor?: Uint8Array;     // Pagination cursor
  limit?: number;          // Max entries to return
  keysOnly?: boolean;      // Return only keys, not values
}
```

### Queue Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `enqueue` | `enqueue(queue, payload, opts?) → Promise<bigint>` | Add message, returns seq |
| `dequeue` | `dequeue(queue, count, opts?) → Promise<DequeueResult>` | Fetch messages |
| `ack` | `ack(queue, seqs, opts?) → Promise<void>` | Acknowledge messages |
| `nack` | `nack(queue, seqs, opts?) → Promise<void>` | Negative ack (retry or DLQ) |
| `dlqList` | `dlqList(queue, opts?) → Promise<DequeueResult>` | List DLQ messages |
| `dlqRequeue` | `dlqRequeue(queue, seqs, opts?) → Promise<void>` | Move DLQ messages back |

#### Enqueue Options

```typescript
interface EnqueueOptions {
  namespace?: string;
  priority?: number;       // Higher = processed first (0-255)
  delayMs?: bigint;        // Delay before message becomes available
  dedupKey?: string;       // Deduplication key
}
```

#### Dequeue Options

```typescript
interface DequeueOptions {
  namespace?: string;
  visibilityTimeoutMs?: number;  // Lease duration in ms
  blockMs?: number;              // Wait for messages if queue empty
}
```

### Action Operations

Actions are registered tasks that can be invoked and executed by workers.

| Method | Signature | Description |
|--------|-----------|-------------|
| `register` | `register(name, actionType?, opts?) → Promise<void>` | Register an action |
| `invoke` | `invoke(name, input, opts?) → Promise<ActionInvokeResult>` | Invoke an action |
| `status` | `status(runId, opts?) → Promise<ActionRunStatus>` | Get run status |
| `list` | `list(opts?) → Promise<ActionListResult>` | List actions |
| `delete` | `delete(name, opts?) → Promise<void>` | Delete an action |

#### Action Example

```typescript
import { FloClient, ActionType } from "@floruntime/node";

const client = new FloClient("localhost:9000");
await client.connect();

// Register an action
await client.action.register("process-image", ActionType.User, {
  timeoutMs: 60000,
  maxRetries: 3,
});

// Invoke an action
const result = await client.action.invoke(
  "process-image",
  encoder.encode(JSON.stringify({ imageUrl: "https://..." })),
  { priority: 10, idempotencyKey: "order-123" }
);
console.log(`Run ID: ${result.runId}`);

// Check status
const status = await client.action.status(result.runId);
console.log(`Status: ${status.status}`);
```

### Worker Operations

Workers execute tasks for registered actions.

| Method | Signature | Description |
|--------|-----------|-------------|
| `register` | `register(workerId, taskTypes, opts?) → Promise<void>` | Register a worker |
| `awaitTask` | `awaitTask(workerId, taskTypes, opts?) → Promise<WorkerAwaitResult>` | Wait for task |
| `touch` | `touch(workerId, taskId, opts?) → Promise<void>` | Extend task lease |
| `complete` | `complete(workerId, taskId, result, opts?) → Promise<void>` | Complete task |
| `fail` | `fail(workerId, taskId, error, opts?) → Promise<void>` | Fail task |
| `list` | `list(opts?) → Promise<WorkerListResult>` | List workers |

#### Worker Example

```typescript
import { FloClient } from "@floruntime/node";

const client = new FloClient("localhost:9000");
await client.connect();

const workerId = "worker-1";
const taskTypes = ["process-image"];

// Register worker
await client.worker.register(workerId, taskTypes);

// Worker loop
while (true) {
  const result = await client.worker.awaitTask(workerId, taskTypes, {
    blockMs: 30000,
  });

  if (!result.task) continue;

  const task = result.task;
  try {
    // Process task
    const output = await processImage(task.payload);
    await client.worker.complete(workerId, task.taskId, output);
  } catch (err) {
    await client.worker.fail(workerId, task.taskId, String(err), {
      retry: true,
    });
  }
}
```

### Stream Operations

Streams are append-only logs for event sourcing and real-time data.

| Method | Signature | Description |
|--------|-----------|-------------|
| `append` | `append(stream, payload, opts?) → Promise<StreamAppendResult>` | Append to stream |
| `read` | `read(stream, opts?) → Promise<StreamReadResult>` | Read from stream |
| `groupJoin` | `groupJoin(stream, group, consumer, opts?) → Promise<void>` | Join consumer group |
| `groupRead` | `groupRead(stream, opts) → Promise<StreamReadResult>` | Read from group |
| `groupAck` | `groupAck(stream, seqs, opts) → Promise<void>` | Ack in group |

#### Stream Example

```typescript
// Append to stream
const result = await client.stream.append(
  "events",
  encoder.encode(JSON.stringify({ event: "click", userId: "123" })),
  { partitionKey: "user-123" }
);
console.log(`Offset: ${result.seq}`);

// Read from stream (from beginning)
const records = await client.stream.read("events", {
  offset: 0n,
  limit: 10,
});

for (const record of records.records) {
  console.log(decoder.decode(record.payload));
}

// Read from timestamp
const recentRecords = await client.stream.read("events", {
  from: Date.now() - 3600_000,  // last hour
  limit: 100,
});

// Consumer groups
await client.stream.groupJoin("events", "processors", "consumer-1");
const groupRecords = await client.stream.groupRead("events", {
  group: "processors",
  consumer: "consumer-1",
  limit: 10,
});
await client.stream.groupAck("events", groupRecords.records.map(r => r.seq), {
  group: "processors",
});
```

## Error Handling

The SDK provides typed errors for different failure modes:

```typescript
import {
  FloError,
  NotFoundError,
  ConflictError,
  BadRequestError,
  TimeoutError,
  isNotFound,
  isConflict,
} from "@floruntime/node";

try {
  await client.kv.put("key", value, { casVersion: 1n });
} catch (err) {
  if (isConflict(err)) {
    // CAS version mismatch - retry with new version
  } else if (isNotFound(err)) {
    // Key not found
  } else if (err instanceof TimeoutError) {
    // Operation timed out
  }
}
```

### Error Types

| Error | Description |
|-------|-------------|
| `FloError` | Base error class |
| `NotConnectedError` | Client not connected |
| `ConnectionError` | Connection failed |
| `TimeoutError` | Operation timed out |
| `NotFoundError` | Resource not found |
| `ConflictError` | CAS version mismatch |
| `BadRequestError` | Invalid request |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server overloaded |
| `InternalError` | Internal server error |

## Development

### Prerequisites

- Node.js >= 18

### Setup

```bash
# Install dependencies
npm install

# Build all packages
npm run build

# Run tests
npm run test
```

## License

MIT

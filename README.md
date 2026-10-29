# Flo JavaScript SDK

JavaScript/TypeScript SDK for the Flo distributed systems platform. This monorepo contains the core protocol package and the Node.js client.

## Packages

| Package | Description | Transport |
|---------|-------------|-----------|
| `@floruntime/core` | Core wire protocol, types, and transport-agnostic operations | - |
| `@floruntime/node` | Node.js client with TCP transport | TCP |

## Installation

```bash
npm install @floruntime/node
# or
yarn install @floruntime/node
```

The client and the server must be built from the same op table: a server refuses a client from another release, and the client refuses its answers (`TableMismatchError`). Upgrade the server and the SDK together.

## Quick Start

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

## API Reference

### Client Options

```typescript
interface ClientOptions {
  namespace?: string;           // Default namespace for operations (default: "default")
  timeoutMs?: number;           // Connection and operation timeout in ms (default: 5000)
  logger?: boolean | Logger;    // true = console logging, false = silent, or custom Logger
  retry?: Partial<RetryPolicy>; // Retries of refused, not-run requests (see Retries)
}
```

### KV Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `get(key, opts?) → Promise<GetResult \| null>` | Value and version; null if not found |
| `put` | `put(key, value, opts?) → Promise<PutResult>` | Set key-value; returns the new version |
| `delete` | `delete(key, opts?) → Promise<boolean>` | Delete key; whether it existed (a missing key is not an error) |
| `mget` | `mget(keys, opts?) → Promise<MGetEntry[]>` | Up to 256 keys of at most 4096 bytes; one entry per key, in order. `routingKey` looks every key up on that routing key's shard |
| `scan` | `scan(prefix, opts?) → Promise<ScanResult>` | One page of key names (`keys`) and the next page's `cursor` (null on the last) |
| `history` | `history(key, opts?) → Promise<HistoryResult>` | A page of `versions`, newest first: `version`, `atMs` (write time), `deleted`, `value`; none for a missing key. `limit` (1 to 65, default 10) and `beforeVersion` (exclusive) pick the page; `more` says older versions follow: pass the page's last version as `beforeVersion` |
| `incr` | `incr(key, opts?) → Promise<KVIncrResult>` | Add `delta` (default 1) to a counter; its new `value` and `version` |
| `expire` | `expire(key, ttlMs, opts?) → Promise<void>` | Set a key's TTL in ms; 0 clears it |
| `exists` | `exists(key, opts?) → Promise<KVExistsResult>` | Whether the key is present (`exists`), and its `version` |
| `jsonGet` | `jsonGet(key, path = "$", opts?) → Promise<JsonGetResult \| null>` | The JSON at `path`; null if the key or path is missing |
| `jsonSet` | `jsonSet(key, path, json, opts?) → Promise<PutResult>` | Set the JSON at `path` ("$" replaces the document) |
| `jsonDel` | `jsonDel(key, path = "$", opts?) → Promise<bigint \| null>` | Remove the JSON at `path`; the new version, or null when "$" deleted the key |
| `begin` | `begin(routingKey, opts?) → Promise<Transaction>` | Open a transaction on `routingKey`'s shard |

Every op takes `namespace` in its options; every op but `scan` takes
`routingKey`, which routes the request in place of the key.
`delete` and `expire` take `casVersion`: the key's version they require.

Every read result (`get`, `jsonGet`, each `mget` entry, `scan`, `history`,
`exists`) carries `stale`. It is set when a halted shard answered: the read
succeeded, served from the shard's state just before the entry it halted
at, and nothing written since. It is not an error.

#### Get Options

```typescript
interface GetOptions {
  namespace?: string;      // Override default namespace
  waitMs?: number;         // Wait up to this long (at most 300000; 0 = don't wait)
  waitFor?: WaitFor;       // WaitFor.Exists (default: answers at once if present) or WaitFor.Change (the next version)
  routingKey?: string;     // Route by this instead of the key
}
```

#### Put Options

```typescript
interface PutOptions {
  namespace?: string;      // Override default namespace
  ttlMs?: bigint;          // Time-to-live in milliseconds (0 = no expiry)
  condition?: Condition;   // Condition.Always (default), Condition.IfAbsent or Condition.IfPresent
  casVersion?: bigint;     // Only write if this is the key's version (0n = it doesn't exist)
  routingKey?: string;     // Route by this instead of the key
}
```

#### Scan Options

```typescript
interface ScanOptions {
  namespace?: string;
  cursor?: Uint8Array;     // The previous page's cursor; unset for the first page
  limit?: number;          // Most names per page, 1 to 1024 (default 1024)
}
```

`history` takes `limit` too: 1 to 65 versions (default 10).

#### Transactions

```typescript
const txn = await client.kv.begin("user:123");
try {
  await txn.put("user:123:name", encoder.encode("Jane"));
  await txn.incr("user:123:visits");
  await txn.expire("user:123:session", 60_000);
  await txn.commit();
} catch (err) {
  await txn.rollback();
  throw err;
}
```

Inside a transaction, `get`, `put`, `delete`, `incr`, `expire` and `exists`
are routed by the transaction's routing key; `delete` and `incr` answer
nothing until commit. `scan`, `mget`, `history` and the JSON ops throw
`TxnUnsupportedOpError`.

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
  priority?: number;       // 0-255, lower is taken first; unset is 0, so it goes before any explicit priority
}
```

#### Dequeue Options

```typescript
interface DequeueOptions {
  namespace?: string;
  blockMs?: number;  // Wait for messages if queue empty (ms; 0 = don't wait, max 300000)
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

// Invoke an action. `labels` (a JSON object) is optional: only workers whose
// registered labels contain every key/value receive the run.
const result = await client.action.invoke(
  "process-image",
  encoder.encode(JSON.stringify({ imageUrl: "https://..." })),
  { labels: JSON.stringify({ gpu: true }) }
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
| `list` | `list({ limit?, cursor?, action? }?) → Promise<WorkerListResult>` | A page of workers in id order, or only those that run `action`; pass `cursor` on for the next, null on the last |
| `info` | `info(workerId, opts?) → Promise<WorkerInfoResult>` | A worker's row (as `list` shows it, with `machineId`), its registered `metadata` (bytes, or null) and its `processes` (`name`, `kind`, `runCount`, `failCount`, `lastRunAtMs`); throws `NotFoundError` for a worker that isn't registered |

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

#### ActionWorker

```typescript
import { ActionWorker } from "@floruntime/node";

const worker = new ActionWorker({ endpoint: "localhost:9000", autoTouch: true });
worker.action("process-image", async (ctx) => ({ done: true }), { timeoutMs: 60000, maxRetries: 5 });
await worker.start();
```

The worker registers each action when it starts, with exactly the options given (the server's defaults when none: a 30 s timeout, 3 attempts). Registering replaces the action's settings, so workers that register one action with different options leave it with whichever registered last.

A run whose handler goes past the action's `timeoutMs` without touching it is taken back and run again, by this worker or another. `autoTouch: true` touches each run every third of its action's timeout while the handler runs; it's off by default. Without it, call `await ctx.touch()` from long handlers. The worker also stops any handler after its own `actionTimeoutMs` (300 s by default) whatever the action's timeout, so raise it for actions that run longer.

### Stream Operations

Streams are append-only logs for event sourcing and real-time data.

| Method | Signature | Description |
|--------|-----------|-------------|
| `append` | `append(stream, payload, opts?) → Promise<StreamAppendResult>` | Append to stream |
| `read` | `read(stream, opts?) → Promise<StreamReadResult>` | Read from stream |
| `groupJoin` | `groupJoin(stream, group, consumer, opts?) → Promise<void>` | Join consumer group |
| `groupRead` | `groupRead(stream, opts) → Promise<StreamReadResult>` | Read from group |
| `groupAck` | `groupAck(stream, ids, opts) → Promise<void>` | Ack in group |

#### Stream Example

```typescript
// Append to stream
const result = await client.stream.append(
  "events",
  encoder.encode(JSON.stringify({ event: "click", userId: "123" })),
  { partitionKey: "user-123" }
);
console.log(`ID: ${result.id.timestampMs}-${result.id.sequence}`);

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
await client.stream.groupAck("events", groupRecords.records.map(r => r.id), {
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
| `BlockTooLongError` | `blockMs` or `waitMs` over 300000, refused before sending |
| `NotFoundError` | Resource not found |
| `ConflictError` | CAS version mismatch |
| `BadRequestError` | Invalid request |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server overloaded, and the client's retries ran out |
| `UnavailableError` | No leader, or the shard isn't taking writes, and the client's retries ran out. With `reason === Reason.ShardHalted` the shard halted, and takes no writes until an operator repairs it; never retried |
| `OutcomePendingError` | The write committed but hasn't applied: its shard halted at it or before it (`reason` ShardHalted, `ran` Unknown); the message says whether it applies when the shard resumes. Never retried |
| `InternalError` | Internal server error |

### Retries

When the server refuses a request as `overloaded` or `unavailable` and says it
didn't run it (`err.ran === Ran.No`), the client sends it again itself, as a
new request, after a jittered wait. Because the server never ran it, this is
safe for writes too. Any other refusal, or one that may have run, comes
straight back. Two never come back to be retried: `unavailable` with reason
`shard_halted` (an operator has to repair the shard, so resending is no
use), and `outcome_pending` (the write is committed; resending it would
write it twice, or halt the shard again).

| `retry` field | Default | |
|---|---|---|
| `maxAttempts` | 4 | Tries in all, the first included; 1 turns retrying off |
| `initialBackoffMs` | 50 | Ceiling of the first wait; it doubles each retry |
| `maxBackoffMs` | 1000 | Largest ceiling |

Unset fields take the defaults; a negative wait counts as 0, and a
`maxBackoffMs` below `initialBackoffMs` is raised to it. Each wait is uniform
between 0 and its ceiling, so with the defaults a refused request is given
back after at most about 350 ms of waiting. The request goes again on the same
connection; the server routes it afresh.

`ActionWorker` and `StreamWorker` wait the same way between failed polls, from
100 ms up to 5 s, starting again from 100 ms after a poll that succeeds; `stop()`
ends a wait at once. A stream worker retries a group join refused this way
instead of stopping. `ActionWorker`, and a `StreamWorker` given an endpoint, take a `retry`
option, with the same fields, for the client they create; a `StreamWorker`
given a client uses that client's. A worker reconnects its client when the connection drops, but once the client's owner calls `close()` the worker stops, and stays stopped even if the client is connected again.

A worker sends its handler's result (complete or fail, ack or nack) again, the
same request, after reconnecting if the connection drops, the send times out or
the answer is garbled, up to 3 sends in all; a refusal is not resent. Once it
is stopping or draining, each result gets up to 2 s of reconnects, then is
dropped. Delivery is at-least-once: a handler may see the same task or record
again, so make it safe to repeat.

`drain()` stops issuing polls and lets the running handlers finish,
heartbeats going on. The server is told (again after the worker registers
anew, and on a retry if the notice is lost), so it assigns an action worker
nothing more, and `start()` returns once the last handler has reported. A
drain started on the server does the same. An `ActionWorker`'s outstanding
poll is answered at once when the server takes the drain, refused
`worker_draining`, so it learns of a server-side drain at once. A
`StreamWorker`'s is not: the server doesn't wake a group read, so a drain
started on the server reaches it at its next heartbeat (`heartbeatIntervalMs`,
30 s by default); and the worker has one connection, which it keeps, so its
outstanding read can wait up to `blockMs`. `stop()` ends polling, also
mid-reconnect or mid-drain, once an outstanding poll returns (within
`blockMs`);
running handlers still finish, and a `StreamWorker` hands back the records of
its last batch it hasn't started. Either one before or during `start()` ends
that run, and both workers can be started again afterwards.

```typescript
// Up to 8 tries, waiting at most 2 s between them
const client = new FloClient("localhost:9000", {
  retry: { maxAttempts: 8, initialBackoffMs: 100, maxBackoffMs: 2000 },
});

// No retries
const noRetries = new FloClient("localhost:9000", { retry: { maxAttempts: 1 } });
```

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

Apache-2.0 - see [LICENSE](LICENSE) for details.

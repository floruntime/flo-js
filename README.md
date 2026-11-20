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

// A dequeue leases its messages; complete them within the lease.
const taken = await client.queue.dequeue("tasks", 10, { consumer: "worker-1" });
for (const msg of taken.messages) console.log(decoder.decode(msg.payload));
if (taken.messages.length > 0) await client.queue.complete("tasks", taken);

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
| `mget` | `mget(keys, opts?) → Promise<MGetEntry[]>` | Up to 256 keys of at most 4096 bytes, from any shards; one entry per key, in order |
| `scan` | `scan(prefix, opts?) → Promise<ScanResult>` | One page of key names (`keys`) and the next page's `cursor` (null on the last) |
| `history` | `history(key, opts?) → Promise<HistoryResult>` | A page of `versions`, newest first: `version`, `atMs` (write time), `deleted`, `value`; none for a missing key. A delete drops the versions before it, so history ends at its tombstone, and the server removes old tombstones in time. `limit` (1 to 65, default 10) and `beforeVersion` (exclusive) pick the page; `more` says older versions follow: pass the page's last version as `beforeVersion` |
| `incr` | `incr(key, opts?) → Promise<KVIncrResult>` | Add `delta` (default 1) to a counter; its new `value` and `version` |
| `expire` | `expire(key, ttlMs, opts?) → Promise<void>` | Set a key's TTL in ms; 0 clears it |
| `exists` | `exists(key, opts?) → Promise<KVExistsResult>` | Whether the key is present (`exists`), and its `version`. **An object, so always truthy: read `.exists`; `if (await kv.exists(k))` is always true** |
| `jsonGet` | `jsonGet(key, path = "$", opts?) → Promise<JsonGetResult \| null>` | The JSON at `path`; null if the key or path is missing |
| `jsonSet` | `jsonSet(key, path, json, opts?) → Promise<PutResult>` | Set the JSON at `path` ("$" replaces the document) |
| `jsonDel` | `jsonDel(key, path = "$", opts?) → Promise<bigint \| null>` | Remove the JSON at `path`; the new version, or null when "$" deleted the key |
| `begin` | `begin(key, opts?) → Promise<Transaction>` | Open a transaction pinned to `key`'s `{tag}` |

Every op takes `namespace` in its options. A key routes by its `{tag}`, the
part between its first `{` and the `}` after it, or by the whole key when it
has none: keys that belong together, in a transaction say, share a `{tag}`
(`{user:1}:balance`, `{user:1}:visits`). Only the first `{` counts. An empty
tag, `{}`, is still a tag: every key with one co-locates. A `{` with no `}`
after it is no tag, so the key routes whole.
`delete` and `expire` take `casVersion`: the key's version they require.
The server decides a `casVersion` or `condition` as the write applies, in
log order, so a refused one still costs a replicated commit. Inside a
transaction, a condition, `casVersion` or `incr` conflict is refused at
`commit`, not at the op.

Every KV read result (`get`, `jsonGet`, each `mget` entry, `scan`,
`history`, `exists`) carries `stale`. It is set when the read was served
from state that may be behind what was committed: a halted shard's, just
before the entry it halted at, or a node whose reads are blocked, serving
what it last applied. The read succeeded; it is not an error. A key such a
read didn't find is not reported as absent: `get` and `jsonGet` throw
`StaleNotFoundError` (a `NotFoundError` with `stale` true), since the key
was absent only as of that state. Only KV reads carry `stale` for now:
other reads (streams, queues) and the read-only client's `get` drop it.

`history` returns one page at a time. To read every kept version:

```typescript
let beforeVersion: bigint | undefined;
for (;;) {
  const page = await client.kv.history("k", { limit: 20, beforeVersion });
  for (const v of page.versions) console.log(v.version, v.deleted, v.value);
  const last = page.versions.at(-1);
  if (!page.more || last === undefined) break;
  beforeVersion = last.version;
}
```

#### Get Options

```typescript
interface GetOptions {
  namespace?: string;      // Override default namespace
  waitMs?: number;         // Wait up to this long (at most 300000; 0 = don't wait)
  waitFor?: WaitFor;       // WaitFor.Exists (default: answers at once if present) or WaitFor.Change (the next version)
}
```

#### Put Options

```typescript
interface PutOptions {
  namespace?: string;      // Override default namespace
  ttlMs?: bigint;          // Time-to-live in milliseconds (0 = no expiry)
  condition?: Condition;   // Condition.Always (default), Condition.IfAbsent or Condition.IfPresent
  casVersion?: bigint;     // Only write if this is the key's version (0n = it doesn't exist)
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
const txn = await client.kv.begin("{user:123}");
try {
  await txn.put("{user:123}:name", encoder.encode("Jane"));
  await txn.incr("{user:123}:visits");
  await txn.expire("{user:123}:session", 60_000);
  await txn.commit();
} catch (err) {
  await txn.rollback();
  throw err;
}
```

A transaction is pinned to the `{tag}` of the key `begin` was given, or to
that whole key when it has none. Inside it, `get`, `put`, `delete`, `incr`,
`expire` and `exists` take keys that route with it; one that routes
elsewhere is refused `BadRequestError` with reason `KeysNotColocated`, not
run (the transaction stays open). `delete` and `incr` answer nothing until
commit. `scan`, `mget`, `history` and the JSON ops throw
`TxnUnsupportedOpError`.
A transaction holds at most 64 ops and 256 KiB by default; both are the
server operator's settings. Past them an op is refused `TxnTooLarge`.

### Queue Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `enqueue` | `enqueue(queue, payload, opts?) → Promise<bigint>` | Add message, returns seq |
| `dequeue` | `dequeue(queue, count?, { consumer?, leaseMs?, blockMs?, ack? }?) → Promise<DequeueResult>` | Take up to `count` (1 to 100, default 1) ready messages, leased to `consumer` |
| `complete` | `complete(queue, delivery, opts?) → Promise<QueueReportResult>` | Complete dequeued messages: they leave the queue |
| `fail` | `fail(queue, delivery, { delayMs?, dead?, error? }?) → Promise<QueueReportResult>` | Fail them: ready again after `delayMs`, or dead once their attempts are spent (at once with `dead`) |
| `touch` | `touch(queue, delivery, opts?) → Promise<QueueTouchResult>` | Keep them: extends the lease once half of it or less is left |
| `purge` | `purge(queue, opts?) → Promise<number>` | Remove every message, ready, leased and dead; answers how many |
| `peek` | `peek(queue, limit?, opts?) → Promise<PeekResult>` | The next ready messages (1 to 100, default 20), in dequeue order, without taking them; up to 4 KiB of each payload |
| `dlqList` | `dlqList(queue, { limit?, cursor? }?) → Promise<DLQListResult>` | A page of dead letters, oldest dead first (1 to 100, default 20), each with its failures, when it died, its last error and up to 4 KiB of its payload; pass the page's `cursor` on, null on the last |
| `dlqRequeue` | `dlqRequeue(queue, seqs, { all?, limit? }?) → Promise<QueueDLQResult>` | Make dead letters ready again, failures reset: those named, or with `all` the oldest, up to `limit` (default 100) a call |
| `dlqDelete` | `dlqDelete(queue, seqs, { all?, limit?, dryRun? }?) → Promise<QueueDLQResult>` | Delete dead letters for good; with `dryRun`, count what it would delete |

#### Enqueue Options

```typescript
interface EnqueueOptions {
  namespace?: string;
  priority?: number;       // 0-255, lower is taken first; unset is 0, so it goes before any explicit priority
  delayMs?: number;        // Ready after this long, at most 86400000 (a day)
}
```

A queue past its cap (messages or held bytes, counting ready, leased and
dead), or a shard whose queues and groups together hold as many bytes as
they may, refuses an enqueue as overloaded with reason `QueueFull`, not run.
It drains as messages are acked, so the client retries it as it does any
such refusal (see Retries), then throws `OverloadedError` (`isQueueFull`):
back off and send again, or consume or purge the queue.

#### Dequeue Options

```typescript
interface DequeueOptions {
  namespace?: string;
  blockMs?: number;  // Wait for messages if queue empty (ms; 0 = don't wait, max 300000)
  consumer?: string; // Who takes them; complete, fail and touch must name the same ("" when unset)
  leaseMs?: number;  // How long they're held without a touch: 5000 to 43200000 (the server's 30 s when unset)
  ack?: boolean;     // Complete them as they're taken: a one-shot consume
}
```

Queues are at-least-once. A dequeue leases its messages to `consumer`; it
doesn't remove them. Complete each within its lease (or touch the lease to
keep it), or it is delivered again with `deliveries` one higher. Pass the
dequeue's result to `complete`, `fail` or `touch`, or it with `messages`
narrowed: a report names the consumer, the delivery's term and index, and
each message's seq and deliveries. Messages a report didn't apply to come
back in `refused`, inside an ok answer, each with a `QueueRefusal`; for a
complete, `Gone` means the message was completed already (a resent
complete), so it is done. With `ack: true` a dequeue completes its messages
as it takes them; if the leader fails in between, they come back once their
lease runs out.

#### Dead letters

A message failed `dead`, or once its attempts are spent, is a dead letter:
it stays in the queue, not delivered, until it is requeued, deleted or
purged. `dlqList` pages through them oldest dead first; the cursor is
opaque. `dlqRequeue` and `dlqDelete` take dead letters by seq (1 to 100),
or with `all: true` (and no seqs) the oldest, a batch of up to `limit` per
call: `more` says whether others remain, so call again while it is set.
Each answers how many it `applied` to (with `dryRun`, would) and the ones it
didn't in `refused`: `NotDead` for a message that isn't dead, `Gone` for
one not in the queue. A requeued message is ready at once at its priority,
with its failures at 0; its deliveries and last error stay.

### Action Operations

Actions are registered tasks that can be invoked and executed by workers.

| Method | Signature | Description |
|--------|-----------|-------------|
| `register` | `register(name, actionType?, opts?) → Promise<void>` | Register an action |
| `invoke` | `invoke(name, input, opts?) → Promise<ActionInvokeResult>` | Invoke an action |
| `status` | `status(runId, opts?) → Promise<ActionRunStatus>` | A run's status; `held` says why a run past its lease hasn't ended yet (its end waits for room on the calling workflow's shard) |
| `list` | `list(opts?) → Promise<ActionListResult>` | A page of actions, each with its version, owner, lease, attempts and creation time |
| `info` | `info(name, opts?) → Promise<ActionInfo>` | An action's definition, its runs by status and completed runs' latencies; `NotFoundError` if it isn't registered |
| `delete` | `delete(name, opts?) → Promise<void>` | Delete an action |

An action's `timeoutMs` is its lease: how long a worker holds a run without
completing, failing or touching it. It is 5000 to 43200000 ms; unset or 0 sends 0,
and the server's default of 300000 applies. Touches extend a lease, but a
worker holds one delivery at most 12 h from when it was delivered.

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
| `awaitTask` | `awaitTask(workerId, actions, { blockMs?, requestKey? }?) → Promise<WorkerAwaitResult>` | Wait for a task and take it |
| `touch` | `touch(workerId, task) → Promise<bigint>` | Keep a task; answers when its lease runs out |
| `complete` | `complete(workerId, task, result, { outcome? }?) → Promise<void>` | Complete a task |
| `fail` | `fail(workerId, task, error, { retry?, abandoned?, delayMs? }?) → Promise<void>` | Fail a task, or hand it back untried |
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
    const output = await processImage(task.input);
    await client.worker.complete(workerId, task, output);
  } catch (err) {
    await client.worker.fail(workerId, task, String(err), {
      retry: true,
    });
  }
}
```

A task is one delivery of a run: its `runId`, `deliveries`, `term` and
`index`, with its `action` and `namespace`. Reports pass the task back, and
the server refuses one for a delivery the run has moved on from (its lease
ran out, another worker holds it, it finished) with `StaleReportError`. A
report sent again after it applied answers ok. `taskId` is the id the CLI
prints, `run:deliveries:term:index:action`; `parseTaskId` reads one.

An await takes the task it answers with. If that answer is lost, sending
the await again with the same `requestKey` gets the same task back.
`touch` extends the lease only once half of it or less is left; touch at
least every half lease.

#### ActionWorker

```typescript
import { ActionWorker } from "@floruntime/node";

const worker = new ActionWorker({ endpoint: "localhost:9000", autoTouch: true });
worker.action("process-image", async (ctx) => ({ done: true }), { timeoutMs: 60000, maxRetries: 5 });
await worker.start();
```

The worker registers each action when it starts, with exactly the options given (when unset, the server's 300 s lease and 3 attempts). Registering replaces the action's settings, so workers that register one action with different options leave it with whichever registered last.

A run whose handler goes past its lease (the action's `timeoutMs`) without touching it is taken back and run again, by this worker or another. `autoTouch: true` touches each run every sixth of its lease, counted from when the task arrived, while the handler runs; it's off by default. Without it, call `await ctx.touch()` from long handlers. However often it is touched, a delivery is held at most 12 h from when it was delivered; past that it is taken back. The worker also stops any handler after its own `actionTimeoutMs` (300 s by default) whatever the action's timeout, so raise it for actions that run longer.

A handler that throws fails the run with retry: it runs again while attempts are left. A handler past `actionTimeoutMs` fails the same way, with retry. Only `NonRetryableError` fails the run for good, at once. At the time limit the worker aborts `ctx.signal`; it can't stop the handler, so one that ignores the signal runs on while its run is retried (perhaps elsewhere), and holds its concurrency slot until it settles. Pass `ctx.signal` to the work it starts, or check `ctx.cancelled`.

`stop()` ends polling once the outstanding await returns (within `blockMs`). Handlers already running finish, and their completes or fails are sent, before `start()` returns (their signals are not aborted; `start()` doesn't wait for a handler past its time limit, whose run was already failed); then the worker deregisters and closes its connection. A result whose connection is lost after the stop is retried for up to 2 s, then dropped, and the server hands the run out again when its lease runs out.

`close()` stops the worker (both `ActionWorker` and `StreamWorker`). close() waits up to 30 s (closeTimeoutMs) for the running loop to send its completes, acks and hand-backs, then closes the client; anything unsent is redelivered when its lease expires.

### Stream Operations

Streams are append-only logs for event sourcing and real-time data.

| Method | Signature | Description |
|--------|-----------|-------------|
| `append` | `append(stream, payload, opts?) → Promise<StreamAppendResult>` | Append to stream |
| `read` | `read(stream, opts?) → Promise<StreamReadResult>` | Read from stream |
| `groupCreate` | `groupCreate(stream, group, { ackTimeoutMs?, maxDeliver? }?) → Promise<boolean>` | Create a group with its lease and attempts; whether it made it |
| `groupRead` | `groupRead(stream, { group, consumer, limit?, blockMs? }) → Promise<StreamGroupReadResult>` | Lease up to `limit` (1 to 100) of the group's records to `consumer` |
| `groupAck` | `groupAck(stream, delivery, { group }) → Promise<StreamGroupReportResult>` | Done with leased records |
| `groupNack` | `groupNack(stream, delivery, { group, delayMs?, dead?, error?, abandon? }) → Promise<StreamGroupReportResult>` | Hand leased records back: ready again, or dead; `abandon` hands back records never started |
| `groupTouch` | `groupTouch(stream, delivery, { group }) → Promise<StreamGroupTouchResult>` | Keep a lease |
| `groupList` | `groupList(stream, opts?) → Promise<StreamGroupListResult>` | The stream's consumer groups, with members, pending records, settings and last delivery |
| `groupMembers` | `groupMembers(stream, group, opts?) → Promise<StreamGroupMembersResult>` | A group's members (the consumers holding its records now), each with how many it holds |
| `groupSteal` | `groupSteal(stream, records, { group, consumer, minIdleMs, leaseMs? }) → Promise<StreamGroupStealResult>` | Take records another consumer has left idle at least `minIdleMs`: a new delivery to `consumer` |
| `groupDlqList` | `groupDlqList(stream, group, { limit?, cursor? }?) → Promise<GroupDLQListResult>` | A page of the group's dead letters, oldest dead first (1 to 100, default 20) |
| `groupDlqRequeue` | `groupDlqRequeue(stream, group, ids, { all?, limit? }?) → Promise<GroupDLQResult>` | Make dead letters ready again, failures reset |
| `groupDlqDelete` | `groupDlqDelete(stream, group, ids, { all?, limit?, dryRun? }?) → Promise<GroupDLQResult>` | Delete dead letters for good |
| `create` | `create(stream, { partitions?, retentionAgeS?, retentionCount? }?) → Promise<void>` | Create a stream with settings |
| `alter` | `alter(stream, { retentionAgeS?, retentionCount? }) → Promise<void>` | Change a stream's retention |
| `info` | `info(stream, opts?) → Promise<StreamInfoResult>` | First and last ids, count, bytes, partitions and retention |

A consumer group hands each of its stream's records to one consumer at a
time, leased: a read takes records whose lease ran out or that were nacked
first, once they are ready again, then the stream's next ones, each with
`deliveries` (1 the first time). It doesn't consume them: ack each within the
group's lease (`ackTimeoutMs`, 5 s to 12 h, 30 s by default), or nack it, or
it is delivered again once the lease runs out. Touch to keep a lease: with
half of it or less left it runs a full lease from now, up to 12 h from the
read. A record nacked or expired `maxDeliver` times (1 to 255, 5 by default)
is dead and isn't delivered again. Delivery is at least once.

Ack, nack and touch name the read they report on: pass its result, or it
with `records` narrowed. A report on a delivery that is over (the lease ran
out, or the record was delivered again since) applies nothing and isn't an
error: `applied` counts what it did, and `refused` names each record it
didn't, with a `QueueRefusal` code (`Gone`, `DeliveredAgain`, `OtherHolder`,
`Expired`, `Released`, `NotHeld`) and a reason. A resent ack whose first
applied answers `Gone`. A report to a group that isn't there (deleted, say)
throws `NotFoundError`.

A nack counts a failure; its `error` (at most 1024 bytes, longer throws
before a round trip) is kept with a dead record. `abandon: true` hands
records back unstarted instead: ready at once, the delivery counted but no
failure, and it takes no `delayMs`, `dead` or `error`. An abandoned record
is never dead-lettered, so a consumer that abandons on errors hands a
poison record out forever: abandon only records you never started (you are
stopping), and nack the ones that failed.

A group read that takes nothing new because the group is at its caps is
refused overloaded with reason `GroupHeldFull` (`isGroupHeldFull`). The caps
count every record the group holds, leased, ready and dead; the refusal also
comes when the shard's queues and groups together hold as many bytes as
they may, and the message says which. Leased records clear as consumers ack
or nack; dead ones stay until the stream is trimmed past them or the group
is deleted; the shard's cap clears as any of its groups and queues drain,
this group's own acks included.
Reading again at once won't help, so the client doesn't retry it.

A read creates the group it names, at the defaults; `groupCreate` makes one
with other settings. Creating a group that exists with the same settings
changes nothing; with others it throws `ConflictError`, which names the
group's own: a create can't change them.

A group's dead letters work as a queue's (see Queue Operations, Dead
letters), named by record id: `groupDlqList` pages them with each one's
deliveries, failures, when it died and its last error; `groupDlqRequeue`
and `groupDlqDelete` take ids or `all` with a `limit`, and refuse a record
that isn't dead `NotDead`. A requeue refuses a record trimmed from the
stream since as `Gone`.

`groupSteal` is an operator's command: a read takes expired records by
itself. It takes records another consumer holds, named with the deliveries
their holder has (a read's result, narrowed), if each has been idle at
least `minIdleMs` (1 ms to 12 h): idle is the time since its holder last
read, stole or touched it, so a holder that touches more often is never
stolen from, and such a record comes back in `refused` as `NotIdle`. Each
one taken gets a new delivery leased to `consumer` (for `leaseMs`, or the
group's lease), its failures kept; ack, nack and touch the result as a
read's. The old holder's reports on it then apply nothing. A steal naming
no records is answered at once, after the group and consumer checks: nothing
stolen and a lease of zeros.

A `StreamWorker` reads its group and runs a handler per record: it acks the
record when the handler returns and nacks it when the handler throws. From
a read's arrival until each of its records is acked or nacked, running or
still waiting for a free handler slot, the worker keeps their leases: one
touch every sixth of the lease (at least 100 ms apart) names the read and
every record still open. It reads the lease off the server's expiry against
this host's clock, the read's and then each touch answer's, so a skewed
clock is corrected as touches come back. A touch refused conflict, not found or bad request
stops touching; a record a touch didn't apply to (a stale report) is
dropped from it; any other failure is retried with a backoff from 100 ms,
never later than the next tick. After `stop()` (or `close()`) the worker
starts no more records: it abandons the unstarted ones, so they are ready
again at once with no failure counted. A stop also aborts each running
handler's `ctx.signal`; a handler that throws once it has aborted is cut
short by the stop, so its record is abandoned too, while one that throws
while the worker runs is nacked. `close()` waits for all of it (running
handlers acked, the rest handed back) before closing a client the worker
created; don't call it from a handler. A read refused `GroupHeldFull` is
tried again from 1 s doubling to 30 s, logged at warn once, as for `limit`;
a report refused not found (the group was deleted) is logged at error. Given `ackTimeoutMs` or `maxDeliver` it creates
its group with them at start; otherwise its first read creates the group at
the defaults.

`stream.read` without `blockMs` of a stream that isn't there (never
created, or deleted) throws `NotFoundError`; a stream that exists with
nothing in the window answers no records. With `blockMs`, a read of a
stream not created yet waits for its first append. A `StreamWorker`'s
`start()` rejects with a not-found its group read is refused, rather than
retrying. A consumer that may start before its stream exists catches
`NotFoundError` and starts again. A `StaleNotFoundError` (still a
`NotFoundError`) from `stream.read` or `ts.read` comes from a node that may
be behind: the stream or series may exist but this node hasn't seen it yet,
so read again rather than treating it as gone.

#### Creating and altering streams

An append makes a stream it names, with one partition and no retention.
`create` makes one with settings: `partitions`, `retentionAgeS` (trim
records older than this) and `retentionCount` (keep at most this many).
`alter` changes retention: each setting given replaces the stream's, one
left out (or 0) is kept. `clearRetentionAge` and `clearRetentionCount`
drop a bound, so the stream keeps records of any age, or any number of
them; setting and clearing the same bound in one alter throws before it
is sent. Only an alter clears: `create` has no clear. A create or alter
whose count bound is u64 max is refused `BadRequestError` ("retention
bound too large"), as is an age over 100 years. It works on a
stream an append made as on one a create made; a stream that doesn't
exist throws `NotFoundError`. `info` shows the settings.

```typescript
await client.stream.create("events", { retentionAgeS: 3600n, retentionCount: 100000n });
await client.stream.alter("events", { clearRetentionAge: true }); // keeps the count bound
```

A create of a stream that exists:

- made by an append and never altered: the create gives it the settings
  asked, and succeeds;
- made by a create, or altered since: it succeeds if it asks for the
  settings the stream has (as a retry does), and is otherwise refused
  `ConflictError`, "the stream exists with other settings; change them
  with stream alter". After an alter, a create must name the altered
  settings, all of them.

A create whose name's 64-bit hash is another stream's in the namespace is
refused `ConflictError` too ("another stream's name hashes as this one's;
choose another name"): pick another name.

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
const records = await client.stream.read("events", { limit: 10 });

for (const record of records.records) {
  console.log(decoder.decode(record.payload));
}

// The next page: `start` is exclusive, so pass the last id seen
const last = records.records.at(-1);
const more = await client.stream.read("events", { start: last?.id, limit: 10 });

// Consumer groups
await client.stream.groupCreate("events", "processors", { ackTimeoutMs: 60_000 });
const read = await client.stream.groupRead("events", {
  group: "processors",
  consumer: "consumer-1",
  limit: 10,
});
if (read.records.length > 0) {
  // ...handle them, then:
  const { applied, refused } = await client.stream.groupAck("events", read, { group: "processors" });
}
```

### Time Series

Every time and window is a bigint of nanoseconds, on the wire and in the API; none is in ms. `nsFromDate` and `dateFromNs` convert to and from a `Date`, which keeps only whole milliseconds.

```typescript
import { TSOrder, nsFromDate } from "@floruntime/core";

// A point with its own time; with no timeNs (or 0) it takes the server's.
const { timeNs } = await client.ts.write("cpu", 0.42, {
  tags: "host=web-01,region=eu",
  timeNs: nsFromDate(new Date()),
});

// One page, newest first: points, then the cursor for the next page.
const page = await client.ts.read("cpu", { tags: "host=web-01", order: TSOrder.NewestFirst, limit: 100 });
const next = page.cursor && (await client.ts.read("cpu", { tags: "host=web-01", order: TSOrder.NewestFirst, limit: 100, cursor: page.cursor }));

// Every point in a range, oldest first, a page at a time.
for await (const p of client.ts.readAll("cpu", { fromNs: 0n, toNs: timeNs })) {
  console.log(p.timeNs, p.value);
}
```

A read merges every series whose tags include each `key=value` of `tags`, in time order (then write order), or newest first. A page goes on strictly after the cursor it is given (before it, newest first), so paging neither repeats nor skips a point that was there for the whole read. Page until the cursor is null: a short page needn't be the last. The cursor is opaque. A read throws `NotFoundError` whenever no series of the field matches the tag filter, not only when the measurement was never written: a tag filter that matches nothing is not found, not empty. `readAll` throws it from the iteration; a range with no points on a series that exists is an empty page. A consumer that may start before its series exists catches `NotFoundError` and starts again. A page's `stale` is set when a node that may be behind served it. `ts.query(measurement, { fromNs?, toNs?, windowNs?, aggregation? })` aggregates points (`avg`, `sum`, `min`, `max` or `count`) in windows aligned to the epoch, each bucket's `startNs` in Unix ns. The window is a bigint of ns, a minute when unset; 0 is refused, and so is one the matching points span more than 1024 windows of: widen the window or narrow the range. `ts.info(measurement)` gives a measurement's series, fields, points, and `oldestNs` and `newestNs`. `ts.floql(query)` runs a FloQL query and answers its points' times in Unix ns. In the query text a bare number in a range is epoch ms (`cpu[1700000000000..1700000060000]`), and a duration (`[1h]`, `[2h..1h]`) is relative to now. An answer of more than 4096 raw points is refused: aggregate them, narrow the query, or page through them with `ts.read`.

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
| `StaleNotFoundError` | A `NotFoundError` from `get` or `jsonGet` served stale (a halted shard, or a node whose reads are blocked): the key was absent as of that state; `stale` is true |
| `ConflictError` | CAS version mismatch, or a condition that didn't hold. Reason `SkippedByRepair`: a repair skipped the log entry or message that carried the request, so it never runs on any node |
| `StaleReportError` | A `ConflictError` (reason `StaleReport`) for an action complete, fail or touch whose delivery the run has moved on from; the message says how |
| `BadRequestError` | Invalid request |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server overloaded, and the client's retries ran out. `reason` `QueueFull`: the queue is at its caps (retried); `GroupHeldFull`: the group (dead records included) or the shard's queues and groups are at their caps (not retried) |
| `UnavailableError` | No leader, or the shard isn't taking writes, and the client's retries ran out. With `reason === Reason.ShardHalted` the shard halted, and takes no writes until an operator repairs it; never retried |
| `LimitError` | The server holds as much of something as it is built to keep: `reason` says which (today `TooManyActions`, past 10,000 actions on the action's shard), and the message names it. The limits are fixed in the server's build, not settings: retrying won't clear it until something is deleted (it isn't permanent). Never run, never retried |
| `OutcomePendingError` | The write committed but hasn't applied: its shard halted at it or before it (`reason` ShardHalted, `ran` Unknown); the message says whether it applies when the shard resumes. Never retried |
| `InternalError` | Internal server error |

### Retries

A refusal whose reason or status this SDK doesn't know still decodes: a
`ServerError` with the number in `reason` (or `status`) and the server's
message; `reasonName` names it `unknown_reason_<n>`.

When the server refuses a request as `overloaded` or `unavailable` and says it
didn't run it (`err.ran === Ran.No`), the client sends it again itself, as a
new request, after a jittered wait. Because the server never ran it, this is
safe for writes too. Any other refusal, or one that may have run, comes
straight back. Three are never retried: `unavailable` with reason
`shard_halted` (an operator has to repair the shard, so resending is no
use), `overloaded` with reason `group_held_full` (the group, or the shard, stays
at its caps until records are acked, nacked, trimmed or deleted), and `outcome_pending` (the write is committed; resending it would
write it twice, or halt the shard again).

`unavailable` (not run) is routine: a stream read whose records are being
read back from disk, or a leader not yet ready, is refused that way and
answers once retried. The client's retries (about 350 ms by default) may not
outlast a disk read; a `StreamWorker`'s group read refused so after them is
read again, the same group and consumer, from 50 ms doubling to 5 s. The
first refusal of a run is logged at warn with the server's message, which
says if the shard is damaged or offline, then at most every 30 s while the
refusals go on. Such a read leased nothing, so the same read is safe to send
again. Your own `stream.read` or `groupRead` throws
`UnavailableError` once the client's retries run out: read again.

Workers are different: registering, a `StreamWorker`'s group create and the
poll loops are safe to repeat, so on `unavailable` with reason
`shard_halted` (not run) a worker keeps trying,
waiting from 100 ms doubling to 30 s and logging each refusal at warn with
the server's message, and carries on by itself once the shard is repaired.
A worker started during a halt comes up once it is. `stop()` or `drain()`
ends a wait at once. An `outcome_pending` refusal of any of those calls
(an action registration, say, committed at the entry the shard halted at)
is not resent: `start()` throws `OutcomePendingError` with the server's
message. A handler's result (complete, fail, ack, nack) refused by a halted
shard is logged and dropped, not resent.

A handler's result refused `overloaded` and not run, once the client's own
retries are spent, is sent again by the worker, the same request, from
100 ms doubling to 5 s, each refusal logged at warn, until the server takes
it or answers otherwise. Any result can be refused so while too many writes
wait for commit, or on a follower whose link to the leader is full. A group
ack or nack's lease keeps running meanwhile, so a long overload can deliver
the record again: at-least-once. A complete or fail that ends a run a
workflow step called is also refused while the queue of messages to the
workflow's shard is full; that run is held meanwhile (`action.status` shows
why in `held`), not handed out again, so the result must get through. After `stop()` or
`drain()` it is tried for up to 2 s more, then dropped with an error.

`limit` is never retried by the client either. For the workers, start()
fails with the limit error; a limit hit while running is logged and backed
off. A registration or a `StreamWorker`'s group create refused at a limit
makes `start()` reject with `LimitError` after that one request, and the
caller decides. A poll refused at a limit is tried again from 1 s doubling
to 30 s, each distinct reason logged at warn once until a poll goes
through, so a running worker neither spins nor stops. A registration (or
group create) refused at a limit after a mid-run reconnect is tried again
the same way, logged at error: until it goes through the worker is not
registered and serves nothing.

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
ends a wait at once. At `start()`, a registration (or a `StreamWorker`'s
group create) refused this way, or by a halted shard, is tried again the
same way instead of stopping; any other refusal makes `start()` reject with
its typed error after that one request. `ActionWorker`, and a `StreamWorker` given an endpoint, take a `retry`
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
its last batch it hasn't started. The hand-back abandons them: ready again
at once, with no failure counted, so a stop never dead-letters them. Either one before or during `start()` ends
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

/**
 * Retrying requests the server refused without running them, and the
 * workers' backoff after a failed poll.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import {
  Backoff,
  ConflictError,
  HEADER_SIZE,
  IncompleteResponseError,
  MAGIC,
  OpCode,
  OutcomePendingError,
  OverloadedError,
  Ran,
  Reason,
  type RetryPolicy,
  ServerError,
  StatusCode,
  TABLE_HASH,
  UnavailableError,
  VERSION,
  computeCRC32C,
  isOutcomePending,
  normalizeRetryPolicy,
} from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";

const enc = new TextEncoder();

function frame(id: bigint, status: number, data: Uint8Array = new Uint8Array(0)): Buffer {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  buf[21] = status;
  view.setBigUint64(24, TABLE_HASH, true);
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32C(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return Buffer.from(buf);
}

/** A refusal of `id` with `status`, `ran` and `reason`. */
function refusal(id: bigint, status: number, ran: Ran, reason: Reason = Reason.Unclassified): Buffer {
  const text = enc.encode("busy");
  const body = new Uint8Array(3 + text.length);
  body[0] = reason & 0xff;
  body[1] = reason >> 8;
  body[2] = ran;
  body.set(text, 3);
  return frame(id, status, body);
}

/** A kv.get answer: [version:u64][value_len:u32][value], then no expiry (0). */
function getReply(id: bigint, value: string): Buffer {
  const v = enc.encode(value);
  const body = new Uint8Array(12 + v.length + 1);
  const view = new DataView(body.buffer);
  view.setBigUint64(0, 1n, true);
  view.setUint32(8, v.length, true);
  body.set(v, 12);
  return frame(id, StatusCode.OK, body);
}

type Seen = { op: number; id: bigint; at: number };

/** A server that answers the nth request of each op (from 0) with reply(op, n, id). */
async function scripted(reply: (op: number, n: number, id: bigint) => Buffer) {
  const seen: Seen[] = [];
  const counts = new Map<number, number>();
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        const op = buf.readUInt16LE(20);
        const id = buf.readBigUInt64LE(8);
        buf = buf.subarray(total);
        const n = counts.get(op) ?? 0;
        counts.set(op, n + 1);
        seen.push({ op, id, at: Date.now() });
        sock.write(reply(op, n, id));
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    seen,
    of: (op: number) => seen.filter((s) => s.op === op),
    close: () => {
      sockets.forEach((s) => s.destroy());
      server.close();
    },
  };
}

const fastRetry: RetryPolicy = { maxAttempts: 4, initialBackoffMs: 1, maxBackoffMs: 2 };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const c of cleanups.reverse()) await c();
  cleanups = [];
});

async function connect(endpoint: string, retry: Partial<RetryPolicy>): Promise<FloClient> {
  const c = new FloClient(endpoint, { timeoutMs: 1000, retry });
  await c.connect();
  cleanups.push(() => c.close());
  return c;
}

async function serve(reply: (op: number, n: number, id: bigint) => Buffer) {
  const srv = await scripted(reply);
  cleanups.push(srv.close);
  return srv;
}

describe("the client retries a refusal the server didn't run", () => {
  for (const status of [StatusCode.Overloaded, StatusCode.Unavailable]) {
    it(`status ${status}, as a new request`, async () => {
      const srv = await serve((_op, n, id) => (n === 0 ? refusal(id, status, Ran.No) : getReply(id, "v")));
      const c = await connect(srv.endpoint, fastRetry);
      const got = await c.kv.get("k");
      expect(new TextDecoder().decode(got?.value)).toBe("v");
      const ids = srv.seen.map((s) => s.id);
      expect(ids.length).toBe(2);
      expect(ids[0]).not.toBe(ids[1]);
    });
  }
});

describe("only refusals that didn't run are retried", () => {
  const cases: [string, (id: bigint) => Buffer, (e: unknown) => void][] = [
    ["overloaded, may have run", (id) => refusal(id, StatusCode.Overloaded, Ran.Unknown), (e) => {
      expect(e).toBeInstanceOf(OverloadedError);
      expect((e as ServerError).ran).toBe(Ran.Unknown);
    }],
    ["unavailable, ran", (id) => refusal(id, StatusCode.Unavailable, Ran.Yes), (e) => {
      expect(e).toBeInstanceOf(UnavailableError);
      expect((e as ServerError).ran).toBe(Ran.Yes);
    }],
    ["conflict, not run", (id) => refusal(id, StatusCode.Conflict, Ran.No), (e) => expect(e).toBeInstanceOf(ConflictError)],
    // An operator has to repair a halted shard: retrying is no use.
    ["unavailable, not run, shard halted", (id) => refusal(id, StatusCode.Unavailable, Ran.No, Reason.ShardHalted), (e) => {
      expect(e).toBeInstanceOf(UnavailableError);
      expect(e).toMatchObject({ reason: Reason.ShardHalted, ran: Ran.No });
    }],
    // Committed, not applied: resending would write it twice.
    ["outcome pending", (id) => refusal(id, StatusCode.OutcomePending, Ran.Unknown, Reason.ShardHalted), (e) => {
      expect(e).toBeInstanceOf(OutcomePendingError);
      expect(e).toMatchObject({ status: StatusCode.OutcomePending, reason: Reason.ShardHalted, ran: Ran.Unknown, serverMessage: "busy" });
      expect(isOutcomePending(e)).toBe(true);
    }],
    ["overloaded, no ran byte", (id) => frame(id, StatusCode.Overloaded, new Uint8Array([0, 0])), (e) =>
      expect(e).toBeInstanceOf(IncompleteResponseError)],
  ];
  for (const [name, reply, check] of cases) {
    it(name, async () => {
      const srv = await serve((_op, _n, id) => reply(id));
      const c = await connect(srv.endpoint, fastRetry);
      const err = await c.kv.get("k").then(() => undefined, (e: unknown) => e);
      check(err);
      expect(srv.seen.length).toBe(1);
    });
  }
});

describe("a halted shard's read", () => {
  it("is a success marked stale, over the wire, and an unknown flag is refused", async () => {
    const flagged = (b: Buffer, flags: number) => {
      b[22] = flags;
      b.writeUInt32LE(computeCRC32C(b.subarray(0, HEADER_SIZE), b.subarray(HEADER_SIZE)), 16);
      return b;
    };
    const srv = await serve((_op, n, id) => flagged(getReply(id, "v"), [0x01, 0x00, 0x02][n]!));
    const c = await connect(srv.endpoint, fastRetry);
    expect(await c.kv.get("k")).toMatchObject({ stale: true });
    expect(await c.kv.get("k")).toMatchObject({ stale: false });
    await expect(c.kv.get("k")).rejects.toBeInstanceOf(IncompleteResponseError);
    expect(srv.seen.length).toBe(3);
  });
});

describe("retries stop at maxAttempts", () => {
  for (const attempts of [4, 1]) {
    it(`maxAttempts ${attempts}`, async () => {
      const srv = await serve((_op, _n, id) => refusal(id, StatusCode.Overloaded, Ran.No));
      const c = await connect(srv.endpoint, { ...fastRetry, maxAttempts: attempts });
      const err = await c.kv.get("k").then(() => undefined, (e: unknown) => e);
      expect(err).toBeInstanceOf(OverloadedError);
      expect((err as ServerError).ran).toBe(Ran.No);
      expect(srv.seen.length).toBe(attempts);
    });
  }

  it("a policy with negative waits still retries, without waiting", async () => {
    const srv = await serve((_op, _n, id) => refusal(id, StatusCode.Overloaded, Ran.No));
    const c = await connect(srv.endpoint, { maxAttempts: 3, initialBackoffMs: -50, maxBackoffMs: -1 });
    await expect(c.kv.get("k")).rejects.toBeInstanceOf(OverloadedError);
    expect(srv.seen.length).toBe(3);
  });
});

describe("Backoff", () => {
  /** Draw `rounds` runs of waits and check each against its ceiling. */
  function check(b: Backoff, ceilings: number[], rounds = 200) {
    for (let i = 0; i < rounds; i++) {
      b.reset();
      for (const c of ceilings) {
        const d = b.next();
        expect(d).toBeGreaterThanOrEqual(0);
        expect(d).toBeLessThanOrEqual(c);
      }
    }
  }

  it("stays under its doubling ceiling", () => {
    check(new Backoff(50, 1000), [50, 100, 200, 400, 800, 1000, 1000]);
  });

  it("fills unset fields from the defaults", () => {
    expect(normalizeRetryPolicy()).toEqual({ maxAttempts: 4, initialBackoffMs: 50, maxBackoffMs: 1000 });
    expect(normalizeRetryPolicy({ maxAttempts: 1 })).toEqual({ maxAttempts: 1, initialBackoffMs: 50, maxBackoffMs: 1000 });
  });

  it("clamps negative waits to 0", () => {
    const p = normalizeRetryPolicy({ initialBackoffMs: -5, maxBackoffMs: -10 });
    expect(p.initialBackoffMs).toBe(0);
    expect(p.maxBackoffMs).toBe(0);
    check(new Backoff(p.initialBackoffMs, p.maxBackoffMs), [0, 0, 0, 0]);
    const q = normalizeRetryPolicy({ initialBackoffMs: -5, maxBackoffMs: 300 });
    expect(q).toMatchObject({ initialBackoffMs: 0, maxBackoffMs: 300 });
  });

  it("makes non-finite settings safe", () => {
    expect(normalizeRetryPolicy({ maxAttempts: NaN, initialBackoffMs: NaN, maxBackoffMs: NaN })).toEqual({
      maxAttempts: 4, initialBackoffMs: 0, maxBackoffMs: 0,
    });
    expect(normalizeRetryPolicy({ maxAttempts: Infinity, initialBackoffMs: Infinity, maxBackoffMs: 20 })).toEqual({
      maxAttempts: 4, initialBackoffMs: 0, maxBackoffMs: 20,
    });
    expect(normalizeRetryPolicy({ maxAttempts: 2.9, initialBackoffMs: 40, maxBackoffMs: Infinity })).toEqual({
      maxAttempts: 2, initialBackoffMs: 40, maxBackoffMs: 40,
    });
    expect(normalizeRetryPolicy({ initialBackoffMs: 1e12, maxBackoffMs: 1e15 })).toMatchObject({
      initialBackoffMs: 2 ** 31 - 1, maxBackoffMs: 2 ** 31 - 1,
    });
  });

  it("raises a max below the initial wait to the initial", () => {
    const p = normalizeRetryPolicy({ initialBackoffMs: 200, maxBackoffMs: 50 });
    expect(p.maxBackoffMs).toBe(200);
    check(new Backoff(p.initialBackoffMs, p.maxBackoffMs), [200, 200, 200, 200]);
  });
});

describe("StreamWorker group join", () => {
  const noRetry = { maxAttempts: 1 };

  it("is retried after refusals it wasn't run for, with the client's retries off", async () => {
    const srv = await serve((op, n, id) =>
      op === OpCode.StreamGroupJoin && n < 3 ? refusal(id, StatusCode.Overloaded, Ran.No) : frame(id, StatusCode.OK)
    );
    const c = await connect(srv.endpoint, noRetry);
    const w = new StreamWorker(c, { stream: "s", blockMs: 30000 }, async () => {});
    const done = w.start();
    for (let i = 0; i < 100 && srv.of(OpCode.StreamGroupRead).length === 0; i++) await sleep(20);
    w.stop();
    await done;
    expect(srv.of(OpCode.StreamGroupJoin).length).toBe(4);
    expect(srv.of(OpCode.StreamGroupRead).length).toBeGreaterThan(0);
  });

  it("is fatal on any other refusal", async () => {
    const srv = await serve((op, _n, id) =>
      op === OpCode.StreamGroupJoin ? refusal(id, StatusCode.Overloaded, Ran.Unknown) : frame(id, StatusCode.OK)
    );
    const c = await connect(srv.endpoint, noRetry);
    const w = new StreamWorker(c, { stream: "s" }, async () => {});
    await expect(w.start()).rejects.toBeInstanceOf(OverloadedError);
    expect(srv.of(OpCode.StreamGroupJoin).length).toBe(1);
  });

  it("stops at once while waiting to retry", async () => {
    const srv = await serve((op, _n, id) =>
      op === OpCode.StreamGroupJoin ? refusal(id, StatusCode.Unavailable, Ran.No) : frame(id, StatusCode.OK)
    );
    const c = await connect(srv.endpoint, noRetry);
    const w = new StreamWorker(c, { stream: "s" }, async () => {});
    const done = w.start();
    await sleep(1000);
    const stopped = Date.now();
    w.stop();
    await done;
    expect(Date.now() - stopped).toBeLessThan(100);
    expect(srv.of(OpCode.StreamGroupRead).length).toBe(0);
  });
});

describe("ActionWorker", () => {
  it("gives its connection the retry policy it is configured with", async () => {
    const srv = await serve((_op, _n, id) => frame(id, StatusCode.OK));
    const w = new ActionWorker({ endpoint: srv.endpoint, retry: { maxAttempts: 7, maxBackoffMs: 9 } });
    w.action("a", async () => new Uint8Array(0));
    const done = w.start();
    await sleep(100);
    const policy = (w as unknown as { client: { retry: unknown } }).client.retry;
    w.stop();
    await done;
    expect(policy).toEqual({ maxAttempts: 7, initialBackoffMs: 50, maxBackoffMs: 50 });
  });
});

describe("StreamWorker given an endpoint", () => {
  it("gives the client it creates the retry policy it is configured with", () => {
    const w = new StreamWorker("127.0.0.1:1", { stream: "s", retry: { maxAttempts: 7, maxBackoffMs: 9 } }, async () => {});
    const policy = (w as unknown as { client: { retry: unknown } }).client.retry;
    expect(policy).toEqual({ maxAttempts: 7, initialBackoffMs: 50, maxBackoffMs: 50 });
  });
});

// Each poll fails with a refusal the client doesn't retry.
const failingWorkers: [string, number, (endpoint: string) => { stop: () => void; done: Promise<void> }][] = [
  ["ActionWorker", OpCode.ActionAwait, (endpoint) => {
    const w = new ActionWorker({ endpoint, blockMs: 30000 });
    w.action("a", async () => new Uint8Array(0));
    return { stop: () => w.stop(), done: w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, (endpoint) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000 }, async () => {});
    return { stop: () => w.stop(), done: w.start() };
  }],
];

describe.each(failingWorkers)("%s after a failed poll", (_name, pollOp, start) => {
  it("backs off from 100 ms doubling, and stops at once during a wait", async () => {
    const srv = await serve((op, _n, id) =>
      op === pollOp ? refusal(id, StatusCode.InternalError, Ran.No) : frame(id, StatusCode.OK)
    );
    const w = start(srv.endpoint);
    // The first two waits total at most 300 ms, so a third poll comes well
    // inside 1500 ms; a flat 1 s wait would allow only two.
    await sleep(1500);
    const stopped = Date.now();
    w.stop();
    await w.done;
    expect(Date.now() - stopped).toBeLessThan(100);
    const polls = srv.of(pollOp);
    expect(polls.length).toBeGreaterThan(2);
    for (let i = 1; i < polls.length; i++) {
      expect(polls[i]!.at - polls[i - 1]!.at).toBeLessThanOrEqual(100 * 2 ** (i - 1) + 50);
    }
  });
});

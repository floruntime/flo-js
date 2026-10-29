/**
 * Worker reconnect: a dropped connection is redialed with backoff, the
 * worker registers (or joins its group) again before polling, and stop()
 * cuts the backoff short.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { HEADER_SIZE, MAGIC, OpCode, TABLE_HASH, VERSION, computeCRC32C, type Logger } from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";
import { reconnect } from "../packages/node/src/reconnect.js";

function reply(id: bigint, data: Uint8Array = new Uint8Array(0)): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  view.setBigUint64(24, TABLE_HASH, true);
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32C(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

/** A task assignment for action "a", invoked by no workflow. */
function oneTask(): Uint8Array {
  const buf = new Uint8Array(2 + 2 + 2 + 1 + 8 + 4 + 1);
  const view = new DataView(buf.buffer);
  view.setUint16(0, 2, true);
  buf.set([0x74, 0x31], 2); // "t1"
  view.setUint16(4, 1, true);
  buf[6] = 0x61; // "a"
  view.setUint32(15, 1, true); // attempt
  buf[19] = 0; // has_caller
  return buf;
}

/** A group read answer holding one record. */
function oneRecord(): Uint8Array {
  const buf = new Uint8Array(4 + 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1, true);
  view.setBigUint64(4, 1n, true);
  view.setUint32(26, 1, true); // payload_len
  buf[30] = 0x78;
  return buf;
}

type Action = "drop" | "park" | Uint8Array | undefined;

/**
 * A server that answers per `script(op, conn)`, where conn counts accepted
 * connections from 0: undefined is an empty OK, "drop" destroys the
 * connection, "park" never answers.
 */
async function fakeServer(script: (op: number, conn: number) => Action) {
  const seen: { op: number; conn: number }[] = [];
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    const conn = sockets.length;
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("error", () => {});
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        const op = buf.readUInt16LE(20);
        const id = buf.readBigUInt64LE(8);
        buf = buf.subarray(total);
        seen.push({ op, conn });
        const a = script(op, conn);
        if (a === "drop") return void sock.destroy();
        if (a === "park") continue;
        sock.write(reply(id, a));
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    seen,
    /** Connections accepted so far. */
    connections: () => sockets.length,
    /** Stop listening and drop every connection, so redials are refused. */
    close: () => {
      server.close();
      sockets.forEach((s) => s.destroy());
    },
  };
}

function recordingLogger() {
  const warnings: string[] = [];
  const logger: Logger = { debug: () => {}, warn: (m) => void warnings.push(m), error: () => {} };
  return { logger, warnings };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(cond: () => boolean, ms: number): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond() && Date.now() < deadline) await sleep(10);
}

type Case = {
  poll: number;
  /** Ops the worker must send again on the new connection before it polls. */
  rejoin: number[];
  /** The op that shows the polled work was handled. */
  handled: number;
  work: Uint8Array;
  start: (endpoint: string, logger: Logger) => { stop: () => void; done: Promise<void> };
};

const cases: [string, Case][] = [
  ["ActionWorker", {
    poll: OpCode.ActionAwait,
    rejoin: [OpCode.ActionRegister, OpCode.WorkerRegister],
    handled: OpCode.ActionComplete,
    work: oneTask(),
    start: (endpoint, logger) => {
      const w = new ActionWorker({ endpoint, blockMs: 30000, logger });
      w.action("a", async () => new Uint8Array(0));
      return { stop: () => w.stop(), done: w.start() };
    },
  }],
  ["StreamWorker", {
    poll: OpCode.StreamGroupRead,
    rejoin: [OpCode.StreamGroupJoin, OpCode.WorkerRegister],
    handled: OpCode.StreamGroupAck,
    work: oneRecord(),
    start: (endpoint, logger) => {
      const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, logger }, async () => {});
      return { stop: () => w.stop(), done: w.start() };
    },
  }],
];

describe.each(cases)("%s reconnect", (_name, c) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("reconnects after a drop mid-poll, rejoins, then handles the next poll's work", async () => {
    // Connection 0 drops at its first poll; connection 1 hands out work once.
    let served = false;
    const srv = await fakeServer((op, conn) => {
      if (op !== c.poll) return undefined;
      if (conn === 0) return "drop";
      if (served) return "park";
      served = true;
      return c.work;
    });
    close = srv.close;
    const { logger, warnings } = recordingLogger();
    const w = c.start(srv.endpoint, logger);
    await until(() => srv.seen.some((s) => s.op === c.handled), 3000);
    w.stop();
    srv.close();
    await w.done;

    const second = srv.seen.filter((s) => s.conn === 1).map((s) => s.op);
    const firstPoll = second.indexOf(c.poll);
    expect(firstPoll).toBeGreaterThan(-1);
    for (const op of c.rejoin) {
      const i = second.indexOf(op);
      expect(i).toBeGreaterThan(-1);
      expect(i).toBeLessThan(firstPoll);
    }
    expect(second).toContain(c.handled);
    expect(warnings.some((m) => m.startsWith("Connection lost"))).toBe(true);
  });

  it("doesn't redial at once, again and again, a server that takes the connection and drops it", async () => {
    // Every poll is dropped, on every connection.
    const srv = await fakeServer((op) => (op === c.poll ? "drop" : undefined));
    close = srv.close;
    const { logger } = recordingLogger();
    const w = c.start(srv.endpoint, logger);
    await sleep(1500);
    w.stop();
    await w.done;
    // Waits of up to 100, 200, 400, 800 ms between redials: a handful, not
    // thousands.
    expect(srv.connections()).toBeLessThan(15);
  });

  it("stops promptly during the reconnect backoff", async () => {
    // The first poll takes the server down, so every redial is refused.
    const srv = await fakeServer((op) => {
      if (op !== c.poll) return undefined;
      setImmediate(() => srv.close());
      return "park";
    });
    close = srv.close;
    const { logger, warnings } = recordingLogger();
    const w = c.start(srv.endpoint, logger);
    await until(() => warnings.some((m) => m.startsWith("Reconnect attempt 1 failed")), 3000);
    expect(warnings.some((m) => m.startsWith("Reconnect attempt 1 failed"))).toBe(true);
    // A retry waits up to 100 ms, doubling; stop() must not wait it out.
    await sleep(100);
    const stopped = Date.now();
    w.stop();
    await w.done;
    expect(Date.now() - stopped).toBeLessThan(200);
  });
});

describe("a StreamWorker given a client", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());
  const noRetry = { maxAttempts: 1 };

  it("redials when a poll times out on a connection that went quiet", async () => {
    // Connection 0 never answers its poll; connection 1 hands out a record.
    let served = false;
    const srv = await fakeServer((op, conn) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (conn === 0) return "park";
      if (served) return "park";
      served = true;
      return oneRecord();
    });
    close = srv.close;
    const { logger } = recordingLogger();
    const client = new FloClient(srv.endpoint, { timeoutMs: 200, retry: noRetry });
    await client.connect();
    const w = new StreamWorker(client, { stream: "s", blockMs: 100, logger }, async () => {});
    const done = w.start();
    await until(() => srv.seen.some((s) => s.op === OpCode.StreamGroupAck), 3000);
    w.stop();
    await done;
    expect(srv.seen.some((s) => s.conn === 1 && s.op === OpCode.StreamGroupAck)).toBe(true);
  });

  it("stops, without reopening it, when its owner closes the client", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? "park" : undefined));
    close = srv.close;
    const { logger, warnings } = recordingLogger();
    const client = new FloClient(srv.endpoint, { retry: noRetry });
    await client.connect();
    const w = new StreamWorker(client, { stream: "s", blockMs: 30000, logger }, async () => {});
    const done = w.start();
    await until(() => srv.seen.some((s) => s.op === OpCode.StreamGroupRead), 3000);
    await client.close();
    await done;
    expect(srv.connections()).toBe(1);
    expect(warnings.some((m) => m.includes("closed by its owner"))).toBe(true);
  });
});

describe("FloClient.connect", () => {
  it("dials once for callers that connect at the same time", async () => {
    const srv = await fakeServer(() => undefined);
    const client = new FloClient(srv.endpoint);
    await Promise.all([client.connect(), client.connect(), client.connect()]);
    await sleep(50);
    expect(srv.connections()).toBe(1);
    await client.close();
    srv.close();
  });
});

describe("reconnect", () => {
  afterEach(() => vi.useRealTimers());

  it("waits as the poll loop does: jittered, the ceiling doubling from 100 ms to 5 s", async () => {
    vi.useFakeTimers();
    const client = new FloClient("127.0.0.1:1");
    client.redial = async () => {
      throw new Error("refused");
    };
    const { logger, warnings } = recordingLogger();
    const stop = new AbortController();
    const done = reconnect(client, stop.signal, logger, { connection: 0, closes: 0 });
    for (let i = 0; i < 8; i++) await vi.advanceTimersByTimeAsync(5_000);
    stop.abort();
    expect(await done).toBe(false);
    const delays = warnings.slice(0, 8).map((m) => Number(/retrying in (\d+) ms/.exec(m)![1]));
    const ceilings = [100, 200, 400, 800, 1600, 3200, 5000, 5000];
    expect(delays).toHaveLength(8);
    delays.forEach((d, i) => {
      expect(d).toBeGreaterThanOrEqual(0);
      expect(d).toBeLessThanOrEqual(ceilings[i]!);
    });
  });
});

describe("reconnect, stopped", () => {
  it("doesn't count a dial that finishes after stop() as reconnected", async () => {
    const client = new FloClient("127.0.0.1:1");
    const stop = new AbortController();
    client.redial = async () => {
      stop.abort();
    };
    const { logger, warnings } = recordingLogger();
    expect(await reconnect(client, stop.signal, logger, { connection: 0, closes: 0 })).toBe(false);
    expect(warnings.some((m) => m.startsWith("Reconnected"))).toBe(false);
  });
});

describe("FloClient.close", () => {
  it("during a dial leaves the client closed, with no socket", async () => {
    const srv = await fakeServer(() => undefined);
    const client = new FloClient(srv.endpoint);
    const dialing = client.connect();
    await client.close();
    await expect(dialing).rejects.toThrow();
    expect(client.isConnected()).toBe(false);
    expect(client.isClosed()).toBe(true);
    srv.close();
  });
});

describe("FloClient.redial", () => {
  it("drops only the connection the caller saw, so callers sharing a client redial it once", async () => {
    const srv = await fakeServer(() => undefined);
    const client = new FloClient(srv.endpoint);
    await client.connect();
    const seen = client.connection();
    await client.redial(seen); // the first caller replaces the connection
    await client.redial(seen); // the second, late, finds it already replaced
    await sleep(50);
    expect(srv.connections()).toBe(2);
    expect(client.isConnected()).toBe(true);
    await client.close();
    srv.close();
  });
});

describe("StreamWorkers sharing a client", () => {
  it("redial its dropped connection once between them, not once each", async () => {
    // The first connection drops at its first read; later ones park reads.
    const srv = await fakeServer((op, conn) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      return conn === 0 ? "drop" : "park";
    });
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 1 } });
    await client.connect();
    const { logger } = recordingLogger();
    const workers = ["a", "b"].map(
      (stream) => new StreamWorker(client, { stream, blockMs: 30000, logger }, async () => {}),
    );
    const done = workers.map((w) => w.start());
    await until(() => srv.seen.filter((s) => s.conn >= 1 && s.op === OpCode.StreamGroupRead).length >= 2, 3000);
    await sleep(500);
    workers.forEach((w) => w.stop());
    await client.close();
    await Promise.all(done);
    expect(srv.connections()).toBe(2);
    srv.close();
  });
});

describe("a StreamWorker whose client is closed and connected again", () => {
  it("stays stopped", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? "park" : undefined));
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 1 } });
    await client.connect();
    const { logger } = recordingLogger();
    const w = new StreamWorker(client, { stream: "s", blockMs: 30000, logger }, async () => {});
    const done = w.start();
    await until(() => srv.seen.some((s) => s.op === OpCode.StreamGroupRead), 3000);
    await client.close();
    await client.connect();
    await done;
    const reads = srv.seen.filter((s) => s.op === OpCode.StreamGroupRead).length;
    await sleep(300);
    expect(srv.seen.filter((s) => s.op === OpCode.StreamGroupRead).length).toBe(reads);
    await client.close();
    srv.close();
  });
});

/**
 * Worker reconnect: a dropped connection is redialed with backoff, the
 * worker registers (or joins its group) again before polling, and stop()
 * cuts the backoff short.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { HEADER_SIZE, MAGIC, OpCode, VERSION, computeCRC32, type Logger } from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";
import { reconnect } from "../packages/node/src/reconnect.js";

function reply(id: bigint, data: Uint8Array = new Uint8Array(0)): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

/** A task assignment for action "a". */
function oneTask(): Uint8Array {
  const buf = new Uint8Array(2 + 2 + 2 + 1 + 8 + 4);
  const view = new DataView(buf.buffer);
  view.setUint16(0, 2, true);
  buf.set([0x74, 0x31], 2); // "t1"
  view.setUint16(4, 1, true);
  buf[6] = 0x61; // "a"
  view.setUint32(15, 1, true); // attempt
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
      const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs: 0, logger });
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
      const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0, logger }, async () => {});
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
    // The first retry waits 1 s; stop() must not.
    await sleep(100);
    const stopped = Date.now();
    w.stop();
    await w.done;
    expect(Date.now() - stopped).toBeLessThan(200);
  });
});

describe("reconnect", () => {
  afterEach(() => vi.useRealTimers());

  it("retries 1 s apart, doubling to a 30 s cap", async () => {
    vi.useFakeTimers();
    const client = new FloClient("127.0.0.1:1");
    client.connect = async () => {
      throw new Error("refused");
    };
    const { logger, warnings } = recordingLogger();
    const stop = new AbortController();
    const done = reconnect(client, stop.signal, logger);
    for (let i = 0; i < 8; i++) await vi.advanceTimersByTimeAsync(30_000);
    stop.abort();
    expect(await done).toBe(false);
    const delays = warnings.slice(0, 8).map((m) => Number(/retrying in (\d+) ms/.exec(m)![1]));
    expect(delays).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  });
});

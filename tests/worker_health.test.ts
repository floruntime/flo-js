/**
 * Worker health: a worker the server removed registers again when its
 * heartbeat answers not found; an await refused as unhealthy heartbeats and
 * backs off, one refused as draining stops; heartbeats report the handlers
 * running; and heartbeats can't be turned off.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { HEADER_SIZE, MAGIC, OpCode, Ran, Reason, StatusCode, TABLE_HASH, VERSION, computeCRC32 } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";

function frame(id: bigint, status: number, data: Uint8Array): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  buf[21] = status;
  view.setBigUint64(24, TABLE_HASH, true);
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

function refusal(status: number, reason: Reason, message: string): Answer {
  return { status, data: new Uint8Array([reason & 0xff, reason >> 8, Ran.No, ...new TextEncoder().encode(message)]) };
}

const notFound = () => refusal(StatusCode.NotFound, Reason.NotFound, "worker not found");
const unhealthy = () => refusal(StatusCode.BadRequest, Reason.WorkerUnhealthy, "worker missed its heartbeats; send one, or register again");
const draining = () => refusal(StatusCode.BadRequest, Reason.WorkerDraining, "worker is draining");

/** A task assignment for action "a", invoked by no workflow. */
function oneTask(): Uint8Array {
  const buf = new Uint8Array(2 + 2 + 2 + 1 + 8 + 4 + 1);
  const view = new DataView(buf.buffer);
  view.setUint16(0, 2, true);
  buf.set([0x74, 0x31], 2); // "t1"
  view.setUint16(4, 1, true);
  buf[6] = 0x61; // "a"
  view.setUint32(15, 1, true); // attempt
  return buf;
}

type Answer = { status?: number; data?: Uint8Array; delayMs?: number } | "park" | undefined;

interface Seen {
  op: number;
  /** The request's payload after the header: namespace, key, value, options. */
  payload: Buffer;
}

/** The current_load a heartbeat carries: its value's first u32. */
function loadOf(payload: Buffer): number {
  let off = 2 + payload.readUInt16LE(0); // namespace
  off += 2 + payload.readUInt16LE(off); // key
  return payload.readUInt32LE(off + 4);
}

/** A server that answers per `script(op, nth)`: undefined is an empty OK, "park" never answers. */
async function fakeServer(script: (op: number, nth: number) => Answer) {
  const seen: Seen[] = [];
  const counts = new Map<number, number>();
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
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
        seen.push({ op, payload: Buffer.from(buf.subarray(HEADER_SIZE, total)) });
        buf = buf.subarray(total);
        const nth = counts.get(op) ?? 0;
        counts.set(op, nth + 1);
        const a = script(op, nth);
        if (a === "park") continue;
        const out = frame(id, a?.status ?? StatusCode.OK, a?.data ?? new Uint8Array(0));
        if (a?.delayMs) setTimeout(() => sock.destroyed || sock.write(out), a.delayMs);
        else sock.write(out);
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    seen,
    ops: () => seen.map((s) => s.op),
    close: () => {
      server.close();
      sockets.forEach((s) => s.destroy());
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(cond: () => boolean, ms: number): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond() && Date.now() < deadline) await sleep(10);
}

const quiet = { debug: () => {}, warn: () => {}, error: () => {} };

type Started = { stop: () => void; drain: () => Promise<void>; done: Promise<void> };

const workers: [string, number, (endpoint: string, heartbeatIntervalMs: number) => Started][] = [
  ["ActionWorker", OpCode.ActionAwait, (endpoint, heartbeatIntervalMs) => {
    const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs, concurrency: 4, machineId: "m1", logger: quiet });
    w.action("a", async () => new Uint8Array(0));
    return { stop: () => w.stop(), drain: () => w.drain(), done: w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, (endpoint, heartbeatIntervalMs) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs, concurrency: 4, machineId: "m1", logger: quiet }, async () => {});
    return { stop: () => w.stop(), drain: () => w.drain(), done: w.start() };
  }],
];

describe.each(workers)("%s removed by the server", (_name, poll, start) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("registers again, with the same options, when a heartbeat answers not found, then keeps polling", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.WorkerHeartbeat && nth === 0) return notFound();
      if (op === poll) return "park";
      return undefined;
    });
    close = srv.close;
    const w = start(srv.endpoint, 50);
    await until(() => srv.ops().filter((o) => o === OpCode.WorkerRegister).length >= 2 && srv.ops().filter((o) => o === OpCode.WorkerHeartbeat).length >= 2, 3000);
    w.stop();
    srv.close();
    await w.done;

    const registers = srv.seen.filter((s) => s.op === OpCode.WorkerRegister);
    expect(registers.length).toBe(2);
    expect(registers[1]!.payload.equals(registers[0]!.payload)).toBe(true);
    const ops = srv.ops();
    // The second register follows the refused heartbeat, and heartbeats go on after it.
    expect(ops.lastIndexOf(OpCode.WorkerRegister)).toBeGreaterThan(ops.indexOf(OpCode.WorkerHeartbeat));
    expect(ops.lastIndexOf(OpCode.WorkerHeartbeat)).toBeGreaterThan(ops.lastIndexOf(OpCode.WorkerRegister));
  });

  it("is drained again after registering again, if it was draining", async () => {
    let drained = false;
    const srv = await fakeServer((op) => {
      if (op === OpCode.WorkerDrain) drained = true;
      // Removed: every heartbeat after the drain is not found.
      if (op === OpCode.WorkerHeartbeat && drained) return notFound();
      if (op === poll) return "park";
      return undefined;
    });
    close = srv.close;
    const w = start(srv.endpoint, 50);
    await until(() => srv.ops().includes(poll), 3000);
    await w.drain();
    await until(() => srv.ops().filter((o) => o === OpCode.WorkerDrain).length >= 2, 3000);
    w.stop();
    srv.close();
    await w.done;

    const ops = srv.ops();
    const secondRegister = ops.indexOf(OpCode.WorkerRegister, ops.indexOf(OpCode.WorkerDrain));
    expect(secondRegister).toBeGreaterThan(-1);
    expect(ops.indexOf(OpCode.WorkerDrain, secondRegister)).toBeGreaterThan(secondRegister);
  });
});

describe("a heartbeat out when the worker stops", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  // The handler holds stop's wait while the late not found arrives.
  const slow: [string, number, Uint8Array, (endpoint: string) => Started][] = [
    ["ActionWorker", OpCode.ActionAwait, oneTask(), (endpoint) => {
      const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs: 50, concurrency: 2, logger: quiet });
      w.action("a", async () => {
        await sleep(400);
        return new Uint8Array(0);
      });
      return { stop: () => w.stop(), drain: () => w.drain(), done: w.start() };
    }],
    ["StreamWorker", OpCode.StreamGroupRead, oneRecord(), (endpoint) => {
      const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 50, concurrency: 2, logger: quiet }, async () => {
        await sleep(400);
      });
      return { stop: () => w.stop(), drain: () => w.drain(), done: w.start() };
    }],
  ];

  it.each(slow)("%s doesn't register again when its answer is not found", async (_name, poll, work, start) => {
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.WorkerHeartbeat) return { ...notFound(), delayMs: 100 };
      // Later polls come back empty, so stop() needn't wait on a parked one.
      if (op === poll) return nth === 0 ? { data: work } : { delayMs: 20 };
      return undefined;
    });
    close = srv.close;
    const w = start(srv.endpoint);
    await until(() => srv.ops().includes(OpCode.WorkerHeartbeat), 3000);
    w.stop();
    await w.done;
    await sleep(200);
    expect(srv.ops().filter((o) => o === OpCode.WorkerRegister).length).toBe(1);
  });
});

describe("ActionWorker await refusals", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("worker_unhealthy: heartbeats at once, then polls again with backoff, not in a hot loop", async () => {
    const srv = await fakeServer((op) => (op === OpCode.ActionAwait ? unhealthy() : undefined));
    close = srv.close;
    const w = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, heartbeatIntervalMs: 60000, logger: quiet });
    w.action("a", async () => new Uint8Array(0));
    const done = w.start();
    await sleep(1000);
    w.stop();
    await done;

    const ops = srv.ops().filter((o) => o === OpCode.ActionAwait || o === OpCode.WorkerHeartbeat);
    const awaits = ops.filter((o) => o === OpCode.ActionAwait).length;
    // Each refused await is followed by a heartbeat before the next await.
    expect(ops[0]).toBe(OpCode.ActionAwait);
    expect(ops[1]).toBe(OpCode.WorkerHeartbeat);
    for (let i = 0; i + 1 < ops.length; i += 2) expect(ops.slice(i, i + 2)).toEqual([OpCode.ActionAwait, OpCode.WorkerHeartbeat]);
    // Waits of up to 100, 200, 400 ms between polls: a handful, not thousands.
    expect(awaits).toBeGreaterThan(1);
    expect(awaits).toBeLessThan(15);
  });

  it("worker_unhealthy whose heartbeat answers not found: registers again", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.ActionAwait) return nth === 0 ? unhealthy() : "park";
      if (op === OpCode.WorkerHeartbeat) return notFound();
      return undefined;
    });
    close = srv.close;
    const w = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, heartbeatIntervalMs: 60000, logger: quiet });
    w.action("a", async () => new Uint8Array(0));
    const done = w.start();
    await until(() => srv.ops().filter((o) => o === OpCode.ActionAwait).length >= 2, 3000);
    w.stop();
    srv.close();
    await done;

    const ops = srv.ops();
    const hb = ops.indexOf(OpCode.WorkerHeartbeat);
    const reRegister = ops.indexOf(OpCode.WorkerRegister, hb);
    expect(reRegister).toBeGreaterThan(hb);
    expect(ops.indexOf(OpCode.ActionAwait, reRegister)).toBeGreaterThan(reRegister);
    const registers = srv.seen.filter((s) => s.op === OpCode.WorkerRegister);
    expect(registers[1]!.payload.equals(registers[0]!.payload)).toBe(true);
  });

  it("worker_draining: stops taking work, and the running handler finishes", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.ActionAwait) return nth === 0 ? { data: oneTask() } : draining();
      return undefined;
    });
    close = srv.close;
    const w = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, heartbeatIntervalMs: 60000, logger: quiet });
    let finished = false;
    w.action("a", async () => {
      await sleep(200);
      finished = true;
      return new Uint8Array(0);
    });
    // start() returns without stop(): the refusal stopped the worker.
    await w.start();
    expect(finished).toBe(true);
    const ops = srv.ops();
    expect(ops.filter((o) => o === OpCode.ActionAwait)).toHaveLength(2);
    expect(ops).toContain(OpCode.ActionComplete);
  });
});

describe.each(workers)("%s heartbeats", (name, poll, start) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("report the handlers running, not a slot held by the outstanding poll", async () => {
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    const work = name === "ActionWorker" ? oneTask() : oneRecord();
    const srv = await fakeServer((op, nth) => {
      if (op !== poll) return undefined;
      // The first poll waits 300 ms, then hands out work; later ones park.
      return nth === 0 ? { data: work, delayMs: 300 } : "park";
    });
    close = srv.close;
    const loads = () => srv.seen.filter((s) => s.op === OpCode.WorkerHeartbeat).map((s) => loadOf(s.payload));
    const w = name === "ActionWorker"
      ? (() => {
        const a = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, heartbeatIntervalMs: 50, logger: quiet });
        a.action("a", async () => {
          await held;
          return new Uint8Array(0);
        });
        return { stop: () => a.stop(), done: a.start() };
      })()
      : (() => {
        const s = new StreamWorker(srv.endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 50, logger: quiet }, () => held);
        return { stop: () => s.stop(), done: s.start() };
      })();
    await sleep(250);
    const whilePolling = loads();
    await until(() => srv.ops().filter((o) => o === poll).length >= 2, 3000);
    const before = loads().length;
    await until(() => loads().length >= before + 3, 3000);
    const whileRunning = loads().slice(before);
    release();
    const handled = name === "ActionWorker" ? OpCode.ActionComplete : OpCode.StreamGroupAck;
    await until(() => srv.ops().includes(handled), 3000);
    w.stop();
    srv.close();
    await w.done;

    expect(whilePolling.length).toBeGreaterThan(0);
    expect(whilePolling.every((l) => l === 0)).toBe(true);
    expect(whileRunning.length).toBeGreaterThan(0);
    expect(whileRunning.every((l) => l === 1)).toBe(true);
  });

  it("can't be turned off, or set past Node's timer limit: such an interval is refused at construction", () => {
    for (const ms of [0, -1, NaN, 2 ** 31, Infinity]) {
      expect(() => start("127.0.0.1:1", ms)).toThrow(/heartbeatIntervalMs must be more than 0 and at most 2147483647/);
    }
  });
});

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

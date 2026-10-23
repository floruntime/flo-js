/**
 * Worker health: a worker the server removed registers again when its
 * heartbeat answers not found; an await refused as unhealthy heartbeats and
 * backs off, one refused as draining stops; heartbeats report the handlers
 * running; and heartbeats can't be turned off.
 */

import { afterEach, describe, expect, it } from "vitest";
import { OpCode, Reason, StatusCode } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";
import { draining, fakeServer, loadOf, notFound, oneRecord, oneTask, quiet, sleep, unhealthy, until } from "./fake_worker_server.js";


type Started = { stop: () => void; drain: () => Promise<void>; done: Promise<void> };

// Each worker's handler takes `handlerMs`.
const workers: [string, number, (endpoint: string, heartbeatIntervalMs: number, handlerMs?: number) => Started][] = [
  ["ActionWorker", OpCode.ActionAwait, (endpoint, heartbeatIntervalMs, handlerMs = 0) => {
    const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs, concurrency: 4, machineId: "m1", logger: quiet });
    w.action("a", async () => {
      await sleep(handlerMs);
      return new Uint8Array(0);
    });
    return { stop: () => w.stop(), drain: () => w.drain(), done: w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, (endpoint, heartbeatIntervalMs, handlerMs = 0) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs, concurrency: 4, machineId: "m1", logger: quiet }, async () => {
      await sleep(handlerMs);
    });
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

describe("a heartbeat out when the run ends", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it.each(workers)("%s doesn't register again when its answer is not found", async (_name, poll, start) => {
    // The not found lands while the run is deregistering, its answer held up.
    const srv = await fakeServer((op) => {
      if (op === OpCode.WorkerHeartbeat) return { ...notFound(), delayMs: 100 };
      if (op === OpCode.WorkerDeregister) return { delayMs: 300 };
      // Polls come back empty, so stop() needn't wait on a parked one.
      if (op === poll) return { delayMs: 20 };
      return undefined;
    });
    close = srv.close;
    const w = start(srv.endpoint, 50);
    await until(() => srv.ops().includes(OpCode.WorkerHeartbeat), 3000);
    w.stop();
    await w.done;
    expect(srv.ops().filter((o) => o === OpCode.WorkerRegister).length).toBe(1);
  });

  it.each(workers)("%s registers again on a not found while handlers it was stopped with still run", async (_name, poll, start) => {
    // Still finishing its work, the worker must stay registered.
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.WorkerHeartbeat && nth === 1) return notFound();
      if (op === poll) return nth === 0 ? { data: poll === OpCode.ActionAwait ? oneTask() : oneRecord() } : { delayMs: 20 };
      return undefined;
    });
    close = srv.close;
    const w = start(srv.endpoint, 50, 400);
    await until(() => srv.ops().includes(OpCode.WorkerHeartbeat), 3000);
    w.stop();
    await w.done;
    const ops = srv.ops();
    expect(ops.filter((o) => o === OpCode.WorkerRegister).length).toBe(2);
    expect(ops.lastIndexOf(OpCode.WorkerRegister)).toBeLessThan(ops.indexOf(OpCode.WorkerDeregister));
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


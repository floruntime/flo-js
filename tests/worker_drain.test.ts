/**
 * Drain and stop, for both workers: a drain lets running handlers finish,
 * heartbeating, and keeps the server told; a drain started on the server
 * does the same; stop and drain hold when they land before start(), and a
 * drain ends a reconnect; a stream worker can be started again, and a stop
 * hands back the records of its last batch it hasn't started.
 */

import { afterEach, describe, expect, it } from "vitest";
import { OpCode } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";
import { type Answer, fakeServer, notFound, oneTask, quiet, sleep, until } from "./fake_worker_server.js";

type Run = { stop: () => void; drain: () => Promise<void>; start: () => Promise<void> };

/** A group read answer holding `n` records. */
function records(n: number): Uint8Array {
  const one = 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4;
  const buf = new Uint8Array(4 + n * one);
  const view = new DataView(buf.buffer);
  view.setUint32(0, n, true);
  for (let i = 0; i < n; i++) {
    const at = 4 + i * one;
    view.setBigUint64(at, BigInt(i + 1), true); // sequence
    view.setUint32(at + 22, 1, true); // payload_len
    buf[at + 26] = 0x78;
  }
  return buf;
}

// Each worker, its poll op and the work a first poll hands it; its handler
// takes `handlerMs` and calls `onStart` first.
const workers: [string, number, Uint8Array, (endpoint: string, handlerMs: number, onStart?: () => void, concurrency?: number) => Run][] = [
  ["ActionWorker", OpCode.ActionAwait, oneTask(), (endpoint, handlerMs, onStart, concurrency = 2) => {
    const w = new ActionWorker({ endpoint, blockMs: BLOCK_MS, heartbeatIntervalMs: 50, concurrency, logger: quiet });
    w.action("a", async () => {
      onStart?.();
      await sleep(handlerMs);
      return new Uint8Array(0);
    });
    return { stop: () => w.stop(), drain: () => w.drain(), start: () => w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, records(1), (endpoint, handlerMs, onStart, concurrency = 2) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: BLOCK_MS, heartbeatIntervalMs: 50, concurrency, logger: quiet }, async () => {
      onStart?.();
      await sleep(handlerMs);
    });
    return { stop: () => w.stop(), drain: () => w.drain(), start: () => w.start() };
  }],
];

const reportOf = (poll: number) => (poll === OpCode.ActionAwait ? OpCode.ActionComplete : OpCode.StreamGroupAck);

/** The workers' poll wait, short so a held poll ends soon. */
const BLOCK_MS = 300;

/**
 * The first poll gets `work`; later ones are held for the worker's wait and
 * then answered empty, as flo does: a drain notice doesn't end a held poll.
 */
function workThenEmpty(poll: number, work: Uint8Array, rest: (op: number, nth: number) => Answer = () => undefined) {
  return (op: number, nth: number): Answer => {
    if (op === poll) return nth === 0 ? { data: work } : { delayMs: BLOCK_MS, data: poll === OpCode.StreamGroupRead ? records(0) : undefined };
    return rest(op, nth);
  };
}

/** A request's value: [ns_len:u16][ns][key_len:u16][key][value_len:u32][value]. */
function valueOf(payload: Buffer): Buffer {
  let at = 2 + payload.readUInt16LE(0);
  at += 2 + payload.readUInt16LE(at);
  const len = payload.readUInt32LE(at);
  return payload.subarray(at + 4, at + 4 + len);
}

describe.each(workers)("%s drain", (_name, poll, work, make) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("lets the running handler finish, heartbeating, and tells the server once", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    close = srv.close;
    let started = false;
    const w = make(srv.endpoint, 400, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    await w.drain();
    // start() returns by itself once the handler has reported.
    await done;
    const ops = srv.ops();
    const drain = ops.indexOf(OpCode.WorkerDrain);
    const report = ops.indexOf(reportOf(poll));
    expect(drain).toBeGreaterThan(-1);
    expect(drain).toBeLessThan(report);
    expect(ops.slice(drain, report).filter((o) => o === OpCode.WorkerHeartbeat).length).toBeGreaterThanOrEqual(3);
    expect(ops.filter((o) => o === OpCode.WorkerDrain).length).toBe(1);
    expect(ops.slice(drain + 1).includes(poll) ? ops.slice(report).includes(poll) : false).toBe(false);
    expect(ops).toContain(OpCode.WorkerDeregister);
  });

  it("started on the server: a Draining heartbeat drains it, and the handler finishes", async () => {
    let status = 0;
    const srv = await fakeServer(workThenEmpty(poll, work, (op) => (op === OpCode.WorkerHeartbeat ? { data: new Uint8Array([status]) } : undefined)));
    close = srv.close;
    let started = false;
    const w = make(srv.endpoint, 400, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    status = 2; // an operator drains it
    await done;
    const ops = srv.ops();
    expect(ops.filter((o) => o === reportOf(poll)).length).toBe(1);
    // The notice goes even for a drain the server announced.
    expect(ops.filter((o) => o === OpCode.WorkerDrain).length).toBe(1);
  });

  it("removed while draining: registers again, the same, and tells the server again", async () => {
    let drained = false;
    const srv = await fakeServer(workThenEmpty(poll, work, (op) => {
      if (op === OpCode.WorkerDrain) drained = true;
      if (op === OpCode.WorkerHeartbeat && drained) return notFound();
      return undefined;
    }));
    close = srv.close;
    let started = false;
    const w = make(srv.endpoint, 500, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    await w.drain();
    await done;
    const ops = srv.ops();
    const again = ops.indexOf(OpCode.WorkerRegister, ops.indexOf(OpCode.WorkerDrain));
    expect(again).toBeGreaterThan(-1);
    expect(ops.indexOf(OpCode.WorkerDrain, again)).toBeGreaterThan(again);
    const registers = srv.seen.filter((s) => s.op === OpCode.WorkerRegister);
    expect(registers[registers.length - 1]!.payload.equals(registers[0]!.payload)).toBe(true);
  });

  it("before start(): ends the run without polling", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    close = srv.close;
    const w = make(srv.endpoint, 0);
    await w.drain();
    await w.start();
    expect(srv.ops()).not.toContain(poll);
  });

  it("with a handler running and the server gone, ends soon after the handler", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    let started = false;
    const w = make(srv.endpoint, 600, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    srv.close(); // every redial is refused from here on
    close = undefined;
    const t0 = Date.now();
    await w.drain();
    await done;
    // The handler, then its result's bounded resends.
    expect(Date.now() - t0).toBeLessThan(4500);
  }, 10_000);

  it("ends a reconnect at once", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    const w = make(srv.endpoint, 0);
    const done = w.start();
    await until(() => srv.ops().includes(OpCode.WorkerHeartbeat), 3000);
    srv.close(); // the connection drops, and every redial is refused
    close = undefined;
    await sleep(300);
    const t0 = Date.now();
    await w.drain();
    await done;
    expect(Date.now() - t0).toBeLessThan(1500);
  });
});

describe.each(workers)("%s stop", (_name, poll, work, make) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("before start(): ends the run at once, registering nothing", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    close = srv.close;
    const w = make(srv.endpoint, 0);
    w.stop();
    await w.start();
    expect(srv.ops()).not.toContain(poll);
    expect(srv.ops()).not.toContain(OpCode.WorkerRegister);
  });

  it("keeps heartbeating while the running handler finishes", async () => {
    const srv = await fakeServer(workThenEmpty(poll, work));
    close = srv.close;
    let started = false;
    const w = make(srv.endpoint, 400, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    w.stop();
    const atStop = srv.ops().length;
    await done;
    const after = srv.ops().slice(atStop);
    const report = after.indexOf(reportOf(poll));
    expect(after.slice(0, report).filter((o) => o === OpCode.WorkerHeartbeat).length).toBeGreaterThanOrEqual(3);
    expect(srv.ops()).not.toContain(OpCode.WorkerDrain);
  });
});

describe("drain notices", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("a notice lost with its connection is sent again while the handler runs", async () => {
    const srv = await fakeServer(workThenEmpty(OpCode.ActionAwait, oneTask(), (op, nth) => (op === OpCode.WorkerDrain && nth < 2 ? "drop" : undefined)));
    close = srv.close;
    let started = false;
    const w = workers[0]![3](srv.endpoint, 2500, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    await w.drain();
    await done;
    expect(srv.ops().filter((o) => o === OpCode.WorkerDrain).length).toBe(3); // two lost, one landed
    expect(srv.ops().filter((o) => o === OpCode.ActionComplete).length).toBe(1);
  }, 10_000);

  it("a stop isn't held up by a notice the server never answers", async () => {
    const srv = await fakeServer(workThenEmpty(OpCode.ActionAwait, oneTask(), (op) => (op === OpCode.WorkerDrain ? "park" : undefined)));
    close = srv.close;
    let started = false;
    const w = workers[0]![3](srv.endpoint, 300, () => (started = true));
    const done = w.start();
    await until(() => started, 3000);
    const draining = w.drain(); // its notice hangs, and so does the next
    // The second is the run's own, sent once the held poll returns.
    await until(() => srv.ops().filter((o) => o === OpCode.WorkerDrain).length >= 2, 3000);
    const t0 = Date.now();
    w.stop();
    await draining;
    await done;
    expect(Date.now() - t0).toBeLessThan(2000);
  });

  it("an idle worker doesn't keep trying", async () => {
    const srv = await fakeServer((op) => (op === OpCode.ActionAwait ? { delayMs: BLOCK_MS } : op === OpCode.WorkerDrain ? "drop" : undefined));
    close = srv.close;
    const w = workers[0]![3](srv.endpoint, 0);
    const done = w.start();
    await until(() => srv.ops().includes(OpCode.ActionAwait), 3000);
    await w.drain();
    await done;
    // drain()'s own try, and the one as the run ends.
    expect(srv.ops().filter((o) => o === OpCode.WorkerDrain).length).toBeLessThanOrEqual(2);
  });
});

describe("StreamWorker", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("can be started again after a stop", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? { delayMs: BLOCK_MS, data: records(0) } : undefined));
    close = srv.close;
    const w = workers[1]![3](srv.endpoint, 0);
    for (let run = 0; run < 2; run++) {
      const done = w.start();
      const before = srv.ops().filter((o) => o === OpCode.StreamGroupRead).length;
      await until(() => srv.ops().filter((o) => o === OpCode.StreamGroupRead).length > before, 3000);
      w.stop();
      await done;
    }
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupJoin).length).toBe(2);
  });

  it("a stop hands back the records of its last batch it hasn't started", async () => {
    const srv = await fakeServer((op, nth) => (op === OpCode.StreamGroupRead ? (nth === 0 ? { data: records(3) } : { delayMs: BLOCK_MS, data: records(0) }) : undefined));
    close = srv.close;
    let started = false;
    const w = workers[1]![3](srv.endpoint, 300, () => (started = true), 1);
    const done = w.start();
    await until(() => started, 3000);
    w.stop();
    await done;
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupAck).length).toBe(1);
    const nacks = srv.seen.filter((s) => s.op === OpCode.StreamGroupNack);
    expect(nacks.length).toBe(1);
    // [group_len:u16][group][consumer_len:u16][consumer][count:u32]([ts:u64][seq:u64])*
    const v = valueOf(nacks[0]!.payload);
    let at = 2 + v.readUInt16LE(0);
    at += 2 + v.readUInt16LE(at);
    const count = v.readUInt32LE(at);
    const seqs = Array.from({ length: count }, (_, i) => v.readBigUInt64LE(at + 4 + i * 16 + 8));
    expect(seqs).toEqual([2n, 3n]); // the two it never started
  });
});

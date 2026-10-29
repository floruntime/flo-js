/**
 * A worker's supervision calls (registering, joining its group, polling)
 * wait out a halted shard and carry on by themselves once it is repaired; a
 * stop or drain ends the wait.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { OpCode, OutcomePendingError, Ran, Reason, StatusCode } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";
import { HALT_MAX_BACKOFF_MS, HaltBackoff } from "../packages/node/src/poll-backoff.js";
import { fakeServer, oneRecord, oneTask, quiet, refusal, sleep, until } from "./fake_worker_server.js";

const halted = () => refusal(StatusCode.Unavailable, Reason.ShardHalted, "shard 0 halted at entry 12");
const PENDING_MSG = "committed at 12, where the shard halted: a repair skips it; don't resend it unchanged";
const pending = () => ({
  status: StatusCode.OutcomePending,
  data: new Uint8Array([Reason.ShardHalted, 0, Ran.Unknown, ...new TextEncoder().encode(PENDING_MSG)]),
});

type Started = { stop: () => void; drain: () => Promise<void>; done: Promise<void>; warned: string[] };

function logger(warned: string[]) {
  return { debug: () => {}, warn: (m: string) => void warned.push(m), error: () => {} };
}

const workers: [string, number[], number, (endpoint: string) => Started][] = [
  ["ActionWorker", [OpCode.ActionRegister, OpCode.WorkerRegister, OpCode.ActionAwait], OpCode.ActionAwait, (endpoint) => {
    const warned: string[] = [];
    const w = new ActionWorker({ endpoint, blockMs: 30000, logger: logger(warned) });
    w.action("a", async () => new Uint8Array(0));
    return { stop: () => w.stop(), drain: () => w.drain(), done: w.start(), warned };
  }],
  ["StreamWorker", [OpCode.StreamGroupJoin, OpCode.WorkerRegister, OpCode.StreamGroupRead], OpCode.StreamGroupRead, (endpoint) => {
    const warned: string[] = [];
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, logger: logger(warned) }, async () => {});
    return { stop: () => w.stop(), drain: () => w.drain(), done: w.start(), warned };
  }],
];

describe.each(workers)("%s on a halted shard", (_name, calls, poll, start) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  for (const call of calls) {
    it(`waits out halt refusals of op 0x${call.toString(16)}, then carries on`, async () => {
      // Refused three times, then the shard is repaired; the poll after that parks.
      const srv = await fakeServer((op, nth) => {
        if (op === call && nth < 3) return halted();
        if (op === poll) return "park";
        return undefined;
      });
      close = srv.close;
      const w = start(srv.endpoint);
      const count = (op: number) => srv.ops().filter((o) => o === op).length;
      await until(() => count(poll) >= (call === poll ? 4 : 1), 5000);
      w.stop();
      srv.close();
      await w.done;
      expect(count(call)).toBeGreaterThanOrEqual(4);
      expect(count(poll)).toBeGreaterThanOrEqual(call === poll ? 4 : 1);
      expect(w.warned.filter((m) => m.includes("refused, shard halted: shard 0 halted at entry 12; retrying in"))).toHaveLength(3);
    });
  }

  for (const call of calls.filter((c) => c !== poll)) {
    for (const end of ["stop", "drain"] as const) {
      it(`ends a wait on op 0x${call.toString(16)} on a shard that stays halted at ${end}()`, async () => {
        const srv = await fakeServer((op) => (op === call ? halted() : undefined));
        close = srv.close;
        const w = start(srv.endpoint);
        const count = () => srv.ops().filter((o) => o === call).length;
        // Past the first few waits, so the next one is long.
        await until(() => count() >= 5, 10000);
        const asked = Date.now();
        if (end === "stop") w.stop();
        else await w.drain();
        await w.done;
        expect(Date.now() - asked).toBeLessThan(1000);
        const after = count();
        await sleep(300);
        expect(count()).toBe(after);
        expect(srv.ops()).not.toContain(poll);
      }, 15000);
    }
  }

  // Committed but not applied: resending it would write it twice, or halt
  // the shard again on every repair.
  for (const call of calls) {
    it(`leaves start() with OutcomePendingError on op 0x${call.toString(16)}, after one attempt`, async () => {
      const srv = await fakeServer((op) => (op === call ? pending() : op === poll ? "park" : undefined));
      close = srv.close;
      const w = start(srv.endpoint);
      const err = await w.done.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(OutcomePendingError);
      expect(err).toMatchObject({ status: StatusCode.OutcomePending, reason: Reason.ShardHalted, ran: Ran.Unknown, serverMessage: PENDING_MSG });
      expect(srv.ops().filter((o) => o === call)).toHaveLength(1);
      expect(w.warned.filter((m) => m.includes("shard halted"))).toHaveLength(0);
    }, 5000);
  }
});

describe("the halt backoff", () => {
  afterEach(() => vi.restoreAllMocks());

  it("doubles from 100 ms to 30 s and no further", () => {
    expect(HALT_MAX_BACKOFF_MS).toBe(30_000);
    // The top of each jittered range: the ceiling itself.
    vi.spyOn(Math, "random").mockReturnValue(0.9999999999);
    const b = new HaltBackoff(quiet);
    const waits = Array.from({ length: 14 }, () => b.nextWait());
    expect(waits.slice(0, 9)).toEqual([100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600]);
    expect(waits.slice(9)).toEqual([30_000, 30_000, 30_000, 30_000, 30_000]);
    expect(Math.max(...waits)).toBe(30_000);
  });
});

// A handler's result is a write: refused by a halted shard, it is logged and
// dropped, never resent.
const results: [string, number, number, (endpoint: string) => { stop: () => void; done: Promise<void> }][] = [
  ["ActionWorker", OpCode.ActionAwait, OpCode.ActionComplete, (endpoint) => {
    const w = new ActionWorker({ endpoint, blockMs: 30000, logger: quiet });
    w.action("a", async () => new Uint8Array(0));
    return { stop: () => w.stop(), done: w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, OpCode.StreamGroupAck, (endpoint) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, logger: quiet }, async () => {});
    return { stop: () => w.stop(), done: w.start() };
  }],
];

describe.each(results)("%s's result on a halted shard", (_name, poll, result, start) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  for (const [what, answer] of [["unavailable, shard halted", halted], ["outcome pending", pending]] as const) {
    it(`is sent once when refused ${what}`, async () => {
      const work = poll === OpCode.ActionAwait ? oneTask() : oneRecord();
      const srv = await fakeServer((op, nth) => {
        if (op === poll) return nth === 0 ? { data: work } : "park";
        if (op === result) return answer();
        return undefined;
      });
      close = srv.close;
      const w = start(srv.endpoint);
      await until(() => srv.ops().includes(result), 3000);
      await sleep(500);
      const sent = srv.ops().filter((o) => o === result).length;
      // The parked poll ends only when the connection does.
      w.stop();
      srv.close();
      await w.done;
      expect(sent).toBe(1);
    });
  }
});

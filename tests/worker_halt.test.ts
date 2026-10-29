/**
 * A worker's supervision calls (registering, joining its group, polling)
 * wait out a halted shard and carry on by themselves once it is repaired; a
 * stop or drain ends the wait.
 */

import { afterEach, describe, expect, it } from "vitest";
import { OpCode, Reason, StatusCode } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";
import { fakeServer, refusal, until } from "./fake_worker_server.js";

const halted = () => refusal(StatusCode.Unavailable, Reason.ShardHalted, "shard 0 halted at entry 12");

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

  for (const end of ["stop", "drain"] as const) {
    it(`ends a wait on a shard that stays halted at ${end}()`, async () => {
      const srv = await fakeServer((op) => (op === calls[0] ? halted() : undefined));
      close = srv.close;
      const w = start(srv.endpoint);
      // Past the first few waits, so the next one is long.
      await until(() => srv.ops().filter((o) => o === calls[0]).length >= 5, 10000);
      const asked = Date.now();
      if (end === "stop") w.stop();
      else await w.drain();
      await w.done;
      expect(Date.now() - asked).toBeLessThan(1000);
      expect(srv.ops()).not.toContain(poll);
    });
  }
});

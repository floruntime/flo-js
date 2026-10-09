/**
 * Worker poll pacing: back off when blocking polls keep coming back empty
 * at once (a full waiter pool), but not when a parked group read is woken
 * empty by an append, and never hold up stop().
 */

import * as net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { HEADER_SIZE, MAGIC, OpCode, VERSION, computeCRC32 } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";

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

/** A group read answer holding one record with sequence `seq`. */
function oneRecord(seq: number): Uint8Array {
  const buf = new Uint8Array(4 + 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1, true);
  view.setBigUint64(4, BigInt(seq), true);
  view.setUint32(26, 1, true); // payload_len
  buf[30] = 0x78;
  return buf;
}

type Answer = { delayMs: number; data?: Uint8Array } | null;
type Script = (op: number, nth: number) => Answer | undefined;

/**
 * A server that answers per `script(op, nth)`: undefined means an empty OK
 * at once, null means never.
 */
async function fakeServer(script: Script) {
  const polls: { op: number; at: number }[] = [];
  const answered: { op: number; at: number; empty: boolean }[] = [];
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
        const nth = counts.get(op) ?? 0;
        counts.set(op, nth + 1);
        polls.push({ op, at: Date.now() });
        const a = script(op, nth);
        if (a === null) continue;
        const send = () => {
          if (sock.destroyed) return;
          answered.push({ op, at: Date.now(), empty: !a?.data });
          sock.write(reply(id, a?.data));
        };
        if (a === undefined) send();
        else setTimeout(send, a.delayMs);
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    polls,
    answered,
    close: () => {
      sockets.forEach((s) => s.destroy());
      server.close();
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const NEVER = 60_000;

type Started = { stop: () => void; done: Promise<void> };
const workers: [string, number, (endpoint: string) => Started][] = [
  ["ActionWorker", OpCode.ActionAwait, (endpoint) => {
    const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs: 0 });
    w.action("a", async () => new Uint8Array(0));
    return { stop: () => w.stop(), done: w.start() };
  }],
  ["StreamWorker", OpCode.StreamGroupRead, (endpoint) => {
    const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0 }, async () => {});
    return { stop: () => w.stop(), done: w.start() };
  }],
];

describe.each(workers)("%s against a full waiter pool", (_name, pollOp, start) => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("backs off when every poll is answered empty at once", async () => {
    const srv = await fakeServer(() => undefined);
    close = srv.close;
    const w = start(srv.endpoint);
    await sleep(600);
    w.stop();
    await w.done;
    const polls = srv.polls.filter((p) => p.op === pollOp).length;
    expect(polls).toBeGreaterThan(1);
    expect(polls).toBeLessThan(20);
  });

  it("stops at once during a pause", async () => {
    const srv = await fakeServer(() => undefined);
    close = srv.close;
    const w = start(srv.endpoint);
    // Pauses so far 0, 50, 100, 200, 400 end near 750 ms; then one of 800.
    await sleep(900);
    const stopped = Date.now();
    w.stop();
    await w.done;
    expect(Date.now() - stopped).toBeLessThan(100);
  });
});

describe("StreamWorker group read wakes", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("re-reads promptly after append wakes", async () => {
    // Each cycle: two wakes 20 ms after parking, then the record. Acks are
    // left unanswered so no reply crosses a parked read.
    const srv = await fakeServer((op, nth) => {
      if (op === OpCode.StreamGroupAck) return null;
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (nth >= 9) return { delayMs: NEVER };
      return nth % 3 === 2 ? { delayMs: 0, data: oneRecord(nth) } : { delayMs: 20 };
    });
    close = srv.close;
    const handled: number[] = [];
    const w = new StreamWorker(
      srv.endpoint,
      { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0 },
      async () => {
        handled.push(Date.now());
      }
    );
    const done = w.start();
    await sleep(800);
    w.stop();
    srv.close();
    await done;

    const wakes = srv.answered.filter((a) => a.op === OpCode.StreamGroupRead && a.empty);
    expect(handled.length).toBe(3);
    for (let i = 0; i < 3; i++) {
      expect(handled[i]! - wakes[2 * i + 1]!.at).toBeLessThan(100);
    }
    // The first early empty in a row re-reads at once; the second pauses 50 ms.
    // Checked on the first cycle only: later cycles have an ack in flight, and
    // on a transport without per-id routing that ack can take a read's reply.
    const reads = srv.polls.filter((p) => p.op === OpCode.StreamGroupRead);
    expect(reads[1]!.at - wakes[0]!.at).toBeLessThan(30);
    expect(reads[2]!.at - wakes[1]!.at).toBeGreaterThanOrEqual(45);
  });

  it("never pauses after empties that took longer than 250 ms", async () => {
    // A consumer that keeps losing the re-read: woken empty after 300 ms.
    const srv = await fakeServer((op, nth) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      return { delayMs: nth < 4 ? 300 : NEVER };
    });
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0 }, async () => {});
    const done = w.start();
    await sleep(1400);
    w.stop();
    srv.close();
    await done;

    const reads = srv.polls.filter((p) => p.op === OpCode.StreamGroupRead);
    const wakes = srv.answered.filter((a) => a.op === OpCode.StreamGroupRead);
    expect(wakes.length).toBe(4);
    for (let i = 0; i < 4; i++) {
      expect(reads[i + 1]!.at - wakes[i]!.at).toBeLessThan(30);
    }
  });
});

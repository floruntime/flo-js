/**
 * `unavailable`, not run, is routine: a group read whose records are being
 * read back from disk is refused that way, having claimed and delivered
 * nothing. A StreamWorker reads again, with the same group and consumer,
 * soon and quietly, and handles the records once they come.
 */

import { afterEach, describe, expect, it } from "vitest";
import { OpCode, Reason, StatusCode, StorageTier, UnavailableError, parseStreamReadResponse } from "@floruntime/core";
import { FloClient, StreamWorker } from "@floruntime/node";
import { fakeServer, oneRecord, refusal, until } from "./fake_worker_server.js";
import { NOT_READY_MAX_MS, NOT_READY_WARN_EVERY_MS, NotReadyWarnings, newNotReadyBackoff } from "../packages/node/src/poll-backoff.js";

// The server's texts for a stream read waiting on its disk.
const DISK_TEXTS = [
  "unavailable: the stream's records are being read from disk — retry",
  "unavailable: the stream's records are being read from disk and weren't back in time — retry",
  "unavailable: the shard's cache of records read from disk is full of ones not yet served — retry",
  "unavailable: the stream's records haven't reached the durable log yet — retry",
];

let close: (() => void) | undefined;
afterEach(() => close?.());

/** The group and consumer a group read names: the first two u16-prefixed strings of its value. */
function groupAndConsumer(payload: Buffer): string {
  let o = 2 + payload.readUInt16LE(0); // namespace
  o += 2 + payload.readUInt16LE(o); // key (stream)
  o += 4; // value length
  const g = payload.readUInt16LE(o);
  const group = payload.subarray(o + 2, o + 2 + g).toString();
  o += 2 + g;
  const c = payload.readUInt16LE(o);
  return `${group}/${payload.subarray(o + 2, o + 2 + c).toString()}`;
}

describe("a group read refused unavailable, not run", () => {
  it("is read again by a StreamWorker, the same group and consumer, warning once, until the records come", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (nth < DISK_TEXTS.length) return refusal(StatusCode.Unavailable, Reason.Unclassified, DISK_TEXTS[nth]!);
      return nth === DISK_TEXTS.length ? { data: oneRecord() } : "park";
    });
    close = srv.close;
    const warned: string[] = [];
    const errors: string[] = [];
    const debug: string[] = [];
    const logger = { debug: (m: string) => void debug.push(m), warn: (m: string) => void warned.push(m), error: (m: string) => void errors.push(m) };
    let handled = 0;
    // No client retries, so every refusal reaches the worker.
    const w = new StreamWorker(srv.endpoint, { stream: "s", group: "g1", consumer: "c1", blockMs: 30000, logger, retry: { maxAttempts: 1 } }, async () => {
      handled++;
    });
    const done = w.start();
    const started = Date.now();
    await until(() => handled === 1, 5000);
    const took = Date.now() - started;
    // The handler has run, but its ack goes out after it returns: wait for it
    // to reach the server before closing that server, or the ack is dropped
    // for want of one (the worker's documented end of a result once stopped).
    await until(() => srv.ops().includes(OpCode.StreamGroupAck), 5000);
    expect(srv.ops()).toContain(OpCode.StreamGroupAck);
    w.stop();
    srv.close();
    await done;
    expect(handled).toBe(1);
    // 50 ms doubling: four waits of at most 50, 100, 200, 400 ms.
    expect(took).toBeLessThan(1500);
    const reads = srv.seen.filter((s) => s.op === OpCode.StreamGroupRead);
    expect(reads.length).toBeGreaterThanOrEqual(DISK_TEXTS.length + 1);
    for (const r of reads) expect(groupAndConsumer(r.payload)).toBe("g1/c1");
    // The first of the run warns, naming the stream and the server's text; the rest go to debug.
    expect(errors).toEqual([]);
    const notReady = warned.filter((m) => m.startsWith("GroupRead on stream s not ready"));
    expect(notReady).toHaveLength(1);
    expect(notReady[0]).toContain(DISK_TEXTS[0]);
    expect(debug.filter((m) => m.startsWith("GroupRead on stream s not ready"))).toHaveLength(DISK_TEXTS.length - 1);
  });

  it("is retried by the client, and once its retries run out, thrown as UnavailableError with ran no", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamRead ? refusal(StatusCode.Unavailable, Reason.Unclassified, DISK_TEXTS[0]!) : undefined));
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 3, initialBackoffMs: 1, maxBackoffMs: 2 } });
    await client.connect();
    try {
      const err = await client.stream.read("s").catch((e: unknown) => e);
      expect(err).toBeInstanceOf(UnavailableError);
      expect(err).toMatchObject({ status: 12, serverMessage: DISK_TEXTS[0] });
      expect(srv.ops().filter((o) => o === OpCode.StreamRead)).toHaveLength(3);
    } finally {
      await client.close();
    }
  });
});

describe("a run of not-ready refusals", () => {
  it("warns at its first, then at most every 30 s while it goes on, and again after a read goes through", () => {
    let now = 0;
    const w = new NotReadyWarnings(() => now);
    expect(w.due()).toBe(true);
    now = 1;
    expect(w.due()).toBe(false);
    now = NOT_READY_WARN_EVERY_MS - 1;
    expect(w.due()).toBe(false);
    now = NOT_READY_WARN_EVERY_MS;
    expect(w.due()).toBe(true);
    now += 5000;
    expect(w.due()).toBe(false);
    w.reset();
    expect(w.due()).toBe(true);
  });

  it("is read again from 50 ms, the ceiling doubling to 5 s and no further", () => {
    const b = newNotReadyBackoff();
    const waits = Array.from({ length: 40 }, () => b.next());
    expect(Math.max(...waits)).toBeLessThanOrEqual(NOT_READY_MAX_MS);
    expect(NOT_READY_MAX_MS).toBe(5000);
    // Full jitter: with the ceiling at 5 s, 30 waits not one over 2.5 s is all but impossible.
    expect(waits.slice(10).some((d) => d > 2500)).toBe(true);
    b.reset();
    expect(b.next()).toBeLessThanOrEqual(50);
  });
});

describe("stream record tier", () => {
  // [count:u32]([seq:u64][ts:i64][tier:u8][partition:u32][key_present:u8][payload_len:u32][payload][header_count:u32])*
  function withTier(tier: number): Uint8Array {
    const buf = oneRecord();
    buf[20] = tier;
    return buf;
  }

  it("reads the server's tiers, 0 hot, 1 warm, 2 disk, and a code it doesn't know as Unknown", () => {
    expect([0, 1, 2, 3, 200].map((t) => parseStreamReadResponse(withTier(t)).records[0]!.tier)).toEqual([
      StorageTier.Hot,
      StorageTier.Warm,
      StorageTier.Disk,
      StorageTier.Unknown,
      StorageTier.Unknown,
    ]);
  });
});

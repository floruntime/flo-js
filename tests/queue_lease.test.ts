/**
 * Queues lease what a dequeue takes until it is completed: dequeue,
 * complete, fail and touch are table ops, and a report names the
 * consumer, the delivery's token and each message's seq and deliveries.
 * Per-message refusals come inside an ok answer.
 */

import { describe, expect, it } from "vitest";
import {
  BlockTooLongError,
  FloError,
  IncompleteResponseError,
  OpCode,
  OptionTag,
  OverloadedError,
  QueueOperations,
  QueueRefusal,
  Ran,
  Reason,
  StatusCode,
} from "@floruntime/core";
import { FloClient } from "@floruntime/node";
import { fakeServer } from "./fake_worker_server.js";

const enc = new TextEncoder();
const text = (s: string) => Array.from(enc.encode(s));
const le = (v: bigint, n: number) => Array.from({ length: n }, (_, i) => Number((BigInt.asUintN(64, v) >> BigInt(8 * i)) & 0xffn));

type Sent = { op: number; ns: string; key: string; value: Uint8Array; options: Uint8Array };

function answering(...answers: (number[] | Uint8Array)[]): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, ns, key: new TextDecoder().decode(key), value, options });
      return { status: StatusCode.OK, data: Uint8Array.from(answers[sent.length - 1] ?? []), requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

// [count:u8]([seq:u64][priority:u32][deliveries:u32][enqueued_at_ms:i64][payload:u32 len])*[term:u64][index:u64][expires_at_ms:i64]
const message = (seq: bigint, deliveries: number, payload: string) => [...le(seq, 8), ...le(2n, 4), ...le(BigInt(deliveries), 4), ...le(1700n, 8), ...le(BigInt(payload.length), 4), ...text(payload)];
const dequeued = [2, ...message(7n, 1, "a"), ...message(9n, 2, "bc"), ...le(3n, 8), ...le(41n, 8), ...le(31700n, 8)];
// consumer [1][u16]["c1"], term, index, [count:u8]([seq:u64][deliveries:u32])*
const delivery = [1, 2, 0, ...text("c1"), ...le(3n, 8), ...le(41n, 8), 2, ...le(7n, 8), ...le(1n, 4), ...le(9n, 8), ...le(2n, 4)];

describe("queue.dequeue", () => {
  it("sends consumer, count, lease_ms, block_ms and ack in its body, and reads the lease it took", async () => {
    const { sender, sent } = answering(dequeued);
    const got = await new QueueOperations(sender).dequeue("jobs", 2, { consumer: "c1", leaseMs: 60000, blockMs: 500, ack: false });
    expect(got).toEqual({
      consumer: "c1",
      term: 3n,
      index: 41n,
      expiresAtMs: 31700n,
      messages: [
        { seq: 7n, payload: enc.encode("a"), enqueuedAtMs: 1700n, deliveries: 1, priority: 2 },
        { seq: 9n, payload: enc.encode("bc"), enqueuedAtMs: 1700n, deliveries: 2, priority: 2 },
      ],
    });
    expect(sent[0]).toMatchObject({ op: OpCode.QueueDequeue, key: "jobs" });
    expect(sent[0]!.op).toBe(0x181);
    expect(Array.from(sent[0]!.value)).toEqual([1, 2, 0, ...text("c1"), 1, ...le(2n, 4), 1, ...le(60000n, 8), 1, ...le(500n, 8), 1, 0]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("takes one, for consumer \"\", with the server's lease and no wait by default", async () => {
    const { sender, sent } = answering([0, ...le(0n, 8), ...le(0n, 8), ...le(0n, 8)]);
    expect((await new QueueOperations(sender).dequeue("jobs")).messages).toEqual([]);
    expect(Array.from(sent[0]!.value)).toEqual([1, 0, 0, 1, ...le(1n, 4), 0, 1, ...le(0n, 8), 1, 0]);
  });

  it("refuses a count, lease or wait the server would, and a cut answer", async () => {
    const q = new QueueOperations(answering(dequeued).sender);
    await expect(q.dequeue("jobs", 0)).rejects.toBeInstanceOf(FloError);
    await expect(q.dequeue("jobs", 101)).rejects.toBeInstanceOf(FloError);
    await expect(q.dequeue("jobs", 1, { leaseMs: 4999 })).rejects.toBeInstanceOf(FloError);
    await expect(q.dequeue("jobs", 1, { leaseMs: 43_200_001 })).rejects.toBeInstanceOf(FloError);
    await expect(q.dequeue("jobs", 1, { blockMs: 300_001 })).rejects.toBeInstanceOf(BlockTooLongError);
    for (let n = 0; n < dequeued.length; n++) {
      await expect(new QueueOperations(answering(dequeued.slice(0, n)).sender).dequeue("jobs")).rejects.toBeInstanceOf(IncompleteResponseError);
    }
  });
});

describe("queue reports", () => {
  const lease = { consumer: "c1", term: 3n, index: 41n, messages: [{ seq: 7n, deliveries: 1 }, { seq: 9n, deliveries: 2 }] };
  // applied, then [count:u8]([seq:u64][why:u8][reason:u8 len])*
  const report = [...le(1n, 4), 1, ...le(9n, 8), 0, 1, ...text("x")];

  it("complete names the delivery, and reads what it applied to and what it refused, inside an ok answer", async () => {
    const { sender, sent } = answering(report);
    expect(await new QueueOperations(sender).complete("jobs", lease)).toEqual({ applied: 1, refused: [{ seq: 9n, why: QueueRefusal.Gone, reason: "x" }] });
    expect(sent[0]).toMatchObject({ op: OpCode.QueueComplete, key: "jobs" });
    expect(Array.from(sent[0]!.value)).toEqual(delivery);
  });

  it("takes a dequeue's result as it stands, its messages carrying their own deliveries", async () => {
    const taken = await new QueueOperations(answering(dequeued).sender).dequeue("jobs", 2, { consumer: "c1" });
    const { sender, sent } = answering(report);
    await new QueueOperations(sender).complete("jobs", taken);
    expect(Array.from(sent[0]!.value)).toEqual(delivery);
  });

  it("fail adds delay_ms, dead and the error, cut to 1024 bytes at a character boundary", async () => {
    const { sender, sent } = answering(report, report);
    const q = new QueueOperations(sender);
    await q.fail("jobs", lease, { delayMs: 250, dead: true, error: "boom" });
    expect(Array.from(sent[0]!.value)).toEqual([...delivery, 1, ...le(250n, 8), 1, 1, 1, 4, 0, ...text("boom")]);
    await q.fail("jobs", lease, { error: "é".repeat(600) });
    const tail = sent[1]!.value.subarray(delivery.length + 9 + 2 + 1);
    expect(tail[0]! | (tail[1]! << 8)).toBe(1024);
    expect(new TextDecoder("utf-8", { fatal: true }).decode(tail.subarray(2))).toBe("é".repeat(512));
  });

  it("touch reads the lease's expiry, then what it applied to", async () => {
    const { sender, sent } = answering([...le(99000n, 8), ...le(2n, 4), 0]);
    expect(await new QueueOperations(sender).touch("jobs", lease)).toEqual({ expiresAtMs: 99000n, applied: 2, refused: [] });
    expect(sent[0]!.op).toBe(OpCode.QueueTouch);
    expect(Array.from(sent[0]!.value)).toEqual(delivery);
  });

  it("refuses a report naming no messages or more than 100, and a refusal code this client doesn't know", async () => {
    const q = new QueueOperations(answering([...le(0n, 4), 1, ...le(9n, 8), 6, 0]).sender);
    await expect(q.complete("jobs", { ...lease, messages: [] })).rejects.toBeInstanceOf(FloError);
    await expect(q.complete("jobs", { ...lease, messages: Array.from({ length: 101 }, (_, i) => ({ seq: BigInt(i), deliveries: 1 })) })).rejects.toBeInstanceOf(FloError);
    await expect(q.complete("jobs", lease)).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("queue.enqueue and purge", () => {
  it("sends delay_ms as a u64 option, only when set", async () => {
    const { sender, sent } = answering(le(5n, 8), le(6n, 8));
    const q = new QueueOperations(sender);
    expect(await q.enqueue("jobs", enc.encode("x"), { delayMs: 1500 })).toBe(5n);
    expect(Array.from(sent[0]!.options)).toEqual([OptionTag.DelayMS, 8, ...le(1500n, 8)]);
    await q.enqueue("jobs", enc.encode("x"));
    expect(sent[1]!.options.length).toBe(0);
    await expect(q.enqueue("jobs", enc.encode("x"), { delayMs: 86_400_001 })).rejects.toBeInstanceOf(FloError);
  });

  it("purges a queue and answers how many messages it removed", async () => {
    const { sender, sent } = answering(le(3n, 4));
    expect(await new QueueOperations(sender).purge("jobs")).toBe(3);
    expect(sent[0]).toMatchObject({ op: OpCode.QueuePurge, key: "jobs" });
  });

  it("retries an enqueue refused as a full queue, then throws OverloadedError with the server's message", async () => {
    const message = "overloaded: queue 'jobs' is full: it holds 10 messages (at most 10) and 10 held bytes (at most 1048576), ready, leased and dead; consume or purge it";
    const srv = await fakeServer((op) =>
      op === OpCode.QueueEnqueue
        ? { status: StatusCode.Overloaded, data: new Uint8Array([Reason.Unclassified, 0, Ran.No, ...text(message)]) }
        : undefined
    );
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 3, initialBackoffMs: 1, maxBackoffMs: 2 } });
    await client.connect();
    try {
      const err = await client.queue.enqueue("jobs", enc.encode("x")).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(OverloadedError);
      expect(err).toMatchObject({ ran: Ran.No, serverMessage: message });
      expect(srv.ops().filter((o) => o === OpCode.QueueEnqueue)).toHaveLength(3);
    } finally {
      await client.close();
      srv.close();
    }
  });
});

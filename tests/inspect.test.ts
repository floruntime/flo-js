/**
 * The reads that show what a shard holds: action.info, queue.peek, a
 * queue's dead letters, a stream's groups and members. Each request and
 * answer per the server's layout, and the stale-report refusal of a report.
 */

import { describe, expect, it } from "vitest";
import {
  ActionOperations,
  ConflictError,
  IncompleteResponseError,
  OpCode,
  QueueOperations,
  Ran,
  Reason,
  StaleReportError,
  StatusCode,
  StreamOperations,
  WorkerOperations,
  createServerError,
  isStaleReport,
} from "@floruntime/core";

const enc = new TextEncoder();
const text = (s: string) => Array.from(enc.encode(s));
const le = (v: bigint, n: number) => Array.from({ length: n }, (_, i) => Number((BigInt.asUintN(64, v) >> BigInt(8 * i)) & 0xffn));
const str16 = (s: string) => [...le(BigInt(enc.encode(s).length), 2), ...text(s)];

type Sent = { op: number; ns: string; key: string; value: Uint8Array; options: Uint8Array };

function answering(data: number[] | Uint8Array, status: number = StatusCode.OK): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, ns, key: new TextDecoder().decode(key), value, options });
      return { status, data: Uint8Array.from(data), requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

describe("action.info", () => {
  const head = [...str16("payments"), ...le(3n, 4), ...le(30000n, 8), ...le(5n, 4), ...le(1700n, 8), ...le(12n, 8), 0, 0];

  it("sends the action as the key, and reads the counts and latencies", async () => {
    const counts = [2n, 40n, 1n, 0n, 40n, 15n, 10n, 50n, 90n].flatMap((v) => [1, ...le(v, 8)]);
    const { sender, sent } = answering([...head, ...counts]);
    expect(await new ActionOperations(sender).info("charge")).toEqual({
      owner: "payments",
      version: 3,
      timeoutMs: 30000n,
      maxAttempts: 5,
      createdAtMs: 1700n,
      ready: 12n,
      readyCapped: false,
      tooManyRuns: false,
      running: 2n,
      completed: 40n,
      failed: 1n,
      timedOut: 0n,
      latencyCount: 40n,
      latencyAvgMs: 15n,
      latencyP50Ms: 10n,
      latencyP95Ms: 50n,
      latencyMaxMs: 90n,
    });
    expect(sent).toEqual([{ op: OpCode.ActionInfo, ns: "default", key: "charge", value: new Uint8Array(0), options: new Uint8Array(0) }]);
  });

  it("leaves the counts out when the shard holds too many runs, and refuses a cut answer", async () => {
    const busy = [...head.slice(0, -1), 1, ...Array(9).fill(0)];
    const info = await new ActionOperations(answering(busy).sender).info("charge");
    expect([info.tooManyRuns, info.running, info.latencyMaxMs]).toEqual([true, undefined, undefined]);
    await expect(new ActionOperations(answering(busy.slice(0, -1)).sender).info("charge")).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("queue.peek", () => {
  it("sends the limit in its body and reads the messages, then whether more may follow", async () => {
    const msg = [...le(7n, 8), ...le(2n, 4), ...le(1n, 4), ...le(1700n, 8), ...le(5000n, 4), ...le(2n, 2), ...text("hi")];
    const { sender, sent } = answering([1, ...msg, 1]);
    expect(await new QueueOperations(sender).peek("jobs", 5)).toEqual({
      messages: [{ seq: 7n, priority: 2, attempts: 1, enqueuedAtMs: 1700n, size: 5000, payload: enc.encode("hi") }],
      more: true,
    });
    expect(sent[0]!.op).toBe(OpCode.QueuePeek);
    expect(sent[0]!.op).toBe(0x180);
    expect(Array.from(sent[0]!.value)).toEqual([1, ...le(5n, 4)]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("asks for 20 by default, and refuses a limit outside 1 to 100", async () => {
    const { sender, sent } = answering([0, 0]);
    expect(await new QueueOperations(sender).peek("jobs")).toEqual({ messages: [], more: false });
    expect(Array.from(sent[0]!.value)).toEqual([1, ...le(20n, 4)]);
    await expect(new QueueOperations(sender).peek("jobs", 0)).rejects.toThrow();
    await expect(new QueueOperations(sender).peek("jobs", 101)).rejects.toThrow();
  });
});

describe("queue.dlqList", () => {
  const letter = (seq: bigint, payload: string, size: number) => [
    ...le(seq, 8),
    ...le(BigInt(size), 4),
    ...le(BigInt(payload.length), 4),
    ...text(payload),
    ...le(1600n, 8),
    ...le(3n, 4),
    9,
    ...le(1700n, 8),
  ];

  it("sends [limit:u32][after:u64] in the value, not options, and reads the page's trailer", async () => {
    const { sender, sent } = answering([...le(2n, 4), ...letter(4n, "a", 1), ...letter(6n, "bb", 9000), ...le(5n, 8), 1, ...le(6n, 8)]);
    const page = await new QueueOperations(sender).dlqList("jobs", { limit: 2, after: 3n });
    expect(page).toEqual({
      messages: [
        { seq: 4n, size: 1, payload: enc.encode("a"), enqueuedAtMs: 1600n, attempts: 3, priority: 9, movedAtMs: 1700n },
        { seq: 6n, size: 9000, payload: enc.encode("bb"), enqueuedAtMs: 1600n, attempts: 3, priority: 9, movedAtMs: 1700n },
      ],
      total: 5n,
      hasMore: true,
      lastSeq: 6n,
    });
    expect(Array.from(sent[0]!.value)).toEqual([...le(2n, 4), ...le(3n, 8)]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("asks for a first page of 100 by default, leaving after out", async () => {
    const { sender, sent } = answering([...le(0n, 4), ...le(0n, 8), 0, ...le(0n, 8)]);
    expect(await new QueueOperations(sender).dlqList("jobs")).toEqual({ messages: [], total: 0n, hasMore: false, lastSeq: 0n });
    expect(Array.from(sent[0]!.value)).toEqual(le(100n, 4));
  });

  it("refuses a cut answer and a limit outside 1 to 100", async () => {
    const whole = [...le(1n, 4), ...letter(4n, "a", 1), ...le(1n, 8), 0, ...le(4n, 8)];
    for (let n = 0; n < whole.length; n++) {
      await expect(new QueueOperations(answering(whole.slice(0, n)).sender).dlqList("q")).rejects.toBeInstanceOf(IncompleteResponseError);
    }
    await expect(new QueueOperations(answering(whole).sender).dlqList("q", { limit: 101 })).rejects.toThrow();
  });
});

describe("stream.groupList and groupMembers", () => {
  it("reads a stream's groups", async () => {
    const group = [...str16("g1"), ...le(2n, 4), ...le(5n, 8), ...le(1600n, 8), ...le(30000n, 4), 10, ...le(1700n, 8), ...le(42n, 8)];
    const { sender, sent } = answering([...le(1n, 2), ...group, 0]);
    expect(await new StreamOperations(sender).groupList("events")).toEqual({
      groups: [{ name: "g1", members: 2, pending: 5n, createdAtMs: 1600n, ackTimeoutMs: 30000, maxDeliver: 10, lastDeliveredMs: 1700n, lastDeliveredSeq: 42n }],
      more: false,
    });
    expect(sent[0]).toMatchObject({ op: OpCode.StreamGroupList, key: "events" });
    expect(sent[0]!.value.length).toBe(0);
  });

  it("sends the group in the body, and reads each member's state and last activity", async () => {
    const m1 = [...str16("c1"), 0, ...le(3n, 4), ...le(1600n, 8), 1, ...le(1700n, 8)];
    const m2 = [...str16("c2"), 1, ...le(0n, 4), ...le(1650n, 8), 0];
    const { sender, sent } = answering([...le(2n, 2), ...m1, ...m2, 1]);
    expect(await new StreamOperations(sender).groupMembers("events", "g1")).toEqual({
      members: [
        { id: "c1", state: "active", pending: 3, joinedAtMs: 1600n, lastActiveMs: 1700n },
        { id: "c2", state: "leaving", pending: 0, joinedAtMs: 1650n, lastActiveMs: undefined },
      ],
      more: true,
    });
    expect(sent[0]).toMatchObject({ op: OpCode.StreamGroupMembers, key: "events" });
    expect(Array.from(sent[0]!.value)).toEqual(str16("g1"));
    const bad = [...le(1n, 2), ...str16("c1"), 2, ...le(0n, 4), ...le(0n, 8), 0, 0];
    await expect(new StreamOperations(answering(bad).sender).groupMembers("events", "g1")).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("a stale report", () => {
  const body = (reason: Reason, message: string) => Uint8Array.from([reason & 0xff, reason >> 8, Ran.No, ...text(message)]);
  const task = { runId: "r1", namespace: "ns", deliveries: 2, term: 1n, index: 9n, action: "a" };

  it("is a StaleReportError, a kind of ConflictError, carrying the server's message", () => {
    const err = createServerError(StatusCode.Conflict, body(Reason.StaleReport, "stale report: the run was delivered again since"));
    expect(err).toBeInstanceOf(StaleReportError);
    expect(err).toBeInstanceOf(ConflictError);
    expect(isStaleReport(err)).toBe(true);
    expect(err).toMatchObject({ reason: Reason.StaleReport, ran: Ran.No, serverMessage: "stale report: the run was delivered again since" });
  });

  it("is not any other conflict", () => {
    const err = createServerError(StatusCode.Conflict, body(Reason.CasMismatch, "cas mismatch"));
    expect(err).toBeInstanceOf(ConflictError);
    expect(err).not.toBeInstanceOf(StaleReportError);
    expect(isStaleReport(err)).toBe(false);
  });

  it("throws from complete, fail and touch; each goes to the run's namespace, keyed by its run id", async () => {
    const { sender, sent } = answering(body(Reason.StaleReport, "stale report: the run was completed"), StatusCode.Conflict);
    const w = new WorkerOperations(sender);
    await expect(w.complete("w1", task, new Uint8Array(0))).rejects.toBeInstanceOf(StaleReportError);
    await expect(w.fail("w1", task, "x")).rejects.toBeInstanceOf(StaleReportError);
    await expect(w.touch("w1", task)).rejects.toBeInstanceOf(StaleReportError);
    expect(sent.map((s) => [s.op, s.ns, s.key])).toEqual([
      [OpCode.ActionComplete, "ns", "r1"],
      [OpCode.ActionFail, "ns", "r1"],
      [OpCode.ActionTouch, "ns", "r1"],
    ]);
  });
});

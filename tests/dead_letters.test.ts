/**
 * Dead letters, a queue's and a consumer group's (list, requeue, delete),
 * and a group steal: each request and answer per the server's table, and
 * the refusal codes not_dead and not_idle.
 */

import { describe, expect, it } from "vitest";
import {
  IncompleteResponseError,
  OpCode,
  QueueOperations,
  QueueRefusal,
  StatusCode,
  StreamID,
  StreamOperations,
  parseGroupReportResponse,
} from "@floruntime/core";

const enc = new TextEncoder();
const text = (s: string) => Array.from(enc.encode(s));
const le = (v: bigint, n: number) => Array.from({ length: n }, (_, i) => Number((BigInt.asUintN(64, v) >> BigInt(8 * i)) & 0xffn));
const str8 = (s: string) => [enc.encode(s).length, ...text(s)];
const str16 = (s: string) => [...le(BigInt(enc.encode(s).length), 2), ...text(s)];

type Sent = { op: number; ns: string; key: string; value: Uint8Array; options: Uint8Array };

function answering(...answers: number[][]): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, ns, key: new TextDecoder().decode(key), value, options });
      return { status: StatusCode.OK, data: Uint8Array.from(answers[Math.min(sent.length, answers.length) - 1] ?? []), requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

/** Every prefix of a whole answer is refused as cut short, and a byte past it as left over. */
async function refusesCut(whole: number[], call: (sender: never) => Promise<unknown>): Promise<void> {
  for (let n = 0; n < whole.length; n++) await expect(call(answering(whole.slice(0, n)).sender)).rejects.toBeInstanceOf(IncompleteResponseError);
  await expect(call(answering([...whole, 0]).sender)).rejects.toBeInstanceOf(IncompleteResponseError);
}

describe("queue dead letters", () => {
  const letter = (seq: bigint, payload: string, error: string) => [
    ...le(seq, 8),
    ...le(9n, 4), // priority
    ...le(5n, 4), // deliveries
    ...le(4n, 4), // failures
    ...le(1600n, 8), // enqueued_at_ms
    ...le(1700n, 8), // dead_at_ms
    ...le(9000n, 4), // size
    ...str16(payload),
    ...str16(error),
  ];
  const cursor = [...le(1_700_000_000n, 8), ...le(6n, 8)];

  it("lists a page: limit and no cursor first, then the cursor the page answered", async () => {
    const page = [2, ...letter(4n, "a", "boom"), ...letter(6n, "bb", ""), 1, ...cursor];
    const { sender, sent } = answering(page, [0, 0]);
    const q = new QueueOperations(sender);
    const first = await q.dlqList("jobs", { limit: 2 });
    expect(first).toEqual({
      messages: [
        { seq: 4n, priority: 9, deliveries: 5, failures: 4, enqueuedAtMs: 1600n, deadAtMs: 1700n, size: 9000, payload: enc.encode("a"), error: "boom" },
        { seq: 6n, priority: 9, deliveries: 5, failures: 4, enqueuedAtMs: 1600n, deadAtMs: 1700n, size: 9000, payload: enc.encode("bb"), error: "" },
      ],
      cursor: Uint8Array.from(cursor),
    });
    expect(sent[0]).toMatchObject({ op: OpCode.QueueDLQList, key: "jobs" });
    expect(Array.from(sent[0]!.value)).toEqual([1, ...le(2n, 4), 0]);
    expect(await q.dlqList("jobs", { cursor: first.cursor })).toEqual({ messages: [], cursor: null });
    expect(Array.from(sent[1]!.value)).toEqual([1, ...le(20n, 4), 1, ...cursor]);
  });

  it("refuses a cut answer, a limit outside 1 to 100 and a cursor not a page's", async () => {
    await refusesCut([1, ...letter(4n, "a", "e"), 1, ...cursor], (s) => new QueueOperations(s).dlqList("q"));
    const q = new QueueOperations(answering([0, 0]).sender);
    await expect(q.dlqList("q", { limit: 0 })).rejects.toThrow(/limit/);
    await expect(q.dlqList("q", { limit: 101 })).rejects.toThrow(/limit/);
    await expect(q.dlqList("q", { cursor: new Uint8Array(15) })).rejects.toThrow(/cursor/);
  });

  it("requeues by seq, and reads not_dead and not_idle refusals", async () => {
    const answer = [...le(1n, 4), 2, ...le(7n, 8), QueueRefusal.NotDead, ...str8("not dead"), ...le(8n, 8), QueueRefusal.NotIdle, ...str8("not idle"), 0];
    const { sender, sent } = answering(answer);
    expect(await new QueueOperations(sender).dlqRequeue("jobs", [5n, 7n, 8n])).toEqual({
      applied: 1,
      refused: [
        { seq: 7n, why: QueueRefusal.NotDead, reason: "not dead" },
        { seq: 8n, why: QueueRefusal.NotIdle, reason: "not idle" },
      ],
      more: false,
    });
    expect(sent[0]).toMatchObject({ op: OpCode.QueueDLQRequeue, key: "jobs" });
    expect(Array.from(sent[0]!.value)).toEqual([1, 3, ...le(5n, 8), ...le(7n, 8), ...le(8n, 8), 1, 0, 1, ...le(100n, 4)]);
  });

  it("deletes all with a limit and dry run, and reads more", async () => {
    const { sender, sent } = answering([...le(3n, 4), 0, 1]);
    expect(await new QueueOperations(sender).dlqDelete("jobs", [], { all: true, limit: 3, dryRun: true })).toEqual({ applied: 3, refused: [], more: true });
    expect(sent[0]).toMatchObject({ op: OpCode.QueueDLQDelete, key: "jobs" });
    expect(Array.from(sent[0]!.value)).toEqual([0, 1, 1, 1, ...le(3n, 4), 1, 1]);
  });

  it("takes some named or all, never both or neither, and refuses an unknown refusal code", async () => {
    const q = new QueueOperations(answering([0, 0, 0, 0, 0]).sender);
    await expect(q.dlqRequeue("q", [])).rejects.toThrow(/all/);
    await expect(q.dlqDelete("q", [1n], { all: true })).rejects.toThrow(/not both/);
    await expect(q.dlqRequeue("q", Array.from({ length: 101 }, (_, i) => BigInt(i)))).rejects.toThrow(/100/);
    const bad = [...le(0n, 4), 1, ...le(7n, 8), 8, ...str8("?"), 0];
    await expect(new QueueOperations(answering(bad).sender).dlqRequeue("q", [7n])).rejects.toBeInstanceOf(IncompleteResponseError);
    await refusesCut([...le(1n, 4), 1, ...le(7n, 8), 6, ...str8("x"), 0], (s) => new QueueOperations(s).dlqDelete("q", [7n]));
  });
});

describe("group dead letters", () => {
  const dead = (ms: bigint, seq: bigint, error: string) => [...le(ms, 8), ...le(seq, 8), ...le(6n, 4), ...le(5n, 4), ...le(1700n, 8), ...str16(error)];
  const cursor = [...le(1_700_000_000n, 8), ...le(100n, 8), ...le(2n, 8)];

  it("lists a page with the group in the body, then the next from its cursor", async () => {
    const { sender, sent } = answering([1, ...dead(100n, 2n, "bad record"), 1, ...cursor], [0, 0]);
    const s = new StreamOperations(sender);
    const page = await s.groupDlqList("events", "g1", { limit: 1 });
    expect(page).toEqual({ dead: [{ id: new StreamID(100n, 2n), deliveries: 6, failures: 5, deadAtMs: 1700n, error: "bad record" }], cursor: Uint8Array.from(cursor) });
    expect(sent[0]).toMatchObject({ op: OpCode.StreamGroupDLQList, key: "events" });
    expect(Array.from(sent[0]!.value)).toEqual([...str16("g1"), 1, ...le(1n, 4), 0]);
    await s.groupDlqList("events", "g1", { cursor: page.cursor });
    expect(Array.from(sent[1]!.value)).toEqual([...str16("g1"), 1, ...le(20n, 4), 1, ...cursor]);
    await refusesCut([1, ...dead(1n, 1n, "e"), 1, ...cursor], (x) => new StreamOperations(x).groupDlqList("e", "g"));
    await expect(s.groupDlqList("events", "g1", { cursor: new Uint8Array(16) })).rejects.toThrow(/cursor/);
    await expect(s.groupDlqList("events", "")).rejects.toThrow(/group/);
  });

  it("requeues and deletes by id, reading refusals with their ids", async () => {
    const answer = [...le(1n, 4), 1, ...le(100n, 8), ...le(3n, 8), QueueRefusal.NotDead, ...str8("not dead"), 0];
    const { sender, sent } = answering(answer);
    const s = new StreamOperations(sender);
    const ids = [new StreamID(100n, 2n), new StreamID(100n, 3n)];
    const want = { applied: 1, refused: [{ id: new StreamID(100n, 3n), why: QueueRefusal.NotDead, reason: "not dead" }], more: false };
    expect(await s.groupDlqRequeue("events", "g1", ids)).toEqual(want);
    expect(await s.groupDlqDelete("events", "g1", ids, { dryRun: true })).toEqual(want);
    const named = [1, 2, ...le(100n, 8), ...le(2n, 8), ...le(100n, 8), ...le(3n, 8), 1, 0, 1, ...le(100n, 4)];
    expect(sent.map((x) => x.op)).toEqual([OpCode.StreamGroupDLQRequeue, OpCode.StreamGroupDLQDelete]);
    expect(Array.from(sent[0]!.value)).toEqual([...str16("g1"), ...named]);
    expect(Array.from(sent[1]!.value)).toEqual([...str16("g1"), ...named, 1, 1]);
    await expect(s.groupDlqDelete("events", "g1", [])).rejects.toThrow(/all/);
  });
});

describe("group steal", () => {
  const records = [
    { id: new StreamID(100n, 1n), deliveries: 1 },
    { id: new StreamID(100n, 2n), deliveries: 1 },
  ];

  it("names the records and their holder's deliveries, and reads the new delivery and refusals", async () => {
    const answer = [
      ...le(3n, 8), // term
      ...le(42n, 8), // index
      ...le(99_000n, 8), // expires_at_ms
      1,
      ...le(100n, 8),
      ...le(1n, 8),
      ...le(2n, 4),
      1,
      ...le(100n, 8),
      ...le(2n, 8),
      QueueRefusal.NotIdle,
      ...str8("not idle"),
    ];
    const { sender, sent } = answering(answer);
    const got = await new StreamOperations(sender).groupSteal("events", records, { group: "g1", consumer: "c2", minIdleMs: 60_000 });
    expect(got).toEqual({
      consumer: "c2",
      term: 3n,
      index: 42n,
      expiresAtMs: 99_000n,
      records: [{ id: new StreamID(100n, 1n), deliveries: 2 }],
      refused: [{ id: new StreamID(100n, 2n), why: QueueRefusal.NotIdle, reason: "not idle" }],
    });
    expect(sent[0]).toMatchObject({ op: OpCode.StreamGroupSteal, key: "events" });
    expect(Array.from(sent[0]!.value)).toEqual([
      ...str16("g1"),
      ...str16("c2"),
      ...le(60_000n, 8),
      0,
      2,
      ...le(100n, 8),
      ...le(1n, 8),
      ...le(1n, 4),
      ...le(100n, 8),
      ...le(2n, 8),
      ...le(1n, 4),
    ]);
    await refusesCut(answer, (s) => new StreamOperations(s).groupSteal("events", records, { group: "g1", consumer: "c2", minIdleMs: 1 }));
  });

  it("sends a lease when given, and refuses bad arguments before a round trip", async () => {
    const { sender, sent } = answering([...le(0n, 24), 0, 0]);
    const s = new StreamOperations(sender);
    await s.groupSteal("events", records.slice(0, 1), { group: "g1", consumer: "c2", minIdleMs: 1, leaseMs: 5000 });
    const v = Array.from(sent[0]!.value);
    expect(v.slice(str16("g1").length + str16("c2").length + 8, str16("g1").length + str16("c2").length + 17)).toEqual([1, ...le(5000n, 8)]);
    for (const opts of [
      { group: "g1", consumer: "", minIdleMs: 1 },
      { group: "g1", consumer: "c", minIdleMs: 0 },
      { group: "g1", consumer: "c", minIdleMs: 43_200_001 },
      { group: "g1", consumer: "c", minIdleMs: 1, leaseMs: 4999 },
    ]) {
      await expect(s.groupSteal("events", records, opts)).rejects.toThrow();
    }
    await expect(s.groupSteal("events", [], { group: "g1", consumer: "c", minIdleMs: 1 })).rejects.toThrow(/1 to 100/);
    expect(sent.length).toBe(1);
  });
});

describe("group ack and nack refusals", () => {
  it("decode not_dead and not_idle rather than failing on them", () => {
    const body = [...le(0n, 4), ...le(2n, 4), ...le(1n, 8), ...le(1n, 8), 6, ...le(1n, 8), ...le(2n, 8), 7];
    const got = parseGroupReportResponse(Uint8Array.from(body), false);
    expect(got.refused.map((r) => r.why)).toEqual([QueueRefusal.NotDead, QueueRefusal.NotIdle]);
    expect(() => parseGroupReportResponse(Uint8Array.from([...le(0n, 4), ...le(1n, 4), ...le(1n, 16), 8]), false)).toThrow(IncompleteResponseError);
  });
});

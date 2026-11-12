/**
 * Consumer groups lease what a read takes, record by record, until it is
 * acked: ack, nack and touch name the read's delivery (term, index) and
 * each record's deliveries. A stale report applies nothing and says why
 * inside an ok answer; it isn't an error.
 */

import { afterEach, describe, expect, it } from "vitest";
import {
  FloError,
  IncompleteResponseError,
  OpCode,
  OptionTag,
  QueueRefusal,
  Reason,
  StatusCode,
  StreamID,
  StreamOperations,
} from "@floruntime/core";
import { FloClient, StreamWorker } from "@floruntime/node";
import { fakeServer, groupRead, groupReportAnswer, refusal, until } from "./fake_worker_server.js";

const enc = new TextEncoder();
const text = (s: string) => Array.from(enc.encode(s));
const le = (v: bigint, n: number) => Array.from({ length: n }, (_, i) => Number((BigInt.asUintN(64, v) >> BigInt(8 * i)) & 0xffn));

type Sent = { op: number; key: string; value: Uint8Array; options: Uint8Array };

function answering(...answers: (number[] | Uint8Array)[]): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, _ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, key: new TextDecoder().decode(key), value, options });
      return { status: StatusCode.OK, data: Uint8Array.from(answers[sent.length - 1] ?? []), requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

const delivery = { consumer: "c1", term: 3n, index: 41n, records: [{ id: new StreamID(5n, 7n), deliveries: 1 }, { id: new StreamID(5n, 9n), deliveries: 2 }] };
// [group][consumer][term:u64][index:u64][count:u32]([ms:u64][seq:u64][deliveries:u32])*
const reportValue = [2, 0, ...text("g1"), 2, 0, ...text("c1"), ...le(3n, 8), ...le(41n, 8), ...le(2n, 4), ...le(5n, 8), ...le(7n, 8), ...le(1n, 4), ...le(5n, 8), ...le(9n, 8), ...le(2n, 4)];

describe("stream.groupRead", () => {
  it("names group and consumer, and reads the delivery's token, expiry and each record's deliveries", async () => {
    const answer = groupRead({ term: 3n, index: 41n, expiresAtMs: 31700n, records: [{ seq: 7n }, { seq: 9n, deliveries: 2 }] });
    const { sender, sent } = answering(answer);
    const got = await new StreamOperations(sender).groupRead("s", { group: "g1", consumer: "c1", limit: 2, blockMs: 500 });
    expect(sent[0]!.op).toBe(OpCode.StreamGroupRead);
    expect(Array.from(sent[0]!.value)).toEqual([2, 0, ...text("g1"), 2, 0, ...text("c1")]);
    expect(Array.from(sent[0]!.options)).toEqual([OptionTag.Count, 4, ...le(2n, 4), OptionTag.BlockMS, 4, ...le(500n, 4)]);
    expect(got).toMatchObject({ consumer: "c1", term: 3n, index: 41n, expiresAtMs: 31700n });
    expect(got.records.map((r) => [r.id.sequence, r.deliveries, r.payload])).toEqual([
      [7n, 1, enc.encode("x")],
      [9n, 2, enc.encode("x")],
    ]);
  });

  it("reads an empty answer as nothing taken", async () => {
    const { sender } = answering(new Uint8Array(28));
    expect(await new StreamOperations(sender).groupRead("s", { group: "g", consumer: "c" })).toEqual({ consumer: "c", term: 0n, index: 0n, expiresAtMs: 0n, records: [] });
  });

  it("refuses an answer cut short or with bytes left over", async () => {
    const whole = groupRead();
    for (const bad of [whole.subarray(0, whole.length - 1), Uint8Array.from([...whole, 0]), new Uint8Array(27)]) {
      await expect(new StreamOperations(answering(bad).sender).groupRead("s", { group: "g", consumer: "c" })).rejects.toBeInstanceOf(IncompleteResponseError);
    }
  });

  it("refuses a limit past 100 and a group name past 256 bytes before a round trip", async () => {
    const { sender, sent } = answering();
    const ops = new StreamOperations(sender);
    await expect(ops.groupRead("s", { group: "g", consumer: "c", limit: 101 })).rejects.toBeInstanceOf(FloError);
    await expect(ops.groupRead("s", { group: "g".repeat(257), consumer: "c" })).rejects.toBeInstanceOf(FloError);
    await expect(ops.groupRead("s", { group: "", consumer: "c" })).rejects.toBeInstanceOf(FloError);
    expect(sent).toHaveLength(0);
  });
});

describe("stream.groupAck, groupNack and groupTouch", () => {
  it("name the delivery and each record's deliveries", async () => {
    const { sender, sent } = answering(groupReportAnswer(2), groupReportAnswer(2), groupReportAnswer(2, [], 99000n));
    const ops = new StreamOperations(sender);
    expect(await ops.groupAck("s", delivery, { group: "g1" })).toEqual({ applied: 2, refused: [] });
    expect(await ops.groupNack("s", delivery, { group: "g1" })).toEqual({ applied: 2, refused: [] });
    expect(await ops.groupTouch("s", delivery, { group: "g1" })).toEqual({ applied: 2, refused: [], expiresAtMs: 99000n });
    expect(sent.map((s) => s.op)).toEqual([OpCode.StreamGroupAck, OpCode.StreamGroupNack, OpCode.StreamGroupTouch]);
    for (const s of sent) expect(Array.from(s.value)).toEqual(reportValue);
    for (const s of sent) expect(s.options).toEqual(new Uint8Array(0));
  });

  it("send a nack's delay, dead and error as options, the error cut to what an option holds", async () => {
    const { sender, sent } = answering(groupReportAnswer(2));
    await new StreamOperations(sender).groupNack("s", delivery, { group: "g1", delayMs: 1500, dead: true, error: "é".repeat(200) });
    const o = Array.from(sent[0]!.options);
    expect(o.slice(0, 10)).toEqual([OptionTag.DelayMS, 8, ...le(1500n, 8)]);
    expect(o.slice(10, 12)).toEqual([OptionTag.Dead, 0]);
    expect(o.slice(12, 14)).toEqual([OptionTag.ErrorText, 254]); // whole characters only
    expect(o.length).toBe(14 + 254);
  });

  it("answer a stale report inside an ok answer: applied nothing, each record with why", async () => {
    const answer = groupReportAnswer(0, [
      { ms: 5n, seq: 7n, why: QueueRefusal.Expired },
      { ms: 5n, seq: 9n, why: QueueRefusal.DeliveredAgain },
    ]);
    const got = await new StreamOperations(answering(answer).sender).groupAck("s", delivery, { group: "g1" });
    expect(got.applied).toBe(0);
    expect(got.refused.map((r) => [r.id.timestampMs, r.id.sequence, r.why])).toEqual([
      [5n, 7n, QueueRefusal.Expired],
      [5n, 9n, QueueRefusal.DeliveredAgain],
    ]);
    expect(got.refused[0]!.reason).toMatch(/^stale report: the lease expired/);
  });

  it("refuse a why this SDK doesn't know, and an answer cut short", async () => {
    const ops = (a: Uint8Array) => new StreamOperations(answering(a).sender);
    await expect(ops(groupReportAnswer(0, [{ why: 6 }])).groupAck("s", delivery, { group: "g1" })).rejects.toBeInstanceOf(IncompleteResponseError);
    await expect(ops(new Uint8Array(7)).groupAck("s", delivery, { group: "g1" })).rejects.toBeInstanceOf(IncompleteResponseError);
    await expect(ops(groupReportAnswer(1)).groupTouch("s", delivery, { group: "g1" })).rejects.toBeInstanceOf(IncompleteResponseError);
  });

  it("refuse a report of no records or more than 100 before a round trip", async () => {
    const { sender, sent } = answering();
    const ops = new StreamOperations(sender);
    await expect(ops.groupAck("s", { ...delivery, records: [] }, { group: "g1" })).rejects.toBeInstanceOf(FloError);
    const many = Array.from({ length: 101 }, (_, i) => ({ id: new StreamID(0n, BigInt(i)), deliveries: 1 }));
    await expect(ops.groupNack("s", { ...delivery, records: many }, { group: "g1" })).rejects.toBeInstanceOf(FloError);
    expect(sent).toHaveLength(0);
  });
});

describe("stream.groupCreate", () => {
  it("sends the lease and attempts as options and says whether it made the group", async () => {
    const { sender, sent } = answering([1], [0]);
    const ops = new StreamOperations(sender);
    expect(await ops.groupCreate("s", "g1", { ackTimeoutMs: 5000, maxDeliver: 2 })).toBe(true);
    expect(await ops.groupCreate("s", "g1")).toBe(false);
    expect(Array.from(sent[0]!.value)).toEqual([2, 0, ...text("g1")]);
    expect(Array.from(sent[0]!.options)).toEqual([OptionTag.AckTimeoutMS, 4, ...le(5000n, 4), OptionTag.MaxDeliver, 1, 2]);
    expect(sent[1]!.options).toEqual(new Uint8Array(0));
  });

  it("refuses a lease outside 5 s to 12 h and attempts outside 1 to 255", async () => {
    const { sender, sent } = answering();
    const ops = new StreamOperations(sender);
    for (const opts of [{ ackTimeoutMs: 4999 }, { ackTimeoutMs: 43_200_001 }, { maxDeliver: 0 }, { maxDeliver: 256 }]) {
      await expect(ops.groupCreate("s", "g1", opts)).rejects.toBeInstanceOf(FloError);
    }
    expect(sent).toHaveLength(0);
  });
});

describe("StreamWorker set-up", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("creates no group without settings: its first read does", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? "park" : undefined));
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", blockMs: 30000 }, async () => {});
    const done = w.start();
    await until(() => srv.ops().includes(OpCode.StreamGroupRead), 3000);
    w.stop();
    srv.close();
    await done;
    expect(srv.ops()).not.toContain(OpCode.StreamGroupCreate);
  });

  it("throws the conflict when its group exists with other settings", async () => {
    const text = "conflict: group 'default' exists with a lease (ack timeout) of 5000 ms and 2 attempts (max deliver); a create can't change them";
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupCreate ? refusal(StatusCode.Conflict, Reason.Unclassified, text) : undefined));
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", ackTimeoutMs: 6000 }, async () => {});
    await expect(w.start()).rejects.toMatchObject({ status: StatusCode.Conflict, serverMessage: text });
    expect(srv.ops()).not.toContain(OpCode.StreamGroupRead);
  });
});

describe("removed group ops", () => {
  it("are gone from the SDK: join, leave, claim and configure_sweeper", () => {
    const names = Object.keys(OpCode);
    for (const gone of ["StreamGroupJoin", "StreamGroupLeave", "StreamGroupClaim", "StreamGroupConfigureSweeper"]) expect(names).not.toContain(gone);
    const ops = StreamOperations.prototype as unknown as Record<string, unknown>;
    expect(ops.groupJoin).toBeUndefined();
    expect(ops.groupLeave).toBeUndefined();
  });
});

describe("StreamWorker leases", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("creates its group with the lease and attempts it was given", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? "park" : undefined));
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", group: "g1", ackTimeoutMs: 6000, maxDeliver: 3, blockMs: 30000 }, async () => {});
    const done = w.start();
    await until(() => srv.ops().includes(OpCode.StreamGroupRead), 3000);
    w.stop();
    srv.close();
    await done;
    const create = srv.seen.find((s) => s.op === OpCode.StreamGroupCreate)!;
    // The options trail the value: [ack_timeout_ms][max_deliver].
    expect(Array.from(create.payload.subarray(-9))).toEqual([OptionTag.AckTimeoutMS, 4, ...le(6000n, 4), OptionTag.MaxDeliver, 1, 3]);
  });

  it("touches a record every sixth of its lease while the handler runs, then acks it with the read's delivery", async () => {
    // A 5 s lease: a touch every 833 ms from the read's arrival.
    let served = false;
    const srv = await fakeServer((op) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (served) return "park";
      served = true;
      return { data: groupRead({ term: 2n, index: 8n, expiresAtMs: BigInt(Date.now() + 5000), records: [{ seq: 4n, deliveries: 3 }] }) };
    });
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", group: "g1", consumer: "c1", blockMs: 30000 }, async () => {
      await new Promise((r) => setTimeout(r, 2000));
    });
    const done = w.start();
    // Wait for the ack itself, not just the handler: closing the server first would drop it.
    await until(() => srv.ops().includes(OpCode.StreamGroupAck), 5000);
    w.stop();
    srv.close();
    await done;
    const read = srv.seen.find((s) => s.op === OpCode.StreamGroupRead)!.at;
    const touches = srv.seen.filter((s) => s.op === OpCode.StreamGroupTouch);
    expect(touches.length).toBe(2);
    expect(touches[0]!.at - read).toBeGreaterThan(700);
    expect(touches[1]!.at - read).toBeGreaterThan(1500);
    const ack = srv.seen.find((s) => s.op === OpCode.StreamGroupAck)!;
    // The value ends [term][index][count]([ms][seq][deliveries]), then no options ([len:u16] 0).
    expect(Array.from(ack.payload.subarray(-42))).toEqual([...le(2n, 8), ...le(8n, 8), ...le(1n, 4), ...le(0n, 8), ...le(4n, 8), ...le(3n, 4), 0, 0]);
    for (const t of touches) expect(t.payload.subarray(-42).equals(ack.payload.subarray(-42))).toBe(true);
  });

  it("warns of a stale ack and doesn't resend it or nack", async () => {
    let served = false;
    const srv = await fakeServer((op) => {
      if (op === OpCode.StreamGroupAck) return { data: groupReportAnswer(0, [{ seq: 1n, why: QueueRefusal.Expired }]) };
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (served) return "park";
      served = true;
      return { data: groupRead() };
    });
    close = srv.close;
    const warned: string[] = [];
    const logger = { debug: () => {}, warn: (m: string) => void warned.push(m), error: () => {} };
    const w = new StreamWorker(srv.endpoint, { stream: "s", blockMs: 30000, logger }, async () => {});
    const done = w.start();
    await until(() => warned.some((m) => m.includes("applied nothing")), 3000);
    w.stop();
    srv.close();
    await done;
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupAck)).toHaveLength(1);
    expect(srv.ops()).not.toContain(OpCode.StreamGroupNack);
    expect(warned.find((m) => m.includes("applied nothing"))).toMatch(/ack of record 0-1 applied nothing: stale report: the lease expired/);
  });

  it("reads groupList and groupMembers unchanged", async () => {
    // Both answers are table schemas; one with nothing in it decodes empty.
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupList || op === OpCode.StreamGroupMembers ? { data: new Uint8Array([0, 0, 0]) } : undefined));
    close = srv.close;
    const c = new FloClient(srv.endpoint);
    await c.connect();
    try {
      expect(await c.stream.groupList("s")).toEqual({ groups: [], more: false });
      expect(await c.stream.groupMembers("s", "g1")).toEqual({ members: [], more: false });
    } finally {
      await c.close();
    }
  });
});

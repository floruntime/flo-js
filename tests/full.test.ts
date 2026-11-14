/**
 * Full queues and groups, refused overloaded with a reason saying which.
 * QueueFull drains as messages are acked, so the client retries it.
 * GroupHeldFull stays until the group's consumers ack or nack, so the
 * client doesn't, and a StreamWorker backs off as for a limit. A report to
 * a group that isn't there is refused not found, and a worker logs it.
 */

import { afterEach, describe, expect, it } from "vitest";
import { OpCode, OverloadedError, Ran, Reason, StatusCode, isGroupHeldFull, isQueueFull, isRefusedUnrun, reasonName } from "@floruntime/core";
import { FloClient, StreamWorker } from "@floruntime/node";
import { fakeServer, groupRead, refusal, until } from "./fake_worker_server.js";

let close: (() => void) | undefined;
afterEach(() => close?.());

const QUEUE_FULL = "overloaded: queue 'jobs' holds 10000 of 10000 messages and 1 of 64 MiB; it drains as messages are acked";
const HELD_FULL = "overloaded: group 'g' of stream 's' holds 10000 of 10000 records; its consumers must ack or nack first";
const fast = { maxAttempts: 4, initialBackoffMs: 1, maxBackoffMs: 2 };

describe("queue_full", () => {
  it("is reason 36 under overloaded, and retried: a send after the queue drains goes through", async () => {
    expect([Reason.QueueFull, Reason.GroupHeldFull]).toEqual([36, 37]);
    expect([reasonName(Reason.QueueFull), reasonName(Reason.GroupHeldFull)]).toEqual(["queue_full", "group_held_full"]);
    const seq = new Uint8Array(8);
    seq[0] = 42;
    const srv = await fakeServer((op, nth) => (op !== OpCode.QueueEnqueue ? undefined : nth < 2 ? refusal(StatusCode.Overloaded, Reason.QueueFull, QUEUE_FULL) : { data: seq }));
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: fast });
    await client.connect();
    try {
      expect(await client.queue.enqueue("jobs", new Uint8Array([1]))).toBe(42n);
      expect(srv.ops().filter((o) => o === OpCode.QueueEnqueue)).toHaveLength(3);
    } finally {
      await client.close();
    }
  });

  it("throws OverloadedError with the reason once the retries run out", async () => {
    const srv = await fakeServer((op) => (op === OpCode.QueueEnqueue ? refusal(StatusCode.Overloaded, Reason.QueueFull, QUEUE_FULL) : undefined));
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: fast });
    await client.connect();
    try {
      const err = await client.queue.enqueue("jobs", new Uint8Array([1])).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(OverloadedError);
      expect(err).toMatchObject({ reason: Reason.QueueFull, ran: Ran.No, serverMessage: QUEUE_FULL });
      expect(isQueueFull(err)).toBe(true);
      expect(isGroupHeldFull(err)).toBe(false);
      expect((err as Error).message).toContain("queue_full");
      expect(srv.ops().filter((o) => o === OpCode.QueueEnqueue)).toHaveLength(4);
    } finally {
      await client.close();
    }
  });
});

describe("group_held_full", () => {
  it("reaches a groupRead caller as OverloadedError after one request, though retries are on", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupRead ? refusal(StatusCode.Overloaded, Reason.GroupHeldFull, HELD_FULL) : undefined));
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: fast });
    await client.connect();
    try {
      const err = await client.stream.groupRead("s", { group: "g", consumer: "c" }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(OverloadedError);
      expect(err).toMatchObject({ reason: Reason.GroupHeldFull, ran: Ran.No, serverMessage: HELD_FULL });
      expect(isGroupHeldFull(err)).toBe(true);
      expect(isRefusedUnrun(err)).toBe(false);
      expect(srv.ops().filter((o) => o === OpCode.StreamGroupRead)).toHaveLength(1);
    } finally {
      await client.close();
    }
  });

  it("makes a StreamWorker wait and read again, warning once however often it comes", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      return nth < 2 ? refusal(StatusCode.Overloaded, Reason.GroupHeldFull, HELD_FULL) : "park";
    });
    close = srv.close;
    const warned: string[] = [];
    const logger = { debug: () => {}, warn: (m: string) => void warned.push(m), error: () => {} };
    const w = new StreamWorker(srv.endpoint, { stream: "s", group: "g", blockMs: 30000, logger, retry: fast }, async () => {});
    const done = w.start();
    await until(() => srv.ops().filter((o) => o === OpCode.StreamGroupRead).length === 3, 10_000);
    w.stop();
    srv.close();
    await done;
    // Each refusal was one request, not retried by the client, and the
    // worker backed off as for a limit, not as for a read not ready.
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupRead)).toHaveLength(3);
    const full = warned.filter((m) => m.includes("group_held_full"));
    expect(full).toEqual([expect.stringContaining("GroupRead refused full (the group's records, dead ones included, or the shard's queues and groups) (group_held_full)")]);
    expect(warned.filter((m) => m.includes("not ready"))).toEqual([]);
  }, 15_000);
});

describe("a report to a group that isn't there", () => {
  it("is logged at error by a StreamWorker, not taken as its records gone, and not followed by a nack", async () => {
    let served = false;
    const srv = await fakeServer((op) => {
      if (op === OpCode.StreamGroupAck) return refusal(StatusCode.NotFound, Reason.NotFound, "not found: no such group");
      if (op !== OpCode.StreamGroupRead) return undefined;
      if (served) return "park";
      served = true;
      return { data: groupRead() };
    });
    close = srv.close;
    const errors: string[] = [];
    const warned: string[] = [];
    const logger = { debug: () => {}, warn: (m: string) => void warned.push(m), error: (m: string) => void errors.push(m) };
    const w = new StreamWorker(srv.endpoint, { stream: "s", group: "g", blockMs: 30000, logger, retry: { maxAttempts: 1 } }, async () => {});
    const done = w.start();
    await until(() => errors.length > 0, 3000);
    w.stop();
    srv.close();
    await done;
    expect(errors).toEqual([expect.stringMatching(/Dropped ack for record 0-1: .*not found: no such group/)]);
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupAck)).toHaveLength(1);
    expect(srv.ops()).not.toContain(OpCode.StreamGroupNack);
    expect(warned.filter((m) => m.includes("gone"))).toEqual([]);
  });
});

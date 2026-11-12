/**
 * Live tests against a real flo server: FLO_ENDPOINT names it (CI starts the
 * commit pinned in .github/flo-ref.env), and FLO_BINARY the binary, for the
 * version check. FLO_EXPIRY_ENDPOINT names a second one that marks a worker
 * unhealthy and removes it after 1 s without a heartbeat. Without their
 * endpoints they skip, except under CI=1, where a missing server is a
 * failure, not a skip.
 */

import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  ActionType,
  ConflictError,
  NoNamespaceError,
  NotFoundError,
  ProcessKind,
  QueueRefusal,
  Ran,
  Reason,
  ServerError,
  StaleReportError,
  StatusCode,
  TABLE_HASH,
  VERSION,
  WorkerStatus,
  WorkerType,
  type TaskAssignment,
  type WorkerInfo,
} from "@floruntime/core";
import { ActionWorker, FloClient } from "@floruntime/node";

const endpoint = process.env.FLO_ENDPOINT ?? "";
const expiryEndpoint = process.env.FLO_EXPIRY_ENDPOINT ?? "";
const ci = process.env.CI === "1";
if (ci && !endpoint) throw new Error("CI=1 but FLO_ENDPOINT is not set");
if (ci && !expiryEndpoint) throw new Error("CI=1 but FLO_EXPIRY_ENDPOINT is not set");
// The one test that checks the pin mustn't vanish quietly in CI.
if (ci && !process.env.FLO_BINARY) throw new Error("CI=1 but FLO_BINARY is not set");

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

/** A connected client in a namespace of its own, so tests on one server don't see each other's data. */
async function liveClient(at = endpoint): Promise<{ client: FloClient; namespace: string }> {
  const namespace = `live-${randomBytes(4).toString("hex")}`;
  const client = new FloClient(at, { namespace, timeoutMs: 10000 });
  await client.connect();
  cleanups.push(() => client.close());
  return { client, namespace };
}

describe.skipIf(!endpoint)("live", () => {
  it.skipIf(!process.env.FLO_BINARY)("the server speaks this SDK's protocol and table", () => {
    const out = execFileSync(process.env.FLO_BINARY!, ["version", "--verbose"], {
      env: { ...process.env, HOME: mkdtempSync(join(tmpdir(), "flo-home-")) },
    }).toString();
    const m = out.match(/protocol (\d+), table 0x([0-9a-f]{16})/);
    expect(m, out).not.toBeNull();
    expect(Number(m![1])).toBe(VERSION);
    expect(BigInt(`0x${m![2]}`)).toBe(TABLE_HASH);
  });

  it("puts, gets and mgets keys", async () => {
    const { client } = await liveClient();
    await client.kv.put("a", encoder.encode("1"));
    await client.kv.put("b", encoder.encode("2"));
    const a = await client.kv.get("a");
    const b = await client.kv.get("b");
    expect(a && decoder.decode(a.value)).toBe("1");
    const entries = await client.kv.mget(["a", "missing", "b"]);
    expect(entries.map((e) => ({ key: e.key, found: e.found, value: e.found ? decoder.decode(e.value) : null, version: e.version }))).toEqual([
      { key: "a", found: true, value: "1", version: a!.version },
      { key: "missing", found: false, value: null, version: 0n },
      { key: "b", found: true, value: "2", version: b!.version },
    ]);
  });

  it("gets a key's expiry, absolute, and none without a TTL", async () => {
    const { client } = await liveClient();
    const ttl = 60000n;
    const before = BigInt(Date.now());
    await client.kv.put("t", encoder.encode("v"), { ttlMs: ttl });
    const after = BigInt(Date.now());
    const got = await client.kv.get("t");
    expect(got!.expiresAtMs).not.toBeNull();
    expect(got!.expiresAtMs! >= before + ttl && got!.expiresAtMs! <= after + ttl, `${got!.expiresAtMs} not in [${before + ttl}, ${after + ttl}]`).toBe(true);
    await client.kv.put("p", encoder.encode("v"));
    expect((await client.kv.get("p"))!.expiresAtMs).toBeNull();
  });

  it("shows a deleted version in a key's history", async () => {
    const { client } = await liveClient();
    await client.kv.put("h", encoder.encode("one"));
    await client.kv.delete("h");
    await client.kv.put("h", encoder.encode("three"));
    const { versions, more, stale } = await client.kv.history("h");
    expect([more, stale]).toEqual([false, false]);
    expect(versions.map((v) => ({ deleted: v.deleted, value: decoder.decode(v.value) }))).toEqual([
      { deleted: false, value: "three" },
      { deleted: true, value: "" },
      { deleted: false, value: "one" },
    ]);
  });

  it("pages a key's history with beforeVersion, newest first, each version once", async () => {
    const { client } = await liveClient();
    const written: bigint[] = [];
    for (let i = 1; i <= 5; i++) written.push((await client.kv.put("pg", encoder.encode(`v${i}`))).version);
    const pages: { versions: bigint[]; more: boolean }[] = [];
    let before: bigint | undefined;
    for (;;) {
      const page = await client.kv.history("pg", { limit: 2, beforeVersion: before });
      pages.push({ versions: page.versions.map((v) => v.version), more: page.more });
      if (!page.more) break;
      before = page.versions[page.versions.length - 1]!.version;
    }
    const newest = [...written].reverse();
    expect(pages).toEqual([
      { versions: newest.slice(0, 2), more: true },
      { versions: newest.slice(2, 4), more: true },
      { versions: newest.slice(4), more: false },
    ]);
    // Before the oldest: nothing, and nothing more.
    expect(await client.kv.history("pg", { limit: 2, beforeVersion: written[0]! })).toEqual({ versions: [], more: false, stale: false });
  });

  it("appends to a stream and reads it back in order", async () => {
    const { client } = await liveClient();
    for (const p of ["one", "two", "three"]) await client.stream.append("events", encoder.encode(p));
    const res = await client.stream.read("events");
    expect(res.records.map((r) => decoder.decode(r.payload))).toEqual(["one", "two", "three"]);
  });

  // The server merges a list from at most 8 shards a page and says whether
  // more follow, so a page can be short while items remain: page with a small
  // limit until the server says it's done, and every name must come back once.
  describe("pages every list through to the end, each name once", () => {
    const names = (prefix: string, n: number) => Array.from({ length: n }, (_, i) => `${prefix}-${String(i).padStart(2, "0")}`);

    /** Calls `page` with each cursor it hands back until `done`, collecting every name. */
    async function pageAll<P>(page: (cursor?: Uint8Array) => Promise<P>, read: (p: P) => { names: string[]; done: boolean; cursor: Uint8Array | null }): Promise<string[]> {
      const got: string[] = [];
      let cursor: Uint8Array | undefined;
      for (let pages = 0; ; pages++) {
        expect(pages, `still paging after ${got.length} names`).toBeLessThan(200);
        const r = read(await page(cursor));
        expect(r.names.length).toBeLessThanOrEqual(3);
        got.push(...r.names);
        if (r.done) return got;
        expect(r.cursor, "more to come but no cursor").not.toBeNull();
        cursor = r.cursor!;
      }
    }

    const expectEachOnce = (got: string[], want: string[]) => expect([...got].sort()).toEqual([...want].sort());

    it("kv.scan", async () => {
      const { client } = await liveClient();
      const want = names("k", 40);
      for (const k of want) await client.kv.put(k, encoder.encode("x"));
      const got = await pageAll(
        (cursor) => client.kv.scan("", { limit: 3, cursor }),
        (p) => ({ names: p.keys, done: p.cursor === null, cursor: p.cursor })
      );
      expectEachOnce(got, want);
    });

    it("action.list", async () => {
      const { client } = await liveClient();
      const want = names("a", 30);
      for (const a of want) await client.action.register(a, ActionType.User, { owner: `team-${a}` });
      const entries: { name: string; owner: string; timeoutMs: bigint; maxAttempts: number; version: number }[] = [];
      const got = await pageAll(
        (cursor) => client.action.list({ limit: 3, cursor }),
        (p) => {
          entries.push(...p.actions);
          return { names: p.actions.map((a) => a.name), done: !p.hasMore, cursor: p.cursor };
        }
      );
      expectEachOnce(got, want);
      for (const e of entries) expect(e).toMatchObject({ owner: `team-${e.name}`, timeoutMs: 300000n, maxAttempts: 3, version: 1 });
    });

    it("workflow.listDefinitions", async () => {
      const { client } = await liveClient();
      const want = names("wf", 30);
      for (const name of want) {
        const yaml = `kind: Workflow\nname: ${name}\nversion: "1.0.0"\n\nstart:\n  run: "@actions/validate"\n  transitions:\n    success: flo.Completed\n    failure: flo.Failed\n`;
        await client.workflow.create(name, yaml);
      }
      const got = await pageAll(
        (cursor) => client.workflow.listDefinitions({ limit: 3, cursor }),
        (p) => ({ names: p.definitions.map((d) => d.name), done: !p.hasMore, cursor: p.cursor })
      );
      expectEachOnce(got, want);
      await client.workflow.disable(want[0]!);
      const enabled = new Map<string, boolean>();
      await pageAll(
        (cursor) => client.workflow.listDefinitions({ limit: 3, cursor }),
        (p) => {
          for (const d of p.definitions) enabled.set(d.name, d.enabled);
          return { names: [], done: !p.hasMore, cursor: p.cursor };
        }
      );
      expect(enabled.get(want[0]!)).toBe(false);
      expect(want.slice(1).every((n) => enabled.get(n) === true)).toBe(true);
    });

    it("processing.list", async () => {
      const { client } = await liveClient();
      const want = names("job", 20);
      const ids: string[] = [];
      // A KV sink: outside the default namespace the server refuses a stream
      // sink on another shard than the job's.
      for (const name of want) {
        const yaml = `kind: Processing\nname: ${name}\n\nsources:\n  - name: in\n    stream:\n      name: ${name}-in\n\nsinks:\n  - name: out\n    kv:\n      key_prefix: ${name}\n`;
        ids.push(await client.processing.submit(yaml));
      }
      cleanups.push(async () => {
        for (const id of ids) await client.processing.cancel(id).catch(() => {});
      });
      const jobs: { name: string; id: string }[] = [];
      await pageAll(
        (cursor) => client.processing.list({ limit: 3, cursor }),
        (p) => {
          for (const j of p.jobs) expect(j.batch_size, j.name).toBeGreaterThan(0);
          jobs.push(...p.jobs.map((j) => ({ name: j.name, id: j.job_id })));
          return { names: p.jobs.map((j) => j.name), done: !p.hasMore, cursor: p.cursor };
        }
      );
      expectEachOnce(
        jobs.map((j) => j.name),
        want
      );
      expectEachOnce(
        jobs.map((j) => j.id),
        ids
      );
    });
  });

  it("shows a job's status with its runtime and definition, and removes it only once it no longer runs", async () => {
    const { client } = await liveClient();
    const yaml = `kind: Processing\nname: tidy\n\nsources:\n  - name: in\n    stream:\n      name: tidy-in\n\nsinks:\n  - name: out\n    kv:\n      key_prefix: tidy\n`;
    const id = await client.processing.submit(yaml);
    cleanups.push(async () => void (await client.processing.cancel(id).catch(() => {})));
    const status = await client.processing.status(id);
    expect(status).toMatchObject({ job_id: id, name: "tidy", status: "running", yaml });
    expect(typeof status.running).toBe("boolean");
    const running = await client.processing.remove(id).catch((e: unknown) => e);
    expect(running).toBeInstanceOf(ConflictError);
    expect(running).toMatchObject({ reason: Reason.JobRunning, ran: Ran.No });
    await client.processing.stop(id);
    await client.processing.remove(id);
    await expect(client.processing.status(id)).rejects.toBeInstanceOf(NotFoundError);
  });

  it("answers stream.info of a stream that doesn't exist not found, not as an empty stream", async () => {
    const { client } = await liveClient();
    await client.stream.append("present", encoder.encode("x")); // creates the namespace
    expect((await client.stream.info("present")).count).toBe(1n);
    const err = await client.stream.info("absent").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err).toMatchObject({ status: StatusCode.NotFound });
  });

  it("refuses a read in a namespace that was never created as no_namespace", async () => {
    const { client } = await liveClient();
    for (const read of [() => client.kv.get("k"), () => client.kv.scan(""), () => client.stream.read("s")]) {
      const err = await read().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NoNamespaceError);
      expect(err).toMatchObject({ status: StatusCode.NoNamespace });
    }
    // A write creates it; the same read then finds nothing.
    await client.kv.put("other", encoder.encode("x"));
    expect(await client.kv.get("k")).toBeNull();
  });

  describe("workflow runs", () => {
    const yaml = (name: string) =>
      `kind: Workflow\nname: ${name}\nversion: "1.0.0"\n\nstart:\n  run: "@actions/validate"\n  transitions:\n    success: flo.Completed\n    failure: flo.Failed\n`;

    it("listRuns pages through every run, each once, newest first", async () => {
      const { client } = await liveClient();
      const workflows = ["wf-a", "wf-b", "wf-c", "wf-d"];
      for (const wf of workflows) await client.workflow.create(wf, yaml(wf));
      const started: { runId: string; workflow: string }[] = [];
      for (let i = 0; i < 5; i++) {
        for (const wf of workflows) started.push({ runId: await client.workflow.start(wf, { i }), workflow: wf });
      }

      async function all(workflowName?: string): Promise<{ runId: string; workflow: string; createdAt: number }[]> {
        const got: { runId: string; workflow: string; createdAt: number }[] = [];
        let cursor: Uint8Array | undefined;
        for (let pages = 0; ; pages++) {
          expect(pages, `still paging after ${got.length} runs`).toBeLessThan(100);
          const p = await client.workflow.listRuns({ workflowName, limit: 3, cursor });
          expect(p.runs.length).toBeLessThanOrEqual(3);
          got.push(...p.runs.map((r) => ({ runId: r.run_id, workflow: r.workflow, createdAt: r.created_at })));
          if (!p.hasMore) return got;
          cursor = p.cursor!;
        }
      }
      const newestFirst = (runs: { runId: string; createdAt: number }[]) =>
        [...runs].sort((a, b) => b.createdAt - a.createdAt || (a.runId < b.runId ? -1 : a.runId > b.runId ? 1 : 0));
      const key = (r: { runId: string; workflow: string }) => `${r.workflow}/${r.runId}`;

      const every = await all();
      expect(every.map(key).sort()).toEqual(started.map(key).sort());
      expect(every).toEqual(newestFirst(every));

      const one = await all("wf-b");
      expect(one.map(key).sort()).toEqual(started.filter((r) => r.workflow === "wf-b").map(key).sort());
      expect(one).toEqual(newestFirst(one));
    }, 30000);

    it("finds a run started with a client-chosen id by that id, and refuses the id again", async () => {
      const { client } = await liveClient();
      // It waits for a signal it is never sent, so it is still running when
      // signalled and cancelled.
      await client.workflow.create("orders", `kind: Workflow\nname: orders\nversion: "1.0.0"\n\nstart:\n  wait_for_signal:\n    type: go\n  transitions:\n    success: flo.Completed\n`);
      await client.workflow.create("other", yaml("other"));
      for (const runId of ["order-123", "run-1", "6f1c2b9e-4d3a-4e8f-9a7b-2c5d1e0f3a4b"]) {
        expect(await client.workflow.start("orders", { id: runId }, { runId })).toBe(runId);
        const status = await client.workflow.status(runId);
        expect(status).toMatchObject({ run_id: runId, workflow: "orders" });
        expect((await client.workflow.history(runId)).length).toBeGreaterThan(0);
        const listed = await client.workflow.listRuns({ workflowName: "orders", search: runId });
        expect(listed.runs.map((r) => r.run_id)).toContain(runId);
        await client.workflow.signal(runId, "nudge", encoder.encode("{}"));
        await client.workflow.cancel(runId, "done");
        expect((await client.workflow.status(runId)).status).toBe("cancelled");
        const err = await client.workflow.start("other", {}, { runId }).catch((e: unknown) => e);
        expect(err, `second start of ${runId}`).toBeInstanceOf(ConflictError);
      }
    }, 30000);
  });

  it("deletes a stream: a non-empty one only with force, a missing one without complaint", async () => {
    const { client } = await liveClient();
    await client.stream.append("doomed", encoder.encode("x"));
    await expect(client.stream.delete("doomed")).rejects.toBeInstanceOf(ConflictError);
    await client.stream.delete("doomed", { force: true });
    expect((await client.stream.read("doomed")).records).toHaveLength(0);
    await client.stream.delete("doomed");
  });

  it("leases what a dequeue takes until it is completed: completed, it is gone; let go, it comes back", async () => {
    const { client } = await liveClient();
    await client.queue.enqueue("jobs", encoder.encode("job-1"));
    const taken = await client.queue.dequeue("jobs", 1, { consumer: "c1", leaseMs: 5000 });
    expect(taken.messages.map((m) => [decoder.decode(m.payload), m.deliveries])).toEqual([["job-1", 1]]);
    expect(taken.expiresAtMs).toBeGreaterThan(BigInt(Date.now()));
    // Leased, not removed: nothing else is ready.
    expect((await client.queue.dequeue("jobs", 1, { consumer: "c2" })).messages).toHaveLength(0);
    expect(await client.queue.complete("jobs", taken)).toEqual({ applied: 1, refused: [] });
    // A resent complete: the message is gone, so done.
    expect((await client.queue.complete("jobs", taken)).refused.map((r) => r.why)).toEqual([QueueRefusal.Gone]);
    expect((await client.queue.dequeue("jobs", 1, { consumer: "c1" })).messages).toHaveLength(0);

    // A lease left to run out is delivered again, deliveries 2.
    await client.queue.enqueue("jobs", encoder.encode("job-2"));
    const first = await client.queue.dequeue("jobs", 1, { consumer: "c1", leaseMs: 5000 });
    expect(first.messages[0]!.deliveries).toBe(1);
    const again = await client.queue.dequeue("jobs", 1, { consumer: "c2", blockMs: 15000 });
    expect(again.messages.map((m) => [decoder.decode(m.payload), m.deliveries])).toEqual([["job-2", 2]]);
    // The first consumer's lease is over: its complete is refused, inside an ok answer.
    const late = await client.queue.complete("jobs", first);
    expect(late.applied).toBe(0);
    expect(late.refused).toHaveLength(1);
    expect(late.refused[0]).toMatchObject({ seq: first.messages[0]!.seq, why: QueueRefusal.DeliveredAgain });
    expect(await client.queue.complete("jobs", again)).toMatchObject({ applied: 1 });
  }, 30000);

  it("fails a dequeued message back to ready, touches a lease, dequeues with ack, delays and purges", async () => {
    const { client } = await liveClient();
    await client.queue.enqueue("jobs", encoder.encode("x"));
    const taken = await client.queue.dequeue("jobs", 1, { consumer: "c1" });
    const touched = await client.queue.touch("jobs", taken);
    expect(touched).toMatchObject({ applied: 1, refused: [] });
    expect(touched.expiresAtMs).toBe(taken.expiresAtMs);
    expect(await client.queue.fail("jobs", taken, { error: "boom" })).toEqual({ applied: 1, refused: [] });
    const back = await client.queue.dequeue("jobs", 1, { consumer: "c1", ack: true });
    expect(back.messages.map((m) => m.deliveries)).toEqual([2]);
    expect(back.expiresAtMs).toBe(0n);
    expect((await client.queue.dequeue("jobs", 1, { consumer: "c1" })).messages).toHaveLength(0);

    await client.queue.enqueue("jobs", encoder.encode("later"), { delayMs: 1500 });
    expect((await client.queue.dequeue("jobs", 1)).messages).toHaveLength(0);
    const later = await client.queue.dequeue("jobs", 1, { blockMs: 5000, ack: true });
    expect(later.messages.map((m) => decoder.decode(m.payload))).toEqual(["later"]);

    for (let i = 0; i < 3; i++) await client.queue.enqueue("jobs", encoder.encode(`${i}`));
    expect(await client.queue.purge("jobs")).toBe(3);
    expect((await client.queue.peek("jobs")).messages).toHaveLength(0);
  }, 30000);

  it("peeks a queue's next messages in dequeue order without taking them, and pages its dead letters", async () => {
    const { client } = await liveClient();
    const seqs: bigint[] = [];
    for (const [p, priority] of [["low", 5], ["high", 1], ["mid", 3]] as const) seqs.push(await client.queue.enqueue("jobs", encoder.encode(p), { priority }));
    const peeked = await client.queue.peek("jobs", 2);
    expect(peeked.messages.map((m) => [decoder.decode(m.payload), m.priority, m.attempts, m.size])).toEqual([
      ["high", 1, 0, 4],
      ["mid", 3, 0, 3],
    ]);
    expect(peeked.more).toBe(true);
    expect((await client.queue.peek("jobs")).messages).toHaveLength(3);
    const taken = await client.queue.dequeue("jobs", 1);
    expect(decoder.decode(taken.messages[0]!.payload)).toBe("high");
    // Nothing has died: an empty first page, and one after a seq.
    expect(await client.queue.dlqList("jobs")).toEqual({ messages: [], total: 0n, hasMore: false, lastSeq: 0n });
    expect(await client.queue.dlqList("jobs", { limit: 1, after: seqs[0]! })).toEqual({ messages: [], total: 0n, hasMore: false, lastSeq: 0n });
  });

  it("lists a stream's consumer groups and a group's members, and refuses an unknown group not found", async () => {
    const { client } = await liveClient();
    await client.stream.append("events", encoder.encode("x"));
    // A read creates the group; its members are the consumers holding its records.
    const read = await client.stream.groupRead("events", { group: "g1", consumer: "c1", limit: 10 });
    expect(read.records).toHaveLength(1);
    expect(await client.stream.groupRead("events", { group: "g1", consumer: "c2", limit: 10 })).toMatchObject({ records: [] });
    const { groups, more } = await client.stream.groupList("events");
    expect(more).toBe(false);
    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({ name: "g1", members: 1, pending: 1n, ackTimeoutMs: 30000, maxDeliver: 5 });
    expect(groups[0]!.lastDeliveredMs).toBeGreaterThan(0n);
    const members = await client.stream.groupMembers("events", "g1");
    expect(members.more).toBe(false);
    expect(members.members).toHaveLength(1);
    expect(members.members[0]).toMatchObject({ id: "c1", state: "active", pending: 1 });
    expect(members.members[0]!.lastActiveMs).toBeDefined();
    await expect(client.stream.groupMembers("events", "nope")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("leases a group's records: acked within the lease, gone; let run out, redelivered; nacked past maxDeliver, dead", async () => {
    const { client } = await liveClient();
    expect(await client.stream.groupCreate("events", "g1", { ackTimeoutMs: 5000, maxDeliver: 2 })).toBe(true);
    // The same settings again change nothing; others are refused, as a create can't change them.
    expect(await client.stream.groupCreate("events", "g1", { ackTimeoutMs: 5000, maxDeliver: 2 })).toBe(false);
    await expect(client.stream.groupCreate("events", "g1", { ackTimeoutMs: 9000 })).rejects.toBeInstanceOf(ConflictError);
    await client.stream.append("events", encoder.encode("a"));
    await client.stream.append("events", encoder.encode("b"));
    const opts = { group: "g1", consumer: "c1" };

    const first = await client.stream.groupRead("events", { ...opts, limit: 10 });
    expect(first.records.map((r) => [decoder.decode(r.payload), r.deliveries])).toEqual([["a", 1], ["b", 1]]);
    expect(first.expiresAtMs).toBeGreaterThan(BigInt(Date.now()));
    // Ack "a" within the lease; "b" stays leased, so a read gets nothing new.
    expect(await client.stream.groupAck("events", { ...first, records: [first.records[0]!] }, { group: "g1" })).toEqual({ applied: 1, refused: [] });
    expect((await client.stream.groupRead("events", { ...opts, limit: 10 })).records).toHaveLength(0);
    // A touch with more than half the lease left applies but moves nothing.
    expect(await client.stream.groupTouch("events", { ...first, records: [first.records[1]!] }, { group: "g1" })).toMatchObject({ applied: 1, refused: [] });

    // Left to run out, "b" comes back with deliveries 2.
    const again = await client.stream.groupRead("events", { ...opts, limit: 10, blockMs: 20000 });
    expect(again.records.map((r) => [decoder.decode(r.payload), r.deliveries])).toEqual([["b", 2]]);
    expect([again.term, again.index]).not.toEqual([first.term, first.index]);
    // The first delivery's report is stale now: it applies nothing, inside an ok answer.
    const stale = await client.stream.groupAck("events", { ...first, records: [first.records[1]!] }, { group: "g1" });
    expect(stale.applied).toBe(0);
    expect(stale.refused.map((r) => [r.id.sequence, r.why])).toEqual([[first.records[1]!.id.sequence, QueueRefusal.DeliveredAgain]]);

    // The expiry was one failure; a nack is the second, and maxDeliver is 2: dead.
    expect(await client.stream.groupNack("events", again, { group: "g1", error: "boom" })).toEqual({ applied: 1, refused: [] });
    expect((await client.stream.groupRead("events", { ...opts, limit: 10, blockMs: 300 })).records).toHaveLength(0);
    const [g] = (await client.stream.groupList("events")).groups;
    expect(g).toMatchObject({ name: "g1", pending: 0n, ackTimeoutMs: 5000, maxDeliver: 2 });
  }, 40000);

  it("runs a KV transaction over keys sharing a {tag}, and refuses a key that routes elsewhere", async () => {
    const { client } = await liveClient();
    const txn = await client.kv.begin("{t}a");
    await txn.put("{t}a", encoder.encode("1"));
    await txn.put("{t}b", encoder.encode("2"));
    expect(await txn.commit()).toMatchObject({ opCount: 2 });
    expect(decoder.decode((await client.kv.get("{t}a"))!.value)).toBe("1");
    expect(decoder.decode((await client.kv.get("{t}b"))!.value)).toBe("2");
    // mget gathers keys from any shard.
    expect((await client.kv.mget(["{t}a", "{t}b", "elsewhere"])).map((e) => e.found)).toEqual([true, true, false]);

    const other = await client.kv.begin("{t}a");
    const err = await other.put("untagged", encoder.encode("3")).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err).toMatchObject({ status: StatusCode.BadRequest, reason: Reason.KeysNotColocated, ran: Ran.No });
    await other.rollback();
    expect(await client.kv.get("untagged")).toBeNull();
  });

  it("runs an invoked action on a worker, and the run's status then carries its output", async () => {
    const { client, namespace } = await liveClient();
    const logged: string[] = [];
    const logger = { debug: (m: string) => void logged.push(m), warn: () => {}, error: (m: string) => void logged.push(m) };
    const worker = new ActionWorker({ endpoint, namespace, workerId: "live-echo", blockMs: 1000, logger });
    let input: string | undefined;
    worker.action("echo", async (ctx) => {
      input = decoder.decode(ctx.payload);
      return encoder.encode("ok");
    });
    const done = worker.start();
    cleanups.push(async () => {
      worker.stop();
      await done;
    });
    await new Promise((r) => setTimeout(r, 200));
    const { runId } = await client.action.invoke("echo", encoder.encode("hi"));
    const deadline = Date.now() + 15000;
    let status = await client.action.status(runId);
    while (status.status !== "completed") {
      expect(Date.now(), `run still ${status.status}; log: ${logged.join(" | ")}`).toBeLessThan(deadline);
      await new Promise((r) => setTimeout(r, 20));
      status = await client.action.status(runId);
    }
    expect(input).toBe("hi");
    expect(decoder.decode(status.output!)).toBe("ok");
    expect(status.attempt).toBe(1);
    expect(status.completedAtMs).toBeDefined();
    expect(status.workerId).toBe("live-echo");
  });

  // The spec every SDK's live suite runs: a delivery's identity echoed in
  // every report, a retried await, stale and resent reports, and the fail path.
  it("live: action round trip", async () => {
    const { client, namespace } = await liveClient();
    await client.action.register("echo", ActionType.User, { timeoutMs: 5000, maxRetries: 3 });
    const { runId } = await client.action.invoke("echo", encoder.encode("ping"));

    const take = async (requestKey: string): Promise<TaskAssignment> => {
      const { task } = await client.worker.awaitTask("w1", ["echo"], { blockMs: 5000, requestKey });
      expect(task, `await ${requestKey}`).not.toBeNull();
      return task!;
    };
    const delivery = (t: TaskAssignment) => ({ runId: t.runId, deliveries: t.deliveries, term: t.term, index: t.index });

    const task = await take("rk-1");
    expect(task).toMatchObject({ runId, namespace, deliveries: 1, action: "echo", attempt: 1, leaseMs: 5000n });
    expect(decoder.decode(task.input)).toBe("ping");
    expect(task.createdAtMs).toBeGreaterThan(0n);
    expect(task.taskId).toBe(`${runId}:1:${task.term}:${task.index}:echo`);

    // The same request key gets the same delivery, not a second one.
    expect(delivery(await take("rk-1"))).toEqual(delivery(task));

    // More than half the lease left: the current expiry, unchanged.
    const expiry = await client.worker.touch("w1", task);
    expect(expiry).toBeGreaterThan(BigInt(Date.now()));
    expect(await client.worker.touch("w1", task)).toBe(expiry);

    const stale = await client.worker.complete("w1", { ...task, deliveries: 2 }, encoder.encode("pong")).catch((e: unknown) => e);
    expect(stale).toBeInstanceOf(StaleReportError);
    expect(stale).toMatchObject({ status: StatusCode.Conflict, reason: Reason.StaleReport, ran: Ran.No });
    expect((stale as StaleReportError).serverMessage.startsWith("stale report:")).toBe(true);

    await client.worker.complete("w1", task, encoder.encode("pong"));
    // A resend of a report that already applied is not an error.
    await client.worker.complete("w1", task, encoder.encode("pong"));
    const other = await client.worker.complete("w1", task, encoder.encode("other")).catch((e: unknown) => e);
    expect(other).toBeInstanceOf(StaleReportError);
    expect(other).toMatchObject({ reason: Reason.StaleReport, serverMessage: "stale report: the run was completed with another result" });

    const status = await client.action.status(runId);
    expect(status.status).toBe("completed");
    expect(decoder.decode(status.output!)).toBe("pong");
    expect(status.outcome).toBeUndefined();

    // The fail path: a retried failure counts an attempt; an abandoned one doesn't.
    const second = await client.action.invoke("echo", encoder.encode("ping 2"));
    const t1 = await take("rk-2");
    expect(t1).toMatchObject({ runId: second.runId, deliveries: 1, attempt: 1 });
    await client.worker.fail("w1", t1, "boom", { retry: true, delayMs: 0 });
    const t2 = await take("rk-3");
    expect(t2).toMatchObject({ runId: second.runId, deliveries: 2, attempt: 2 });
    await client.worker.fail("w1", t2, "", { abandoned: true });
    const t3 = await take("rk-4");
    expect(t3).toMatchObject({ runId: second.runId, deliveries: 3, attempt: 2 });
    await client.worker.complete("w1", t3, encoder.encode("done"));
    expect((await client.action.status(second.runId)).status).toBe("completed");
  }, 30000);

  it("shows an action's definition, its runs by status and the latency of completed ones", async () => {
    const { client } = await liveClient();
    await client.action.register("tally", ActionType.User, { timeoutMs: 60000, maxRetries: 4, owner: "team-a" });
    for (let i = 0; i < 3; i++) await client.action.invoke("tally", encoder.encode(`${i}`));
    const { task } = await client.worker.awaitTask("w1", ["tally"], { blockMs: 5000 });
    await client.worker.complete("w1", task!, new Uint8Array(0), { outcome: "counted" });
    const info = await client.action.info("tally");
    expect(info).toMatchObject({ owner: "team-a", version: 1, timeoutMs: 60000n, maxAttempts: 4, ready: 2n, readyCapped: false, tooManyRuns: false, completed: 1n, latencyCount: 1n });
    expect(info.createdAtMs).toBeGreaterThan(0n);
    expect((await client.action.status(task!.runId)).outcome).toBe("counted");
    await expect(client.action.info("missing")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("refuses a lease outside 5 s to 12 h, as the server says", async () => {
    const { client } = await liveClient();
    for (const timeoutMs of [4999, 43_200_001]) {
      const err = await client.action.register("bad", ActionType.User, { timeoutMs }).catch((e: unknown) => e);
      expect(err, `${timeoutMs}`).toMatchObject({ status: StatusCode.BadRequest });
    }
    await client.action.register("default-lease", ActionType.User, { timeoutMs: 0 });
    expect((await client.action.info("default-lease")).timeoutMs).toBe(300000n);
    await client.action.register("unset-lease");
    expect((await client.action.info("unset-lease")).timeoutMs).toBe(300000n);
  });

  it("lists registered workers a page at a time, in id order, with their options", async () => {
    const { client } = await liveClient();
    for (const id of ["w-c", "w-a", "w-b"]) {
      await client.worker.register(id, ["a"], { workerType: WorkerType.Action, maxConcurrency: 3, machineId: "m-live" });
    }
    const first = await client.worker.list({ limit: 2 });
    expect(first.workers.map((w) => w.id)).toEqual(["w-a", "w-b"]);
    expect(first.workers[0]).toMatchObject({ type: WorkerType.Action, status: WorkerStatus.Active, maxConcurrency: 3, machineId: "m-live" });
    expect(decoder.decode(first.cursor!)).toBe("w-b");
    const second = await client.worker.list({ limit: 2, cursor: first.cursor });
    expect(second.workers.map((w) => w.id)).toEqual(["w-c"]);
    expect(second.cursor).toBeNull();
  });

  it("shows a worker's row, metadata and processes in worker.info", async () => {
    const { client } = await liveClient();
    await client.worker.register("w-info", [], {
      workerType: WorkerType.Action,
      maxConcurrency: 4,
      machineId: "m-live",
      metadata: '{"zone":"a"}',
      processes: [
        { name: "echo", kind: ProcessKind.Action },
        { name: "events/g", kind: ProcessKind.StreamConsumer },
      ],
    });
    const got = await client.worker.info("w-info");
    expect(got.worker).toMatchObject({ id: "w-info", type: WorkerType.Action, status: WorkerStatus.Active, maxConcurrency: 4, machineId: "m-live" });
    expect(decoder.decode(got.metadata!)).toBe('{"zone":"a"}');
    expect(got.processes.map((p) => ({ name: p.name, kind: p.kind, runCount: p.runCount, failCount: p.failCount }))).toEqual([
      { name: "echo", kind: ProcessKind.Action, runCount: 0n, failCount: 0n },
      { name: "events/g", kind: ProcessKind.StreamConsumer, runCount: 0n, failCount: 0n },
    ]);
  });

  it("refuses a registration of 1025 processes, as the server says", async () => {
    const { client } = await liveClient();
    const processes = Array.from({ length: 1025 }, (_, i) => ({ name: `p${i}`, kind: ProcessKind.Action }));
    const err = await client.worker.register("w-many", [], { processes }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err).toMatchObject({ status: StatusCode.BadRequest, reason: Reason.OutOfRange, ran: Ran.No, serverMessage: "more than 1024 processes" });
  });

  it("refuses a process name or machine id past its limit as field_too_long", async () => {
    const { client } = await liveClient();
    const cases: [string, Parameters<FloClient["worker"]["register"]>[2], string][] = [
      ["w-long-name", { processes: [{ name: "p".repeat(4097), kind: ProcessKind.Action }] }, "process name longer than 4096 bytes"],
      ["w-long-machine", { machineId: "m".repeat(256) }, "machine id longer than 255 bytes"],
    ];
    for (const [id, opts, message] of cases) {
      const err = await client.worker.register(id, ["a"], opts).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ServerError);
      expect(err).toMatchObject({ status: StatusCode.BadRequest, reason: Reason.FieldTooLong, ran: Ran.No, serverMessage: message });
    }
  });

  it("lists only the workers that run an action", async () => {
    const { client } = await liveClient();
    await client.worker.register("w-x1", ["x"]);
    await client.worker.register("w-y", ["y"]);
    await client.worker.register("w-x2", ["x", "y"]);
    const ids = async (action?: string) => (await client.worker.list({ action })).workers.map((w) => w.id);
    expect(await ids("x")).toEqual(["w-x1", "w-x2"]);
    expect(await ids("y")).toEqual(["w-x2", "w-y"]);
    expect(await ids("z")).toEqual([]);
    expect(await ids()).toEqual(["w-x1", "w-x2", "w-y"]);
  });

  it("answers worker.info for a missing worker not found", async () => {
    const { client } = await liveClient();
    await client.worker.register("w-other", ["a"]); // creates the namespace
    const err = await client.worker.info("w-missing").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err).toMatchObject({ status: StatusCode.NotFound, reason: Reason.NotFound });
  });

  // A handler past its 5 s lease runs once when the worker touches it:
  // without autoTouch the server would take the run back and hand it out
  // again.
  it("keeps a long handler from running twice with autoTouch", async () => {
    const { client, namespace } = await liveClient();
    const logged: string[] = [];
    const logger = { debug: (m: string) => void logged.push(m), warn: () => {}, error: (m: string) => void logged.push(m) };
    const worker = new ActionWorker({ endpoint, namespace, blockMs: 500, logger, autoTouch: true });
    let runs = 0;
    worker.action(
      "slow",
      async () => {
        runs++;
        await new Promise((r) => setTimeout(r, 8000));
        return encoder.encode("ok");
      },
      { timeoutMs: 5000 }
    );
    const done = worker.start();
    cleanups.push(async () => {
      worker.stop();
      await done;
    });
    await new Promise((r) => setTimeout(r, 200));
    await client.action.invoke("slow", new Uint8Array(0));
    const deadline = Date.now() + 15000;
    while (!logged.some((m) => m.startsWith("Action complete sent: slow"))) {
      expect(Date.now(), `log: ${logged.join(" | ")}`).toBeLessThan(deadline);
      await new Promise((r) => setTimeout(r, 50));
    }
    await new Promise((r) => setTimeout(r, 1500)); // a re-run would have started by now
    expect(runs).toBe(1);
  }, 40000);

  // A drain the server starts mid-run lets the running action complete once,
  // then ends start() and deregisters the worker.
  it("finishes the running action once when the server drains the worker, then stops", async () => {
    const { client, namespace } = await liveClient();
    const worker = new ActionWorker({ endpoint, namespace, workerId: "live-drain", blockMs: 1000, heartbeatIntervalMs: 200 });
    let runs = 0;
    let running = false;
    worker.action("slow", async () => {
      runs++;
      running = true;
      await new Promise((r) => setTimeout(r, 1000));
      return encoder.encode("ok");
    });
    const done = worker.start();
    await new Promise((r) => setTimeout(r, 200));
    const { runId } = await client.action.invoke("slow", new Uint8Array(0));
    const deadline = Date.now() + 15000;
    while (!running) {
      expect(Date.now()).toBeLessThan(deadline);
      await new Promise((r) => setTimeout(r, 20));
    }
    await client.worker.drain("live-drain");
    await done; // returns by itself
    const status = await client.action.status(runId);
    expect(status.status).toBe("completed");
    expect(status.attempt).toBe(1);
    expect(runs).toBe(1);
    const listed = await client.worker.list({});
    expect(listed.workers.map((w) => w.id)).not.toContain("live-drain");
  }, 30000);
});

describe.skipIf(!expiryEndpoint)("live, on a server that removes silent workers", () => {
  // Heartbeats every 3 s against a 1 s removal time: the server removes the
  // worker between beats, and the next beat answers not found.
  it("a removed worker registers again with the same options, and still runs actions", async () => {
    const { client, namespace } = await liveClient(expiryEndpoint);
    // Lists below may run before the worker registers, the write that would
    // create the namespace; one never created answers no_namespace.
    await client.kv.put("made", encoder.encode("1"));
    const warned: string[] = [];
    const logger = { debug: () => {}, warn: (m: string) => void warned.push(m), error: () => {} };
    const workerId = `exp-${randomBytes(4).toString("hex")}`;
    const worker = new ActionWorker({
      endpoint: expiryEndpoint,
      namespace,
      workerId,
      concurrency: 3,
      machineId: "m-exp",
      blockMs: 1000,
      heartbeatIntervalMs: 3000,
      logger,
    });
    worker.action("echo", async () => encoder.encode("ok"));
    const done = worker.start();
    cleanups.push(async () => {
      worker.stop();
      await done;
    });

    const find = async (): Promise<WorkerInfo | undefined> => (await client.worker.list()).workers.find((w) => w.id === workerId);
    const waitFor = async (what: string, cond: (w: WorkerInfo | undefined) => boolean): Promise<WorkerInfo | undefined> => {
      const deadline = Date.now() + 10000;
      for (;;) {
        const w = await find();
        if (cond(w)) return w;
        expect(Date.now(), `${what}; warnings: ${warned.join(" | ")}`).toBeLessThan(deadline);
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    const first = await waitFor("never registered", (w) => w !== undefined);
    await waitFor("never removed", (w) => w === undefined);
    const again = await waitFor("never registered again", (w) => w !== undefined);
    expect(warned.some((m) => m.includes("registering again"))).toBe(true);
    expect(again!.registeredAtMs).toBeGreaterThan(first!.registeredAtMs);
    expect(again).toMatchObject({ type: WorkerType.Action, maxConcurrency: 3, machineId: "m-exp" });

    const { runId } = await client.action.invoke("echo", new Uint8Array(0));
    const deadline = Date.now() + 10000;
    let status = await client.action.status(runId);
    while (status.status !== "completed") {
      expect(Date.now(), `run still ${status.status}`).toBeLessThan(deadline);
      await new Promise((r) => setTimeout(r, 20));
      status = await client.action.status(runId);
    }
    expect(status.workerId).toBe(workerId);
  }, 30000);
});

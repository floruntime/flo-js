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
  BadRequestError,
  ConflictError,
  NoNamespaceError,
  NotFoundError,
  OpCode,
  OptionTag,
  OptionsBuilder,
  ProcessKind,
  QueueRefusal,
  Ran,
  Reason,
  ServerError,
  StaleReportError,
  StatusCode,
  TABLE_HASH,
  TSOrder,
  nsFromDate,
  type TSPoint,
  VERSION,
  WorkerStatus,
  WorkerType,
  type TaskAssignment,
  type WorkerInfo,
  groupNackValue,
} from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";

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

  // A delete frees what the key held, so the version before it is gone.
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

  it("shows a TS source's late points against the edge at or above its lateness window", async () => {
    const { client } = await liveClient();
    const yaml = `kind: Processing\nname: late\n\nsources:\n  - name: in\n    ts:\n      measurement: late-in\n      lateness_ms: 20000\n\nsinks:\n  - name: out\n    stream:\n      name: late-out\n`;
    const id = await client.processing.submit(yaml);
    cleanups.push(async () => void (await client.processing.cancel(id).catch(() => {})));
    const deadline = Date.now() + 10000;
    let status = await client.processing.status(id);
    while (!status.running) {
      expect(Date.now(), "the job never ran on this node").toBeLessThan(deadline);
      await new Promise((r) => setTimeout(r, 50));
      status = await client.processing.status(id);
    }
    // 20 s reports against the 30 s edge.
    expect([status.late_edge_ms, status.late_points]).toEqual([30000, 0]);
  });

  it("shows a job's status with its runtime and definition, and removes it only once it no longer runs", async () => {
    const { client } = await liveClient();
    const yaml = `kind: Processing\nname: tidy\n\nsources:\n  - name: in\n    stream:\n      name: tidy-in\n\nsinks:\n  - name: out\n    kv:\n      key_prefix: tidy\n`;
    const id = await client.processing.submit(yaml);
    cleanups.push(async () => void (await client.processing.cancel(id).catch(() => {})));
    const status = await client.processing.status(id);
    expect(status).toMatchObject({ job_id: id, name: "tidy", status: "running", yaml });
    expect(typeof status.running).toBe("boolean");
    // A stream source counts no late points.
    expect([status.late_edge_ms, status.late_points]).toEqual([null, 0]);
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

  it("reads a stream never written as not found, one read past its end as empty, and a blocking read waits for it", async () => {
    const { client } = await liveClient();
    await client.stream.append("there", encoder.encode("x"));
    const err = await client.stream.read("never").catch((e) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err.serverMessage).toBe("stream not found");
    const all = await client.stream.read("there");
    expect(all.records).toHaveLength(1);
    expect(await client.stream.read("there", { start: all.records[0]!.id })).toEqual({ records: [] });
    // A blocking read of a stream not created yet waits rather than failing.
    expect(await client.stream.read("later", { blockMs: 200 })).toEqual({ records: [] });
  });

  it("deletes a stream: a non-empty one only with force, a missing one without complaint", async () => {
    const { client } = await liveClient();
    await client.stream.append("doomed", encoder.encode("x"));
    await expect(client.stream.delete("doomed")).rejects.toBeInstanceOf(ConflictError);
    await client.stream.delete("doomed", { force: true });
    // Deleted, it isn't there: a read is not found.
    await expect(client.stream.read("doomed")).rejects.toBeInstanceOf(NotFoundError);
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

  it("peeks a queue's next messages in dequeue order without taking them, and lists no dead letters", async () => {
    const { client } = await liveClient();
    const seqs: bigint[] = [];
    for (const [p, priority] of [["low", 5], ["high", 1], ["mid", 3]] as const) seqs.push(await client.queue.enqueue("jobs", encoder.encode(p), { priority }));
    const peeked = await client.queue.peek("jobs", 2);
    expect(peeked.messages.map((m) => [decoder.decode(m.payload), m.priority, m.deliveries, m.size])).toEqual([
      ["high", 1, 0, 4],
      ["mid", 3, 0, 3],
    ]);
    expect(peeked.more).toBe(true);
    expect((await client.queue.peek("jobs")).messages).toHaveLength(3);
    const taken = await client.queue.dequeue("jobs", 1);
    expect(decoder.decode(taken.messages[0]!.payload)).toBe("high");
    // Nothing has died: an empty last page.
    expect(await client.queue.dlqList("jobs")).toEqual({ messages: [], cursor: null });
    expect(seqs).toHaveLength(3);
  });

  it("pages a queue's dead letters, requeues and deletes them, and refuses ones that aren't dead", async () => {
    const { client } = await liveClient();
    for (const p of ["a", "bb", "ccc"]) await client.queue.enqueue("jobs", encoder.encode(p));
    const taken = await client.queue.dequeue("jobs", 3, { consumer: "c1" });
    expect(taken.messages).toHaveLength(3);
    expect(await client.queue.fail("jobs", taken, { dead: true, error: "boom" })).toEqual({ applied: 3, refused: [] });
    const live = await client.queue.enqueue("jobs", encoder.encode("alive"));
    const deadSeqs = taken.messages.map((m) => m.seq);

    // Two pages: the first has a cursor, the last none.
    const first = await client.queue.dlqList("jobs", { limit: 2 });
    expect(first.messages).toHaveLength(2);
    expect(first.cursor).not.toBeNull();
    const last = await client.queue.dlqList("jobs", { limit: 2, cursor: first.cursor });
    expect(last.cursor).toBeNull();
    const all = [...first.messages, ...last.messages];
    expect(all.map((m) => m.seq).sort()).toEqual([...deadSeqs].sort());
    for (const m of all) {
      expect(m).toMatchObject({ deliveries: 1, failures: 1, error: "boom" });
      expect(m.deadAtMs).toBeGreaterThan(0n);
      expect(decoder.decode(m.payload)).toBe(["a", "bb", "ccc"][deadSeqs.indexOf(m.seq)]);
      expect(m.size).toBe(m.payload.length);
    }

    // A dry run deletes nothing; a requeue of a live message is refused not_dead, of none gone.
    expect(await client.queue.dlqDelete("jobs", [deadSeqs[0]!], { dryRun: true })).toEqual({ applied: 1, refused: [], more: false });
    expect((await client.queue.dlqList("jobs")).messages).toHaveLength(3);
    const requeued = await client.queue.dlqRequeue("jobs", [deadSeqs[0]!, live, 1n << 40n]);
    expect(requeued.applied).toBe(1);
    expect(requeued.more).toBe(false);
    expect(requeued.refused.map((r) => [r.seq, r.why])).toEqual([
      [live, QueueRefusal.NotDead],
      [1n << 40n, QueueRefusal.Gone],
    ]);
    expect(requeued.refused[0]!.reason.length).toBeGreaterThan(0);
    // Requeued, it is ready again, its failures reset.
    const back = await client.queue.peek("jobs");
    expect(back.messages.map((m) => m.seq).sort()).toEqual([deadSeqs[0]!, live].sort());

    // Delete all, a batch at a time.
    expect(await client.queue.dlqDelete("jobs", [], { all: true, limit: 1 })).toEqual({ applied: 1, refused: [], more: true });
    expect(await client.queue.dlqDelete("jobs", [], { all: true })).toEqual({ applied: 1, refused: [], more: false });
    expect(await client.queue.dlqList("jobs")).toEqual({ messages: [], cursor: null });
    const notDead = await client.queue.dlqDelete("jobs", [live]);
    expect(notDead.applied).toBe(0);
    expect(notDead.refused.map((r) => r.why)).toEqual([QueueRefusal.NotDead]);
  }, 30000);

  it("pages a group's dead letters, requeues and deletes them, and refuses ones that aren't dead", async () => {
    const { client } = await liveClient();
    for (const p of ["a", "b", "c"]) await client.stream.append("events", encoder.encode(p));
    const opts = { group: "g1", consumer: "c1" };
    const read = await client.stream.groupRead("events", { ...opts, limit: 10 });
    expect(read.records).toHaveLength(3);
    const [r0, r1, r2] = read.records;
    const nacked = await client.stream.groupNack("events", { ...read, records: [r0!, r1!] }, { group: "g1", dead: true, error: "bad record" });
    expect(nacked).toEqual({ applied: 2, refused: [] });

    const first = await client.stream.groupDlqList("events", "g1", { limit: 1 });
    expect(first.dead).toHaveLength(1);
    expect(first.cursor).not.toBeNull();
    const last = await client.stream.groupDlqList("events", "g1", { cursor: first.cursor });
    expect(last.cursor).toBeNull();
    const dead = [...first.dead, ...last.dead];
    expect(dead.map((d) => d.id.toString()).sort()).toEqual([r0!.id.toString(), r1!.id.toString()].sort());
    for (const d of dead) {
      expect(d).toMatchObject({ deliveries: 1, failures: 1, error: "bad record" });
      expect(d.deadAtMs).toBeGreaterThan(0n);
    }

    // r2 is leased, not dead: a requeue or delete naming it is refused not_dead.
    const requeued = await client.stream.groupDlqRequeue("events", "g1", [r0!.id, r2!.id]);
    expect(requeued.applied).toBe(1);
    expect(requeued.more).toBe(false);
    expect(requeued.refused.map((r) => [r.id.toString(), r.why])).toEqual([[r2!.id.toString(), QueueRefusal.NotDead]]);
    expect(requeued.refused[0]!.reason.length).toBeGreaterThan(0);
    // Requeued, it is read again.
    const again = await client.stream.groupRead("events", { group: "g1", consumer: "c2", limit: 10 });
    expect(again.records.map((r) => [r.id.toString(), r.deliveries])).toEqual([[r0!.id.toString(), 2]]);

    expect(await client.stream.groupDlqDelete("events", "g1", [r1!.id], { dryRun: true })).toEqual({ applied: 1, refused: [], more: false });
    expect((await client.stream.groupDlqList("events", "g1")).dead).toHaveLength(1);
    const deleted = await client.stream.groupDlqDelete("events", "g1", [], { all: true });
    expect(deleted).toEqual({ applied: 1, refused: [], more: false });
    expect(await client.stream.groupDlqList("events", "g1")).toEqual({ dead: [], cursor: null });
    const notDead = await client.stream.groupDlqDelete("events", "g1", [r2!.id]);
    expect(notDead.refused.map((r) => r.why)).toEqual([QueueRefusal.NotDead]);

    await expect(client.stream.groupDlqList("events", "nope")).rejects.toBeInstanceOf(NotFoundError);
    await expect(client.stream.groupDlqRequeue("events", "nope", [], { all: true })).rejects.toBeInstanceOf(NotFoundError);
  }, 30000);

  it("steals a group's idle records to another consumer, refusing ones not idle long enough", async () => {
    const { client } = await liveClient();
    await client.stream.append("events", encoder.encode("x"));
    const read = await client.stream.groupRead("events", { group: "g1", consumer: "c1", limit: 10 });
    expect(read.records).toHaveLength(1);
    const steal = { group: "g1", consumer: "c2" };

    // Not idle for 12 h: refused not_idle, nothing taken.
    const early = await client.stream.groupSteal("events", read.records, { ...steal, minIdleMs: 43_200_000 });
    expect(early.records).toEqual([]);
    expect(early.expiresAtMs).toBe(0n);
    expect(early.refused.map((r) => [r.id.toString(), r.why])).toEqual([[read.records[0]!.id.toString(), QueueRefusal.NotIdle]]);
    expect(early.refused[0]!.reason.length).toBeGreaterThan(0);

    await new Promise((r) => setTimeout(r, 100));
    const stolen = await client.stream.groupSteal("events", read.records, { ...steal, minIdleMs: 50, leaseMs: 5000 });
    expect(stolen.refused).toEqual([]);
    expect(stolen.consumer).toBe("c2");
    expect(stolen.records.map((r) => [r.id.toString(), r.deliveries])).toEqual([[read.records[0]!.id.toString(), 2]]);
    expect(stolen.expiresAtMs).toBeGreaterThan(BigInt(Date.now()));
    const members = await client.stream.groupMembers("events", "g1");
    expect(members.members.map((m) => [m.id, m.pending])).toEqual([["c2", 1]]);

    // The old holder's delivery is over; the thief's acks.
    const stale = await client.stream.groupAck("events", read, { group: "g1" });
    expect(stale.applied).toBe(0);
    expect(stale.refused).toHaveLength(1);
    expect(await client.stream.groupAck("events", stolen, { group: "g1" })).toEqual({ applied: 1, refused: [] });

    await expect(client.stream.groupSteal("events", read.records, { group: "nope", consumer: "c2", minIdleMs: 1 })).rejects.toBeInstanceOf(NotFoundError);
  }, 30000);

  it("alters a stream an append made, info shows it, and a create must then name its settings", async () => {
    const { client } = await liveClient();
    await client.stream.append("made", encoder.encode("x"));
    expect(await client.stream.info("made")).toMatchObject({ count: 1n, partitionCount: 1, retentionAgeS: 0n, retentionCount: 0n });
    await client.stream.alter("made", { retentionCount: 1000n });
    expect(await client.stream.info("made")).toMatchObject({ count: 1n, retentionAgeS: 0n, retentionCount: 1000n });
    await client.stream.alter("made", { retentionAgeS: 3600n });
    expect(await client.stream.info("made")).toMatchObject({ retentionAgeS: 3600n, retentionCount: 1000n });

    // Altered, it has settings: a create naming others is a conflict; the same ones succeed.
    const err = await client.stream.create("made").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ConflictError);
    expect((err as ConflictError).message).toContain("the stream exists with other settings");
    await client.stream.create("made", { retentionAgeS: 3600n, retentionCount: 1000n });

    // A create of a stream an append made, never altered, gives it the settings asked.
    await client.stream.append("plain", encoder.encode("y"));
    await client.stream.create("plain", { retentionCount: 50n });
    expect(await client.stream.info("plain")).toMatchObject({ count: 1n, retentionCount: 50n });
    await expect(client.stream.create("plain", { retentionCount: 51n })).rejects.toBeInstanceOf(ConflictError);

    await expect(client.stream.alter("never", { retentionCount: 1n })).rejects.toBeInstanceOf(NotFoundError);
  });

  it("an alter clears a retention bound; set and clear, a create's clear and a bound of u64 max are refused", async () => {
    const { client, namespace } = await liveClient();
    await client.stream.create("ret", { retentionAgeS: 3600n, retentionCount: 100n });
    await client.stream.alter("ret", { clearRetentionAge: true });
    expect(await client.stream.info("ret")).toMatchObject({ retentionAgeS: 0n, retentionCount: 100n });
    await client.stream.alter("ret", { clearRetentionCount: true });
    expect(await client.stream.info("ret")).toMatchObject({ retentionAgeS: 0n, retentionCount: 0n });
    // A create must now name no bounds.
    await client.stream.create("ret");

    // Set and clear of one bound: this SDK refuses it, and so does the server.
    await client.stream.alter("ret", { retentionCount: 100n });
    await expect(client.stream.alter("ret", { retentionCount: 5n, clearRetentionCount: true })).rejects.toThrow(/both set retentionCount and clear it/);
    const both = await client.sendRequest(OpCode.StreamAlter, namespace, encoder.encode("ret"), new Uint8Array(0), new OptionsBuilder().addU64(OptionTag.RetentionCount, 5n).addFlag(OptionTag.RetentionCountClear).build());
    expect(both.status).toBe(StatusCode.BadRequest);
    expect(decoder.decode(both.data.subarray(3))).toContain("both sets and clears");
    expect(await client.stream.info("ret")).toMatchObject({ retentionCount: 100n });

    // stream.create can't send a clear; sent by hand, it is refused.
    const value = new Uint8Array([1, 0, 0, 0]);
    const created = await client.sendRequest(OpCode.StreamCreate, namespace, encoder.encode("fresh"), value, new OptionsBuilder().addFlag(OptionTag.RetentionAgeClear).build());
    expect(created.status).toBe(StatusCode.BadRequest);
    expect(decoder.decode(created.data.subarray(3))).toContain("only an alter drops a retention bound");
    await expect(client.stream.info("fresh")).rejects.toBeInstanceOf(NotFoundError);

    // u64 max is how the log carries a clear: a client's own is refused.
    const max = (1n << 64n) - 1n;
    for (const call of [() => client.stream.create("huge", { retentionCount: max }), () => client.stream.alter("ret", { retentionCount: max })]) {
      const err = await call().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BadRequestError);
      expect((err as BadRequestError).message).toContain("retention bound too large");
    }
    await expect(client.stream.alter("ret", { retentionAgeS: max })).rejects.toBeInstanceOf(BadRequestError);
    expect(await client.stream.info("ret")).toMatchObject({ retentionAgeS: 0n, retentionCount: 100n });
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

  /** A group's records by state, from stream.group.info: [pending:u64][holders:u32][created_at_ns:u64][ready:u64][leased:u64][dead:u64][trimmed_pending:u64]. */
  async function groupCounts(client: FloClient, namespace: string, stream: string, group: string) {
    const g = encoder.encode(group);
    const value = new Uint8Array(2 + g.length);
    new DataView(value.buffer).setUint16(0, g.length, true);
    value.set(g, 2);
    const resp = await client.sendRequest(OpCode.StreamGroupInfo, namespace, encoder.encode(stream), value, new Uint8Array(0));
    expect(resp.status).toBe(StatusCode.OK);
    expect(resp.data.length).toBe(52);
    const v = new DataView(resp.data.buffer, resp.data.byteOffset, resp.data.byteLength);
    return { ready: v.getBigUint64(20, true), leased: v.getBigUint64(28, true), dead: v.getBigUint64(36, true) };
  }

  it("a stopped worker abandons its records: unstarted, or cut short by the stop; redelivered with deliveries 2, never dead", async () => {
    const { client, namespace } = await liveClient();
    for (const p of ["a", "b", "c"]) await client.stream.append("events", encoder.encode(p));
    // maxDeliver 1: a counted failure would kill a record at once.
    let started = 0;
    const w = new StreamWorker(client, { stream: "events", group: "g1", consumer: "w1", ackTimeoutMs: 30000, maxDeliver: 1, concurrency: 1, batchSize: 3, blockMs: 1000 }, async (ctx) => {
      started++;
      // A handler the stop cuts short: it ends on the worker's signal, throwing.
      await new Promise<void>((resolve) => ctx.signal.addEventListener("abort", () => resolve(), { once: true }));
      throw new Error("stopped");
    });
    const done = w.start();
    const deadline = Date.now() + 10000;
    while (started === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    expect(started).toBe(1);
    // "b" and "c" wait for the one slot; the stop hands them back, and "a" too.
    w.stop();
    await done;

    const opts = { group: "g1", consumer: "c2", limit: 10 };
    let again = await client.stream.groupRead("events", opts);
    expect(again.records.map((r) => [decoder.decode(r.payload), r.deliveries])).toEqual([["a", 2], ["b", 2], ["c", 2]]);
    expect(await groupCounts(client, namespace, "events", "g1")).toEqual({ ready: 0n, leased: 3n, dead: 0n });

    // Abandoned again and again, past maxDeliver: each comes back, none dies.
    for (let n = 3; n <= 5; n++) {
      expect(await client.stream.groupNack("events", again, { group: "g1", abandon: true })).toEqual({ applied: 3, refused: [] });
      again = await client.stream.groupRead("events", opts);
      expect(again.records.map((r) => r.deliveries)).toEqual([n, n, n]);
    }
    expect(await groupCounts(client, namespace, "events", "g1")).toEqual({ ready: 0n, leased: 3n, dead: 0n });
    // Abandon takes no failure's options: the server refuses them too.
    const raw = await client.sendRequest(OpCode.StreamGroupNack, namespace, encoder.encode("events"), groupNackValue("g1", again, new Uint8Array(0)), new OptionsBuilder().addFlag(OptionTag.Abandon).addFlag(OptionTag.Dead).build());
    expect(raw.status).toBe(StatusCode.BadRequest);
    // A nack, though, counts a failure: past maxDeliver 1, they're dead.
    expect(await client.stream.groupNack("events", again, { group: "g1" })).toEqual({ applied: 3, refused: [] });
    expect(await groupCounts(client, namespace, "events", "g1")).toEqual({ ready: 0n, leased: 0n, dead: 3n });
  }, 30000);

  it("close() on a worker with its own client acks the running record and abandons the rest before closing", async () => {
    // A worker given an endpoint owns its client, in namespace "default".
    const { client } = await liveClient();
    const stream = `close-${randomBytes(4).toString("hex")}`;
    for (const p of ["a", "b", "c"]) await client.stream.append(stream, encoder.encode(p), { namespace: "default" });
    let started = 0;
    const w = new StreamWorker(endpoint, { stream, group: "g1", consumer: "w1", ackTimeoutMs: 30000, maxDeliver: 1, concurrency: 1, batchSize: 3, blockMs: 1000 }, async () => {
      started++;
      await new Promise((r) => setTimeout(r, 300));
    });
    const done = w.start();
    const deadline = Date.now() + 10000;
    while (started === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    expect(started).toBe(1);
    await w.close();
    await done;
    // "a" was acked; "b" and "c" come back at once, not dead.
    const again = await client.stream.groupRead(stream, { namespace: "default", group: "g1", consumer: "c2", limit: 10 });
    expect(again.records.map((r) => [decoder.decode(r.payload), r.deliveries])).toEqual([["b", 2], ["c", 2]]);
    expect(await groupCounts(client, "default", stream, "g1")).toEqual({ ready: 0n, leased: 2n, dead: 0n });
  }, 30000);

  it("dead-letters a record with a 1024-byte nack error; the server refuses 1025", async () => {
    const { client, namespace } = await liveClient();
    await client.stream.append("events", encoder.encode("a"));
    const read = await client.stream.groupRead("events", { group: "g1", consumer: "c1", limit: 10 });
    expect(read.records).toHaveLength(1);
    // Past the client's own check, the server refuses a longer one as a bad request.
    const tooLong = await client.sendRequest(OpCode.StreamGroupNack, namespace, encoder.encode("events"), groupNackValue("g1", read, new Uint8Array(1025).fill(0x78)), new Uint8Array(0));
    expect(tooLong.status).toBe(StatusCode.BadRequest);
    await expect(client.stream.groupNack("events", read, { group: "g1", error: "x".repeat(1025) })).rejects.toThrow(/at most 1024 bytes/);
    expect(await client.stream.groupNack("events", read, { group: "g1", dead: true, error: "é".repeat(512) })).toEqual({ applied: 1, refused: [] });
    expect(await groupCounts(client, namespace, "events", "g1")).toEqual({ ready: 0n, leased: 0n, dead: 1n });
  });

  it("refuses a report to a deleted group not found, not as its records gone", async () => {
    const { client, namespace } = await liveClient();
    await client.stream.append("events", encoder.encode("a"));
    const read = await client.stream.groupRead("events", { group: "g1", consumer: "c1", limit: 10 });
    expect(read.records).toHaveLength(1);
    const g = encoder.encode("g1");
    const value = new Uint8Array([g.length, 0, ...g]);
    expect((await client.sendRequest(OpCode.StreamGroupDelete, namespace, encoder.encode("events"), value, new Uint8Array(0))).status).toBe(StatusCode.OK);
    for (const report of [
      () => client.stream.groupAck("events", read, { group: "g1" }),
      () => client.stream.groupNack("events", read, { group: "g1" }),
      () => client.stream.groupNack("events", read, { group: "g1", abandon: true }),
      () => client.stream.groupTouch("events", read, { group: "g1" }),
    ]) {
      const err = await report().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NotFoundError);
      expect(err).toMatchObject({ reason: Reason.NotFound });
    }
  });

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
    // Nothing holds its end: it isn't a workflow step's.
    expect(status.held).toBeUndefined();

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

  it("close() on an ActionWorker with an action running sends its complete before closing", async () => {
    const { client, namespace } = await liveClient();
    const worker = new ActionWorker({ endpoint, namespace, blockMs: 1000 });
    let started = false;
    worker.action("slow", async () => {
      started = true;
      await new Promise((r) => setTimeout(r, 400));
      return encoder.encode("done");
    });
    const done = worker.start();
    await new Promise((r) => setTimeout(r, 200));
    const { runId } = await client.action.invoke("slow", new Uint8Array(0));
    const deadline = Date.now() + 10000;
    while (!started && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    expect(started).toBe(true);
    await worker.close();
    await done;
    // The complete went out before close() returned: no wait for a lease.
    const status = await client.action.status(runId);
    expect(status.status).toBe("completed");
    expect(decoder.decode(status.output!)).toBe("done");
  }, 30000);
});

describe.skipIf(!endpoint)("live time series", () => {
  const T0 = 1_700_000_000_000_000_000n;

  /** Every point a read pages through in `order`, a page of `limit` at a time, and how many pages it took. */
  async function pageAll(client: FloClient, measurement: string, order: TSOrder, limit: number, tags?: string): Promise<{ points: TSPoint[]; pages: number }> {
    const points: TSPoint[] = [];
    let cursor: Uint8Array | undefined;
    for (let pages = 1; ; pages++) {
      const page = await client.ts.read(measurement, { order, limit, cursor, tags });
      points.push(...page.points);
      if (page.cursor === null) return { points, pages };
      cursor = page.cursor;
    }
  }

  it("pages 5000 points over 3 series through, both ways, each once and in order", async () => {
    const { client } = await liveClient();
    // Series s's point j at T0 + j µs: the three series share each time.
    const written: { s: number; j: number }[] = [];
    for (let i = 0; i < 5000; i++) written.push({ s: i % 3, j: Math.floor(i / 3) });
    for (let i = 0; i < written.length; i += 250) {
      await Promise.all(written.slice(i, i + 250).map(({ s, j }) => client.ts.write("m5k", s * 10_000 + j, { tags: `host=h${s}`, timeNs: T0 + BigInt(j) * 1000n })));
    }
    const oldest = await pageAll(client, "m5k", TSOrder.OldestFirst, 1000);
    const newest = await pageAll(client, "m5k", TSOrder.NewestFirst, 1000);
    expect([oldest.pages, newest.pages]).toEqual([5, 5]);
    for (const { points } of [oldest, newest]) {
      expect(points.length).toBe(5000);
      expect(new Set(points.map((p) => p.value)).size).toBe(5000);
      for (const p of points) expect(p.timeNs).toBe(T0 + BigInt(p.value % 10_000) * 1000n);
    }
    for (let i = 1; i < 5000; i++) expect(oldest.points[i]!.timeNs >= oldest.points[i - 1]!.timeNs).toBe(true);
    // Newest first is the same order, backwards: ties go by write order too.
    expect(newest.points).toEqual([...oldest.points].reverse());
    // The iterator walks the same points.
    const all: TSPoint[] = [];
    for await (const p of client.ts.readAll("m5k", { limit: 777 })) all.push(p);
    expect(all).toEqual(oldest.points);
    // A tag filter takes only its series.
    const h1 = await pageAll(client, "m5k", TSOrder.OldestFirst, 4096, "host=h1");
    expect(h1.points.length).toBe(written.filter((w) => w.s === 1).length);
    expect(h1.points.every((p) => Math.floor(p.value / 10_000) === 1)).toBe(true);
  }, 60000);

  it("reads the last point written first, newest first with limit 1", async () => {
    const { client } = await liveClient();
    await client.ts.write("last", 1, { timeNs: T0 });
    await client.ts.write("last", 2, { timeNs: T0 + 5n });
    await client.ts.write("last", 3, { timeNs: T0 + 5n });
    const page = await client.ts.read("last", { order: TSOrder.NewestFirst, limit: 1 });
    expect(page.points).toEqual([{ timeNs: T0 + 5n, value: 3 }]);
    expect(page.cursor).not.toBeNull();
  });

  it("reads back a time that isn't a whole ms exactly, as the write answered it", async () => {
    const { client } = await liveClient();
    const t = 1_700_000_000_123_456_789n;
    expect((await client.ts.write("exact", 4.5, { timeNs: t })).timeNs).toBe(t);
    expect(await client.ts.read("exact", { fromNs: t, toNs: t })).toEqual({ points: [{ timeNs: t, value: 4.5 }], cursor: null, stale: false });
    // No time: the server's own, in ns.
    const before = nsFromDate(new Date());
    const now = (await client.ts.write("exact", 1)).timeNs;
    expect(now >= before - 1_000_000_000n && now <= nsFromDate(new Date()) + 1_000_000_000n).toBe(true);
  });

  it("refuses a time before 1970 and a timestamp option not 8 bytes", async () => {
    const { client, namespace } = await liveClient();
    const err = await client.ts.write("bad", 1, { timeNs: -1n }).catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("ts write: a point's time must not be before 1970");
    const value = new Uint8Array(8);
    const raw = await client.sendRequest(OpCode.TSWrite, namespace, encoder.encode("bad"), value, new OptionsBuilder().addU32(OptionTag.TSTimestampNS, 5).build());
    expect(raw.status).toBe(StatusCode.BadRequest);
    expect(decoder.decode(raw.data.subarray(3))).toBe("ts write: the timestamp option must be an 8-byte i64 of nanoseconds");
  });

  it("reads a series that doesn't exist as not found, and a range with no points as an empty page", async () => {
    const { client } = await liveClient();
    await client.ts.write("real", 1, { timeNs: T0, tags: "host=a" });
    const err = await client.ts.read("never").catch((e) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err.serverMessage).toBe("not found: 'never' has no series of field 'value'");
    await expect(client.ts.read("real", { field: "other" })).rejects.toBeInstanceOf(NotFoundError);
    await expect(client.ts.read("real", { tags: "host=b" })).rejects.toBeInstanceOf(NotFoundError);
    const iterate = async () => {
      for await (const _ of client.ts.readAll("never")) void _;
    };
    await expect(iterate()).rejects.toBeInstanceOf(NotFoundError);
    expect(await client.ts.read("real", { fromNs: T0 + 1n })).toEqual({ points: [], cursor: null, stale: false });
  });

  it("refuses a cursor that isn't one a page answered, and an empty measurement", async () => {
    const { client } = await liveClient();
    await client.ts.write("cur", 1, { timeNs: T0 });
    const err = await client.ts.read("cur", { cursor: new Uint8Array(5) }).catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("bad request: a cursor is the 16 bytes a page answered");
    await expect(client.ts.read("")).rejects.toThrow(BadRequestError);
  });

  it("queries windows in ns: from, to and window over points at known ns times", async () => {
    const { client } = await liveClient();
    // A point every second, 123 ns past it, for 6 s; windows of 2 s.
    for (let k = 0; k < 6; k++) await client.ts.write("q", k + 1, { timeNs: T0 + BigInt(k) * 1_000_000_000n + 123n });
    const got = await client.ts.query("q", { fromNs: T0, toNs: T0 + 5_000_000_000n, windowNs: 2_000_000_000n, aggregation: "sum" });
    expect(got).toEqual([
      {
        key: "q",
        buckets: [
          { startNs: T0, value: 1 + 2 },
          { startNs: T0 + 2_000_000_000n, value: 3 + 4 },
          { startNs: T0 + 4_000_000_000n, value: 5 },
        ],
      },
    ]);
    // A bound inside a second: the 123 ns past it counts.
    const narrow = await client.ts.query("q", { fromNs: T0 + 124n, toNs: T0 + 1_000_000_123n, windowNs: 2_000_000_000n, aggregation: "count" });
    expect(narrow).toEqual([{ key: "q", buckets: [{ startNs: T0, value: 1 }] }]);
  });

  it("refuses a query window that gives more than 1024 windows, and a 0 window, with the server's text", async () => {
    const { client } = await liveClient();
    for (let i = 0; i < 1100; i += 250) {
      await Promise.all(Array.from({ length: Math.min(250, 1100 - i) }, (_, k) => client.ts.write("wide", 1, { timeNs: T0 + BigInt(i + k) * 1000n })));
    }
    const err = await client.ts.query("wide", { windowNs: 1000n }).catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("bad request: the matching points span more than 1024 windows of this width; widen the window (it is in nanoseconds) or narrow the range");
    const zero = await client.ts.query("wide", { windowNs: 0n }).catch((e) => e);
    expect(zero).toBeInstanceOf(BadRequestError);
    expect(zero.serverMessage).toBe("ts query: a window must be more than 0 ns");
    expect((await client.ts.query("wide", { windowNs: 2_000_000n }))[0]!.buckets.length).toBe(1);
  }, 30000);

  it("answers FloQL points at the ns times written, and ts.info's oldest and newest in ns", async () => {
    const { client } = await liveClient();
    // Inside FloQL's default range, the last hour, and not whole ms.
    const base = nsFromDate(new Date()) - 600_000_000_000n + 123_456_789n;
    const times = [base, base + 1_000_001n, base + 2_000_002n];
    for (const [i, t] of times.entries()) await client.ts.write("fns", i, { timeNs: t });
    const series = await client.ts.floql("fns");
    expect(series.map((s) => s.points)).toEqual([times.map((t, i) => ({ timeNs: t, value: i }))]);
    const info = await client.ts.info("fns");
    expect(info).toMatchObject({ series: 1, fields: ["value"], points: 3n, oldestNs: times[0], newestNs: times[2] });
    await expect(client.ts.info("never")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("refuses a raw FloQL answer of more than 4096 points with the server's text", async () => {
    const { client } = await liveClient();
    // Inside FloQL's default range, the last hour.
    const base = nsFromDate(new Date()) - 600_000_000_000n;
    for (let i = 0; i < 4100; i += 250) {
      await Promise.all(Array.from({ length: Math.min(250, 4100 - i) }, (_, k) => client.ts.write("fq", i + k, { timeNs: base + BigInt(i + k) * 1000n })));
    }
    const err = await client.ts.floql("fq").catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("bad request: the answer holds more than 4096 points; aggregate them, narrow the query, or page through them with ts read");
    const avg = await client.ts.floql("fq | avg()");
    expect(avg.length).toBe(1);
  }, 60000);
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

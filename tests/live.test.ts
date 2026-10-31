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
import { ConflictError, NoNamespaceError, NotFoundError, ProcessKind, Ran, Reason, ServerError, StatusCode, TABLE_HASH, VERSION, WorkerStatus, WorkerType, type WorkerInfo } from "@floruntime/core";
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
      for (const a of want) await client.action.register(a);
      const got = await pageAll(
        (cursor) => client.action.list({ limit: 3, cursor }),
        (p) => ({ names: p.names, done: !p.hasMore, cursor: p.cursor })
      );
      expectEachOnce(got, want);
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

  it("enqueues, dequeues and acks a message", async () => {
    const { client } = await liveClient();
    await client.queue.enqueue("jobs", encoder.encode("job-1"));
    const res = await client.queue.dequeue("jobs", 1);
    expect(res.messages.map((m) => decoder.decode(m.payload))).toEqual(["job-1"]);
    await client.queue.ack("jobs", [res.messages[0]!.seq]);
    expect((await client.queue.peek("jobs", 10)).messages).toHaveLength(0);
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

  // A handler past its action's 1 s timeout runs once when the worker touches
  // it: without autoTouch the server would take the run back and hand it out
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
        await new Promise((r) => setTimeout(r, 2500));
        return encoder.encode("ok");
      },
      { timeoutMs: 1000 }
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
  }, 30000);

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

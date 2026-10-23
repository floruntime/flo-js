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
import { TABLE_HASH, VERSION, WorkerStatus, WorkerType, type WorkerInfo } from "@floruntime/core";
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
    const versions = await client.kv.history("h");
    expect(versions.map((v) => ({ deleted: v.deleted, value: decoder.decode(v.value) }))).toEqual([
      { deleted: false, value: "three" },
      { deleted: true, value: "" },
      { deleted: false, value: "one" },
    ]);
  });

  it("appends to a stream and reads it back in order", async () => {
    const { client } = await liveClient();
    for (const p of ["one", "two", "three"]) await client.stream.append("events", encoder.encode(p));
    const res = await client.stream.read("events");
    expect(res.records.map((r) => decoder.decode(r.payload))).toEqual(["one", "two", "three"]);
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

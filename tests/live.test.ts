/**
 * Live tests against a real flo server: FLO_ENDPOINT names it (CI starts the
 * commit pinned in .github/flo-ref.env), and FLO_BINARY the binary, for the
 * version check. Without FLO_ENDPOINT they skip, except under CI=1, where a
 * missing server is a failure, not a skip.
 */

import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { TABLE_HASH, VERSION } from "@floruntime/core";
import { ActionWorker, FloClient } from "@floruntime/node";

const endpoint = process.env.FLO_ENDPOINT ?? "";
const ci = process.env.CI === "1";
if (ci && !endpoint) throw new Error("CI=1 but FLO_ENDPOINT is not set");
// The one test that checks the pin mustn't vanish quietly in CI.
if (ci && !process.env.FLO_BINARY) throw new Error("CI=1 but FLO_BINARY is not set");

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

/** A connected client in a namespace of its own, so tests on one server don't see each other's data. */
async function liveClient(): Promise<{ client: FloClient; namespace: string }> {
  const namespace = `live-${randomBytes(4).toString("hex")}`;
  const client = new FloClient(endpoint, { namespace, timeoutMs: 10000 });
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
    const worker = new ActionWorker({ endpoint, namespace, blockMs: 1000, heartbeatIntervalMs: 0, logger });
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
  });

  // A handler past its action's 1 s timeout runs once when the worker touches
  // it: without autoTouch the server would take the run back and hand it out
  // again.
  it("keeps a long handler from running twice with autoTouch", async () => {
    const { client, namespace } = await liveClient();
    const logged: string[] = [];
    const logger = { debug: (m: string) => void logged.push(m), warn: () => {}, error: (m: string) => void logged.push(m) };
    const worker = new ActionWorker({ endpoint, namespace, blockMs: 500, heartbeatIntervalMs: 0, logger, autoTouch: true });
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
});

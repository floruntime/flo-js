/**
 * Every list op carries its page size and cursor in the request value as
 * [limit:u32 LE][cursor bytes], not as options. kv.scan and worker.list are
 * table ops and code them as their schemas' fields.
 */

import { describe, it, expect } from "vitest";
import {
  ActionOperations,
  KVOperations,
  KVReadOnlyOperations,
  OpCode,
  OptionTag,
  ProcessKind,
  ProcessingOperations,
  WorkerOperations,
  WorkerStatus,
  WorkerType,
  IncompleteResponseError,
  FloError,
  WorkflowOperations,
  serializeListValue,
} from "@floruntime/core";
import { isTableOp } from "../packages/core/src/kv_body.js";

interface Sent {
  op: number;
  value: Uint8Array;
  options: Uint8Array;
}

/** A sender that records the request, then fails it so no reply is parsed. */
function capture(): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, _ns: string, _key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, value, options });
      throw new Error("captured");
    },
  };
  return { sender: sender as never, sent };
}

const limitOf = (v: Uint8Array) => new DataView(v.buffer, v.byteOffset, v.byteLength).getUint32(0, true);

/** Option tags present in a TLV options block: [tag:u8][len:u8][value]*. */
function tagsOf(options: Uint8Array): number[] {
  const tags: number[] = [];
  for (let i = 0; i < options.length; i += 2 + options[i + 1]!) tags.push(options[i]!);
  return tags;
}

const cursor = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);

describe("serializeListValue", () => {
  it("writes limit then cursor", () => {
    expect(Array.from(serializeListValue(7, cursor))).toEqual([7, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef]);
  });

  it("defaults to limit 0 (server default) and no cursor", () => {
    expect(Array.from(serializeListValue())).toEqual([0, 0, 0, 0]);
  });
});

describe("list requests", () => {
  const cases: [string, number, (s: never) => Promise<unknown>][] = [
    ["processing.list", OpCode.ProcessingList, (s) => new ProcessingOperations(s).list({ limit: 5, cursor })],
    [
      "workflow.listDefinitions",
      OpCode.WorkflowListDefinitions,
      (s) => new WorkflowOperations(s).listDefinitions({ limit: 5, cursor }),
    ],
  ];

  for (const [name, op, call] of cases) {
    it(`${name} sends [limit][cursor] in the value`, async () => {
      const { sender, sent } = capture();
      await expect(call(sender)).rejects.toThrow("captured");
      expect(sent).toHaveLength(1);
      expect(sent[0]!.op).toBe(op);
      expect(limitOf(sent[0]!.value)).toBe(5);
      expect(Array.from(sent[0]!.value.subarray(4))).toEqual(Array.from(cursor));
      expect(tagsOf(sent[0]!.options)).not.toContain(OptionTag.Limit);
    });
  }

  it("action.list sends [limit][cursor] in the value", async () => {
    const { sender, sent } = capture();
    await expect(new ActionOperations(sender).list({ limit: 5 })).rejects.toThrow("captured");
    await expect(new ActionOperations(sender).list({ limit: 5, cursor })).rejects.toThrow("captured");
    expect(sent[0]!.op).toBe(OpCode.ActionList);
    expect(Array.from(sent[0]!.value)).toEqual([5, 0, 0, 0]);
    expect(Array.from(sent[1]!.value)).toEqual([5, 0, 0, 0, ...cursor]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("kv.scan and the web kv.scan send limit (present) then a u16-prefixed cursor, and no options", async () => {
    for (const call of [
      (s: never) => new KVOperations(s).scan("p", { limit: 5, cursor }),
      (s: never) => new KVReadOnlyOperations(s).scan("p", { limit: 5, cursor }),
    ]) {
      const { sender, sent } = capture();
      await expect(call(sender)).rejects.toThrow("captured");
      expect(sent[0]!.op).toBe(OpCode.KVScan);
      expect(Array.from(sent[0]!.value)).toEqual([1, 5, 0, 0, 0, 4, 0, ...cursor]);
      expect(sent[0]!.options.length).toBe(0);
    }
  });

  it("kv.scan asks for 1024 names (the server's default) from the first page by default", async () => {
    const { sender, sent } = capture();
    await expect(new KVOperations(sender).scan("p")).rejects.toThrow("captured");
    expect(Array.from(sent[0]!.value)).toEqual([1, 0, 4, 0, 0, 0, 0]);
  });
});

/** One worker.list row as the server encodes it. */
function row(id: string, opts: { type?: number; status?: number; machine?: string } = {}): number[] {
  const out: number[] = [];
  const le = (v: bigint, n: number) => {
    for (let i = 0n; i < BigInt(n); i++) out.push(Number((BigInt.asUintN(64, v) >> (8n * i)) & 0xffn));
  };
  const idb = new TextEncoder().encode(id);
  le(BigInt(idb.length), 2);
  out.push(...idb, opts.type ?? 0, opts.status ?? 0);
  le(3n, 4); // current_load
  le(8n, 4); // max_concurrency
  le(5n, 8); // tasks_completed
  le(1n, 8); // tasks_failed
  le(1000n, 8); // registered_at_ms
  le(-2n, 8); // last_heartbeat_ms
  if (opts.machine === undefined) out.push(0);
  else {
    const m = new TextEncoder().encode(opts.machine);
    out.push(1, m.length, ...m);
  }
  return out;
}

function page(rows: number[][], cursor?: string): Uint8Array {
  const out = [rows.length & 0xff, rows.length >> 8, ...rows.flat()];
  if (cursor === undefined) out.push(0);
  else {
    const c = new TextEncoder().encode(cursor);
    out.push(1, c.length & 0xff, c.length >> 8, ...c);
  }
  return new Uint8Array(out);
}

/** A sender that records the request and answers OK with `data`. */
function answering(data: Uint8Array): { sender: never; sent: Sent[] } {
  const sent: Sent[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, _ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, value, options });
      expect(key.length).toBe(0);
      return { status: 0, data, requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

/** An action.list answer: [count:u32]([name_len:u16][name][value_len:u32])*[has_more:u8][cursor_len:u16][cursor]. */
function actionPage(names: string[], next?: Uint8Array): Uint8Array {
  const out = [names.length, 0, 0, 0];
  for (const n of names) {
    const b = new TextEncoder().encode(n);
    out.push(b.length & 0xff, b.length >> 8, ...b, 0, 0, 0, 0);
  }
  const c = next ?? new Uint8Array(0);
  out.push(next ? 1 : 0, c.length & 0xff, c.length >> 8, ...c);
  return new Uint8Array(out);
}

describe("action.list", () => {
  it("returns a page of names and the cursor that asks for the next", async () => {
    const first = answering(actionPage(["a", "b"], cursor));
    expect(await new ActionOperations(first.sender).list({ limit: 2 })).toEqual({ names: ["a", "b"], hasMore: true, cursor });
    const last = answering(actionPage(["c"]));
    expect(await new ActionOperations(last.sender).list({ limit: 2, cursor })).toEqual({ names: ["c"], hasMore: false, cursor: null });
    expect(Array.from(last.sent[0]!.value)).toEqual([2, 0, 0, 0, ...cursor]);
  });

  it("refuses an answer cut short, with bytes left over, or with has_more and no cursor", async () => {
    const whole = actionPage(["a"], cursor);
    for (let n = 0; n < whole.length; n++) {
      await expect(new ActionOperations(answering(whole.subarray(0, n)).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
    }
    await expect(new ActionOperations(answering(new Uint8Array([...whole, 0])).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
    await expect(new ActionOperations(answering(new Uint8Array([0, 0, 0, 0, 1, 0, 0])).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("worker.list", () => {
  it("is a table op: its body is limit (present), then a u16-prefixed cursor and action", async () => {
    expect(isTableOp(OpCode.WorkerList)).toBe(true);
    const { sender, sent } = answering(page([]));
    await new WorkerOperations(sender).list({ limit: 5, cursor });
    expect(sent[0]!.op).toBe(OpCode.WorkerList);
    expect(Array.from(sent[0]!.value)).toEqual([1, 5, 0, 0, 0, 4, 0, ...cursor, 0, 0]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("sends an action to list only the workers that run it", async () => {
    const { sender, sent } = answering(page([]));
    await new WorkerOperations(sender).list({ limit: 5, action: "echo" });
    expect(Array.from(sent[0]!.value)).toEqual([1, 5, 0, 0, 0, 0, 0, 4, 0, ...new TextEncoder().encode("echo")]);
  });

  it("asks for 1024 workers (the server's default) from the first page by default", async () => {
    const { sender, sent } = answering(page([]));
    await new WorkerOperations(sender).list();
    expect(Array.from(sent[0]!.value)).toEqual([1, 0, 4, 0, 0, 0, 0, 0, 0]);
  });

  it("refuses a limit outside 1 to 1024 before sending", async () => {
    for (const limit of [0, 1025, 1.5]) {
      const { sender, sent } = answering(page([]));
      await expect(new WorkerOperations(sender).list({ limit })).rejects.toBeInstanceOf(FloError);
      expect(sent).toHaveLength(0);
    }
  });

  it("decodes the page's rows and its cursor", async () => {
    const data = page([row("w1", { machine: "m" }), row("w2", { type: 1, status: 3 })], "w2");
    const res = await new WorkerOperations(answering(data).sender).list();
    expect(res.workers).toEqual([
      { id: "w1", type: WorkerType.Action, status: WorkerStatus.Active, currentLoad: 3, maxConcurrency: 8, tasksCompleted: 5n, tasksFailed: 1n, registeredAtMs: 1000n, lastHeartbeatMs: -2n, machineId: "m" },
      { id: "w2", type: WorkerType.Stream, status: WorkerStatus.Unhealthy, currentLoad: 3, maxConcurrency: 8, tasksCompleted: 5n, tasksFailed: 1n, registeredAtMs: 1000n, lastHeartbeatMs: -2n, machineId: undefined },
    ]);
    expect(new TextDecoder().decode(res.cursor!)).toBe("w2");
  });

  it("reads a null cursor on the last page", async () => {
    const res = await new WorkerOperations(answering(page([row("w1")])).sender).list();
    expect(res.workers.map((w) => w.id)).toEqual(["w1"]);
    expect(res.cursor).toBeNull();
  });

  it("refuses a cut answer, bytes left over, and a type or status it doesn't know", async () => {
    const whole = page([row("w1", { machine: "m" })], "w1");
    for (let n = 0; n < whole.length; n++) {
      await expect(new WorkerOperations(answering(whole.subarray(0, n)).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
    }
    const extra = new Uint8Array([...whole, 0]);
    await expect(new WorkerOperations(answering(extra).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
    for (const bad of [page([row("w", { type: 2 })]), page([row("w", { status: 4 })])]) {
      await expect(new WorkerOperations(answering(bad).sender).list()).rejects.toBeInstanceOf(IncompleteResponseError);
    }
  });
});

/** A worker.info answer: the row, the metadata (absent when undefined), then the processes. */
function info(r: number[], metadata: string | undefined, processes: [string, number, bigint, bigint, bigint][]): Uint8Array {
  const out = [...r];
  const le = (v: bigint, n: number) => {
    for (let i = 0n; i < BigInt(n); i++) out.push(Number((BigInt.asUintN(64, v) >> (8n * i)) & 0xffn));
  };
  if (metadata === undefined) out.push(0);
  else {
    const m = new TextEncoder().encode(metadata);
    out.push(1);
    le(BigInt(m.length), 2);
    out.push(...m);
  }
  le(BigInt(processes.length), 2);
  for (const [name, kind, runs, fails, lastRun] of processes) {
    const n = new TextEncoder().encode(name);
    le(BigInt(n.length), 2);
    out.push(...n, kind);
    le(runs, 8);
    le(fails, 8);
    le(lastRun, 8);
  }
  return new Uint8Array(out);
}

/** A sender that records the request and its key, and answers OK with `data`. */
function answeringInfo(data: Uint8Array): { sender: never; sent: (Sent & { key: string })[] } {
  const sent: (Sent & { key: string })[] = [];
  const sender = {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op: number, _ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
      sent.push({ op, key: new TextDecoder().decode(key), value, options });
      return { status: 0, data, requestId: 0n };
    },
  };
  return { sender: sender as never, sent };
}

describe("worker.info", () => {
  const w1 = { id: "w1", type: WorkerType.Action, status: WorkerStatus.Active, currentLoad: 3, maxConcurrency: 8, tasksCompleted: 5n, tasksFailed: 1n, registeredAtMs: 1000n, lastHeartbeatMs: -2n };

  it("is a table op with an empty body, keyed by the worker id", async () => {
    expect(isTableOp(OpCode.WorkerInfo)).toBe(true);
    const { sender, sent } = answeringInfo(info(row("w1"), undefined, []));
    await new WorkerOperations(sender).info("w1");
    expect(sent[0]).toEqual({ op: OpCode.WorkerInfo, key: "w1", value: new Uint8Array(0), options: new Uint8Array(0) });
  });

  it("decodes the row as worker.list does, the metadata and the processes", async () => {
    const data = info(row("w1", { machine: "m" }), '{"a":1}', [
      ["echo", ProcessKind.Action, 4n, 1n, 2000n],
      ["events/g", ProcessKind.StreamConsumer, 0n, 0n, 0n],
    ]);
    expect(await new WorkerOperations(answeringInfo(data).sender).info("w1")).toEqual({
      worker: { ...w1, machineId: "m" },
      metadata: new TextEncoder().encode('{"a":1}'),
      processes: [
        { name: "echo", kind: ProcessKind.Action, runCount: 4n, failCount: 1n, lastRunAtMs: 2000n },
        { name: "events/g", kind: ProcessKind.StreamConsumer, runCount: 0n, failCount: 0n, lastRunAtMs: 0n },
      ],
    });
  });

  it("decodes one with no metadata and no processes", async () => {
    const res = await new WorkerOperations(answeringInfo(info(row("w1"), undefined, [])).sender).info("w1");
    expect(res).toEqual({ worker: { ...w1, machineId: undefined }, metadata: null, processes: [] });
  });

  it("refuses a process kind it doesn't know", async () => {
    const bad = info(row("w1"), undefined, [["p", 2, 0n, 0n, 0n]]);
    const err = await new WorkerOperations(answeringInfo(bad).sender).info("w1").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(IncompleteResponseError);
    expect((err as Error).message).toContain("process kind 2");
  });

  it("refuses a cut answer and bytes left over", async () => {
    const whole = info(row("w1", { machine: "m" }), "x", [["p", ProcessKind.Action, 1n, 0n, 5n]]);
    for (let n = 0; n < whole.length; n++) {
      await expect(new WorkerOperations(answeringInfo(whole.subarray(0, n)).sender).info("w1")).rejects.toBeInstanceOf(IncompleteResponseError);
    }
    await expect(new WorkerOperations(answeringInfo(new Uint8Array([...whole, 0])).sender).info("w1")).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

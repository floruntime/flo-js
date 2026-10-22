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

  it("action.list sends [limit] in the value", async () => {
    const { sender, sent } = capture();
    await expect(new ActionOperations(sender).list({ limit: 5 })).rejects.toThrow("captured");
    expect(sent[0]!.op).toBe(OpCode.ActionList);
    expect(Array.from(sent[0]!.value)).toEqual([5, 0, 0, 0]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("kv.scan and the web kv.scan send limit (present) then a u8-prefixed cursor, and no options", async () => {
    for (const call of [
      (s: never) => new KVOperations(s).scan("p", { limit: 5, cursor }),
      (s: never) => new KVReadOnlyOperations(s).scan("p", { limit: 5, cursor }),
    ]) {
      const { sender, sent } = capture();
      await expect(call(sender)).rejects.toThrow("captured");
      expect(sent[0]!.op).toBe(OpCode.KVScan);
      expect(Array.from(sent[0]!.value)).toEqual([1, 5, 0, 0, 0, 4, ...cursor]);
      expect(sent[0]!.options.length).toBe(0);
    }
  });

  it("kv.scan asks for 1024 names (the server's default) from the first page by default", async () => {
    const { sender, sent } = capture();
    await expect(new KVOperations(sender).scan("p")).rejects.toThrow("captured");
    expect(Array.from(sent[0]!.value)).toEqual([1, 0, 4, 0, 0, 0]);
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

describe("worker.list", () => {
  it("is a table op: its body is limit (present) then a u16-prefixed cursor", async () => {
    expect(isTableOp(OpCode.WorkerList)).toBe(true);
    const { sender, sent } = answering(page([]));
    await new WorkerOperations(sender).list({ limit: 5, cursor });
    expect(sent[0]!.op).toBe(OpCode.WorkerList);
    expect(Array.from(sent[0]!.value)).toEqual([1, 5, 0, 0, 0, 4, 0, ...cursor]);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("asks for 1024 workers (the server's default) from the first page by default", async () => {
    const { sender, sent } = answering(page([]));
    await new WorkerOperations(sender).list();
    expect(Array.from(sent[0]!.value)).toEqual([1, 0, 4, 0, 0, 0, 0]);
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

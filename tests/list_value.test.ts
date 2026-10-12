/**
 * Every list op carries its page size and cursor in the request value as
 * [limit:u32 LE][cursor bytes], not as options. kv.scan is a table op and
 * codes them as its schema's fields.
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
  WorkflowOperations,
  serializeListValue,
} from "@floruntime/core";

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

  it("action.list and worker.list send [limit] in the value", async () => {
    for (const [op, call] of [
      [OpCode.ActionList, (s: never) => new ActionOperations(s).list({ limit: 5 })],
      [OpCode.WorkerList, (s: never) => new WorkerOperations(s).list({ limit: 5 })],
    ] as const) {
      const { sender, sent } = capture();
      await expect(call(sender)).rejects.toThrow("captured");
      expect(sent[0]!.op).toBe(op);
      expect(Array.from(sent[0]!.value)).toEqual([5, 0, 0, 0]);
      expect(sent[0]!.options.length).toBe(0);
    }
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

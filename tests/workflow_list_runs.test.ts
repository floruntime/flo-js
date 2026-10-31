/**
 * workflow.listRuns: the value it sends, and how it reads the answer.
 */

import { describe, expect, it } from "vitest";
import {
  FloError,
  IncompleteResponseError,
  OpCode,
  StatusCode,
  WorkflowOperations,
  decodeListRunsAnswer,
  type RawResponse,
} from "@floruntime/core";

const enc = new TextEncoder();

interface Sent {
  opCode: number;
  namespace: string;
  key: Uint8Array;
  value: Uint8Array;
}

function fakeWorkflow(answer: Uint8Array = page([], false, new Uint8Array(0))) {
  const sent: Sent[] = [];
  const workflow = new WorkflowOperations({
    getNamespace: (override?: string) => override ?? "default",
    sendRequest: async (opCode, namespace, key, value): Promise<RawResponse> => {
      sent.push({ opCode, namespace, key, value });
      return { status: StatusCode.OK, data: answer, requestId: 1n };
    },
  });
  return { workflow, sent };
}

function u16(n: number): number[] {
  return [n & 0xff, n >> 8];
}
function str16(s: string): number[] {
  const b = enc.encode(s);
  return [...u16(b.length), ...b];
}
function i64(n: bigint): number[] {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigInt64(0, n, true);
  return [...b];
}

/** An answer: [count:u32](entry)*[has_more:u8][cursor_len:u16][cursor]. */
function page(runs: [string, string, string, bigint][], hasMore: boolean, cursor: Uint8Array): Uint8Array {
  const out = [runs.length & 0xff, (runs.length >> 8) & 0xff, 0, 0];
  for (const [id, wf, status, at] of runs) out.push(...str16(id), ...str16(wf), ...str16(status), ...i64(at));
  out.push(hasMore ? 1 : 0, ...u16(cursor.length), ...cursor);
  return new Uint8Array(out);
}

describe("workflow.listRuns", () => {
  it("sends every field, empty ones as a zero length, and no key for every workflow", async () => {
    const { workflow, sent } = fakeWorkflow();
    await workflow.listRuns();
    expect(sent[0]!.opCode).toBe(0x346);
    expect(sent[0]!.opCode).toBe(OpCode.WorkflowListRuns);
    expect(sent[0]!.key).toHaveLength(0);
    // limit 0 (the server's 100), then status, cursor and search, each empty.
    expect(Array.from(sent[0]!.value)).toEqual([0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  });

  it("sends limit, status, cursor and search in that order, the workflow as the key", async () => {
    const { workflow, sent } = fakeWorkflow();
    const cursor = new Uint8Array([9, 8, 7]);
    await workflow.listRuns({ workflowName: "orders", namespace: "ns", limit: 5, status: "running", cursor, search: "Ab" });
    expect(sent[0]!.namespace).toBe("ns");
    expect(sent[0]!.key).toEqual(enc.encode("orders"));
    expect(Array.from(sent[0]!.value)).toEqual([5, 0, 0, 0, ...str16("running"), ...u16(3), 9, 8, 7, ...str16("Ab")]);
  });

  it("refuses a limit that isn't a u32 before sending", async () => {
    const { workflow, sent } = fakeWorkflow();
    for (const limit of [-1, 1.5, 2 ** 32]) await expect(workflow.listRuns({ limit })).rejects.toBeInstanceOf(FloError);
    expect(sent).toHaveLength(0);
  });

  it("returns the page's runs, in the server's order, and its cursor", async () => {
    const cursor = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 0x72]);
    const { workflow } = fakeWorkflow(page([["r-2", "b", "running", 20n], ["r-1", "a", "completed", 10n]], true, cursor));
    expect(await workflow.listRuns({ limit: 2 })).toEqual({
      runs: [
        { run_id: "r-2", workflow: "b", status: "running", created_at: 20 },
        { run_id: "r-1", workflow: "a", status: "completed", created_at: 10 },
      ],
      hasMore: true,
      cursor,
    });
  });

  it("returns a null cursor on the last page", () => {
    expect(decodeListRunsAnswer(page([], false, new Uint8Array(0)))).toEqual({ runs: [], hasMore: false, cursor: null });
  });

  it("refuses has_more and the cursor disagreeing, trailing bytes and a short answer", () => {
    const bad = [
      page([], true, new Uint8Array(0)),
      page([], false, new Uint8Array([1])),
      new Uint8Array([...page([], false, new Uint8Array(0)), 0]),
      page([["r", "w", "running", 1n]], false, new Uint8Array(0)).subarray(0, 10),
      new Uint8Array([...page([], false, new Uint8Array(0)).subarray(0, 4), 2, 0, 0]),
    ];
    for (const data of bad) expect(() => decodeListRunsAnswer(data)).toThrow(IncompleteResponseError);
  });
});

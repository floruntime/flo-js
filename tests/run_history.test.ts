/**
 * workflow.history and action.history: one request and one page shape for
 * both, sent as table ops; and workflow.status's output and its preview flag.
 */

import { describe, expect, it } from "vitest";
import {
  ActionOperations,
  FloError,
  IncompleteResponseError,
  MAX_RUN_HISTORY_LIMIT,
  OpCode,
  StatusCode,
  WorkflowOperations,
  decodeRunHistoryAnswer,
  decodeWorkflowStatusAnswer,
  encodeRunHistoryBody,
  type RawResponse,
  type RunHistoryEvent,
} from "@floruntime/core";
import { isTableOp } from "../packages/core/src/kv_body.js";

const enc = new TextEncoder();

function le(n: bigint, bytes: number): number[] {
  const out: number[] = [];
  for (let i = 0; i < bytes; i++) out.push(Number((BigInt.asUintN(bytes * 8, n) >> BigInt(8 * i)) & 0xffn));
  return out;
}
const u16 = (n: number) => le(BigInt(n), 2);
const u32 = (n: number) => le(BigInt(n), 4);

/** A page as the server encodes it. */
function page(events: RunHistoryEvent[], cursor: bigint | null): Uint8Array {
  const out = [...u16(events.length)];
  for (const e of events) {
    const t = enc.encode(e.type);
    const x = enc.encode(e.extra);
    out.push(...u32(t.length), ...t, ...u32(e.detail.length), ...e.detail, ...u16(x.length), ...x, ...le(e.timestampMs, 8));
  }
  out.push(cursor === null ? 0 : 1);
  if (cursor === null) out.push(0);
  else out.push(1, ...le(cursor, 8));
  return new Uint8Array(out);
}

const ev = (type: string, detail: string, extra = "", timestampMs = 1700000000000n): RunHistoryEvent => ({ type, detail: enc.encode(detail), extra, timestampMs });

describe("a run history page", () => {
  it("decodes an empty last page", () => {
    expect(decodeRunHistoryAnswer(page([], null))).toEqual({ events: [], hasMore: false, cursor: null });
  });

  it("decodes events with their extra, and the cursor when more follow", () => {
    const events = [ev("action_invoked", "in"), ev("action_delivered", "w1", "truncated"), ev("action_settled", "out", "approved", -1n)];
    expect(decodeRunHistoryAnswer(page(events, 7n))).toEqual({ events, hasMore: true, cursor: 7n });
    expect(decodeRunHistoryAnswer(page(events, (1n << 64n) - 1n)).cursor).toBe((1n << 64n) - 1n);
  });

  it("reads a detail past 64 KiB, its length a u32", () => {
    const big: RunHistoryEvent = { type: "workflow_output", detail: new Uint8Array(65536).fill(7), extra: "", timestampMs: 5n };
    const p = decodeRunHistoryAnswer(page([big], null));
    expect(p.events[0]!.detail).toEqual(big.detail);
  });

  it("refuses a cut answer, bytes left over, and has_more without a cursor or a cursor without it", () => {
    const whole = page([ev("a", "b", "c")], 3n);
    for (let n = 0; n < whole.length; n++) expect(() => decodeRunHistoryAnswer(whole.subarray(0, n))).toThrow(IncompleteResponseError);
    expect(() => decodeRunHistoryAnswer(new Uint8Array([...whole, 0]))).toThrow(IncompleteResponseError);
    expect(() => decodeRunHistoryAnswer(new Uint8Array([0, 0, 1, 0]))).toThrow(IncompleteResponseError);
    expect(() => decodeRunHistoryAnswer(new Uint8Array([0, 0, 0, 1, ...le(3n, 8)]))).toThrow(IncompleteResponseError);
    expect(() => decodeRunHistoryAnswer(new Uint8Array([...u16(MAX_RUN_HISTORY_LIMIT + 1), 0, 0]))).toThrow(IncompleteResponseError);
  });
});

describe("a run history request", () => {
  it("sends each field absent when not given, so the server's defaults hold", () => {
    expect(encodeRunHistoryBody()).toEqual(new Uint8Array([0, 0, 0]));
    expect(encodeRunHistoryBody({})).toEqual(new Uint8Array([0, 0, 0]));
    expect(encodeRunHistoryBody({ cursor: null })).toEqual(new Uint8Array([0, 0, 0]));
  });

  it("sends each field given present", () => {
    expect(encodeRunHistoryBody({ limit: 1000, cursor: 9n, newestFirst: true })).toEqual(new Uint8Array([1, ...u32(1000), 1, ...le(9n, 8), 1, 1]));
    expect(encodeRunHistoryBody({ newestFirst: false })).toEqual(new Uint8Array([0, 0, 1, 0]));
  });

  it("refuses a limit outside 1 to 1000 and a cursor outside a u64 before sending", () => {
    for (const limit of [0, -1, 1001, 1.5]) expect(() => encodeRunHistoryBody({ limit })).toThrow(FloError);
    expect(() => encodeRunHistoryBody({ cursor: -1n })).toThrow(FloError);
    expect(() => encodeRunHistoryBody({ cursor: 1n << 64n })).toThrow(FloError);
  });

  it("is sent framed as a table op, for both families, keyed by the run id", async () => {
    expect([OpCode.ActionHistory, OpCode.WorkflowHistory]).toEqual([0x310, 0x345]);
    expect(isTableOp(OpCode.ActionHistory)).toBe(true);
    expect(isTableOp(OpCode.WorkflowHistory)).toBe(true);
    expect(isTableOp(OpCode.WorkflowStatus)).toBe(false);

    const sent: { opCode: number; key: string; value: Uint8Array }[] = [];
    const answer = page([ev("workflow_started", "{}")], null);
    const sender = {
      getNamespace: (o?: string) => o ?? "default",
      sendRequest: async (opCode: OpCode, _ns: string, key: Uint8Array, value: Uint8Array): Promise<RawResponse> => {
        sent.push({ opCode, key: new TextDecoder().decode(key), value });
        return { status: StatusCode.OK, data: answer, requestId: 1n };
      },
    };
    const want = { events: [ev("workflow_started", "{}")], hasMore: false, cursor: null };
    expect(await new WorkflowOperations(sender).history("wf-1", { limit: 5 })).toEqual(want);
    expect(await new ActionOperations(sender).history("act-1", { cursor: 4n })).toEqual(want);
    expect(sent).toEqual([
      { opCode: OpCode.WorkflowHistory, key: "wf-1", value: new Uint8Array([1, ...u32(5), 0, 0]) },
      { opCode: OpCode.ActionHistory, key: "act-1", value: new Uint8Array([0, 1, ...le(4n, 8), 0]) },
    ]);
  });
});

/** workflow.status's answer as the server encodes it. */
function statusAnswer(opts: { status?: number; input?: string; started?: bigint; completed?: bigint; wait?: string; output?: Uint8Array; truncated?: boolean } = {}): Uint8Array {
  const str16 = (s: string) => [...u16(enc.encode(s).length), ...enc.encode(s)];
  const input = enc.encode(opts.input ?? "");
  const out = [...str16("wf-1"), ...str16("orders"), ...str16("1.0.0"), opts.status ?? 3, ...str16("start"), ...u32(input.length), ...input, ...le(1000n, 8)];
  for (const t of [opts.started, opts.completed]) {
    if (t === undefined) out.push(0);
    else out.push(1, ...le(t, 8));
  }
  if (opts.wait === undefined) out.push(0);
  else out.push(1, ...str16(opts.wait));
  if (opts.output === undefined) out.push(0);
  else out.push(1, ...u32(opts.output.length), ...opts.output, opts.truncated ? 1 : 0);
  return new Uint8Array(out);
}

describe("workflow.status", () => {
  it("decodes a running run with its input and no output", () => {
    const s = decodeWorkflowStatusAnswer(statusAnswer({ status: 2, input: '{"a":1}', started: 1000n, wait: "go" }));
    expect(s).toEqual({
      run_id: "wf-1",
      workflow: "orders",
      version: "1.0.0",
      status: "waiting",
      current_step: "start",
      input: enc.encode('{"a":1}'),
      created_at: 1000n,
      started_at: 1000n,
      wait_signal: "go",
      output_truncated: false,
    });
  });

  it("decodes an ended run's output preview and whether it was cut", () => {
    const preview = new Uint8Array(256).fill(0x61);
    const s = decodeWorkflowStatusAnswer(statusAnswer({ started: 1000n, completed: 2000n, output: preview, truncated: true }));
    expect([s.status, s.input.length, s.completed_at, s.output, s.output_truncated]).toEqual(["completed", 0, 2000n, preview, true]);
    const short = decodeWorkflowStatusAnswer(statusAnswer({ completed: 2000n, output: enc.encode("ok") }));
    expect([short.output, short.output_truncated]).toEqual([enc.encode("ok"), false]);
  });

  it("refuses a cut answer and bytes left over", () => {
    const whole = statusAnswer({ started: 1000n, completed: 2000n, wait: "w", output: enc.encode("ok"), truncated: true });
    for (let n = 0; n < whole.length; n++) expect(() => decodeWorkflowStatusAnswer(whole.subarray(0, n))).toThrow(IncompleteResponseError);
    expect(() => decodeWorkflowStatusAnswer(new Uint8Array([...whole, 0]))).toThrow(IncompleteResponseError);
  });
});

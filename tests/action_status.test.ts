import { describe, expect, it } from "vitest";
import { IncompleteResponseError, decodeActionStatusAnswer } from "@floruntime/core";

/** action.status's answer as the server encodes it. */
function answer(status: number, opts: { started?: bigint; completed?: bigint; attempt?: number; output?: string; error?: string; worker?: string } = {}): Uint8Array {
  const parts: number[] = [status];
  const i64 = (v: bigint) => {
    const b = new DataView(new ArrayBuffer(8));
    b.setBigInt64(0, v, true);
    parts.push(...new Uint8Array(b.buffer));
  };
  const u16 = (v: number) => parts.push(v & 0xff, v >> 8);
  i64(1000n);
  for (const t of [opts.started, opts.completed]) {
    if (t === undefined) parts.push(0);
    else {
      parts.push(1);
      i64(t);
    }
  }
  const a = opts.attempt ?? 0;
  parts.push(a & 0xff, (a >> 8) & 0xff, (a >> 16) & 0xff, a >>> 24);
  for (const s of [opts.output, opts.error, opts.worker]) {
    if (s === undefined) parts.push(0);
    else {
      const b = new TextEncoder().encode(s);
      parts.push(1);
      u16(b.length);
      parts.push(...b);
    }
  }
  return new Uint8Array(parts);
}

describe("action.status", () => {
  it("decodes a completed run with its output", () => {
    const s = decodeActionStatusAnswer("r", answer(2, { started: 1100n, completed: 1200n, attempt: 1, output: '{"ok":true}' }));
    expect(s).toEqual({
      runId: "r",
      status: "completed",
      createdAtMs: 1000n,
      startedAtMs: 1100n,
      completedAtMs: 1200n,
      attempt: 1,
      output: new TextEncoder().encode('{"ok":true}'),
      errorMessage: undefined,
      workerId: undefined,
    });
  });

  it("decodes the worker that holds the run, last", () => {
    const s = decodeActionStatusAnswer("r", answer(1, { started: 1100n, attempt: 1, worker: "host-1a2b" }));
    expect([s.status, s.workerId, s.output, s.errorMessage]).toEqual(["running", "host-1a2b", undefined, undefined]);
  });

  it("decodes a pending run and a timed-out one with its error", () => {
    expect(decodeActionStatusAnswer("r", answer(0)).status).toBe("pending");
    const t = decodeActionStatusAnswer("r", answer(5, { attempt: 3, error: "boom" }));
    expect([t.status, t.attempt, t.errorMessage, t.output]).toEqual(["timed_out", 3, "boom", undefined]);
  });

  it("refuses a cut answer and a status this client doesn't know", () => {
    const whole = answer(2, { attempt: 1, output: "x", worker: "w" });
    for (let n = 0; n < whole.length; n++) {
      expect(() => decodeActionStatusAnswer("r", whole.subarray(0, n))).toThrow(IncompleteResponseError);
    }
    expect(() => decodeActionStatusAnswer("r", answer(9))).toThrow(IncompleteResponseError);
  });
});

describe("action.invoke's answer", () => {
  it("is the run id alone, not the length prefix and has_output byte around it", async () => {
    const { parseActionInvokeAnswer } = await import("@floruntime/core");
    const id = new TextEncoder().encode("act-1a2b-3");
    expect(parseActionInvokeAnswer(new Uint8Array([id.length, 0, ...id, 0]))).toBe("act-1a2b-3");
    expect(() => parseActionInvokeAnswer(new Uint8Array([0, 0, 0]))).toThrow(IncompleteResponseError);
    expect(() => parseActionInvokeAnswer(new Uint8Array([9, 0, 0x61]))).toThrow(IncompleteResponseError);
    // The has_output byte is there and 0, and nothing follows it.
    expect(() => parseActionInvokeAnswer(new Uint8Array([id.length, 0, ...id]))).toThrow(IncompleteResponseError);
    expect(() => parseActionInvokeAnswer(new Uint8Array([id.length, 0, ...id, 1]))).toThrow(IncompleteResponseError);
    expect(() => parseActionInvokeAnswer(new Uint8Array([id.length, 0, ...id, 0, 0]))).toThrow(IncompleteResponseError);
  });
});

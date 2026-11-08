/** Processing status and list answers decode per the server's layouts. */

import { describe, it, expect } from "vitest";
import { IncompleteResponseError, OpCode, ProcessingOperations, StatusCode } from "@floruntime/core";

const enc = new TextEncoder();

function bytes(...parts: (Uint8Array | number[])[]): Uint8Array {
  const out: number[] = [];
  for (const p of parts) out.push(...p);
  return new Uint8Array(out);
}
const str = (s: string) => bytes([s.length & 0xff, s.length >> 8], enc.encode(s));
function le(n: number, size: 2 | 4 | 8): Uint8Array {
  const b = new Uint8Array(size);
  const v = new DataView(b.buffer);
  if (size === 2) v.setUint16(0, n, true);
  else if (size === 4) v.setUint32(0, n, true);
  else v.setBigUint64(0, BigInt(n), true);
  return b;
}

function replying(data: Uint8Array): never {
  return {
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async () => ({ status: StatusCode.OK, data }),
  } as never;
}

describe("processing answers", () => {
  const head = (status: number) =>
    bytes(str("job-1"), str("pipe"), [status], le(50, 4), le(1234, 8), le(99, 8));
  const status = (data: Uint8Array) => new ProcessingOperations(replying(data)).status("job-1");
  // [running:u8][records_in:u64][records_out:u64][watermark_ms:i64][tick_avg_us:u64]
  // [tick_max_us:u64][uptime_ms:u64][yaml_len:u32][yaml]
  const tail = (yaml = "kind: Processing") =>
    bytes([1], le(10, 8), le(9, 8), le(1700, 8), le(12, 8), le(40, 8), le(5000, 8), le(yaml.length, 4), enc.encode(yaml));

  it("decodes a running job's status with no failure reason", async () => {
    expect(await status(bytes(head(0), le(0, 2), tail()))).toEqual({
      job_id: "job-1",
      name: "pipe",
      status: "running",
      batch_size: 50,
      records_processed: 1234,
      created_at: 99,
      failure_reason: null,
      running: true,
      records_in: 10,
      records_out: 9,
      watermark_ms: 1700,
      tick_avg_us: 12,
      tick_max_us: 40,
      uptime_ms: 5000,
      yaml: "kind: Processing",
    });
  });

  it("decodes a failed job's failure reason", async () => {
    const got = await status(bytes(head(3), str("sink refused"), tail()));
    expect(got.status).toBe("failed");
    expect(got.failure_reason).toBe("sink refused");
  });

  it("decodes completed", async () => {
    expect((await status(bytes(head(4), le(0, 2), tail()))).status).toBe("completed");
  });

  it("refuses a status answer whose reason runs past the end", async () => {
    await expect(status(bytes(head(3), le(20, 2), enc.encode("short")))).rejects.toThrow(IncompleteResponseError);
  });

  it("refuses a status answer cut inside its runtime, or with bytes after the definition", async () => {
    const whole = bytes(head(3), str("x"), tail());
    await expect(status(whole.subarray(0, whole.length - 1))).rejects.toThrow(IncompleteResponseError);
    await expect(status(bytes(whole, [0]))).rejects.toThrow(IncompleteResponseError);
  });

  it("refuses an unknown job status", async () => {
    await expect(status(bytes(head(5), le(0, 2), tail()))).rejects.toThrow(IncompleteResponseError);
  });

  it("decodes list", async () => {
    const data = bytes(
      le(2, 4),
      str("a"), str("j1"), str("RUNNING"), le(7, 8), le(50, 4), le(1234, 8),
      str("b"), str("j2"), str("FAILED: sink refused"), le(8, 8), le(1, 4), le(0, 8),
      [0], le(0, 2),
    );
    expect(await new ProcessingOperations(replying(data)).list()).toEqual({
      jobs: [
        { name: "a", job_id: "j1", status: "RUNNING", created_at: 7, batch_size: 50, records_processed: 1234 },
        { name: "b", job_id: "j2", status: "FAILED: sink refused", created_at: 8, batch_size: 1, records_processed: 0 },
      ],
      hasMore: false,
      cursor: null,
    });
  });

  it("removes a job: the job id is the key, the request has no body, and the answer is empty", async () => {
    const seen: { op: number; key: string; value: Uint8Array }[] = [];
    const sender = {
      getNamespace: (ns?: string) => ns ?? "default",
      sendRequest: async (op: number, _ns: string, key: Uint8Array, value: Uint8Array) => {
        seen.push({ op, key: new TextDecoder().decode(key), value });
        return { status: StatusCode.OK, data: new Uint8Array(0) };
      },
    } as never;
    await new ProcessingOperations(sender).remove("job-1");
    expect(seen).toEqual([{ op: OpCode.ProcessingRemove, key: "job-1", value: new Uint8Array(0) }]);
  });
});

/** Processing status and list answers decode per the server's layouts. */

import { describe, it, expect } from "vitest";
import { ProcessingOperations, StatusCode } from "@floruntime/core";

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
  it("decodes status", async () => {
    const data = bytes(str("job-1"), str("pipe"), [1], le(50, 4), le(1234, 8), le(99, 8));
    expect(await new ProcessingOperations(replying(data)).status("job-1")).toEqual({
      job_id: "job-1",
      name: "pipe",
      status: "stopped",
      batch_size: 50,
      records_processed: 1234,
      created_at: 99,
    });
  });

  it("decodes list", async () => {
    const data = bytes(
      le(2, 4),
      str("a"), str("j1"), str("running"), le(7, 8),
      str("b"), str("j2"), str("failed"), le(8, 8),
      [0], le(0, 2),
    );
    expect(await new ProcessingOperations(replying(data)).list()).toEqual([
      { name: "a", job_id: "j1", status: "running", created_at: 7 },
      { name: "b", job_id: "j2", status: "failed", created_at: 8 },
    ]);
  });
});

import { describe, it, expect } from "vitest";
import { parseStreamReadResponse, StorageTier } from "@floruntime/core";

// [count:u32]([seq:u64][ts:i64][tier:u8][partition:u32][key_present:u8][payload_len:u32][payload][header_count:u32])*
function oneRecord(tier: number): Uint8Array {
  const buf = new Uint8Array(4 + 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1, true);
  view.setBigUint64(4, 1n, true);
  view.setBigInt64(12, 1000n, true);
  buf[20] = tier;
  buf[30] = 0x78; // payload "x"
  view.setUint32(26, 1, true);
  return buf;
}

describe("stream record tier", () => {
  it("reads the server's tier bytes: 0 is hot, 1 is warm", () => {
    expect(parseStreamReadResponse(oneRecord(0)).records[0].tier).toBe(StorageTier.Hot);
    expect(parseStreamReadResponse(oneRecord(1)).records[0].tier).toBe(StorageTier.Warm);
  });
});

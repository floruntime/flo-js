/**
 * blockMs validation: 0 means don't wait, 300000 ms is the ceiling.
 */

import { describe, it, expect } from "vitest";
import {
  BlockTooLongError,
  MAX_BLOCK_MS,
  OptionTag,
  OptionsBuilder,
} from "@floruntime/core";
import { Worker } from "@floruntime/node";

describe("blockMs ceiling", () => {
  it("accepts 0 and the ceiling", () => {
    expect(() => new OptionsBuilder().addU32(OptionTag.BlockMS, 0)).not.toThrow();
    expect(() => new OptionsBuilder().addU32(OptionTag.BlockMS, MAX_BLOCK_MS)).not.toThrow();
  });

  it("refuses more than 300000 ms before the round trip", () => {
    expect(() => new OptionsBuilder().addU32(OptionTag.BlockMS, MAX_BLOCK_MS + 1)).toThrow(
      BlockTooLongError
    );
    expect(() => new OptionsBuilder().addU32(OptionTag.BlockMS, MAX_BLOCK_MS + 1)).toThrow(
      /at most 300000 ms/
    );
  });

  it("leaves other u32 options alone", () => {
    expect(() => new OptionsBuilder().addU32(OptionTag.Count, MAX_BLOCK_MS + 1)).not.toThrow();
  });
});

describe("Worker blockMs", () => {
  const blockMsOf = (w: Worker): number =>
    (w as unknown as { config: { blockMs: number } }).config.blockMs;

  it("treats 0 as the 30000 default", () => {
    expect(blockMsOf(new Worker({ endpoint: "localhost:1", blockMs: 0 }))).toBe(30000);
    expect(blockMsOf(new Worker({ endpoint: "localhost:1" }))).toBe(30000);
  });

  it("keeps an explicit value", () => {
    expect(blockMsOf(new Worker({ endpoint: "localhost:1", blockMs: 1000 }))).toBe(1000);
  });

  it("refuses more than 300000 ms at config time", () => {
    expect(() => new Worker({ endpoint: "localhost:1", blockMs: MAX_BLOCK_MS + 1 })).toThrow(
      BlockTooLongError
    );
  });
});

/**
 * blockMs ceiling, and the workers' reading of 0 as their default.
 */

import { describe, it, expect } from "vitest";
import {
  BlockTooLongError,
  FloError,
  MAX_BLOCK_MS,
  OptionTag,
  OptionsBuilder,
  QueueOperations,
} from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";

const addBlockMs = (v: number) => () => new OptionsBuilder().addU32(OptionTag.BlockMS, v);

describe("OptionsBuilder blockMs", () => {
  it("accepts 0 and the ceiling", () => {
    expect(addBlockMs(0)).not.toThrow();
    expect(addBlockMs(MAX_BLOCK_MS)).not.toThrow();
  });

  it("refuses more than 300000 ms", () => {
    expect(addBlockMs(MAX_BLOCK_MS + 1)).toThrow(BlockTooLongError);
    expect(addBlockMs(MAX_BLOCK_MS + 1)).toThrow(/at most 300000 ms/);
  });

  it("refuses more than 300000 ms of wait_ms too", () => {
    expect(() => new OptionsBuilder().addU32(OptionTag.WaitMS, MAX_BLOCK_MS + 1)).toThrow(BlockTooLongError);
  });

  it("refuses values the u32 encoding would mangle", () => {
    for (const v of [-1, 0.5, NaN]) {
      expect(addBlockMs(v)).toThrow(FloError);
    }
  });

  it("leaves other u32 options alone", () => {
    expect(() => new OptionsBuilder().addU32(OptionTag.Count, MAX_BLOCK_MS + 1)).not.toThrow();
  });

  it("refuses before the request is sent", async () => {
    let sent = false;
    const queue = new QueueOperations({
      getNamespace: () => "default",
      sendRequest: async () => {
        sent = true;
        throw new Error("unreachable");
      },
    } as never);
    await expect(
      queue.dequeue("q", 1, { blockMs: MAX_BLOCK_MS + 1 })
    ).rejects.toThrow(BlockTooLongError);
    expect(sent).toBe(false);
  });
});

const blockMsOf = (w: unknown): number =>
  (w as { config: { blockMs: number } }).config.blockMs;
const workers: [string, (blockMs?: number) => unknown][] = [
  ["ActionWorker", (blockMs) => new ActionWorker({ endpoint: "localhost:1", blockMs })],
  ["StreamWorker", (blockMs) => new StreamWorker("localhost:1", { stream: "s", blockMs }, async () => {})],
];

describe.each(workers)("%s blockMs", (_name, worker) => {
  it("treats unset or 0 as 30000, since 0 would spin the poll loop", () => {
    expect(blockMsOf(worker(0))).toBe(30000);
    expect(blockMsOf(worker())).toBe(30000);
  });

  it("keeps an explicit value", () => {
    expect(blockMsOf(worker(1000))).toBe(1000);
  });

  it("refuses invalid values at config time", () => {
    expect(() => worker(MAX_BLOCK_MS + 1)).toThrow(BlockTooLongError);
    for (const v of [-1, 0.5, NaN]) {
      expect(() => worker(v)).toThrow(FloError);
    }
  });
});

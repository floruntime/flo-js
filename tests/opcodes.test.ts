/**
 * The op codes this SDK knows, against the server's table: retired codes
 * are gone, and codes only a node or its admin socket serves are known but
 * never sent.
 */

import { describe, expect, it } from "vitest";
import { OpCode } from "@floruntime/core";

const codes = Object.values(OpCode) as number[];

describe("OpCode", () => {
  it("drops the retired namespace settings ops, 0x014 and 0x015", () => {
    expect(codes).not.toContain(0x014);
    expect(codes).not.toContain(0x015);
    expect(Object.keys(OpCode).some((n) => n.startsWith("NamespaceConfig"))).toBe(false);
  });

  it("knows namespace.ensure and the cluster ops at the server's codes", () => {
    expect(OpCode.NamespaceEnsure).toBe(0x016);
    expect(OpCode.ClusterPromote).toBe(0x032);
    expect(OpCode.ClusterRemove).toBe(0x033);
    expect(OpCode.ClusterRepairSkip).toBe(0x034);
  });

  it("drops the retired action report codes 0x306-0x309 and the old queue peek 0x17C", () => {
    for (const code of [0x306, 0x307, 0x308, 0x309, 0x17c]) expect(codes).not.toContain(code);
  });

  it("knows the action ops on the log, the leader reads and processing.remove at the server's codes", () => {
    expect([OpCode.ActionAwait, OpCode.ActionComplete, OpCode.ActionFail, OpCode.ActionTouch, OpCode.ActionDeliver, OpCode.ActionInfo]).toEqual([0x30a, 0x30b, 0x30c, 0x30d, 0x30e, 0x30f]);
    expect([OpCode.StreamGroupList, OpCode.StreamGroupMembers, OpCode.QueuePeek, OpCode.ProcessingRemove]).toEqual([0x15d, 0x15e, 0x180, 0x365]);
  });

  it("gives every op its own code", () => {
    expect(new Set(codes).size).toBe(codes.length);
  });
});

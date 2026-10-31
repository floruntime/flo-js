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

  it("gives every op its own code", () => {
    expect(new Set(codes).size).toBe(codes.length);
  });
});

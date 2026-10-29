/**
 * The status codes and refusal reasons are written out by hand; this checks
 * them against the server's own list, the ops.json `zig build gen` writes,
 * named by FLO_OPS_JSON. Without it the test skips, except under CI=1.
 */

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { Reason, reasonName, StatusCode } from "@floruntime/core";

const path = process.env.FLO_OPS_JSON ?? "";
if (process.env.CI === "1" && !path) throw new Error("CI=1 but FLO_OPS_JSON is not set");

interface Named {
  code: number;
  name: string;
}

/** The SDK's name for a server name: snake_case to PascalCase, ok as OK. */
const sdkName = (name: string) => (name === "ok" ? "OK" : name.replace(/(^|_)([a-z])/g, (_, __, c: string) => c.toUpperCase()));

const sorted = (xs: Named[]) => [...xs].sort((a, b) => a.code - b.code);

describe.skipIf(!path)("ops.json", () => {
  const ops = path ? (JSON.parse(readFileSync(path, "utf8")) as { statuses: Named[]; reasons: Named[] }) : { statuses: [], reasons: [] };

  it("lists this SDK's status codes", () => {
    const sdk = Object.entries(StatusCode).map(([name, code]) => ({ code, name }));
    expect(sorted(sdk)).toEqual(sorted(ops.statuses.map((s) => ({ code: s.code, name: sdkName(s.name) }))));
  });

  it("lists this SDK's reasons", () => {
    const sdk = Object.entries(Reason)
      .filter(([, code]) => typeof code === "number")
      .map(([name, code]) => ({ code: code as number, name }));
    expect(sorted(sdk)).toEqual(sorted(ops.reasons.map((r) => ({ code: r.code, name: sdkName(r.name) }))));
    for (const r of ops.reasons) expect(reasonName(r.code as Reason)).toBe(r.name);
  });
});

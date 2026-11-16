/** A stream or series read answered not found throws; an ok empty answer stays empty. */

import { describe, expect, it } from "vitest";
import { NotFoundError, Ran, Reason, StatusCode, StreamOperations, TSOperations, type RawResponse } from "@floruntime/core";

const refusal = (message: string): RawResponse =>
  ({ status: StatusCode.NotFound, data: new Uint8Array([Reason.NotFound, 0, Ran.No, ...new TextEncoder().encode(message)]) }) as RawResponse;

function sender(answer: RawResponse): never {
  return { getNamespace: (ns?: string) => ns ?? "default", sendRequest: async () => answer } as never;
}

describe("a read of something not there", () => {
  it("stream.read answered not found throws NotFoundError, not an empty page", async () => {
    const err = await new StreamOperations(sender(refusal("stream not found"))).read("s").catch((e) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err.serverMessage).toBe("stream not found");
  });

  it("stream.read answered ok with no records stays empty", async () => {
    const ok = { status: StatusCode.OK, data: new Uint8Array(4) } as RawResponse;
    expect(await new StreamOperations(sender(ok)).read("s")).toEqual({ records: [] });
  });

  it("ts.read answered not found throws NotFoundError; ok with no points is an empty last page", async () => {
    const err = await new TSOperations(sender(refusal("not found: 'cpu' has no series of field 'value'"))).read("cpu").catch((e) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err.serverMessage).toBe("not found: 'cpu' has no series of field 'value'");
    const empty = { status: StatusCode.OK, data: new Uint8Array([0, 0, 0]) } as RawResponse;
    expect(await new TSOperations(sender(empty)).read("cpu")).toEqual({ points: [], cursor: null });
  });
});

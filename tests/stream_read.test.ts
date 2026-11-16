/** A stream or series read answered not found throws; an ok empty answer stays empty. */

import { describe, expect, it } from "vitest";
import { BadRequestError, NotFoundError, Ran, Reason, StaleNotFoundError, StatusCode, StreamOperations, TSOperations, type RawResponse } from "@floruntime/core";

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
    expect(await new TSOperations(sender(empty)).read("cpu")).toEqual({ points: [], cursor: null, stale: false });
  });
});

describe("a stale answer", () => {
  const stale = (r: RawResponse): RawResponse => ({ ...r, stale: true }) as RawResponse;

  it("stream.read: a stale not-found is StaleNotFoundError, still a NotFoundError; an unstale one is plain", async () => {
    const err = await new StreamOperations(sender(stale(refusal("stream not found")))).read("s").catch((e) => e);
    expect(err).toBeInstanceOf(StaleNotFoundError);
    expect(err).toBeInstanceOf(NotFoundError);
    expect([err.stale, err.serverMessage, err.reason]).toEqual([true, "stream not found", Reason.NotFound]);
    const plain = await new StreamOperations(sender(refusal("stream not found"))).read("s").catch((e) => e);
    expect(plain).toBeInstanceOf(NotFoundError);
    expect(plain).not.toBeInstanceOf(StaleNotFoundError);
  });

  it("ts.read: a stale not-found is StaleNotFoundError, still a NotFoundError; an unstale one is plain", async () => {
    const msg = "not found: 'cpu' has no series of field 'value'";
    const err = await new TSOperations(sender(stale(refusal(msg)))).read("cpu").catch((e) => e);
    expect(err).toBeInstanceOf(StaleNotFoundError);
    expect(err).toBeInstanceOf(NotFoundError);
    const plain = await new TSOperations(sender(refusal(msg))).read("cpu").catch((e) => e);
    expect(plain).toBeInstanceOf(NotFoundError);
    expect(plain).not.toBeInstanceOf(StaleNotFoundError);
  });

  it("ts.readAll throws a stale not-found as StaleNotFoundError", async () => {
    const run = async () => {
      for await (const _ of new TSOperations(sender(stale(refusal("not found")))).readAll("cpu")) void _;
    };
    await expect(run()).rejects.toBeInstanceOf(StaleNotFoundError);
  });

  it("ts.read: a page carries the answer's stale flag", async () => {
    const empty = { status: StatusCode.OK, data: new Uint8Array([0, 0, 0]) } as RawResponse;
    expect((await new TSOperations(sender(stale(empty))).read("cpu")).stale).toBe(true);
    expect((await new TSOperations(sender(empty)).read("cpu")).stale).toBe(false);
  });

  it("a stale refusal other than not found stays its own error", async () => {
    const bad = { status: StatusCode.BadRequest, data: new Uint8Array([Reason.Malformed, 0, Ran.No]), stale: true } as RawResponse;
    const err = await new TSOperations(sender(bad)).read("cpu").catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err).not.toBeInstanceOf(StaleNotFoundError);
  });
});

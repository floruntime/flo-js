/**
 * stream.info: a stream that doesn't exist is not found, not an empty
 * stream; info carries its retention. stream.create and alter: the
 * settings each sends.
 */

import { describe, expect, it } from "vitest";
import { IncompleteResponseError, NotFoundError, OpCode, OptionTag, Ran, Reason, StatusCode, StreamID, StreamOperations } from "@floruntime/core";

describe("stream.info", () => {
  it("throws NotFoundError when the server answers not_found", async () => {
    const message = "stream not found";
    const body = new Uint8Array(3 + message.length);
    body[0] = Reason.NotFound & 0xff;
    body[1] = Reason.NotFound >> 8;
    body[2] = Ran.No;
    body.set(new TextEncoder().encode(message), 3);
    const stream = new StreamOperations({
      getNamespace: (override?: string) => override ?? "default",
      sendRequest: async () => ({ status: StatusCode.NotFound, data: body, requestId: 1n }),
    });
    const err = await stream.info("missing").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err).toMatchObject({ status: StatusCode.NotFound, ran: Ran.No });
    expect((err as Error).message).toContain(message);
  });
});

type Sent = { op: number; key: string; value: Uint8Array; options: Uint8Array };

function answering(data: Uint8Array): { stream: StreamOperations; sent: Sent[] } {
  const sent: Sent[] = [];
  const stream = new StreamOperations({
    getNamespace: (override?: string) => override ?? "default",
    sendRequest: async (op, _ns, key, value, options) => {
      sent.push({ op, key: new TextDecoder().decode(key), value, options });
      return { status: StatusCode.OK, data, requestId: 1n };
    },
  });
  return { stream, sent };
}

const le = (v: bigint, n: number) => Array.from({ length: n }, (_, i) => Number((v >> BigInt(8 * i)) & 0xffn));

describe("stream.info's retention", () => {
  it("reads the retention age and count after the partition count", async () => {
    const body = Uint8Array.from([...le(10n, 8), ...le(1n, 8), ...le(20n, 8), ...le(4n, 8), ...le(7n, 8), ...le(300n, 8), ...le(2n, 4), ...le(3600n, 8), ...le(500n, 8), ...le(0n, 8)]);
    expect(await answering(body).stream.info("s")).toEqual({
      firstId: new StreamID(10n, 1n),
      lastId: new StreamID(20n, 4n),
      count: 7n,
      bytes: 300n,
      partitionCount: 2,
      retentionAgeS: 3600n,
      retentionCount: 500n,
    });
    await expect(answering(body.subarray(0, 75)).stream.info("s")).rejects.toBeInstanceOf(IncompleteResponseError);
    await expect(answering(new Uint8Array(77)).stream.info("s")).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("stream.create and alter", () => {
  /** Options as [tag:u8][len:u8][value]. */
  const u64opt = (tag: number, v: bigint) => [tag, 8, ...le(v, 8)];

  it("create sends the partition count in the value and retention as options", async () => {
    const { stream, sent } = answering(new Uint8Array(0));
    await stream.create("s", { partitions: 4, retentionAgeS: 60n, retentionCount: 10n });
    await stream.create("t");
    expect(sent.map((x) => [x.op, x.key])).toEqual([
      [OpCode.StreamCreate, "s"],
      [OpCode.StreamCreate, "t"],
    ]);
    expect(Array.from(sent[0]!.value)).toEqual(le(4n, 4));
    expect(Array.from(sent[0]!.options)).toEqual([...u64opt(OptionTag.RetentionAge, 60n), ...u64opt(OptionTag.RetentionCount, 10n)]);
    expect(Array.from(sent[1]!.value)).toEqual(le(1n, 4));
    expect(sent[1]!.options.length).toBe(0);
    await expect(stream.create("s", { partitions: 0 })).rejects.toThrow(/partitions/);
  });

  it("alter sends only the settings given", async () => {
    const { stream, sent } = answering(new Uint8Array(0));
    await stream.alter("s", { retentionCount: 5n });
    expect(sent[0]!.op).toBe(OpCode.StreamAlter);
    expect(sent[0]!.value.length).toBe(0);
    expect(Array.from(sent[0]!.options)).toEqual(u64opt(OptionTag.RetentionCount, 5n));
    await expect(stream.alter("s", { retentionAgeS: -1n })).rejects.toThrow(/u64/);
  });
});

/**
 * stream.delete: what goes on the wire, and how a refusal comes back.
 */

import { describe, expect, it } from "vitest";
import {
  ConflictError,
  OpCode,
  OptionTag,
  Ran,
  Reason,
  StatusCode,
  StreamOperations,
  type RawResponse,
} from "@floruntime/core";

interface Sent {
  opCode: number;
  namespace: string;
  key: Uint8Array;
  value: Uint8Array;
  options: Uint8Array;
}

function fakeStream(answer: RawResponse = { status: StatusCode.OK, data: new Uint8Array(0), requestId: 1n }) {
  const sent: Sent[] = [];
  const stream = new StreamOperations({
    getNamespace: (override?: string) => override ?? "default",
    sendRequest: async (opCode, namespace, key, value, options) => {
      sent.push({ opCode, namespace, key, value, options });
      return answer;
    },
  });
  return { stream, sent };
}

const enc = new TextEncoder();

describe("stream.delete", () => {
  it("sends the stream name as the key, an empty value and no options", async () => {
    const { stream, sent } = fakeStream();
    await stream.delete("events");
    expect(sent).toHaveLength(1);
    expect(sent[0]!.opCode).toBe(0x140);
    expect(sent[0]!.opCode).toBe(OpCode.StreamDelete);
    expect(sent[0]!.namespace).toBe("default");
    expect(sent[0]!.key).toEqual(enc.encode("events"));
    expect(sent[0]!.value).toHaveLength(0);
    expect(sent[0]!.options).toHaveLength(0);
  });

  it("sends force as the void flag 0x2c, and only when it is true", async () => {
    const { stream, sent } = fakeStream();
    await stream.delete("events", { force: true, namespace: "other" });
    await stream.delete("events", { force: false });
    expect(OptionTag.Force).toBe(0x2c);
    expect(sent[0]!.namespace).toBe("other");
    expect(Array.from(sent[0]!.options)).toEqual([0x2c, 0]);
    expect(sent[1]!.options).toHaveLength(0);
  });

  it("throws ConflictError, with the server's message, for a non-empty stream without force", async () => {
    const message = "stream is not empty; pass --force to delete";
    const body = new Uint8Array(3 + message.length);
    body[0] = Reason.Unclassified & 0xff;
    body[1] = Reason.Unclassified >> 8;
    body[2] = Ran.No;
    body.set(enc.encode(message), 3);
    const { stream } = fakeStream({ status: StatusCode.Conflict, data: body, requestId: 1n });
    const err = await stream.delete("events").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ConflictError);
    expect((err as Error).message).toContain(message);
  });
});

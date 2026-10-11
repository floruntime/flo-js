/**
 * kv.get and kv.set as table ops: schema-coded bodies out, the ops'
 * response schemas back.
 */
import { describe, it, expect } from "vitest";
import {
  BlockTooLongError,
  Condition,
  IncompleteResponseError,
  KVOperations,
  MAX_KV_VALUE_SIZE,
  MAX_WAIT_MS,
  OpCode,
  OptionTag,
  Ran,
  type RawResponse,
  Reason,
  ServerError,
  StatusCode,
  Transaction,
  WaitFor,
} from "../packages/core/src/index.js";
import { decodeGetAnswer, decodeSetAnswer } from "../packages/core/src/kv_body.js";
import { requestBlockMs, serializeRequest } from "../packages/core/src/wire.js";

const enc = new TextEncoder();

function u64(v: bigint): number[] {
  return Array.from({ length: 8 }, (_, i) => Number((v >> BigInt(8 * i)) & 0xffn));
}

interface Sent {
  op?: OpCode;
  key?: string;
  value?: Uint8Array;
  options?: Uint8Array;
}

function sender(answer: RawResponse) {
  const sent: Sent = {};
  return {
    sent,
    s: {
      getNamespace: (ns?: string) => ns ?? "default",
      sendRequest: async (op: OpCode, _ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
        Object.assign(sent, { op, key: new TextDecoder().decode(key), value, options });
        return answer;
      },
    },
  };
}

const getAnswer = (version: bigint, value: string): RawResponse => {
  const v = enc.encode(value);
  return { status: StatusCode.OK, data: new Uint8Array([...u64(version), v.length, 0, 0, 0, ...v]), requestId: 0n };
};

function refusal(status: StatusCode, reason: Reason): RawResponse {
  return { status, data: new Uint8Array([reason, 0, Ran.No, ...enc.encode("why")]), requestId: 0n };
}

describe("kv.get", () => {
  it("sends wait_ms, wait_for, routing_key and txn in order", async () => {
    const { s, sent } = sender(getAnswer(4n, "hi"));
    const got = await new KVOperations(s).get("k", { waitMs: 5000, waitFor: WaitFor.Change, routingKey: "rk" });
    expect(got).toEqual({ version: 4n, value: enc.encode("hi") });
    expect(sent.value).toEqual(new Uint8Array([1, ...u64(5000n), 1, 1, 1, 2, 0, ...enc.encode("rk"), 0]));
    expect(sent.options).toEqual(new Uint8Array(0));
  });

  it("is absent on reason not_found, and throws on any other not-found", async () => {
    expect(await new KVOperations(sender(refusal(StatusCode.NotFound, Reason.NotFound)).s).get("k")).toBeNull();
    const err = await new KVOperations(sender(refusal(StatusCode.NotFound, Reason.NoSuchTransaction)).s).get("k").catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect((err as ServerError).reason).toBe(Reason.NoSuchTransaction);
  });

  it("refuses a wait past the cap before a round trip, and the read deadline covers a wait", () => {
    const { s } = sender(getAnswer(1n, ""));
    expect(new KVOperations(s).get("k", { waitMs: MAX_WAIT_MS + 1 })).rejects.toBeInstanceOf(BlockTooLongError);
    const body = new Uint8Array([1, ...u64(7000n), 1, 0, 0, 0]);
    const req = serializeRequest(1n, OpCode.KVGet, enc.encode("ns"), enc.encode("k"), body, new Uint8Array(0));
    expect(requestBlockMs(req)).toBe(7000);
    // The table frame: no value length or options trailer.
    expect(new DataView(req.buffer).getUint32(4, true)).toBe(2 + 2 + 2 + 1 + body.length);
  });
});

describe("kv.set", () => {
  it("sends condition and cas_version, and reads its version", async () => {
    const { s, sent } = sender({ status: StatusCode.OK, data: new Uint8Array([1, ...u64(9n)]), requestId: 0n });
    const r = await new KVOperations(s).put("k", enc.encode("v"), { condition: Condition.IfAbsent, casVersion: 0n });
    expect(r.version).toBe(9n);
    expect(sent.value).toEqual(new Uint8Array([1, 0, 0, 0, ...enc.encode("v"), 0, 1, 1, 1, ...u64(0n), 0, 0]));
  });

  it("refuses a value past the cap before a round trip", async () => {
    const { s } = sender(getAnswer(1n, ""));
    await expect(new KVOperations(s).put("k", new Uint8Array(MAX_KV_VALUE_SIZE + 1))).rejects.toThrow("at most");
  });
});

describe("a transaction", () => {
  it("sends its id and routing key in kv.get and kv.set, and commits by id alone", async () => {
    const id = (3n << 48n) | 7n;
    const { s, sent } = sender(getAnswer(0n, "5"));
    const txn = new Transaction(s, "ns", "rk", id, 0n);
    await txn.get("k");
    expect(sent.value).toEqual(new Uint8Array([1, ...u64(0n), 1, 0, 1, 2, 0, ...enc.encode("rk"), 1, ...u64(id)]));

    const put = sender({ status: StatusCode.OK, data: new Uint8Array([0]), requestId: 0n });
    const t2 = new Transaction(put.s, "ns", "rk", id, 0n);
    expect((await t2.put("k", enc.encode("v"))).version).toBe(0n);
    expect(put.sent.value!.slice(-14)).toEqual(new Uint8Array([1, 2, 0, ...enc.encode("rk"), 1, ...u64(id)]));

    const commit = sender({ status: StatusCode.OK, data: new Uint8Array([1, ...u64(5n), 1, 0]), requestId: 0n });
    await new Transaction(commit.s, "ns", "rk", id, 0n).commit();
    expect(commit.sent.key).toBe("");
    expect(commit.sent.options).toEqual(new Uint8Array([OptionTag.TxnID, 8, ...u64(id)]));
  });
});

describe("answers", () => {
  it("decode whole or not at all", () => {
    expect(() => decodeGetAnswer(new Uint8Array([...u64(1n), 3, 0, 0, 0, 1]))).toThrow(IncompleteResponseError);
    expect(() => decodeSetAnswer(new Uint8Array([2]))).toThrow(IncompleteResponseError);
    expect(decodeSetAnswer(new Uint8Array([0])).version).toBe(0n);
  });
});

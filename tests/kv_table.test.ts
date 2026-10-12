/**
 * Every KV op is a table op: schema-coded bodies out, the ops' response
 * schemas back. The hex vectors come from the server's own schemas.
 */
import { describe, it, expect } from "vitest";
import {
  BlockTooLongError,
  Condition,
  FloError,
  IncompleteResponseError,
  KVOperations,
  MAX_HISTORY_LIMIT,
  MAX_JSON_DOCUMENT,
  MAX_KV_VALUE_SIZE,
  MAX_SCAN_LIMIT,
  MAX_TTL_MS,
  MAX_WAIT_MS,
  OpCode,
  Ran,
  type RawResponse,
  Reason,
  reasonName,
  ServerError,
  StatusCode,
  Transaction,
  WaitFor,
} from "../packages/core/src/index.js";
import {
  decodeDeleteAnswer,
  decodeGetAnswer,
  decodeIncrAnswer,
  decodeSetAnswer,
  encodeIncrBody,
  isTableOp,
} from "../packages/core/src/kv_body.js";
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

    const commit = sender({ status: StatusCode.OK, data: new Uint8Array([...u64(5n), 1, 0]), requestId: 0n });
    expect(await new Transaction(commit.s, "ns", "rk", id, 0n).commit()).toEqual({ commitIndex: 5n, opCount: 1 });
    expect(commit.sent.op).toBe(OpCode.KVCommitTxn);
    expect(commit.sent.key).toBe("");
    expect(commit.sent.value).toEqual(new Uint8Array(u64(id)));
    expect(commit.sent.options).toEqual(new Uint8Array(0));
  });
});

describe("answers", () => {
  it("decode whole or not at all", () => {
    expect(() => decodeGetAnswer(new Uint8Array([...u64(1n), 3, 0, 0, 0, 1]))).toThrow(IncompleteResponseError);
    expect(() => decodeSetAnswer(new Uint8Array([2]))).toThrow(IncompleteResponseError);
    expect(decodeSetAnswer(new Uint8Array([0])).version).toBe(0n);
  });
});

const hex = (h: string) => new Uint8Array((h.match(/../g) ?? []).map((b) => parseInt(b, 16)));
const ok = (h: string): RawResponse => ({ status: StatusCode.OK, data: hex(h), requestId: 0n });

/** A sender that answers each request with the next of `answers`. */
function seq(...answers: RawResponse[]) {
  const sent: Required<Sent>[] = [];
  return {
    sent,
    s: {
      getNamespace: (ns?: string) => ns ?? "default",
      sendRequest: async (op: OpCode, _ns: string, key: Uint8Array, value: Uint8Array, options: Uint8Array) => {
        sent.push({ op, key: new TextDecoder().decode(key), value, options });
        const a = answers.shift();
        if (!a) throw new Error("no answer left");
        return a;
      },
    },
  };
}

describe("request bodies match the server's vectors", () => {
  const cases: [string, OpCode, string, string, (kv: KVOperations) => Promise<unknown>, string, unknown][] = [
    ["delete", OpCode.KVDelete, "k", "010700000000000000010200726b00", (kv) => kv.delete("k", { casVersion: 7n, routingKey: "rk" }), "0101", true],
    ["scan", OpCode.KVScan, "p", "0102000000026331", (kv) => kv.scan("p", { limit: 2, cursor: enc.encode("c1") }), "02000100610200626201026332", { keys: ["a", "bb"], cursor: enc.encode("c2") }],
    ["history", OpCode.KVHistory, "k", "0102000000", (kv) => kv.history("k", { limit: 2 }), "010300000000000000e803000000000000020000007633", [{ version: 3n, atMs: 1000n, value: enc.encode("v3") }]],
    ["incr", OpCode.KVIncr, "k", "01fbffffffffffffff0000", (kv) => kv.incr("k", { delta: -5n }), "010400000000000000012a00000000000000", { version: 4n, value: 42n }],
    ["expire", OpCode.KVExpire, "k", "60ea000000000000000000", (kv) => kv.expire("k", 60000), "", undefined],
    ["exists", OpCode.KVExists, "k", "0000", (kv) => kv.exists("k"), "010500000000000000", true],
    ["jsonGet", OpCode.KVJsonGet, "k", "010300242e6100", (kv) => kv.jsonGet("k", "$.a"), "0300000000000000020000007b7d", { version: 3n, value: enc.encode("{}") }],
    ["jsonSet", OpCode.KVJsonSet, "k", "01010024020000007b7d010200726b", (kv) => kv.jsonSet("k", "$", enc.encode("{}"), { routingKey: "rk" }), "0300000000000000", { version: 3n }],
    ["jsonDel", OpCode.KVJsonDel, "k", "010300242e6200", (kv) => kv.jsonDel("k", "$.b"), "00", null],
  ];
  for (const [name, op, key, body, call, answer, want] of cases) {
    it(name, async () => {
      const { s, sent } = seq(ok(answer));
      expect(await call(new KVOperations(s))).toEqual(want);
      expect(sent[0]).toEqual({ op, key, value: hex(body), options: new Uint8Array(0) });
    });
  }

  it("jsonGet and jsonDel default the path to $", async () => {
    const { s, sent } = seq(ok("0300000000000000020000007b7d"), ok("00"));
    await new KVOperations(s).jsonGet("k");
    await new KVOperations(s).jsonDel("k");
    expect(sent[0]!.value).toEqual(hex("0101002400"));
    expect(sent[1]!.value).toEqual(hex("0101002400"));
  });

  it("scan's last page has no cursor", async () => {
    const { s } = seq(ok("000000"));
    expect(await new KVOperations(s).scan("")).toEqual({ keys: [], cursor: null });
  });

  it("mget", async () => {
    const { s, sent } = seq(ok("0200010200000000000000010000007800000000000000000000000000" + "00"));
    expect(await new KVOperations(s).mget(["a", "b"])).toEqual([
      { key: "a", found: true, version: 2n, value: enc.encode("x") },
      { key: "b", found: false, version: 0n, value: new Uint8Array(0) },
    ]);
    expect(sent[0]).toEqual({ op: OpCode.KVMGet, key: "", value: hex("0200010061010062"), options: new Uint8Array(0) });
  });
});

describe("the vectors inside a transaction", () => {
  it("incr sends txn and answers nothing; delete answers no existed", () => {
    expect(encodeIncrBody({ delta: -5n }, 9n)).toEqual(hex("01fbffffffffffffff00010900000000000000"));
    expect(decodeIncrAnswer(hex("0000"))).toEqual({ version: null, value: null });
    expect(decodeDeleteAnswer(hex("00"))).toBeNull();
  });

  it("every KV op is a table op", () => {
    const kv = Object.entries(OpCode).filter(([n]) => n.startsWith("KV"));
    expect(kv).toHaveLength(15);
    for (const [, op] of kv) expect(isTableOp(op)).toBe(true);
    expect(isTableOp(OpCode.QueueEnqueue)).toBe(false);
  });
});

describe("kv.delete", () => {
  it("of a missing key is not an error: it didn't exist", async () => {
    expect(await new KVOperations(seq(ok("0100")).s).delete("k")).toBe(false);
  });
});

describe("kv.mget", () => {
  it("asks again from next until every key is answered, in order", async () => {
    // The first answer fills up after two keys and says next = 2.
    const { s, sent } = seq(
      ok("0200010200000000000000010000007800000000000000000000000000010200"),
      ok("01000103000000000000000100000079" + "00")
    );
    const got = await new KVOperations(s).mget(["a", "b", "c"]);
    expect(got.map((e) => [e.key, e.found, e.version])).toEqual([
      ["a", true, 2n],
      ["b", false, 0n],
      ["c", true, 3n],
    ]);
    expect(sent).toHaveLength(2);
    expect(sent[0]!.value).toEqual(hex("030001006101006201006" + "3"));
    expect(sent[1]!.value).toEqual(hex("0100010063"));
  });

  it("refuses an answer that makes no progress", async () => {
    const { s } = seq(ok("0000010000"));
    await expect(new KVOperations(s).mget(["a"])).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("transaction ops", () => {
  const id = 9n;
  it("carry the transaction's routing key and id", async () => {
    const { s, sent } = seq(ok("00"), ok("0000"), ok(""), ok("000000000000000000"));
    const txn = new Transaction(s, "ns", "rk", id, 0n);
    await txn.delete("k");
    await txn.incr("k", -5n);
    await txn.expire("k", 0);
    expect(await txn.exists("k")).toBe(false);
    const rk = "010200726b";
    const t = "01" + "0900000000000000";
    expect(sent.map((x) => [x.op, x.key])).toEqual([
      [OpCode.KVDelete, "k"],
      [OpCode.KVIncr, "k"],
      [OpCode.KVExpire, "k"],
      [OpCode.KVExists, "k"],
    ]);
    expect(sent[0]!.value).toEqual(hex("00" + rk + t));
    expect(sent[1]!.value).toEqual(hex("01fbffffffffffffff" + rk + t));
    expect(sent[2]!.value).toEqual(hex("0000000000000000" + "00" + rk + t));
    expect(sent[3]!.value).toEqual(hex(rk + t));
  });

  it("begin sends the routing key as the key and no body; rollback sends {txn} with no key", async () => {
    const { s, sent } = seq(ok("01000000000000000200000000000000"), ok(""));
    const txn = await new KVOperations(s).begin("rk");
    expect([txn.id, txn.pinnedHash]).toEqual([1n, 2n]);
    expect(sent[0]).toEqual({ op: OpCode.KVBeginTxn, key: "rk", value: new Uint8Array(0), options: new Uint8Array(0) });
    await txn.rollback();
    expect(sent[1]).toEqual({ op: OpCode.KVRollbackTxn, key: "", value: hex("0100000000000000"), options: new Uint8Array(0) });
  });

  it("a not-found other than not_found is an error", async () => {
    const { s } = seq(refusal(StatusCode.NotFound, Reason.NoSuchTransaction));
    const err = await new KVOperations(s).jsonGet("k").catch((e) => e);
    expect((err as ServerError).reason).toBe(Reason.NoSuchTransaction);
    expect(await new KVOperations(seq(refusal(StatusCode.NotFound, Reason.NotFound)).s).jsonGet("k")).toBeNull();
  });
});

describe("bounds are refused before a round trip", () => {
  const refused: [string, (kv: KVOperations) => Promise<unknown>][] = [
    ["mget of 257 keys", (kv) => kv.mget(Array.from({ length: 257 }, (_, i) => `k${i}`))],
    ["scan limit 0", (kv) => kv.scan("", { limit: 0 })],
    ["scan limit 1025", (kv) => kv.scan("", { limit: MAX_SCAN_LIMIT + 1 })],
    ["scan cursor of 65 bytes", (kv) => kv.scan("", { cursor: new Uint8Array(65) })],
    ["history limit 66", (kv) => kv.history("k", { limit: MAX_HISTORY_LIMIT + 1 })],
    ["expire past MAX_TTL_MS", (kv) => kv.expire("k", MAX_TTL_MS + 1n)],
    ["expire negative", (kv) => kv.expire("k", -1)],
    ["incr past i64", (kv) => kv.incr("k", { delta: 1n << 63n })],
    ["a JSON document too long", (kv) => kv.jsonSet("k", "$", new Uint8Array(MAX_JSON_DOCUMENT + 1))],
    ["a JSON path too long", (kv) => kv.jsonGet("k", "$" + "a".repeat(1024))],
    ["a routing key too long", (kv) => kv.exists("k", { routingKey: "r".repeat(4097) })],
    ["begin without a routing key", (kv) => kv.begin("")],
  ];
  for (const [name, call] of refused) {
    it(name, async () => {
      const { s, sent } = seq();
      await expect(call(new KVOperations(s))).rejects.toBeInstanceOf(FloError);
      expect(sent).toHaveLength(0);
    });
  }
});

describe("answers are decoded whole", () => {
  const truncated: [string, (kv: KVOperations) => Promise<unknown>, string][] = [
    ["delete, trailing", (kv) => kv.delete("k"), "010100"],
    ["delete, bad bool", (kv) => kv.delete("k"), "0102"],
    ["delete, null outside a transaction", (kv) => kv.delete("k"), "00"],
    ["scan, truncated name", (kv) => kv.scan("p"), "0100050061"],
    ["history, truncated", (kv) => kv.history("k"), "0103000000"],
    ["incr, truncated", (kv) => kv.incr("k"), "010400000000000000"],
    ["expire, trailing", (kv) => kv.expire("k", 1), "00"],
    ["exists, trailing", (kv) => kv.exists("k"), "01050000000000000000"],
    ["jsonSet, short", (kv) => kv.jsonSet("k", "$", enc.encode("{}")), "03000000"],
    ["mget, trailing", (kv) => kv.mget(["a"]), "01000100000000000000000000000000ff"],
  ];
  for (const [name, call, answer] of truncated) {
    it(name, async () => {
      await expect(call(new KVOperations(seq(ok(answer)).s))).rejects.toBeInstanceOf(IncompleteResponseError);
    });
  }

  it("for the transaction answers too", async () => {
    const { s } = seq(ok("0100000000000000020000000000000000"));
    await expect(new KVOperations(s).begin("rk")).rejects.toBeInstanceOf(IncompleteResponseError);
    const c = seq(ok("0a0000000000000002"));
    await expect(new Transaction(c.s, "ns", "rk", 1n, 0n).commit()).rejects.toBeInstanceOf(IncompleteResponseError);
  });
});

describe("the reason list", () => {
  it("names wrong_type and too_many_transactions", () => {
    expect(reasonName(Reason.WrongType)).toBe("wrong_type");
    expect(reasonName(Reason.TooManyTransactions)).toBe("too_many_transactions");
    expect([Reason.WrongRoutingKey, Reason.WrongType, Reason.TooManyTransactions]).toEqual([18, 24, 25]);
  });
});

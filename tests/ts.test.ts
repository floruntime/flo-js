/** Time-series requests and answers, against a fake sender. */

import { describe, expect, it } from "vitest";
import {
  BadRequestError,
  FloError,
  IncompleteResponseError,
  NotFoundError,
  OpCode,
  OptionTag,
  Ran,
  Reason,
  StatusCode,
  TSOperations,
  TSOrder,
  dateFromNs,
  decodeTSReadAnswer,
  encodeTSReadBody,
  nsFromDate,
  type RawResponse,
} from "@floruntime/core";

const enc = new TextEncoder();

function bytes(...parts: (Uint8Array | number[])[]): Uint8Array {
  const out: number[] = [];
  for (const p of parts) out.push(...p);
  return new Uint8Array(out);
}
function u16(n: number): number[] {
  return [n & 0xff, n >> 8];
}
function u32(n: number): number[] {
  return [n & 0xff, (n >> 8) & 0xff, (n >> 16) & 0xff, n >>> 24];
}
function i64(n: bigint): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigInt64(0, n, true);
  return b;
}
function f64(n: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setFloat64(0, n, true);
  return b;
}
function page(points: [bigint, number][], cursor: Uint8Array | null): Uint8Array {
  return bytes(u16(points.length), ...points.map(([t, v]) => bytes(i64(t), f64(v))), cursor ? bytes([1, cursor.length], cursor) : [0]);
}
function refusal(status: number, message: string): RawResponse {
  return { status, data: bytes(u16(Reason.Malformed), [Ran.No], enc.encode(message)) } as RawResponse;
}

interface Sent {
  op: OpCode;
  namespace: string;
  key: string;
  value: Uint8Array;
  options: Uint8Array;
}

/** A TSOperations whose requests get `answers` in turn; `sent` records them. */
function fake(...answers: RawResponse[]): { ts: TSOperations; sent: Sent[] } {
  const sent: Sent[] = [];
  const ts = new TSOperations({
    getNamespace: (ns?: string) => ns ?? "default",
    sendRequest: async (op, namespace, key, value, options) => {
      sent.push({ op, namespace, key: new TextDecoder().decode(key), value, options });
      const a = answers.shift();
      if (!a) throw new Error("no answer left");
      return a;
    },
  });
  return { ts, sent };
}
const ok = (data: Uint8Array) => ({ status: StatusCode.OK, data }) as RawResponse;

const CURSOR = Uint8Array.from({ length: 16 }, (_, i) => i + 1);

describe("ts.read request", () => {
  it("codes every field, each present, in the table's order", () => {
    const body = encodeTSReadBody({ field: "temp", tags: "host=a", fromNs: 5n, toNs: 1_700_000_000_123_456_789n, limit: 100, order: TSOrder.NewestFirst, cursor: CURSOR });
    expect(body).toEqual(
      bytes(
        [1], u16(4), enc.encode("temp"),
        [1], u16(6), enc.encode("host=a"),
        [1], i64(5n),
        [1], i64(1_700_000_000_123_456_789n),
        [1], u32(100),
        [1], [1],
        [1], [16], CURSOR,
      ),
    );
  });

  it("sends the defaults present, and no range", () => {
    expect(encodeTSReadBody()).toEqual(bytes([1], u16(5), enc.encode("value"), [1], u16(0), [0], [0], [1], u32(4096), [1], [0], [1], [0]));
  });

  it("refuses a bad limit, range, tag filter or cursor before a round trip", () => {
    expect(() => encodeTSReadBody({ limit: 0 })).toThrow(FloError);
    expect(() => encodeTSReadBody({ limit: 4097 })).toThrow(FloError);
    expect(() => encodeTSReadBody({ fromNs: -1n })).toThrow(FloError);
    expect(() => encodeTSReadBody({ toNs: 1n << 63n })).toThrow(FloError);
    expect(() => encodeTSReadBody({ tags: "x".repeat(1025) })).toThrow(FloError);
    expect(() => encodeTSReadBody({ cursor: new Uint8Array(17) })).toThrow(FloError);
  });

  it("goes to ts.read with the measurement as the key and no options", async () => {
    const { ts, sent } = fake(ok(page([], null)));
    await ts.read("cpu", { namespace: "n" });
    expect(sent.map((s) => [s.op, s.namespace, s.key, s.options.length])).toEqual([[OpCode.TSRead, "n", "cpu", 0]]);
  });
});

describe("ts.read answer", () => {
  it("decodes points and a cursor", () => {
    expect(decodeTSReadAnswer(page([[1n, 1.5], [1_700_000_000_123_456_789n, -2]], CURSOR))).toEqual({
      points: [{ timeNs: 1n, value: 1.5 }, { timeNs: 1_700_000_000_123_456_789n, value: -2 }],
      cursor: CURSOR,
      stale: false,
    });
  });

  it("decodes the last page, with no cursor", () => {
    expect(decodeTSReadAnswer(page([[7n, 0]], null))).toEqual({ points: [{ timeNs: 7n, value: 0 }], cursor: null, stale: false });
  });

  it("refuses an answer cut short or with bytes left over", () => {
    const whole = page([[1n, 1]], null);
    expect(() => decodeTSReadAnswer(whole.subarray(0, whole.length - 1))).toThrow(IncompleteResponseError);
    expect(() => decodeTSReadAnswer(bytes(whole, [0]))).toThrow(IncompleteResponseError);
  });

  it("refuses a bad cursor with the server's bad request", async () => {
    const { ts } = fake(refusal(StatusCode.BadRequest, "bad request: a cursor is the 16 bytes a page answered"));
    const err = await ts.read("cpu", { cursor: new Uint8Array(5) }).catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("bad request: a cursor is the 16 bytes a page answered");
  });

  it("throws NotFoundError for a not-found answer", async () => {
    const { ts } = fake(refusal(StatusCode.NotFound, "not found"));
    await expect(ts.read("cpu")).rejects.toThrow(NotFoundError);
  });
});

describe("ts.readAll", () => {
  it("pages on a short page while there is a cursor, and stops at none", async () => {
    const c2 = CURSOR.map((b) => b + 16);
    const { ts, sent } = fake(ok(page([[1n, 1]], CURSOR)), ok(page([], c2)), ok(page([[3n, 3]], null)));
    const got: bigint[] = [];
    for await (const p of ts.readAll("cpu", { limit: 2, order: TSOrder.NewestFirst })) got.push(p.timeNs);
    expect(got).toEqual([1n, 3n]);
    expect(sent.length).toBe(3);
    // Each page names the order, then the cursor the last one answered.
    const tail = (s: Sent, n: number) => s.value.subarray(s.value.length - n);
    expect(tail(sent[0]!, 4)).toEqual(bytes([1, TSOrder.NewestFirst], [1, 0]));
    expect(tail(sent[1]!, 20)).toEqual(bytes([1, TSOrder.NewestFirst], [1, 16], CURSOR));
    expect(tail(sent[2]!, 20)).toEqual(bytes([1, TSOrder.NewestFirst], [1, 16], c2));
  });

  it("throws a page's refusal from the iteration, a not-found too", async () => {
    const { ts } = fake(ok(page([[1n, 1]], CURSOR)), refusal(StatusCode.NotFound, "not found"));
    const got: bigint[] = [];
    const run = async () => {
      for await (const p of ts.readAll("cpu")) got.push(p.timeNs);
    };
    await expect(run()).rejects.toThrow(NotFoundError);
    expect(got).toEqual([1n]);
  });
});

describe("ts.write", () => {
  const answer = ok(bytes(i64(42n), i64(1_700_000_000_123_456_789n), i64(9n)));

  it("sends its time as an i64 of ns, one that isn't a whole ms exactly", async () => {
    const { ts, sent } = fake(answer);
    const got = await ts.write("cpu", 1.25, { field: "f", tags: "host=a", timeNs: 1_700_000_000_123_456_789n });
    expect(got).toEqual({ seriesHash: 42n, timeNs: 1_700_000_000_123_456_789n, sequence: 9n });
    const s = sent[0]!;
    expect([s.op, s.key]).toEqual([OpCode.TSWrite, "cpu"]);
    expect(s.value).toEqual(f64(1.25));
    expect(s.options).toEqual(bytes([OptionTag.TSField, 1], enc.encode("f"), [OptionTag.TSTags, 6], enc.encode("host=a"), [OptionTag.TSTimestampNS, 8], i64(1_700_000_000_123_456_789n)));
  });

  it("sends no time when given none, for the server's", async () => {
    const { ts, sent } = fake(answer);
    await ts.write("cpu", 1);
    expect(sent[0]!.options.length).toBe(0);
  });

  it("surfaces the server's refusal of a time before 1970", async () => {
    const { ts, sent } = fake(refusal(StatusCode.BadRequest, "ts write: a point's time must not be before 1970"));
    const err = await ts.write("cpu", 1, { timeNs: -1n }).catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe("ts write: a point's time must not be before 1970");
    expect(sent[0]!.options).toEqual(bytes([OptionTag.TSTimestampNS, 8], i64(-1n)));
  });

  it("refuses a time past an i64 before a round trip", async () => {
    const { ts, sent } = fake();
    await expect(ts.write("cpu", 1, { timeNs: 1n << 63n })).rejects.toThrow(FloError);
    expect(sent).toEqual([]);
  });
});

describe("ts.floql", () => {
  it("sends the query as the value and decodes its series, times in ms", async () => {
    const key = enc.encode("cpu");
    const field = enc.encode("value");
    const { ts, sent } = fake(ok(bytes(u32(1), u32(3), key, u32(5), field, u32(2), i64(1000n), f64(1), i64(2000n), f64(2))));
    expect(await ts.floql("cpu[1h]")).toEqual([{ key: "cpu", field: "value", points: [{ timeMs: 1000n, value: 1 }, { timeMs: 2000n, value: 2 }] }]);
    expect([sent[0]!.op, sent[0]!.key, new TextDecoder().decode(sent[0]!.value)]).toEqual([OpCode.TSFloQL, "", "cpu[1h]"]);
  });

  it("surfaces the refusal of an answer past 4096 points with the server's text", async () => {
    const text = "bad request: the answer holds more than 4096 points; aggregate them, narrow the query, or page through them with ts read";
    const { ts } = fake(refusal(StatusCode.BadRequest, text));
    const err = await ts.floql("cpu").catch((e) => e);
    expect(err).toBeInstanceOf(BadRequestError);
    expect(err.serverMessage).toBe(text);
  });
});

describe("Date helpers", () => {
  it("convert at ms, dropping what a Date can't hold", () => {
    expect(nsFromDate(new Date(1_700_000_000_123))).toBe(1_700_000_000_123_000_000n);
    expect(dateFromNs(1_700_000_000_123_456_789n).getTime()).toBe(1_700_000_000_123);
    expect(dateFromNs(-1n).getTime()).toBe(-1);
  });
});

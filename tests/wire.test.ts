/**
 * Wire format unit tests.
 */

import { describe, it, expect } from "vitest";
import {
  MAGIC,
  VERSION,
  TABLE_HASH,
  TableMismatchError,
  HEADER_SIZE,
  OpCode,
  StatusCode,
  OptionTag,
  OptionsBuilder,
  KVOperations,
  type KVRequestSender,
  type RawResponse,
  computeCRC32,
  serializeRequest,
  parseResponseHeader,
  parseDequeueResponse,
  parseEnqueueResponse,
  serializeSeqs,
  IncompleteResponseError,
  serializeActionInvokeValue,
  FloError,
} from "@floruntime/core";

describe("Wire Protocol", () => {
  describe("Constants", () => {
    it("should have correct magic value", () => {
      // "FLO\0" in little-endian = 0x004F4C46
      expect(MAGIC).toBe(0x004f4c46);
    });

    it("should have correct version", () => {
      expect(VERSION).toBe(0x02);
    });

    it("should have correct header size", () => {
      expect(HEADER_SIZE).toBe(32);
    });
  });

  describe("OptionsBuilder", () => {
    it("should build empty options", () => {
      const builder = new OptionsBuilder();
      const options = builder.build();
      expect(options.length).toBe(0);
    });

    it("should build u8 option", () => {
      const builder = new OptionsBuilder();
      builder.addU8(OptionTag.Priority, 5);
      const options = builder.build();
      expect(options).toEqual(new Uint8Array([0x10, 1, 5]));
    });

    it("should build u32 option", () => {
      const builder = new OptionsBuilder();
      builder.addU32(OptionTag.Limit, 100);
      const options = builder.build();
      // 100 = 0x64 in little-endian: [0x64, 0x00, 0x00, 0x00]
      expect(options).toEqual(new Uint8Array([0x05, 4, 0x64, 0x00, 0x00, 0x00]));
    });

    it("should build u64 option", () => {
      const builder = new OptionsBuilder();
      builder.addU64(OptionTag.MaxAgeSeconds, 3600n);
      const options = builder.build();
      // 3600 = 0x0E10 in little-endian
      expect(options).toEqual(
        new Uint8Array([0x26, 8, 0x10, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
      );
    });

    it("should build flag option", () => {
      const builder = new OptionsBuilder();
      builder.addFlag(OptionTag.DryRun);
      const options = builder.build();
      expect(options).toEqual(new Uint8Array([0x28, 0]));
    });

    it("should chain multiple options", () => {
      const builder = new OptionsBuilder();
      builder.addU8(OptionTag.Priority, 5).addU32(OptionTag.Limit, 10);
      const options = builder.build();
      expect(options.length).toBe(9); // 3 + 6
    });
  });

  describe("CRC32", () => {
    it("should compute CRC32 correctly", () => {
      const header = new Uint8Array(HEADER_SIZE);
      const payload = new Uint8Array([1, 2, 3, 4]);

      const crc = computeCRC32(header, payload);
      expect(typeof crc).toBe("number");
      expect(crc).toBeGreaterThanOrEqual(0);
      expect(crc).toBeLessThanOrEqual(0xffffffff);
    });

    it("should produce different CRC for different data", () => {
      const header = new Uint8Array(HEADER_SIZE);
      const payload1 = new Uint8Array([1, 2, 3, 4]);
      const payload2 = new Uint8Array([5, 6, 7, 8]);

      const crc1 = computeCRC32(header, payload1);
      const crc2 = computeCRC32(header, payload2);

      expect(crc1).not.toBe(crc2);
    });
  });

  describe("serializeRequest", () => {
    it("should serialize a basic request", () => {
      const textEncoder = new TextEncoder();
      const request = serializeRequest(
        1n,
        OpCode.KVGet,
        textEncoder.encode("myns"),
        textEncoder.encode("key1"),
        new Uint8Array(0),
        new Uint8Array(0)
      );

      // Check magic (little-endian)
      const view = new DataView(request.buffer);
      expect(view.getUint32(0, true)).toBe(MAGIC);

      // Check opcode (u16 at 20-21)
      expect(view.getUint16(20, true)).toBe(OpCode.KVGet);

      // Version, then the pinned table hash at 24-31
      expect(request[22]).toBe(VERSION);
      expect(view.getBigUint64(24, true)).toBe(0xc8a2c3c65e95d11dn);
    });

    it("should include namespace in payload", () => {
      const textEncoder = new TextEncoder();
      const request = serializeRequest(
        1n,
        OpCode.KVGet,
        textEncoder.encode("test"),
        textEncoder.encode("key"),
        new Uint8Array(0),
        new Uint8Array(0)
      );

      // After header, first 2 bytes are namespace length
      const view = new DataView(request.buffer);
      expect(view.getUint16(HEADER_SIZE, true)).toBe(4); // "test" length

      // Next bytes should be "test"
      const nsBytes = request.slice(HEADER_SIZE + 2, HEADER_SIZE + 2 + 4);
      expect(new TextDecoder().decode(nsBytes)).toBe("test");
    });
  });

  describe("parseResponseHeader", () => {
    it("should parse valid header", () => {
      const header = new Uint8Array(HEADER_SIZE);
      const view = new DataView(header.buffer);

      view.setUint32(0, MAGIC, true);
      view.setUint32(4, 100, true); // dataLen
      view.setBigUint64(8, 42n, true); // requestId
      view.setUint32(16, 0x12345678, true); // crc
      header[20] = VERSION;
      view.setBigUint64(24, TABLE_HASH, true);
      header[21] = StatusCode.OK;

      const [status, dataLen, requestId, crc] = parseResponseHeader(header);

      expect(status).toBe(StatusCode.OK);
      expect(dataLen).toBe(100);
      expect(requestId).toBe(42n);
      expect(crc).toBe(0x12345678);
    });

    it("should throw on invalid magic", () => {
      const header = new Uint8Array(HEADER_SIZE);
      const view = new DataView(header.buffer);
      view.setUint32(0, 0xdeadbeef, true); // wrong magic

      expect(() => parseResponseHeader(header)).toThrow("invalid protocol magic");
    });

    it("refuses an answer from another protocol version or table, naming both", () => {
      const cases: [number, bigint, string][] = [
        [1, 0n, "flo: server protocol 1, client protocol 2: upgrade the client"],
        [
          VERSION,
          TABLE_HASH + 1n,
          `flo: server table 0x${(TABLE_HASH + 1n).toString(16).padStart(16, "0")}, client table 0x${TABLE_HASH.toString(16).padStart(16, "0")}: upgrade the client`,
        ],
      ];
      for (const [version, table, message] of cases) {
        const header = new Uint8Array(HEADER_SIZE);
        const view = new DataView(header.buffer);
        view.setUint32(0, MAGIC, true);
        header[20] = version;
        view.setBigUint64(24, table, true);
        expect(() => parseResponseHeader(header)).toThrow(TableMismatchError);
        expect(() => parseResponseHeader(header)).toThrow(message);
      }
    });
  });

  describe("parseDequeueResponse", () => {
    it("should parse empty dequeue response", () => {
      const data = new Uint8Array(4);
      const view = new DataView(data.buffer);
      view.setUint32(0, 0, true); // count = 0

      const result = parseDequeueResponse(data);
      expect(result.messages).toEqual([]);
    });

    it("parses every message of a multi-message answer as the server writes it", () => {
      // [seq:u64][payload_len:u32][payload][enqueued_at_ms:i64][delivery_count:u32][priority:u8]
      const want = [
        { seq: 42n, payload: "hi", enqueuedAtMs: 1700000000000n, deliveryCount: 1, priority: 0 },
        { seq: 43n, payload: "two", enqueuedAtMs: 1700000000005n, deliveryCount: 2, priority: 7 },
        { seq: 44n, payload: "", enqueuedAtMs: 1700000000009n, deliveryCount: 3, priority: 255 },
      ];
      const size = 4 + want.reduce((n, m) => n + 8 + 4 + m.payload.length + 13, 0);
      const data = new Uint8Array(size);
      const view = new DataView(data.buffer);
      view.setUint32(0, want.length, true);
      let at = 4;
      for (const m of want) {
        view.setBigUint64(at, m.seq, true);
        view.setUint32(at + 8, m.payload.length, true);
        data.set(new TextEncoder().encode(m.payload), at + 12);
        at += 12 + m.payload.length;
        view.setBigInt64(at, m.enqueuedAtMs, true);
        view.setUint32(at + 8, m.deliveryCount, true);
        view.setUint8(at + 12, m.priority);
        at += 13;
      }

      const result = parseDequeueResponse(data);

      expect(
        result.messages.map((m) => ({
          seq: m.seq,
          payload: new TextDecoder().decode(m.payload),
          enqueuedAtMs: m.enqueuedAtMs,
          deliveryCount: m.deliveryCount,
          priority: m.priority,
        }))
      ).toEqual(want);
    });

    it("refuses a message whose trailer is cut short", () => {
      const data = new Uint8Array(4 + 12 + 1 + 12);
      const view = new DataView(data.buffer);
      view.setUint32(0, 1, true);
      view.setBigUint64(4, 1n, true);
      view.setUint32(12, 1, true);
      expect(() => parseDequeueResponse(data)).toThrow(IncompleteResponseError);
    });
  });

  describe("parseEnqueueResponse", () => {
    it("should parse enqueue response", () => {
      const data = new Uint8Array(8);
      const view = new DataView(data.buffer);
      view.setBigUint64(0, 12345n, true);

      const seq = parseEnqueueResponse(data);
      expect(seq).toBe(12345n);
    });
  });

  describe("serializeSeqs", () => {
    it("should serialize empty sequence list", () => {
      const data = serializeSeqs([]);
      const view = new DataView(data.buffer);
      expect(view.getUint32(0, true)).toBe(0);
      expect(data.length).toBe(4);
    });

    it("should serialize sequence list", () => {
      const data = serializeSeqs([1n, 2n, 3n]);
      const view = new DataView(data.buffer);

      expect(view.getUint32(0, true)).toBe(3);
      expect(view.getBigUint64(4, true)).toBe(1n);
      expect(view.getBigUint64(12, true)).toBe(2n);
      expect(view.getBigUint64(20, true)).toBe(3n);
    });
  });

  describe("KV TTL", () => {
    const ok: RawResponse = { status: StatusCode.OK, data: new Uint8Array(), requestId: 0n };

    function capture(answer: RawResponse = ok) {
      const sent: { value?: Uint8Array; options?: Uint8Array } = {};
      const sender: KVRequestSender = {
        getNamespace: (ns) => ns ?? "default",
        sendRequest: async (_op, _ns, _key, value, options) => {
          sent.value = value;
          sent.options = options;
          return answer;
        },
      };
      return { sender, sent };
    }

    it("encodes put ttlMs as kv.set's ttl_ms field: present, then a u64 of milliseconds", async () => {
      const { sender, sent } = capture({ status: StatusCode.OK, data: new Uint8Array([1, 3, 0, 0, 0, 0, 0, 0, 0]), requestId: 0n });
      const r = await new KVOperations(sender).put("k", new Uint8Array([1]), { ttlMs: 90_000n });
      expect(r.version).toBe(3n);
      // value [len:u32][1], ttl_ms present 90000, condition always, no cas, routing key or txn
      expect(sent.value).toEqual(
        new Uint8Array([1, 0, 0, 0, 1, 1, 0x90, 0x5f, 0x01, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0])
      );
      expect(sent.options).toEqual(new Uint8Array(0));
    });

    it("encodes expire's ttl_ms as a u64 of milliseconds, 0 clearing the TTL", async () => {
      const { sender, sent } = capture();
      await new KVOperations(sender).expire("k", 90_000n);
      expect(sent.value).toEqual(new Uint8Array([0x90, 0x5f, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]));
      await new KVOperations(sender).expire("k", 0);
      expect(sent.value).toEqual(new Uint8Array(11));
      expect(sent.options).toEqual(new Uint8Array(0));
    });
  });

  describe("Action invoke value", () => {
    const enc = new TextEncoder();

    it("writes has_labels=0 then the input when there are no labels", () => {
      expect(serializeActionInvokeValue(enc.encode("x"))).toEqual(new Uint8Array([0, 0x78]));
    });

    it("writes has_labels=1, a u16 LE length and the labels before the input", () => {
      const labels = '{"gpu":true}';
      expect(labels.length).toBe(12);
      expect(serializeActionInvokeValue(enc.encode("x"), labels)).toEqual(
        new Uint8Array([1, 12, 0, ...enc.encode(labels), 0x78])
      );
    });

    it("throws rather than truncate labels over 65535 bytes", () => {
      const labels = JSON.stringify({ k: "a".repeat(65535) });
      expect(() => serializeActionInvokeValue(enc.encode("x"), labels)).toThrow(FloError);
    });
  });
});

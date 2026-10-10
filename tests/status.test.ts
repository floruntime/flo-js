/**
 * Mapping response statuses to errors, including statuses this SDK predates.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  HEADER_SIZE,
  InternalError,
  MAGIC,
  OpCode,
  OverloadedError,
  ServerError,
  StatusCode,
  UnavailableError,
  VERSION,
  TABLE_HASH,
  computeCRC32,
  createServerError,
  isInternal,
  isUnavailable,
  parseRawResponse,
  serializeRequest,
} from "@floruntime/core";
import { FloClient } from "@floruntime/node";

const enc = new TextEncoder();
const UNAVAILABLE_MSG = "shard 3 is offline; an operator must restart it";

function response(id: bigint, data: Uint8Array = new Uint8Array(0), status = 0): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  view.setBigUint64(24, TABLE_HASH, true);
  buf[21] = status;
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

/** A KV get reply body: [version:u64 LE][value]. */
function getBody(version: bigint, value: string): Uint8Array {
  const v = enc.encode(value);
  const buf = new Uint8Array(8 + v.length);
  new DataView(buf.buffer).setBigUint64(0, version, true);
  buf.set(v, 8);
  return buf;
}

describe("createServerError", () => {
  it("maps status 12 to UnavailableError carrying the server's message", () => {
    const err = createServerError(12 as StatusCode, enc.encode(UNAVAILABLE_MSG));
    expect(err).toBeInstanceOf(UnavailableError);
    expect(err.status).toBe(StatusCode.Unavailable);
    expect(err.serverMessage).toBe(UNAVAILABLE_MSG);
    expect(err.message).toContain(UNAVAILABLE_MSG);
    expect(isUnavailable(err)).toBe(true);
  });

  it("maps an unknown status to a plain ServerError naming the number and the message", () => {
    const err = createServerError(200 as StatusCode, enc.encode("from the future"));
    expect(err.constructor).toBe(ServerError);
    expect(err.status).toBe(200);
    expect(err.message).toContain("200");
    expect(err.message).toContain("from the future");
  });

  it("keeps internal_error apart from the retryable statuses", () => {
    const err = createServerError(StatusCode.InternalError, enc.encode("committed but not applied"));
    expect(err).toBeInstanceOf(InternalError);
    expect(isInternal(err)).toBe(true);
    expect(isUnavailable(err)).toBe(false);
    expect(err).not.toBeInstanceOf(OverloadedError);
  });
});

describe("a non-ok reply leaves the connection framed for the next one", () => {
  let server: net.Server | undefined;
  let client: FloClient | undefined;
  afterEach(async () => {
    await client?.close();
    server?.close();
  });

  /** Answer the first request with `status`, then every later one ok. */
  async function connect(status: number) {
    server = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      const ids: bigint[] = [];
      sock.on("data", (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        while (buf.length >= HEADER_SIZE) {
          const total = HEADER_SIZE + buf.readUInt32LE(4);
          if (buf.length < total) return;
          ids.push(buf.readBigUInt64LE(8));
          buf = buf.subarray(total);
        }
        // Both replies back to back in one write, once both requests are in.
        if (ids.length === 2) {
          sock.write(
            Buffer.concat([
              response(ids[0]!, enc.encode(UNAVAILABLE_MSG), status),
              response(ids[1]!, getBody(7n, "v")),
            ])
          );
        }
      });
    });
    await new Promise<void>((r) => server!.listen(0, "127.0.0.1", r));
    const { port } = server.address() as net.AddressInfo;
    client = new FloClient(`127.0.0.1:${port}`, { timeoutMs: 2000 });
    await client.connect();
    return client;
  }

  for (const [status, check] of [
    [12, (e: unknown) => expect(e).toBeInstanceOf(UnavailableError)],
    [200, (e: unknown) => expect((e as ServerError).message).toContain("200")],
  ] as const) {
    it(`over TCP, after status ${status}`, async () => {
      const c = await connect(status);
      const [first, second] = await Promise.allSettled([c.kv.get("a"), c.kv.get("b")]);
      expect(first.status).toBe("rejected");
      const err = (first as PromiseRejectedResult).reason;
      expect(err).toBeInstanceOf(ServerError);
      expect(err.serverMessage).toBe(UNAVAILABLE_MSG);
      check(err);
      expect(second.status).toBe("fulfilled");
      const got = (second as PromiseFulfilledResult<Awaited<ReturnType<FloClient["kv"]["get"]>>>).value;
      expect(got?.version).toBe(7n);
      expect(new TextDecoder().decode(got?.value)).toBe("v");
    });
  }

});

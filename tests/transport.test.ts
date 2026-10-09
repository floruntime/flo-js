/**
 * Transport deadlines for blocking requests, and routing replies by request id.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  HEADER_SIZE,
  MAGIC,
  OpCode,
  OptionTag,
  OptionsBuilder,
  TimeoutError,
  VERSION,
  computeCRC32,
  parseResponseHeader,
  requestBlockMs,
  serializeRequest,
} from "@floruntime/core";
import { TcpTransport } from "@floruntime/node";
import { WebSocketTransport } from "@floruntime/web";

const enc = new TextEncoder();

function request(id: bigint, blockMs?: number): Uint8Array {
  const opts = new OptionsBuilder().addU32(OptionTag.Count, 1);
  if (blockMs !== undefined) opts.addU32(OptionTag.BlockMS, blockMs);
  return serializeRequest(id, OpCode.QueueDequeue, enc.encode("ns"), enc.encode("q"), new Uint8Array(0), opts.build());
}

function response(id: bigint, data: Uint8Array = new Uint8Array(0)): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  buf[21] = 0;
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

const idOf = (resp: Uint8Array) => parseResponseHeader(resp.subarray(0, HEADER_SIZE))[2];
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A server that answers request `id` after `delayMs(id)`. */
async function fakeServer(delayMs: (id: bigint) => number): Promise<{ endpoint: string; close: () => void }> {
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        const id = buf.readBigUInt64LE(8);
        buf = buf.subarray(total);
        setTimeout(() => sock.destroyed || sock.write(response(id)), delayMs(id));
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    close: () => {
      sockets.forEach((s) => s.destroy());
      server.close();
    },
  };
}

describe("requestBlockMs", () => {
  it("reads BlockMS or WaitMS from a serialized request", () => {
    expect(requestBlockMs(request(1n))).toBe(0);
    expect(requestBlockMs(request(1n, 30000))).toBe(30000);
    const watch = serializeRequest(1n, OpCode.Get, enc.encode("ns"), enc.encode("k"), new Uint8Array(0),
      new OptionsBuilder().addU32(OptionTag.WaitMS, 1234).build());
    expect(requestBlockMs(watch)).toBe(1234);
  });
});

describe("TcpTransport", () => {
  let close: (() => void) | undefined;
  let transport: TcpTransport | undefined;
  afterEach(async () => {
    await transport?.close();
    close?.();
  });

  it("waits out a blocking request longer than its timeout", async () => {
    const srv = await fakeServer(() => 250);
    close = srv.close;
    transport = new TcpTransport(srv.endpoint, { timeoutMs: 100 });
    await transport.connect();
    const resp = await transport.sendAndReceive(request(1n, 200));
    expect(idOf(resp)).toBe(1n);
  });

  it("still times out a blocking request the server never answers", async () => {
    const srv = await fakeServer(() => 10_000);
    close = srv.close;
    transport = new TcpTransport(srv.endpoint, { timeoutMs: 50 });
    await transport.connect();
    const started = Date.now();
    await expect(transport.sendAndReceive(request(1n, 100))).rejects.toThrow(TimeoutError);
    expect(Date.now() - started).toBeGreaterThanOrEqual(140);
  });

  it("drops a reply that comes after its request timed out", async () => {
    const srv = await fakeServer((id) => (id === 1n ? 150 : 60));
    close = srv.close;
    transport = new TcpTransport(srv.endpoint, { timeoutMs: 100 });
    await transport.connect();
    await expect(transport.sendAndReceive(request(1n))).rejects.toThrow(TimeoutError);
    await sleep(20);
    // Reply 1 arrives while request 2 is waiting.
    const resp = await transport.sendAndReceive(request(2n));
    expect(idOf(resp)).toBe(2n);
  });

  it("gives each of several requests in flight its own reply", async () => {
    const srv = await fakeServer((id) => (id === 1n ? 150 : 20));
    close = srv.close;
    transport = new TcpTransport(srv.endpoint, { timeoutMs: 1000 });
    await transport.connect();
    const [a, b] = await Promise.all([
      transport.sendAndReceive(request(1n, 500)),
      transport.sendAndReceive(request(2n)),
    ]);
    expect(idOf(a)).toBe(1n);
    expect(idOf(b)).toBe(2n);
  });
});

describe("WebSocketTransport", () => {
  class FakeWebSocket {
    static OPEN = 1;
    static last: FakeWebSocket;
    readyState = 1;
    binaryType = "";
    sent: Uint8Array[] = [];
    onopen?: () => void;
    onmessage?: (ev: { data: ArrayBuffer }) => void;
    onclose?: (ev: unknown) => void;
    onerror?: () => void;
    constructor() {
      FakeWebSocket.last = this;
      setTimeout(() => this.onopen?.(), 0);
    }
    send(data: Uint8Array) {
      this.sent.push(data);
    }
    close() {}
    reply(id: bigint) {
      const r = response(id);
      this.onmessage?.({ data: r.buffer.slice(r.byteOffset, r.byteOffset + r.byteLength) as ArrayBuffer });
    }
  }

  afterEach(() => vi.unstubAllGlobals());

  async function open(timeoutMs: number) {
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const t = new WebSocketTransport("ws://flo.test", { timeoutMs });
    await t.connect();
    return { t, ws: FakeWebSocket.last };
  }

  it("waits out a blocking request longer than its timeout", async () => {
    const { t, ws } = await open(50);
    const p = t.sendAndReceive(request(1n, 200));
    await sleep(120);
    ws.reply(1n);
    expect(idOf(await p)).toBe(1n);
  });

  it("does not hand a timed-out request's late reply to the push handler", async () => {
    const { t, ws } = await open(30);
    const pushes: number[] = [];
    t.onPushMessage((id) => pushes.push(id));
    await expect(t.sendAndReceive(request(7n))).rejects.toThrow(TimeoutError);
    ws.reply(7n);
    expect(pushes).toEqual([]);
  });
});

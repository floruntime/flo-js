/**
 * Workers back off when blocking polls keep coming back empty at once, as
 * the server answers them when its waiter pool is full.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { HEADER_SIZE, MAGIC, OpCode, VERSION, computeCRC32 } from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";

function emptyOk(id: bigint): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  view.setUint32(16, computeCRC32(buf, new Uint8Array(0)), true);
  return buf;
}

/** A server that answers every request at once with an empty OK. */
async function fullPoolServer(): Promise<{ endpoint: string; ops: number[]; close: () => void }> {
  const ops: number[] = [];
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        ops.push(buf.readUInt16LE(20));
        sock.write(emptyOk(buf.readBigUInt64LE(8)));
        buf = buf.subarray(total);
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    ops,
    close: () => {
      sockets.forEach((s) => s.destroy());
      server.close();
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("worker poll loop against immediate empty answers", () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it("ActionWorker backs off", async () => {
    const srv = await fullPoolServer();
    close = srv.close;
    const worker = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, heartbeatIntervalMs: 0 });
    worker.action("a", async () => new Uint8Array(0));
    const done = worker.start();
    await sleep(600);
    worker.stop();
    await done;
    const polls = srv.ops.filter((op) => op === OpCode.ActionAwait).length;
    expect(polls).toBeGreaterThan(1);
    expect(polls).toBeLessThan(20);
  });

  it("StreamWorker backs off", async () => {
    const srv = await fullPoolServer();
    close = srv.close;
    const worker = new StreamWorker(
      srv.endpoint,
      { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0 },
      async () => {}
    );
    const done = worker.start();
    await sleep(600);
    worker.stop();
    await done;
    const polls = srv.ops.filter((op) => op === OpCode.StreamGroupRead).length;
    expect(polls).toBeGreaterThan(1);
    expect(polls).toBeLessThan(20);
  });
});

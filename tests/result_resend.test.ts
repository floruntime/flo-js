/**
 * Worker results: the handler's outcome alone picks complete or fail, ack or
 * nack. A result whose connection drops or times out is sent again, the same
 * request, after a reconnect, up to 3 sends; a refusal is not resent.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import {
  HEADER_SIZE,
  MAGIC,
  OpCode,
  Ran,
  Reason,
  StatusCode,
  TABLE_HASH,
  VERSION,
  computeCRC32,
  type Logger,
} from "@floruntime/core";
import { ActionWorker, StreamWorker } from "@floruntime/node";

function frame(id: bigint, status: number, data: Uint8Array = new Uint8Array(0)): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + data.length);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, data.length, true);
  view.setBigUint64(8, id, true);
  buf[20] = VERSION;
  buf[21] = status;
  view.setBigUint64(24, TABLE_HASH, true);
  buf.set(data, HEADER_SIZE);
  view.setUint32(16, computeCRC32(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

/** A task assignment for action "a", task id "t1". */
function oneTask(): Uint8Array {
  const buf = new Uint8Array(2 + 2 + 2 + 1 + 8 + 4);
  const view = new DataView(buf.buffer);
  view.setUint16(0, 2, true);
  buf.set([0x74, 0x31], 2); // "t1"
  view.setUint16(4, 1, true);
  buf[6] = 0x61; // "a"
  view.setUint32(15, 1, true); // attempt
  return buf;
}

/** A group read answer holding one record, id 0-1. */
function oneRecord(): Uint8Array {
  const buf = new Uint8Array(4 + 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1, true);
  view.setBigUint64(4, 1n, true);
  view.setUint32(26, 1, true); // payload_len
  buf[30] = 0x78;
  return buf;
}

/** A conflict refusal the server ran: not retried by the client. */
const REFUSE = "refuse";

type Action = "drop" | "park" | typeof REFUSE | Uint8Array | undefined;
type Seen = { op: number; conn: number; bytes: Buffer };

/**
 * A server that answers per `script(op, conn)`, conn counting accepted
 * connections from 0: undefined is an empty OK, "drop" destroys the
 * connection, "park" never answers, "refuse" is a refusal.
 */
async function fakeServer(script: (op: number, conn: number) => Action) {
  const seen: Seen[] = [];
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    const conn = sockets.length;
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("error", () => {});
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        const bytes = Buffer.from(buf.subarray(0, total));
        const op = buf.readUInt16LE(20);
        const id = buf.readBigUInt64LE(8);
        buf = buf.subarray(total);
        seen.push({ op, conn, bytes });
        const a = script(op, conn);
        if (a === "drop") return void sock.destroy();
        if (a === "park") continue;
        if (a === REFUSE) {
          const body = new Uint8Array([Reason.Unclassified, 0, Ran.Yes, 0x6e, 0x6f]);
          sock.write(frame(id, StatusCode.Conflict, body));
          continue;
        }
        sock.write(frame(id, StatusCode.OK, a));
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    seen,
    of: (op: number) => seen.filter((s) => s.op === op),
    close: () => {
      server.close();
      sockets.forEach((s) => s.destroy());
    },
  };
}

function recordingLogger() {
  const errors: string[] = [];
  const logger: Logger = { debug: () => {}, warn: () => {}, error: (m) => void errors.push(m) };
  return { logger, errors };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(cond: () => boolean, ms: number): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond() && Date.now() < deadline) await sleep(10);
}

/** The frame with its request id (bytes 8..16) and CRC (16..20) zeroed. */
function masked(b: Buffer): Buffer {
  const c = Buffer.from(b);
  c.fill(0, 8, 20);
  return c;
}

let cleanups: (() => unknown)[] = [];
// In order: the server goes first, so a worker's parked poll ends.
afterEach(async () => {
  for (const c of cleanups) await c();
  cleanups = [];
});

async function serve(script: (op: number, conn: number) => Action) {
  const srv = await fakeServer(script);
  cleanups.push(srv.close);
  return srv;
}

/** A server that hands out one task, on the first await only. */
function oneTaskThen(rest: (op: number, conn: number) => Action) {
  let served = false;
  return (op: number, conn: number): Action => {
    if (op === OpCode.ActionAwait) {
      if (served) return "park";
      served = true;
      return oneTask();
    }
    return rest(op, conn);
  };
}

/** A server that hands out one record, on the first read only. */
function oneRecordThen(rest: (op: number, conn: number) => Action) {
  let served = false;
  return (op: number, conn: number): Action => {
    if (op === OpCode.StreamGroupRead) {
      if (served) return "park";
      served = true;
      return oneRecord();
    }
    return rest(op, conn);
  };
}

function actionWorker(endpoint: string, logger: Logger, handler: () => Promise<Uint8Array> = async () => new Uint8Array([1, 2, 3])) {
  const w = new ActionWorker({ endpoint, blockMs: 30000, heartbeatIntervalMs: 0, logger });
  w.action("a", handler);
  const done = w.start();
  cleanups.push(async () => {
    w.stop();
    await done;
  });
  return { w, done };
}

function streamWorker(endpoint: string, logger: Logger) {
  const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, heartbeatIntervalMs: 0, logger }, async () => {});
  const done = w.start();
  cleanups.push(async () => {
    w.stop();
    await done;
  });
  return { w, done };
}

describe("ActionWorker results", () => {
  it("resends a complete whose connection drops, byte for byte, and never sends a fail", async () => {
    const srv = await serve(oneTaskThen((op, conn) => (op === OpCode.ActionComplete && conn === 0 ? "drop" : undefined)));
    const { logger } = recordingLogger();
    actionWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.ActionComplete).length >= 2, 3000);
    await sleep(100);

    const completes = srv.of(OpCode.ActionComplete);
    expect(completes.map((s) => s.conn)).toEqual([0, 1]);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
    // The same request apart from its id (and so its CRC).
    expect(masked(completes[1]!.bytes).equals(masked(completes[0]!.bytes))).toBe(true);
    expect(completes[1]!.bytes.readBigUInt64LE(8)).not.toBe(completes[0]!.bytes.readBigUInt64LE(8));
  });

  it("resends a complete that times out instead of failing the task", async () => {
    // Connection 0 never answers the complete; the client gives up after 5 s.
    const srv = await serve(oneTaskThen((op, conn) => (op === OpCode.ActionComplete && conn === 0 ? "park" : undefined)));
    const { logger } = recordingLogger();
    actionWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.ActionComplete).length >= 2, 9000);
    await sleep(100);
    expect(srv.of(OpCode.ActionComplete).map((s) => s.conn)).toEqual([0, 1]);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
  }, 15000);

  it("doesn't resend a refused complete, nor turn it into a fail", async () => {
    const srv = await serve(oneTaskThen((op) => (op === OpCode.ActionComplete ? REFUSE : undefined)));
    const { logger, errors } = recordingLogger();
    actionWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.ActionComplete).length >= 1, 3000);
    await sleep(300);
    expect(srv.of(OpCode.ActionComplete)).toHaveLength(1);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
    expect(errors.some((m) => m.includes("complete for task t1"))).toBe(true);
  });

  it("sends a complete at most 3 times, then drops it with an error naming the task", async () => {
    const srv = await serve(oneTaskThen((op) => (op === OpCode.ActionComplete ? "drop" : undefined)));
    const { logger, errors } = recordingLogger();
    actionWorker(srv.endpoint, logger);
    await until(() => errors.some((m) => m.includes("task t1")), 5000);
    await sleep(300);
    expect(srv.of(OpCode.ActionComplete)).toHaveLength(3);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
    expect(errors.some((m) => m.includes("complete for task t1") && m.includes("after 3 sends"))).toBe(true);
  });

  it("sends the complete of a handler still running at stop()", async () => {
    const srv = await serve(oneTaskThen(() => undefined));
    const { logger } = recordingLogger();
    let release!: () => void;
    const running = new Promise<void>((r) => (release = r));
    let started = false;
    const { w, done } = actionWorker(srv.endpoint, logger, async () => {
      started = true;
      await running;
      return new Uint8Array(0);
    });
    await until(() => started, 3000);
    w.stop();
    await sleep(50);
    release();
    await until(() => srv.of(OpCode.ActionComplete).length >= 1, 3000);
    srv.close(); // ends the parked poll
    await done;
    expect(srv.of(OpCode.ActionComplete)).toHaveLength(1);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
  });
});

describe("StreamWorker results", () => {
  it("resends an ack whose connection drops, byte for byte, and never sends a nack", async () => {
    const srv = await serve(oneRecordThen((op, conn) => (op === OpCode.StreamGroupAck && conn === 0 ? "drop" : undefined)));
    const { logger } = recordingLogger();
    streamWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.StreamGroupAck).length >= 2, 3000);
    await sleep(100);

    const acks = srv.of(OpCode.StreamGroupAck);
    expect(acks.map((s) => s.conn)).toEqual([0, 1]);
    expect(srv.of(OpCode.StreamGroupNack)).toHaveLength(0);
    expect(masked(acks[1]!.bytes).equals(masked(acks[0]!.bytes))).toBe(true);
  });

  it("doesn't resend a refused ack, nor turn it into a nack", async () => {
    const srv = await serve(oneRecordThen((op) => (op === OpCode.StreamGroupAck ? REFUSE : undefined)));
    const { logger, errors } = recordingLogger();
    streamWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.StreamGroupAck).length >= 1, 3000);
    await sleep(300);
    expect(srv.of(OpCode.StreamGroupAck)).toHaveLength(1);
    expect(srv.of(OpCode.StreamGroupNack)).toHaveLength(0);
    expect(errors.some((m) => m.includes("ack for record 0-1"))).toBe(true);
  });
});

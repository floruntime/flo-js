/**
 * Worker results: the handler's outcome alone picks complete or fail, ack or
 * nack. A result whose connection drops or times out is sent again, the same
 * request, after a reconnect, up to 3 sends; a refusal is not resent.
 */

import * as net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  HEADER_SIZE,
  MAGIC,
  OpCode,
  Ran,
  Reason,
  StatusCode,
  TABLE_HASH,
  VERSION,
  computeCRC32C,
  type Logger,
} from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";

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
  view.setUint32(16, computeCRC32C(buf.subarray(0, HEADER_SIZE), buf.subarray(HEADER_SIZE)), true);
  return buf;
}

/** A task assignment for action "a", task id "t1", invoked by no workflow. */
function oneTask(): Uint8Array {
  const buf = new Uint8Array(2 + 2 + 2 + 1 + 8 + 4 + 1);
  const view = new DataView(buf.buffer);
  view.setUint16(0, 2, true);
  buf.set([0x74, 0x31], 2); // "t1"
  view.setUint16(4, 1, true);
  buf[6] = 0x61; // "a"
  view.setUint32(15, 1, true); // attempt
  buf[19] = 0; // has_caller
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

/** An overloaded refusal the server didn't run, then the connection drops. */
const BUSY_THEN_DROP = "busy-then-drop";
/** An overloaded refusal the server didn't run; the connection stays up. */
const OVERLOADED = "overloaded";
/** An OK answer whose CRC is wrong. */
const BAD_CRC = "bad-crc";
/** An answer whose header has the wrong magic. */
const BAD_MAGIC = "bad-magic";

type Action =
  | "drop"
  | "park"
  | typeof REFUSE
  | typeof BUSY_THEN_DROP
  | typeof OVERLOADED
  | typeof BAD_CRC
  | typeof BAD_MAGIC
  | Uint8Array
  | undefined;
type Seen = { op: number; conn: number; bytes: Buffer };

/**
 * A server that answers per `script(op, conn)`, conn counting accepted
 * connections from 0: undefined is an empty OK, "drop" destroys the
 * connection, "park" never answers; the named constants above say the rest.
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
        if (a === OVERLOADED) {
          const body = new Uint8Array([Reason.Unclassified, 0, Ran.No, 0x6e, 0x6f]);
          sock.write(frame(id, StatusCode.Overloaded, body));
          continue;
        }
        if (a === BUSY_THEN_DROP) {
          const body = new Uint8Array([Reason.Unclassified, 0, Ran.No, 0x6e, 0x6f]);
          sock.write(frame(id, StatusCode.Overloaded, body));
          return void sock.end(() => sock.destroy());
        }
        if (a === BAD_CRC || a === BAD_MAGIC) {
          const f = frame(id, StatusCode.OK);
          if (a === BAD_CRC) f[16] = f[16]! ^ 0xff;
          else f[0] = f[0]! ^ 0xff;
          sock.write(f);
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
    connections: () => sockets.length,
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

/** The retry byte of an ActionFail request frame. */
function failRetryByte(b: Buffer): number {
  let o = HEADER_SIZE;
  o += 2 + b.readUInt16LE(o); // namespace
  o += 2 + b.readUInt16LE(o); // key (worker id)
  o += 4; // value length
  o += 2 + b.readUInt16LE(o); // action name
  o += 2 + b.readUInt16LE(o); // task id
  return b[o]!;
}

/** A handler that runs until release() is called. */
function heldHandler() {
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  const state = { started: false, release: () => release() };
  const handler = async () => {
    state.started = true;
    await held;
    return new Uint8Array(0);
  };
  return { state, handler };
}

function actionWorker(
  endpoint: string,
  logger: Logger,
  handler: () => Promise<Uint8Array> = async () => new Uint8Array([1, 2, 3]),
  actionTimeoutMs?: number,
) {
  const w = new ActionWorker({ endpoint, blockMs: 30000, logger, actionTimeoutMs });
  w.action("a", handler);
  const done = w.start();
  cleanups.push(async () => {
    w.stop();
    await done;
  });
  return { w, done };
}

function streamWorker(endpoint: string, logger: Logger) {
  const w = new StreamWorker(endpoint, { stream: "s", blockMs: 30000, logger }, async () => {});
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

  it("fails a task whose handler runs past actionTimeoutMs, without retry, and sends no complete", async () => {
    const srv = await serve(oneTaskThen(() => undefined));
    const { logger } = recordingLogger();
    actionWorker(srv.endpoint, logger, () => new Promise<Uint8Array>(() => {}), 100);
    await until(() => srv.of(OpCode.ActionFail).length >= 1, 3000);
    await sleep(100);
    const fails = srv.of(OpCode.ActionFail);
    expect(fails).toHaveLength(1);
    expect(failRetryByte(fails[0]!.bytes)).toBe(0);
    expect(srv.of(OpCode.ActionComplete)).toHaveLength(0);
  });

  it.each([
    ["fails its checksum", BAD_CRC],
    ["has a bad header", BAD_MAGIC],
  ] as const)("resends a complete whose answer %s, and sends no fail", async (_name, bad) => {
    let answered = false;
    const srv = await serve(
      oneTaskThen((op) => {
        if (op !== OpCode.ActionComplete || answered) return undefined;
        answered = true;
        return bad;
      }),
    );
    const { logger } = recordingLogger();
    actionWorker(srv.endpoint, logger);
    await until(() => srv.of(OpCode.ActionComplete).length >= 2, 3000);
    await sleep(100);
    expect(srv.of(OpCode.ActionComplete)).toHaveLength(2);
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
  });

  it("resends, within the grace after stop(), a complete whose connection drops", async () => {
    const srv = await serve(oneTaskThen((op, conn) => (op === OpCode.ActionComplete && conn === 0 ? "drop" : undefined)));
    const { logger } = recordingLogger();
    const { state, handler } = heldHandler();
    const { w, done } = actionWorker(srv.endpoint, logger, handler);
    await until(() => state.started, 3000);
    const stopped = Date.now();
    w.stop();
    state.release();
    await until(() => srv.of(OpCode.ActionComplete).length >= 2, 3000);
    expect(Date.now() - stopped).toBeLessThan(2000);
    expect(srv.of(OpCode.ActionComplete).map((s) => s.conn)).toEqual([0, 1]);
    srv.close(); // ends the parked poll
    await done;
    expect(srv.of(OpCode.ActionFail)).toHaveLength(0);
  });

  it("drops the complete about 2 s after stop() when the server is gone, and start() returns", async () => {
    const srv = await serve(oneTaskThen(() => undefined));
    const { logger, errors } = recordingLogger();
    const { state, handler } = heldHandler();
    const { w, done } = actionWorker(srv.endpoint, logger, handler);
    await until(() => state.started, 3000);
    const stopped = Date.now();
    w.stop();
    srv.close();
    await sleep(50);
    state.release();
    await done;
    const took = Date.now() - stopped;
    expect(took).toBeGreaterThanOrEqual(1900);
    expect(took).toBeLessThan(3500);
    expect(errors.some((m) => m.includes("complete for task t1"))).toBe(true);
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

describe("StreamWorkers sharing a client", () => {
  afterEach(() => vi.restoreAllMocks());

  it("resend an ack without redialing a connection the other worker already replaced", async () => {
    // The first ack is refused as busy and connection 0 drops; the client
    // retries it 300 ms later, after a poller redialed, on connection 1, where
    // it goes unanswered and times out. Its resend then needs no redial.
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    let acks = 0;
    const srv = await serve(
      oneRecordThen((op) => {
        if (op !== OpCode.StreamGroupAck) return undefined;
        acks++;
        if (acks === 1) return BUSY_THEN_DROP;
        if (acks === 2) return "park";
        return undefined;
      }),
    );
    const client = new FloClient(srv.endpoint, {
      timeoutMs: 300,
      retry: { maxAttempts: 2, initialBackoffMs: 300, maxBackoffMs: 300 },
    });
    await client.connect();
    cleanups.push(() => client.close());
    const { logger } = recordingLogger();
    const workers = ["a", "b"].map(
      (stream) => new StreamWorker(client, { stream, blockMs: 30000, logger }, async () => {}),
    );
    const done = workers.map((w) => w.start());
    cleanups.push(async () => {
      workers.forEach((w) => w.stop());
      await client.close();
      await Promise.all(done);
    });
    await until(() => srv.of(OpCode.StreamGroupAck).length >= 3, 5000);
    await sleep(200);
    expect(srv.of(OpCode.StreamGroupAck).map((s) => s.conn)).toEqual([0, 1, 1]);
    expect(srv.of(OpCode.StreamGroupNack)).toHaveLength(0);
    expect(srv.connections()).toBe(2);
  });
});

describe("ActionWorker registration and auto-touch", () => {
  function worker(endpoint: string, autoTouch: boolean, timeoutMs: number, handlerMs: number) {
    const w = new ActionWorker({
      endpoint,
      blockMs: 30000,
      logger: recordingLogger().logger,
      autoTouch,
      retry: { maxAttempts: 1 },
    });
    w.action("a", async () => {
      await sleep(handlerMs);
      return new Uint8Array(0);
    }, { timeoutMs, owner: "payments", namespace: "elsewhere" });
    const done = w.start();
    cleanups.push(async () => {
      w.stop();
      await done;
    });
    return w;
  }

  it("registers the action with the options it was given", async () => {
    const srv = await serve(oneTaskThen(() => undefined));
    worker(srv.endpoint, false, 600000, 0);
    await until(() => srv.of(OpCode.ActionComplete).length > 0, 5000);
    const want = Buffer.alloc(11 + 8);
    want.writeUInt8(0, 0);
    want.writeUInt32LE(600000, 1);
    want.writeUInt32LE(3, 5);
    want.writeUInt16LE(8, 9);
    want.write("payments", 11);
    const [reg] = srv.of(OpCode.ActionRegister);
    expect(reg!.bytes.includes(want)).toBe(true);
    // The worker's namespace, not the options': "default".
    expect(reg!.bytes.includes(Buffer.from("elsewhere"))).toBe(false);
  });

  it("touches a running task every third of its timeout, and stops with the handler", async () => {
    const srv = await serve(oneTaskThen(() => undefined));
    worker(srv.endpoint, true, 300, 450);
    await until(() => srv.of(OpCode.ActionComplete).length > 0, 5000);
    await sleep(300);
    const touches = srv.of(OpCode.ActionTouch);
    expect(touches.length).toBeGreaterThanOrEqual(2);
    expect(touches.length).toBeLessThanOrEqual(5);
    const completeAt = srv.seen.indexOf(srv.of(OpCode.ActionComplete)[0]!);
    for (const t of touches) expect(srv.seen.indexOf(t)).toBeLessThan(completeAt);
    // Each asks for this worker's timeout: "t1" then u32 300.
    const extend = Buffer.alloc(4);
    extend.writeUInt32LE(300);
    for (const t of touches) expect(t.bytes.includes(Buffer.concat([Buffer.from("t1"), extend]))).toBe(true);
  });

  it("keeps touching after a refusal that isn't final, such as overloaded", async () => {
    const srv = await serve(oneTaskThen((op) => (op === OpCode.ActionTouch ? OVERLOADED : undefined)));
    worker(srv.endpoint, true, 300, 450);
    await until(() => srv.of(OpCode.ActionComplete).length > 0, 5000);
    expect(srv.of(OpCode.ActionTouch).length).toBeGreaterThanOrEqual(2);
  });

  it("sends the complete while a touch is still in flight, on the same connection", async () => {
    const srv = await serve(oneTaskThen((op) => (op === OpCode.ActionTouch ? "park" : undefined)));
    worker(srv.endpoint, true, 300, 150);
    await until(() => srv.of(OpCode.ActionComplete).length > 0, 5000);
    expect(srv.of(OpCode.ActionTouch)).toHaveLength(1);
    expect(srv.connections()).toBe(1);
  });

  it("refuses options that don't fit a u32", () => {
    const w = new ActionWorker({ endpoint: "127.0.0.1:1", logger: false });
    expect(() => w.action("a", async () => new Uint8Array(0), { timeoutMs: -1 })).toThrow(RangeError);
    expect(() => w.action("b", async () => new Uint8Array(0), { maxRetries: 2 ** 32 })).toThrow(RangeError);
    expect(() => w.action("c", async () => new Uint8Array(0), { timeoutMs: 1.5 })).toThrow(RangeError);
  });

  it("touches nothing without autoTouch, or when the action never times out", async () => {
    const off = await serve(oneTaskThen(() => undefined));
    worker(off.endpoint, false, 300, 450);
    const never = await serve(oneTaskThen(() => undefined));
    worker(never.endpoint, true, 0, 300);
    await until(() => off.of(OpCode.ActionComplete).length > 0 && never.of(OpCode.ActionComplete).length > 0, 5000);
    await sleep(200);
    expect(off.of(OpCode.ActionTouch)).toHaveLength(0);
    expect(never.of(OpCode.ActionTouch)).toHaveLength(0);
  });

  it("stops touching once a touch is refused as a conflict", async () => {
    const srv = await serve(oneTaskThen((op) => (op === OpCode.ActionTouch ? REFUSE : undefined)));
    worker(srv.endpoint, true, 300, 450);
    await until(() => srv.of(OpCode.ActionComplete).length > 0, 5000);
    await sleep(200);
    expect(srv.of(OpCode.ActionTouch)).toHaveLength(1);
  });
});

describe("serializeActionRegisterValue", () => {
  it("refuses a timeout or attempt count outside u32, and an owner longer than 65535 bytes", async () => {
    const { serializeActionRegisterValue } = await import("@floruntime/core");
    expect(() => serializeActionRegisterValue(0, -1, 3)).toThrow(RangeError);
    expect(() => serializeActionRegisterValue(0, 30000, 2 ** 32)).toThrow(RangeError);
    expect(() => serializeActionRegisterValue(0, 30000, 3, "x".repeat(65536))).toThrow(RangeError);
    expect(serializeActionRegisterValue(0, 30000, 3, "x".repeat(65535)).length).toBe(11 + 65535);
  });
});

describe("stream.append", () => {
  it("sends the record batch-framed, as the server reads an append", async () => {
    // An append's answer: [sequence:u64][timestamp_ms:i64].
    const srv = await serve((op) => (op === OpCode.StreamAppend ? new Uint8Array(16) : undefined));
    const client = new FloClient(srv.endpoint);
    await client.connect();
    cleanups.push(() => client.close());
    await client.stream.append("e", new TextEncoder().encode("hi"), { headers: { k: "v" } });
    const [frame] = srv.of(OpCode.StreamAppend);
    // [count=1][len=2]["hi"][headers=1][1]["k"][1]["v"], whole at the end of the value.
    const batch = Buffer.from([1, 0, 0, 0, 2, 0, 0, 0, 0x68, 0x69, 1, 0, 1, 0, 0x6b, 1, 0, 0x76]);
    expect(frame!.bytes.includes(batch)).toBe(true);
  });
});

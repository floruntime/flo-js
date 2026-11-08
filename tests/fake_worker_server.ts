/**
 * A fake server for worker tests, answering each request as a script says,
 * and the bodies it answers with.
 */

import * as net from "node:net";
import { HEADER_SIZE, MAGIC, OpCode, Ran, Reason, StatusCode, TABLE_HASH, VERSION, computeCRC32C } from "@floruntime/core";

export function frame(id: bigint, status: number, data: Uint8Array): Uint8Array {
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

export function refusal(status: number, reason: Reason, message: string): Answer {
  return { status, data: new Uint8Array([reason & 0xff, reason >> 8, Ran.No, ...new TextEncoder().encode(message)]) };
}

export const notFound = () => refusal(StatusCode.NotFound, Reason.NotFound, "worker not found");
export const unhealthy = () => refusal(StatusCode.BadRequest, Reason.WorkerUnhealthy, "worker missed its heartbeats; send one, or register again");
export const draining = () => refusal(StatusCode.BadRequest, Reason.WorkerDraining, "worker is draining");

export interface FakeTask {
  run?: string;
  namespace?: string;
  deliveries?: number;
  term?: bigint;
  index?: bigint;
  action?: string;
  createdAtMs?: bigint;
  attempt?: number;
  leaseMs?: bigint;
  callerRun?: string;
  callerWorkflow?: string;
  input?: Uint8Array;
}

/**
 * action.await's answer holding one task: [present:u8=1][run:u16][namespace:u8]
 * [deliveries:u32][term:u64][index:u64][action:u16][created_at_ms:i64][attempt:u32]
 * [lease_ms:u64][caller_run?:u16][caller_workflow?:u16][input:u32].
 */
export function taskAnswer(t: FakeTask = {}): Uint8Array {
  const enc = new TextEncoder();
  const out: number[] = [1];
  const u = (v: bigint, n: number) => {
    for (let i = 0; i < n; i++) out.push(Number((BigInt.asUintN(64, v) >> BigInt(8 * i)) & 0xffn));
  };
  const str = (s: string, n: number) => {
    const b = enc.encode(s);
    u(BigInt(b.length), n);
    out.push(...b);
  };
  const opt = (s: string | undefined) => {
    if (s === undefined) return out.push(0);
    out.push(1);
    str(s, 2);
  };
  str(t.run ?? "t1", 2);
  str(t.namespace ?? "default", 1);
  u(BigInt(t.deliveries ?? 1), 4);
  u(t.term ?? 1n, 8);
  u(t.index ?? 10n, 8);
  str(t.action ?? "a", 2);
  u(t.createdAtMs ?? 0n, 8);
  u(BigInt(t.attempt ?? 1), 4);
  u(t.leaseMs ?? 30000n, 8);
  opt(t.callerRun);
  opt(t.callerWorkflow);
  const input = t.input ?? new Uint8Array(0);
  u(BigInt(input.length), 4);
  out.push(...input);
  return Uint8Array.from(out);
}

/** A task of action "a", invoked by no workflow. */
export function oneTask(): Uint8Array {
  return taskAnswer();
}

/** action.await's answer with no task. */
export const noTask = () => new Uint8Array([0]);

/** action.touch's answer: the lease's expiry. */
export const touchAnswer = (expiresAtMs = 0n) => {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigInt64(0, expiresAtMs, true);
  return b;
};

export type Answer = { status?: number; data?: Uint8Array; delayMs?: number } | "park" | "drop" | undefined;

interface Seen {
  op: number;
  /** The request's payload after the header: namespace, key, value, options. */
  payload: Buffer;
  /** When it arrived, performance.now(). */
  at: number;
}

/** The current_load a heartbeat carries: its value's first u32. */
export function loadOf(payload: Buffer): number {
  let off = 2 + payload.readUInt16LE(0); // namespace
  off += 2 + payload.readUInt16LE(off); // key
  return payload.readUInt32LE(off + 4);
}

/** The answer an OK with no data stands for: no task for an await, an expiry for a touch, else empty. */
export function okAnswer(op: number): Uint8Array {
  if (op === OpCode.ActionAwait) return noTask();
  if (op === OpCode.ActionTouch) return touchAnswer();
  return new Uint8Array(0);
}

/** A server that answers per `script(op, nth)`: undefined is an OK as okAnswer has it, "park" never answers, "drop" closes the connection. */
export async function fakeServer(script: (op: number, nth: number) => Answer) {
  const seen: Seen[] = [];
  const counts = new Map<number, number>();
  const sockets: net.Socket[] = [];
  const server = net.createServer((sock) => {
    sockets.push(sock);
    let buf = Buffer.alloc(0);
    sock.on("error", () => {});
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= HEADER_SIZE) {
        const total = HEADER_SIZE + buf.readUInt32LE(4);
        if (buf.length < total) return;
        const op = buf.readUInt16LE(20);
        const id = buf.readBigUInt64LE(8);
        seen.push({ op, payload: Buffer.from(buf.subarray(HEADER_SIZE, total)), at: performance.now() });
        buf = buf.subarray(total);
        const nth = counts.get(op) ?? 0;
        counts.set(op, nth + 1);
        const a = script(op, nth);
        if (a === "park") continue;
        if (a === "drop") {
          sock.destroy();
          return;
        }
        const status = a?.status ?? StatusCode.OK;
        const out = frame(id, status, a?.data ?? (status === StatusCode.OK ? okAnswer(op) : new Uint8Array(0)));
        if (a?.delayMs) setTimeout(() => sock.destroyed || sock.write(out), a.delayMs);
        else sock.write(out);
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as net.AddressInfo;
  return {
    endpoint: `127.0.0.1:${port}`,
    seen,
    ops: () => seen.map((s) => s.op),
    close: () => {
      server.close();
      sockets.forEach((s) => s.destroy());
    },
  };
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function until(cond: () => boolean, ms: number): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond() && Date.now() < deadline) await sleep(10);
}

export const quiet = { debug: () => {}, warn: () => {}, error: () => {} };

/** A group read answer holding one record. */
export function oneRecord(): Uint8Array {
  const buf = new Uint8Array(4 + 8 + 8 + 1 + 4 + 1 + 4 + 1 + 4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1, true);
  view.setBigUint64(4, 1n, true);
  view.setUint32(26, 1, true); // payload_len
  buf[30] = 0x78;
  return buf;
}

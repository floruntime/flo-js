/**
 * Time-series operations: write a point, read a measurement field's points a
 * page at a time, and run a FloQL query.
 *
 * Every time on the wire is i64 Unix nanoseconds, a bigint here. `Date`
 * holds only milliseconds, so `nsFromDate` and `dateFromNs` are helpers, not
 * the way in or out.
 */

import { createReadError, createServerError, FloError, IncompleteResponseError } from "./errors.js";
import { AnswerReader, BodyWriter } from "./kv_body.js";
import { OpCode, OptionTag, type RawResponse, StatusCode } from "./types.js";
import { OptionsBuilder } from "./wire.js";

/** The most points one ts.read page holds. */
export const MAX_TS_READ = 4096;
/** A ts.read tag filter's longest text, in bytes. */
export const MAX_TS_TAG_FILTER = 1024;
/** A ts.read cursor's length. */
export const TS_CURSOR_LEN = 16;

const I64_MAX = (1n << 63n) - 1n;
const I64_MIN = -(1n << 63n);

/** Which way a ts.read goes through the points. */
export enum TSOrder {
  OldestFirst = 0,
  NewestFirst = 1,
}

/** One point: its time in Unix ns, and its value. */
export interface TSPoint {
  timeNs: bigint;
  value: number;
}

export interface TSWriteOptions {
  /** The field written; "value" by default. */
  field?: string;
  /** The point's tags, `key=value` pairs separated by commas. */
  tags?: string;
  /**
   * The point's time in Unix ns; the server's time when unset or 0. The
   * server refuses a negative one (bad request).
   */
  timeNs?: bigint;
  namespace?: string;
}

export interface TSWriteResult {
  seriesHash: bigint;
  /** The time the point was written at, in Unix ns: the server's own when the write named none. */
  timeNs: bigint;
  sequence: bigint;
}

export interface TSReadOptions {
  /** The field read; "value" by default. */
  field?: string;
  /** Only series whose tags include every `key=value` pair of this comma-separated list. */
  tags?: string;
  /** The earliest time, inclusive, in Unix ns. */
  fromNs?: bigint;
  /** The latest time, inclusive, in Unix ns. */
  toNs?: bigint;
  /** Most points on a page, 1 to 4096; 4096 by default. */
  limit?: number;
  /** OldestFirst by default. */
  order?: TSOrder;
  /** The cursor the previous page answered; unset for the first page. */
  cursor?: Uint8Array;
  namespace?: string;
}

export interface TSReadPage {
  /** Every matching series' points, merged in time order (then write order), or newest first. */
  points: TSPoint[];
  /** Where the next page starts; null on the last page. Opaque. */
  cursor: Uint8Array | null;
  /** The page was served by a node that may be behind: points written lately may be missing from it. */
  stale: boolean;
}

/** One FloQL point. FloQL answers in Unix ms. */
export interface TSFloQLPoint {
  timeMs: bigint;
  value: number;
}

export interface TSFloQLSeries {
  key: string;
  field: string;
  points: TSFloQLPoint[];
}

/** `date` in Unix ns. */
export function nsFromDate(date: Date): bigint {
  return BigInt(date.getTime()) * 1_000_000n;
}

/** Unix ns `ns` as a Date, to the millisecond below it. */
export function dateFromNs(ns: bigint): Date {
  const ms = ns / 1_000_000n - (ns < 0n && ns % 1_000_000n !== 0n ? 1n : 0n);
  return new Date(Number(ms));
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

function checkTime(name: string, v: bigint | undefined, min: bigint): void {
  if (v !== undefined && (typeof v !== "bigint" || v < min || v > I64_MAX)) {
    throw new FloError(`flo: ${name} must be a bigint of ns from ${min} to ${I64_MAX}, got ${v}`);
  }
}

/** ts.read's request: field, tags, from_ns, to_ns, limit, order, cursor. */
export function encodeTSReadBody(opts?: TSReadOptions): Uint8Array {
  const field = textEncoder.encode(opts?.field ?? "value");
  if (field.length > 0xffff) throw new FloError(`flo: field is ${field.length} bytes, at most 65535`);
  const tags = textEncoder.encode(opts?.tags ?? "");
  if (tags.length > MAX_TS_TAG_FILTER) throw new FloError(`flo: tag filter is ${tags.length} bytes, at most ${MAX_TS_TAG_FILTER}`);
  checkTime("fromNs", opts?.fromNs, 0n);
  checkTime("toNs", opts?.toNs, 0n);
  const limit = opts?.limit ?? MAX_TS_READ;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_TS_READ) {
    throw new FloError(`flo: limit must be a whole number from 1 to ${MAX_TS_READ}, got ${limit}`);
  }
  const order = opts?.order ?? TSOrder.OldestFirst;
  if (order !== TSOrder.OldestFirst && order !== TSOrder.NewestFirst) throw new FloError(`flo: unknown order ${order}`);
  const cursor = opts?.cursor ?? new Uint8Array(0);
  if (cursor.length > TS_CURSOR_LEN) throw new FloError(`flo: cursor is ${cursor.length} bytes, at most ${TS_CURSOR_LEN}`);

  const w = new BodyWriter();
  w.u8(1);
  w.bytes16(field);
  w.u8(1);
  w.bytes16(tags);
  for (const t of [opts?.fromNs, opts?.toNs]) {
    if (t === undefined) {
      w.u8(0);
    } else {
      w.u8(1);
      w.i64(t);
    }
  }
  w.u8(1);
  w.u32(limit);
  w.u8(1);
  w.u8(order);
  w.u8(1);
  w.bytes8(cursor);
  return w.build();
}

/** ts.read's answer: points, then the next page's cursor (null on the last page). */
export function decodeTSReadAnswer(data: Uint8Array, stale = false): TSReadPage {
  const r = new AnswerReader(data);
  const points: TSPoint[] = [];
  for (let n = r.u16(); n > 0; n--) points.push({ timeNs: r.i64(), value: r.f64() });
  const cursor = r.present() ? r.bytes8() : null;
  r.end();
  return { points, cursor, stale };
}

/** ts.write's answer: [series_hash:u64][time_ns:i64][sequence:u64]. */
export function decodeTSWriteAnswer(data: Uint8Array): TSWriteResult {
  const r = new AnswerReader(data);
  const seriesHash = r.u64();
  const timeNs = r.i64();
  const sequence = r.u64();
  r.end();
  return { seriesHash, timeNs, sequence };
}

/** FloQL's answer: [series:u32] then each [key_len:u32][key][field_len:u32][field][count:u32] and its [time_ms:i64][value:f64] points. */
export function decodeFloQLAnswer(data: Uint8Array): TSFloQLSeries[] {
  const r = new AnswerReader(data);
  const out: TSFloQLSeries[] = [];
  for (let s = r.u32(); s > 0; s--) {
    const key = utf8(r.bytes32());
    const field = utf8(r.bytes32());
    const points: TSFloQLPoint[] = [];
    for (let n = r.u32(); n > 0; n--) points.push({ timeMs: r.i64(), value: r.f64() });
    out.push({ key, field, points });
  }
  r.end();
  return out;
}

function utf8(b: Uint8Array): string {
  try {
    return textDecoder.decode(b);
  } catch {
    throw new IncompleteResponseError("malformed UTF-8 string");
  }
}

/** The client a TSOperations sends through. */
export interface TSRequestSender {
  sendRequest(opCode: OpCode, namespace: string, key: Uint8Array, value: Uint8Array, options: Uint8Array): Promise<RawResponse>;
  getNamespace(override?: string): string;
}

export class TSOperations {
  constructor(private readonly sender: TSRequestSender) {}

  /** Write one point of `measurement`'s field. */
  async write(measurement: string, value: number, opts?: TSWriteOptions): Promise<TSWriteResult> {
    checkTime("timeNs", opts?.timeNs, I64_MIN);
    const options = new OptionsBuilder();
    if (opts?.field !== undefined) options.addString(OptionTag.TSField, opts.field);
    if (opts?.tags !== undefined && opts.tags !== "") options.addString(OptionTag.TSTags, opts.tags);
    if (opts?.timeNs !== undefined) options.addI64(OptionTag.TSTimestampNS, opts.timeNs);
    const body = new Uint8Array(8);
    new DataView(body.buffer).setFloat64(0, value, true);
    const resp = await this.sender.sendRequest(OpCode.TSWrite, this.sender.getNamespace(opts?.namespace), textEncoder.encode(measurement), body, options.build());
    if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
    return decodeTSWriteAnswer(resp.data);
  }

  /**
   * One page of `measurement`'s points. A page goes on strictly after the
   * cursor it is given (strictly before it, newest first), so paging neither
   * repeats nor skips a point that was there for the whole read. Page until
   * the cursor is null: a short page is not the last one. It throws
   * NotFoundError whenever no series of the field matches the tag filter,
   * not only when the measurement was never written: a tag filter that
   * matches nothing is not found, not empty. A series with no points in the
   * range answers an empty page. A StaleNotFoundError (a NotFoundError)
   * comes from a node that may be behind: the series may exist but this
   * node hasn't seen it yet, so read again rather than treating it as gone.
   */
  async read(measurement: string, opts?: TSReadOptions): Promise<TSReadPage> {
    const resp = await this.sender.sendRequest(OpCode.TSRead, this.sender.getNamespace(opts?.namespace), textEncoder.encode(measurement), encodeTSReadBody(opts), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) throw createReadError(resp);
    return decodeTSReadAnswer(resp.data, resp.stale === true);
  }

  /**
   * Every point `read` would page through, from `opts.cursor` on, one page
   * of `opts.limit` at a time. A refusal of any page, a not-found included,
   * is thrown from the iteration rather than ending it: a consumer that may
   * start before its series exists catches NotFoundError and starts again.
   * A stale not-found is thrown as StaleNotFoundError, as `read` throws it.
   * It yields points, not pages, so a page's `stale` isn't carried.
   */
  async *readAll(measurement: string, opts?: TSReadOptions): AsyncGenerator<TSPoint, void, undefined> {
    let cursor = opts?.cursor;
    for (;;) {
      const page = await this.read(measurement, { ...opts, cursor });
      yield* page.points;
      if (page.cursor === null) return;
      cursor = page.cursor;
    }
  }

  /**
   * Run a FloQL query. Its answer's times are Unix ms. An answer of more
   * than 4096 raw points is refused bad request: aggregate them, narrow the
   * query, or page through them with `read`.
   */
  async floql(query: string, opts?: { namespace?: string }): Promise<TSFloQLSeries[]> {
    const resp = await this.sender.sendRequest(OpCode.TSFloQL, this.sender.getNamespace(opts?.namespace), new Uint8Array(0), textEncoder.encode(query), new Uint8Array(0));
    if (resp.status !== StatusCode.OK) throw createServerError(resp.status, resp.data);
    return decodeFloQLAnswer(resp.data);
  }
}

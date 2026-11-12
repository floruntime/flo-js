/**
 * Status limit: the node is at a limit an operator sets. Never run, and
 * terminal: the client never retries it. start() fails with the limit
 * error; a limit hit while running is logged and backed off, warning once
 * per distinct reason. And a transaction's key
 * that routes elsewhere is refused keys_not_colocated, not run, not retried.
 */

import { afterEach, describe, expect, it } from "vitest";
import {
  BadRequestError,
  LimitError,
  OpCode,
  Ran,
  Reason,
  StatusCode,
  isLimit,
  isRefusedUnrun,
  reasonName,
} from "@floruntime/core";
import { ActionWorker, FloClient, StreamWorker } from "@floruntime/node";
import { fakeServer, quiet, refusal, sleep, until } from "./fake_worker_server.js";

let close: (() => void) | undefined;
afterEach(() => close?.());

const LIMIT_TEXT = "limit: this node holds 10000 actions, as many as it registers; delete one first";

describe("a limit refusal", () => {
  it("decodes every limit reason by name", () => {
    expect(StatusCode.Limit).toBe(15);
    expect([Reason.MemoryBudget, Reason.TooManyActions, Reason.TooManyWorkflowDefinitions, Reason.TooManyWorkflowRuns]).toEqual([32, 33, 34, 35]);
    expect([32, 33, 34, 35].map((r) => reasonName(r as Reason))).toEqual(["memory_budget", "too_many_actions", "too_many_workflow_definitions", "too_many_workflow_runs"]);
  });

  it("is a LimitError carrying the reason and the server's message, sent once though retries are on", async () => {
    const srv = await fakeServer((op) => (op === OpCode.ActionRegister ? refusal(StatusCode.Limit, Reason.TooManyActions, LIMIT_TEXT) : undefined));
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 5, initialBackoffMs: 1, maxBackoffMs: 2 } });
    await client.connect();
    try {
      const err = await client.action.register("a").catch((e: unknown) => e);
      expect(err).toBeInstanceOf(LimitError);
      expect(isLimit(err)).toBe(true);
      expect(isRefusedUnrun(err)).toBe(false);
      expect(err).toMatchObject({ status: StatusCode.Limit, reason: Reason.TooManyActions, ran: Ran.No, serverMessage: LIMIT_TEXT });
      expect((err as Error).message).toContain("too_many_actions");
      expect(srv.ops().filter((o) => o === OpCode.ActionRegister)).toHaveLength(1);
    } finally {
      await client.close();
    }
  });

});

describe("start() refused at a limit", () => {
  it("rejects an ActionWorker's start with LimitError after exactly one registration", async () => {
    const srv = await fakeServer((op) => (op === OpCode.ActionRegister ? refusal(StatusCode.Limit, Reason.TooManyActions, LIMIT_TEXT) : undefined));
    close = srv.close;
    const w = new ActionWorker({ endpoint: srv.endpoint, blockMs: 30000, logger: quiet, retry: { maxAttempts: 4, initialBackoffMs: 1, maxBackoffMs: 2 } });
    w.action("a", async () => new Uint8Array(0));
    const err = await w.start().catch((e: unknown) => e);
    // The refusal has come back, so the one request reached the fake.
    expect(err).toBeInstanceOf(LimitError);
    expect(err).toMatchObject({ reason: Reason.TooManyActions, serverMessage: LIMIT_TEXT });
    await until(() => srv.ops().includes(OpCode.ActionRegister), 3000);
    await sleep(100);
    expect(srv.ops().filter((o) => o === OpCode.ActionRegister)).toHaveLength(1);
    expect(srv.ops()).not.toContain(OpCode.ActionAwait);
  });

  it("rejects a StreamWorker's start with LimitError after exactly one group create", async () => {
    const srv = await fakeServer((op) => (op === OpCode.StreamGroupCreate ? refusal(StatusCode.Limit, Reason.TooManyActions, LIMIT_TEXT) : undefined));
    close = srv.close;
    const w = new StreamWorker(srv.endpoint, { stream: "s", ackTimeoutMs: 30000, blockMs: 30000, logger: quiet, retry: { maxAttempts: 4, initialBackoffMs: 1, maxBackoffMs: 2 } }, async () => {});
    const err = await w.start().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LimitError);
    expect(err).toMatchObject({ reason: Reason.TooManyActions, serverMessage: LIMIT_TEXT });
    await until(() => srv.ops().includes(OpCode.StreamGroupCreate), 3000);
    await sleep(100);
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupCreate)).toHaveLength(1);
    expect(srv.ops()).not.toContain(OpCode.WorkerRegister);
    expect(srv.ops()).not.toContain(OpCode.StreamGroupRead);
  });
});

describe("a StreamWorker's group read at a limit", () => {
  it("waits and reads again, warning once for the reason however often it comes", async () => {
    const srv = await fakeServer((op, nth) => {
      if (op !== OpCode.StreamGroupRead) return undefined;
      return nth < 2 ? refusal(StatusCode.Limit, Reason.MemoryBudget, "limit: memory budget spent") : "park";
    });
    close = srv.close;
    const warned: string[] = [];
    const logger = { debug: () => {}, warn: (m: string) => void warned.push(m), error: () => {} };
    const w = new StreamWorker(srv.endpoint, { stream: "s", blockMs: 30000, logger, retry: { maxAttempts: 1 } }, async () => {});
    const done = w.start();
    await until(() => srv.ops().filter((o) => o === OpCode.StreamGroupRead).length === 3, 10_000);
    w.stop();
    srv.close();
    await done;
    expect(srv.ops().filter((o) => o === OpCode.StreamGroupRead)).toHaveLength(3);
    expect(warned.filter((m) => m.includes("refused at a limit"))).toEqual([expect.stringContaining("GroupRead refused at a limit (memory_budget)")]);
  }, 15_000);
});

describe("a transaction key that routes elsewhere", () => {
  it("is refused keys_not_colocated as a bad request, not run, sent once", async () => {
    const text = "bad request: this key routes elsewhere than the key the transaction was begun with; give every key in a transaction the same {tag}, e.g. {user:1}:balance";
    const begin = new Uint8Array(16);
    begin[0] = 1;
    const srv = await fakeServer((op) => {
      if (op === OpCode.KVBeginTxn) return { data: begin };
      if (op === OpCode.KVPut) return refusal(StatusCode.BadRequest, Reason.KeysNotColocated, text);
      return undefined;
    });
    close = srv.close;
    const client = new FloClient(srv.endpoint, { retry: { maxAttempts: 5, initialBackoffMs: 1, maxBackoffMs: 2 } });
    await client.connect();
    try {
      const txn = await client.kv.begin("{t}a");
      const err = await txn.put("b", new TextEncoder().encode("1")).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BadRequestError);
      expect(err).toMatchObject({ reason: Reason.KeysNotColocated, ran: Ran.No, serverMessage: text });
      expect((err as Error).message).toContain("keys_not_colocated");
      expect(srv.ops().filter((o) => o === OpCode.KVPut)).toHaveLength(1);
    } finally {
      await client.close();
    }
  });
});

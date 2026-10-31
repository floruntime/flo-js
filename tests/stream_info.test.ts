/**
 * stream.info: a stream that doesn't exist is not found, not an empty stream.
 */

import { describe, expect, it } from "vitest";
import { NotFoundError, Ran, Reason, StatusCode, StreamOperations } from "@floruntime/core";

describe("stream.info", () => {
  it("throws NotFoundError when the server answers not_found", async () => {
    const message = "stream not found";
    const body = new Uint8Array(3 + message.length);
    body[0] = Reason.NotFound & 0xff;
    body[1] = Reason.NotFound >> 8;
    body[2] = Ran.No;
    body.set(new TextEncoder().encode(message), 3);
    const stream = new StreamOperations({
      getNamespace: (override?: string) => override ?? "default",
      sendRequest: async () => ({ status: StatusCode.NotFound, data: body, requestId: 1n }),
    });
    const err = await stream.info("missing").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NotFoundError);
    expect(err).toMatchObject({ status: StatusCode.NotFound, ran: Ran.No });
    expect((err as Error).message).toContain(message);
  });
});

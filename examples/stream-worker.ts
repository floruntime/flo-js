/**
 * Example: StreamWorker API usage with the Flo Node.js SDK
 *
 * This example demonstrates how to use the StreamWorker class to consume
 * stream records via consumer groups.
 *
 * Usage:
 *   npx tsx examples/stream-worker.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:9000
 *   - A stream named "events" created:  flo stream create events
 *   - pnpm install && pnpm build
 */

import { FloClient, StreamWorker, StreamContext } from "@floruntime/node";

// =============================================================================
// Data Types
// =============================================================================

interface UserEvent {
  type: string;
  userId: string;
  timestamp: string;
  data?: Record<string, unknown>;
}

// =============================================================================
// Handler
// =============================================================================

async function processEvent(ctx: StreamContext): Promise<void> {
  const event = ctx.json<UserEvent>();

  console.log(
    `[${ctx.stream}/${ctx.group}] Processing event: ` +
      `type=${event.type}, userId=${event.userId}, ` +
      `streamId=${ctx.streamId}`
  );

  // Simulate processing
  await sleep(200);

  console.log(`  Event processed successfully`);
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const endpoint = process.env.FLO_ENDPOINT || "localhost:9000";
  const namespace = process.env.FLO_NAMESPACE || "myapp";

  // Create a client
  const client = new FloClient(endpoint, { namespace });
  await client.connect();

  // Create stream worker
  const worker = new StreamWorker(
    client,
    {
      stream: "events",
      group: "processors",
      concurrency: 5,
      batchSize: 10,
      logger: process.env.FLO_DEBUG === "1" || process.env.FLO_DEBUG === "true",
    },
    processEvent
  );

  // Handle shutdown signals
  let shuttingDown = false;

  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("\nReceived shutdown signal");
    worker.stop();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  // Start consuming (blocks until stopped)
  console.log("Starting stream worker...");
  try {
    await worker.start();
  } catch (e) {
    if (!shuttingDown) {
      console.error("Stream worker error:", e);
    }
  } finally {
    await worker.close();
    await client.close();
    console.log("Stream worker shutdown complete");
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main();

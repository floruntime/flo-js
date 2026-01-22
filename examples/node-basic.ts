/**
 * Basic example of using the Flo Node.js SDK.
 *
 * Usage:
 *   npx tsx examples/node-basic.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:9000
 *   - pnpm install && pnpm build
 */

import { FloClient, isNotFound } from "@floruntime/node";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

async function main() {
  // Create client
  const client = new FloClient("localhost:9000", {
    namespace: "example",
    debug: true,
  });

  try {
    // Connect to server
    await client.connect();
    console.log("Connected to Flo server");

    // === KV Operations ===
    console.log("\n=== KV Operations ===");

    // Put a key
    await client.kv.put("greeting", textEncoder.encode("Hello, Flo!"));
    console.log("Put key: greeting");

    // Get the key
    const value = await client.kv.get("greeting");
    if (value) {
      console.log(`Got value: ${textDecoder.decode(value)}`);
    }

    // Put with TTL
    await client.kv.put("temp-key", textEncoder.encode("expires soon"), {
      ttlSeconds: 60n,
    });
    console.log("Put key with TTL: temp-key");

    // Scan keys with prefix
    await client.kv.put("user:1", textEncoder.encode("Alice"));
    await client.kv.put("user:2", textEncoder.encode("Bob"));
    await client.kv.put("user:3", textEncoder.encode("Charlie"));

    const scanResult = await client.kv.scan("user:", { limit: 10 });
    console.log(`Scan found ${scanResult.entries.length} entries:`);
    for (const entry of scanResult.entries) {
      console.log(
        `  ${textDecoder.decode(entry.key)} = ${entry.value ? textDecoder.decode(entry.value) : "(null)"}`
      );
    }

    // Delete a key
    await client.kv.delete("temp-key");
    console.log("Deleted key: temp-key");

    // Get non-existent key (returns null)
    const missing = await client.kv.get("does-not-exist");
    console.log(`Non-existent key: ${missing === null ? "null" : "found"}`);

    // === Queue Operations ===
    console.log("\n=== Queue Operations ===");

    // Enqueue messages
    const seq1 = await client.queue.enqueue(
      "tasks",
      textEncoder.encode(JSON.stringify({ task: "send-email", to: "user@example.com" }))
    );
    console.log(`Enqueued message with seq: ${seq1}`);

    const seq2 = await client.queue.enqueue(
      "tasks",
      textEncoder.encode(JSON.stringify({ task: "process-image", id: 123 })),
      { priority: 10 } // Higher priority
    );
    console.log(`Enqueued high-priority message with seq: ${seq2}`);

    // Dequeue messages
    const result = await client.queue.dequeue("tasks", 5, {
      visibilityTimeoutMs: 30000,
    });
    console.log(`Dequeued ${result.messages.length} messages:`);

    for (const msg of result.messages) {
      console.log(`  seq=${msg.seq}: ${textDecoder.decode(msg.payload)}`);
    }

    // Acknowledge messages
    if (result.messages.length > 0) {
      const seqs = result.messages.map((m) => m.seq);
      await client.queue.ack("tasks", seqs);
      console.log(`Acknowledged ${seqs.length} messages`);
    }

    // Enqueue with delay
    const seq3 = await client.queue.enqueue(
      "tasks",
      textEncoder.encode(JSON.stringify({ task: "delayed-task" })),
      { delayMs: 5000n } // 5 second delay
    );
    console.log(`Enqueued delayed message with seq: ${seq3}`);

    console.log("\nDone!");
    console.log("\nTip: For workers and actions, use the high-level Worker API.");
    console.log("     See examples/worker.ts for a complete example.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    // Always close the connection
    await client.close();
    console.log("Disconnected");
  }
}

main();

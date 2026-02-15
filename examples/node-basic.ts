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

import { FloClient } from "@floruntime/node";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

async function main() {
  // Create client
  const client = new FloClient("localhost:9000", {
    namespace: "example",
    logger: false, // Set to true to see debug output
  });

  try {
    // Connect to server
    await client.connect();
    console.log("Connected to Flo server");

    /*
    // === KV Operations ===
    console.log("\n=== KV Operations ===");

    // Put some test data
    await client.kv.put("user:1", textEncoder.encode("Alice"));
    await client.kv.put("user:2", textEncoder.encode("Bob"));
    await client.kv.put("user:3", textEncoder.encode("Charlie"));
    console.log("Created test users");

    // === Using the new Cursor API ===
    console.log("\n--- Cursor API Examples ---");

    // Example 1: Async iteration (recommended for most cases)
    console.log("\n1. Async iteration with for await...of:");
    for await (const entry of client.kv.scanCursor("user:")) {
      console.log(`  ${textDecoder.decode(entry.key)} = ${textDecoder.decode(entry.value!)}`);
    }

    // Example 2: Get all entries as array
    console.log("\n2. Get all entries as array:");
    const allUsers = await client.kv.scanCursor("user:").toArray();
    console.log(`  Found ${allUsers.length} users`);

    // Example 3: Manual pagination
    console.log("\n3. Manual pagination (limit: 2 per page):");
    const cursor = client.kv.scanCursor("user:", { limit: 2 });
    let pageNum = 1;
    while (cursor.hasMore) {
      const page = await cursor.next();
      console.log(`  Page ${pageNum}: ${page.entries.length} entries`);
      for (const entry of page.entries) {
        console.log(`    - ${textDecoder.decode(entry.key)}`);
      }
      pageNum++;
    }

    // Example 4: Using scan() directly (low-level, single page)
    console.log("\n4. Low-level scan() (single page):");
    const scanResult = await client.kv.scan("user:", { limit: 2 });
    console.log(`  Found ${scanResult.entries.length} entries, hasMore: ${scanResult.hasMore}`);

    // Clean up
    await client.kv.delete("user:1");
    await client.kv.delete("user:2");
    await client.kv.delete("user:3");
    console.log("\nCleaned up test data");


    
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

    */

    // === Stream Operations ===
    console.log("\n=== Stream Operations ===");

    /* Append records to a stream
    const appendResult1 = await client.stream.append(
      "events",
      textEncoder.encode(JSON.stringify({ type: "user2.login", userId: "1234" }))
    );
    console.log(`Appended to stream: seq=${appendResult1.seq} timestamp=${appendResult1.timestamp}`);
*/
  /* const appendResult2 = await client.stream.append(
      "events",
      textEncoder.encode(JSON.stringify({ type: "user.action", action: "click" })),
      { partitionKey: "user-123" }
    );
    console.log(`Appended with partition key: seq=${appendResult2.seq} timestamp=${appendResult2.timestamp}`);
/*
    // Read from stream (from beginning)
    const readResult = await client.stream.read("events", { offset: 0n, limit: 10 });
    console.log(`Read ${readResult.records.length} records from stream:`);
    for (const record of readResult.records) {
      console.log(`  seq=${record.seq}: ${textDecoder.decode(record.payload)}`);
    }



    // Read from specific offset
    if (readResult.records.length > 0) {
      const firstSeq = readResult.records[0].seq;
      const fromOffset = await client.stream.read("events", { offset: firstSeq, limit: 5 });
      console.log(`Read ${fromOffset.records.length} records from offset ${firstSeq}`);
    }

    console.log("\nDone!");
    console.log("\nTip: For workers and actions, use the high-level Worker API.");
    console.log("     See examples/worker.ts for a complete example.");
*/
  } catch (err) {
    console.error("Error:", err);
  } finally {
    // Always close the connection
    await client.close();
    console.log("Disconnected");
  }
}

main();

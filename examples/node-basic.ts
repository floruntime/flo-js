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
  const client = new FloClient("localhost:9000", {
    namespace: "example",
    logger: false, // Set to true to see debug output
  });

  try {
    await client.connect();
    console.log("Connected to Flo server");

    // === KV ===
    await client.kv.put("user:1", textEncoder.encode("Alice"));
    const user = await client.kv.get("user:1");
    if (user) {
      console.log(`user:1 = ${textDecoder.decode(user.value)} (version ${user.version})`);
    }
    await client.kv.delete("user:1");

    // === Streams ===
    for (const action of ["login", "click", "logout"]) {
      await client.stream.append("events", textEncoder.encode(JSON.stringify({ action })));
    }

    const first = await client.stream.read("events", { limit: 2 });
    for (const record of first.records) {
      console.log(`  ${record.id.timestampMs}-${record.id.sequence}: ${textDecoder.decode(record.payload)}`);
    }

    // `start` is exclusive: pass the last id seen to read what came after it.
    const last = first.records.at(-1);
    if (last) {
      const rest = await client.stream.read("events", { start: last.id, limit: 10 });
      console.log(`${rest.records.length} more record(s) after ${last.id.timestampMs}-${last.id.sequence}`);
    }
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("Disconnected");
  }
}

main();

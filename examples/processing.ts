/**
 * Processing pipeline example for the Flo Node.js SDK.
 *
 * Webhook Account Filter scenario:
 *   1. KV store holds known account IDs: account:<id> → metadata
 *   2. Incoming payment webhooks land in "inbound-webhooks" stream
 *   3. Processing pipeline filters deposits + cross-references accounts
 *      via kv_lookup, routing matches to "relevant-deposits"
 *   4. Script verifies output against KV to confirm correctness
 *
 * Demonstrates:
 *   - Processing sync (declarative pipeline submission)
 *   - Processing status, list, stop
 *   - KV + Stream interop with pipelines
 *   - Cross-referencing pipeline output against KV
 *
 * Usage:
 *   npx tsx examples/processing.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:4453
 *   - pnpm install && pnpm build
 */

import { FloClient } from "@floruntime/node";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// --- Known accounts (registered in our system) ---
const KNOWN_ACCOUNTS: Record<string, { name: string; tier: string; status: string }> = {
  "acct-7xKXtg2CW87d": { name: "Alice", tier: "premium", status: "active" },
  "acct-9WzDXwBbmkg8": { name: "Bob", tier: "standard", status: "active" },
  "acct-3Kz9bFdRDkfS": { name: "Carol", tier: "premium", status: "active" },
  "acct-HN7cABqLq46E": { name: "Dave", tier: "standard", status: "active" },
  "acct-Fz2K6yPqVJTx": { name: "Eve", tier: "premium", status: "active" },
};

// Accounts belonging to another system (NOT in KV — should be filtered out)
const OTHER_ACCOUNTS = [
  "acct-BzQ8kPLM5x7y",
  "acct-VpR3nWk8L2xQ",
  "acct-Tk7jR9mB2xLC",
  "acct-Jw4nK8mR2pLQ",
  "acct-Mn6pQ8rK2tLX",
  "acct-Rp3sT8uL2vMX",
  "acct-Xy9zW8vK2tLM",
];

// --- Simulated inbound payment webhook events ---
function buildWebhooks(): { webhook_id: string; event: string; account_id: string; amount: string; currency: string; ref: string; timestamp: string }[] {
  const knownKeys = Object.keys(KNOWN_ACCOUNTS);
  return [
    // Deposits for OUR accounts (should pass through)
    { webhook_id: "wh-001", event: "deposit.completed", account_id: knownKeys[0]!, amount: "2.5",   currency: "USD", ref: "ref-abc1", timestamp: "2026-03-14T00:01:00Z" },
    { webhook_id: "wh-002", event: "deposit.completed", account_id: knownKeys[1]!, amount: "100.0", currency: "EUR", ref: "ref-abc2", timestamp: "2026-03-14T00:02:00Z" },
    { webhook_id: "wh-003", event: "deposit.pending",   account_id: knownKeys[2]!, amount: "0.5",   currency: "USD", ref: "ref-abc3", timestamp: "2026-03-14T00:03:00Z" },
    { webhook_id: "wh-004", event: "deposit.completed", account_id: knownKeys[3]!, amount: "50.0",  currency: "GBP", ref: "ref-abc4", timestamp: "2026-03-14T00:04:00Z" },
    { webhook_id: "wh-005", event: "deposit.completed", account_id: knownKeys[4]!, amount: "1.0",   currency: "USD", ref: "ref-abc5", timestamp: "2026-03-14T00:05:00Z" },
    // Other system's accounts (should be filtered OUT)
    { webhook_id: "wh-006", event: "deposit.completed", account_id: OTHER_ACCOUNTS[0]!, amount: "10.0",  currency: "USD", ref: "ref-xyz1", timestamp: "2026-03-14T00:06:00Z" },
    { webhook_id: "wh-007", event: "deposit.completed", account_id: OTHER_ACCOUNTS[1]!, amount: "200.0", currency: "EUR", ref: "ref-xyz2", timestamp: "2026-03-14T00:07:00Z" },
    { webhook_id: "wh-008", event: "deposit.pending",   account_id: OTHER_ACCOUNTS[2]!, amount: "5.0",   currency: "USD", ref: "ref-xyz3", timestamp: "2026-03-14T00:08:00Z" },
    { webhook_id: "wh-009", event: "deposit.completed", account_id: OTHER_ACCOUNTS[3]!, amount: "75.0",  currency: "GBP", ref: "ref-xyz4", timestamp: "2026-03-14T00:09:00Z" },
    // More of ours (interleaved)
    { webhook_id: "wh-010", event: "deposit.completed", account_id: knownKeys[0]!, amount: "1.25",  currency: "USD", ref: "ref-abc6", timestamp: "2026-03-14T00:10:00Z" },
    { webhook_id: "wh-011", event: "deposit.completed", account_id: knownKeys[1]!, amount: "500.0", currency: "EUR", ref: "ref-abc7", timestamp: "2026-03-14T00:11:00Z" },
    // More unknown
    { webhook_id: "wh-012", event: "deposit.completed", account_id: OTHER_ACCOUNTS[4]!, amount: "8.0",   currency: "USD", ref: "ref-xyz5", timestamp: "2026-03-14T00:12:00Z" },
    { webhook_id: "wh-013", event: "deposit.pending",   account_id: OTHER_ACCOUNTS[5]!, amount: "300.0", currency: "EUR", ref: "ref-xyz6", timestamp: "2026-03-14T00:13:00Z" },
    // One more of ours
    { webhook_id: "wh-014", event: "deposit.completed", account_id: knownKeys[2]!, amount: "3.0",  currency: "USD", ref: "ref-abc8", timestamp: "2026-03-14T00:14:00Z" },
    // Unknown
    { webhook_id: "wh-015", event: "deposit.completed", account_id: OTHER_ACCOUNTS[6]!, amount: "42.0", currency: "GBP", ref: "ref-xyz7", timestamp: "2026-03-14T00:15:00Z" },
  ];
}

// --- Pipeline YAML definition ---
const webhookFilterPipelineYAML = `\
kind: Processing
name: webhook-account-filter
namespace: default

sources:
  - name: webhooks
    stream:
      name: inbound-webhooks
      partitions: all

operators:
  - type: filter
    name: deposits-only
    condition: "value_contains:deposit"

  - type: kv_lookup
    name: check-known-account
    lookup_key: "account:\${$.account_id}"
    namespace: default
    mode: filter

sinks:
  - name: output
    stream:
      name: relevant-deposits
`;

async function main() {
  const client = new FloClient("localhost:4453", {
    namespace: "default",
    logger: true,
  });

  try {
    await client.connect();
    console.log("Connected to Flo server\n");

    // ============================================================
    // 1. Populate KV with known accounts
    // ============================================================
    console.log("=== 1. Populate KV with Known Accounts ===");

    for (const [accountId, meta] of Object.entries(KNOWN_ACCOUNTS)) {
      const key = `account:${accountId}`;
      await client.kv.put(key, textEncoder.encode(JSON.stringify(meta)));
      console.log(`  Stored ${meta.name} → account:${accountId}`);
    }

    console.log(`  ✓ Stored ${Object.keys(KNOWN_ACCOUNTS).length} known accounts in KV\n`);

    // ============================================================
    // 2. Populate inbound-webhooks stream with mixed deposits
    // ============================================================
    console.log("=== 2. Populate inbound-webhooks Stream ===");

    const webhooks = buildWebhooks();
    let ourCount = 0;
    let otherCount = 0;

    for (const wh of webhooks) {
      await client.stream.append(
        "inbound-webhooks",
        textEncoder.encode(JSON.stringify(wh))
      );
      const isOurs = wh.account_id in KNOWN_ACCOUNTS;
      if (isOurs) ourCount++;
      else otherCount++;
    }

    console.log(`  Appended ${webhooks.length} webhooks: ${ourCount} ours, ${otherCount} other system's`);
    console.log();

    // ============================================================
    // 3. Submit processing pipeline via sync
    // ============================================================
    console.log("=== 3. Submit Processing Pipeline ===");

    const syncResult = await client.processing.sync(webhookFilterPipelineYAML);
    console.log(`  Synced "${syncResult.name}": ${syncResult.action} (job_id: ${syncResult.job_id})`);

    // Re-sync — always submits a new job (no version guard like workflows)
    const syncResult2 = await client.processing.sync(webhookFilterPipelineYAML);
    console.log(`  Re-sync "${syncResult2.name}": ${syncResult2.action} (job_id: ${syncResult2.job_id})`);
    console.log();

    // ============================================================
    // 4. Check pipeline status
    // ============================================================
    console.log("=== 4. Pipeline Status ===");

    const status = await client.processing.status(syncResult.job_id);
    console.log(`  Job: ${status.job_id}`);
    console.log(`  Name: ${status.name}`);
    console.log(`  Status: ${status.status}`);
    console.log(`  Parallelism: ${status.parallelism}`);
    console.log(`  Batch size: ${status.batch_size}`);
    console.log(`  Records processed: ${status.records_processed}`);
    console.log();

    // ============================================================
    // 5. List all processing jobs
    // ============================================================
    console.log("=== 5. List Processing Jobs ===");

    const jobs = await client.processing.list();
    for (const job of jobs) {
      console.log(`  ${job.name} (${job.job_id}) — ${job.status}, parallelism=${job.parallelism}`);
    }
    console.log();

    // ============================================================
    // 6. Wait for pipeline to process, then read output
    // ============================================================
    console.log("=== 6. Wait + Read Pipeline Output ===");

    console.log("  Waiting 5s for pipeline to process...");
    await delay(5000);

    const output = await client.stream.read("relevant-deposits", { limit: 50 });
    console.log(`  Read ${output.records.length} records from relevant-deposits`);
    console.log();

    // ============================================================
    // 7. Cross-reference output against KV
    // ============================================================
    console.log("=== 7. Cross-Reference Output Against KV ===");

    let kvMatched = 0;
    let kvUnmatched = 0;
    const seenAccounts = new Set<string>();

    for (const record of output.records) {
      const payload = textDecoder.decode(record.payload);
      try {
        const data = JSON.parse(payload) as { account_id?: string; webhook_id?: string };
        if (!data.account_id) continue;

        seenAccounts.add(data.account_id);

        const kvResult = await client.kv.get(`account:${data.account_id}`);
        if (kvResult) {
          const meta = JSON.parse(textDecoder.decode(kvResult)) as { name: string };
          console.log(`  ${data.webhook_id} → ${data.account_id.slice(0, 12)}... → ${meta.name} (OURS ✓)`);
          kvMatched++;
        } else {
          console.log(`  ${data.webhook_id} → ${data.account_id.slice(0, 12)}... → (OTHER SYSTEM ✗)`);
          kvUnmatched++;
        }
      } catch {
        // Non-JSON payload, skip
      }
    }

    console.log();

    // ============================================================
    // 8. Validate results
    // ============================================================
    console.log("=== 8. Validation ===");

    const totalRecords = output.records.length;
    console.log(`  Total pipeline output records: ${totalRecords}`);
    console.log(`  Our accounts (KV match):       ${kvMatched}`);
    console.log(`  Other system (no KV match):    ${kvUnmatched}`);
    console.log();

    if (kvMatched >= 7) {
      console.log(`  ✓ KV cross-ref identified ${kvMatched} deposits for our accounts (expected >= 7 of 8)`);
    } else {
      console.log(`  ✗ KV cross-ref identified only ${kvMatched} deposits for our accounts (expected >= 7)`);
    }

    if (totalRecords >= 10) {
      console.log(`  ✓ Pipeline processed ${totalRecords} deposits (expected >= 10 of 15)`);
    } else {
      console.log(`  ⚠ Pipeline only processed ${totalRecords} deposits (expected >= 10 — may need more time)`);
    }

    // ============================================================
    // 9. KV account lookups (demo)
    // ============================================================
    console.log("\n=== 9. KV Account Lookups (Demo) ===");

    for (const [accountId, meta] of Object.entries(KNOWN_ACCOUNTS)) {
      const result = await client.kv.get(`account:${accountId}`);
      if (result) {
        console.log(`  account:${accountId} → ${JSON.stringify(meta)}`);
      }
    }

    // ============================================================
    // 10. Stop the first pipeline job (cleanup)
    // ============================================================
    console.log("\n=== 10. Stop Pipeline (Cleanup) ===");

    try {
      await client.processing.stop(syncResult.job_id);
      console.log(`  Stopped job ${syncResult.job_id}`);
    } catch (e) {
      console.log(`  Stop: ${e instanceof Error ? e.message : e}`);
    }

    // Also stop the second sync'd job
    try {
      await client.processing.stop(syncResult2.job_id);
      console.log(`  Stopped job ${syncResult2.job_id}`);
    } catch (e) {
      console.log(`  Stop: ${e instanceof Error ? e.message : e}`);
    }

    // Final status check
    const finalStatus = await client.processing.status(syncResult.job_id);
    console.log(`  Final status: ${finalStatus.status} (records_processed: ${finalStatus.records_processed})`);

    console.log("\nDone ✓");
  } finally {
    await client.close();
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

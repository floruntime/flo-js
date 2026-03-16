/**
 * Workflow example for the Flo Node.js SDK.
 *
 * Demonstrates:
 * - Declarative sync (sync, syncBytes)
 * - Imperative workflow ops (create, start, status, signal, cancel)
 * - Definition management (getDefinition, listDefinitions, disable, enable)
 * - History and run listing
 *
 * Usage:
 *   npx tsx examples/workflow-basic.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:9000
 *   - pnpm install && pnpm build
 */

import { FloClient } from "@floruntime/node";

// --- Sample workflow definitions ---

const orderWorkflowYAML = `\
kind: Workflow
name: process-order
version: "1.0.0"
idempotency: required

start:
  run: "@actions/validate-order"
  transitions:
    success: charge_payment
    failure: flo.Failed

steps:
  charge_payment:
    run: "@actions/charge-payment"
    transitions:
      success: ship_order
      failure: flo.Failed

  ship_order:
    run: "@actions/ship-order"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`;

const approvalWorkflowYAML = `\
kind: Workflow
name: expense-approval
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: wait_for_approval
    failure: flo.Failed

steps:
  wait_for_approval:
    waitForSignal:
      type: "approval_decision"
      timeoutMs: 86400000
      onTimeout: flo.Failed
    transitions:
      success: process_expense
      failure: flo.Failed

  process_expense:
    run: "@actions/process-expense"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`;

// Same shape but with a 3-second timeout for e2e signal timeout testing
const signalTimeoutWorkflowYAML = `\
kind: Workflow
name: signal-timeout-test
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: wait_for_approval
    failure: flo.Failed

steps:
  wait_for_approval:
    waitForSignal:
      type: "approval_decision"
      timeoutMs: 3000
      onTimeout: flo.Failed
    transitions:
      success: process_expense
      failure: flo.Failed

  process_expense:
    run: "@actions/process-expense"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`;

// Outcome-based routing: review-order returns named outcomes
// that map to different workflow branches
const outcomeWorkflowYAML = `\
kind: Workflow
name: order-review
version: "1.0.0"

start:
  run: "@actions/review-order"
  transitions:
    approved: fulfill
    rejected: notify_rejection
    needs_review: manual_review
    failure: flo.Failed

steps:
  fulfill:
    run: "@actions/fulfill-order"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  notify_rejection:
    run: "@actions/notify-rejection"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  manual_review:
    run: "@actions/manual-review"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`;

async function main() {
  const client = new FloClient("localhost:4453", {
    namespace: "example",
    logger: true,
  });

  try {
    await client.connect();
    console.log("Connected to Flo server\n");

    // ============================================================
    // 1. Declarative Sync — safe to call on every boot
    // ============================================================
    console.log("\n=== Declarative Sync ===");

    const syncResult = await client.workflow.sync(orderWorkflowYAML);
    console.log(
      `Synced "${syncResult.name}" v${syncResult.version}: ${syncResult.action}`
    );

    // Sync again — should be "unchanged" since version hasn't changed
    const syncResult2 = await client.workflow.sync(orderWorkflowYAML);
    console.log(
      `Re-sync "${syncResult2.name}" v${syncResult2.version}: ${syncResult2.action}`
    );

    // Sync from raw bytes
    const encoder = new TextEncoder();
    const syncResult3 = await client.workflow.syncBytes(
      encoder.encode(approvalWorkflowYAML)
    );
    console.log(
      `Synced "${syncResult3.name}" v${syncResult3.version}: ${syncResult3.action}`
    );

    // Sync outcome-based workflow
    const syncResult4 = await client.workflow.sync(outcomeWorkflowYAML);
    console.log(
      `Synced "${syncResult4.name}" v${syncResult4.version}: ${syncResult4.action}`
    );

    // Sync signal-timeout-test workflow
    const syncResult5 = await client.workflow.sync(signalTimeoutWorkflowYAML);
    console.log(
      `Synced "${syncResult5.name}" v${syncResult5.version}: ${syncResult5.action}`
    );

    // ============================================================
    // 2. List definitions
    // ============================================================
    console.log("\n=== List Definitions ===");

    const defs = await client.workflow.listDefinitions();
    for (const def of defs) {
      console.log(`  ${def.name} v${def.version} (created: ${def.created_at})`);
    }

    // ============================================================
    // 3. Get definition YAML
    // ============================================================
    console.log("\n=== Get Definition ===");

    const yaml = await client.workflow.getDefinition("process-order");
    if (yaml) {
      console.log(`  Retrieved YAML (${yaml.length} bytes)`);
    }

    // ============================================================
    // 4. Start a workflow run
    // ============================================================
    console.log("\n=== Start Workflow ===");

    const runId = await client.workflow.start(
      "process-order",
      JSON.stringify({ orderId: "ORD-1234", amount: 99.99 })
    );
    console.log(`  Started run: ${runId}`);

    // ============================================================
    // 5. Check status (allow async actions to complete)
    // ============================================================
    console.log("\n=== Workflow Status ===");

    await delay(2000); // give the worker time to process
    const status = await client.workflow.status(runId);
    console.log(`  Run: ${status.run_id}`);
    console.log(`  Workflow: ${status.workflow} v${status.version}`);
    console.log(`  Status: ${status.status}`);
    console.log(`  Step: ${status.current_step}`);

    // ── Start a failing workflow (amount > $2000 → validate-order rejects)
    console.log("\n=== Start Failing Workflow ===");

    const failRunId = await client.workflow.start(
      "process-order",
      JSON.stringify({ orderId: "ORD-BIGSPEND", amount: 5000 })
    );
    console.log(`  Started run: ${failRunId}`);
    await delay(2000);

    const failStatus = await client.workflow.status(failRunId);
    console.log(`  Status: ${failStatus.status} (step: ${failStatus.current_step})`);
    if (failStatus.status === "failed") {
      console.log("  ✓ Workflow correctly failed (non-retryable error)");
    } else {
      console.log(`  ⚠ Expected 'failed' but got '${failStatus.status}'`);
    }
    const failHistory = await client.workflow.history(failRunId, { limit: 10 });
    for (const event of failHistory) {
      console.log(`  [${event.type}] ${event.detail}`);
    }

    // ============================================================
    // 6. Signal tests — advance via signal + timeout fallback
    // ============================================================
    console.log("\n=== Signal Tests ===");

    // ── 6a. Signal advances the workflow ──
    console.log("\n  --- 6a. Signal Advances Workflow ---");

    const sigRunId = await client.workflow.start(
      "expense-approval",
      JSON.stringify({ expense: "Travel", amount: 500 })
    );
    console.log(`  Started signal run: ${sigRunId}`);

    // Wait for the run to reach the wait_for_approval step
    {
      const dl = Date.now() + 10000;
      let s = await client.workflow.status(sigRunId);
      while (Date.now() < dl && s.current_step !== "wait_for_approval" && s.status !== "waiting") {
        await delay(300);
        s = await client.workflow.status(sigRunId);
      }
      console.log(`  Before signal: step=${s.current_step} status=${s.status}`);
    }

    // Send the approval signal
    await client.workflow.signal(
      sigRunId,
      "approval_decision",
      JSON.stringify({ approved: true, approver: "manager@corp.com" })
    );
    console.log("  Sent approval_decision signal");

    // Wait for workflow to complete
    {
      const dl = Date.now() + 10000;
      let s = await client.workflow.status(sigRunId);
      while (Date.now() < dl && s.status !== "completed" && s.status !== "failed") {
        await delay(300);
        s = await client.workflow.status(sigRunId);
      }
      console.log(`  After signal: step=${s.current_step} status=${s.status}`);
      if (s.status === "completed") {
        console.log("  ✓ Signal correctly advanced the workflow to completion");
      } else {
        console.log(`  ⚠ Expected 'completed' but got '${s.status}'`);
      }
    }

    // Show history
    const sigHist = await client.workflow.history(sigRunId, { limit: 15 });
    for (const event of sigHist) {
      console.log(`    [${event.type}] ${event.detail}`);
    }

    // ── 6b. Signal timeout fails the workflow ──
    console.log("\n  --- 6b. Signal Timeout ---");

    const timeoutRunId = await client.workflow.start(
      "signal-timeout-test",
      JSON.stringify({ expense: "Conference", amount: 200 })
    );
    console.log(`  Started timeout run: ${timeoutRunId}`);

    // Wait for the run to reach the wait step
    {
      const dl = Date.now() + 10000;
      let s = await client.workflow.status(timeoutRunId);
      while (Date.now() < dl && s.current_step !== "wait_for_approval" && s.status !== "waiting") {
        await delay(300);
        s = await client.workflow.status(timeoutRunId);
      }
      console.log(`  Waiting: step=${s.current_step} status=${s.status}`);
    }

    // Do NOT send a signal — let the 3-second timeout expire
    console.log("  Waiting for 3s timeout to expire...");
    await delay(5000);

    {
      const s = await client.workflow.status(timeoutRunId);
      console.log(`  After timeout: step=${s.current_step} status=${s.status}`);
      if (s.status === "failed" || s.status === "timed_out") {
        console.log("  ✓ Workflow correctly timed out");
      } else {
        console.log(`  ⚠ Expected 'failed' or 'timed_out' but got '${s.status}'`);
      }
    }

    // Show history
    const toHist = await client.workflow.history(timeoutRunId, { limit: 15 });
    for (const event of toHist) {
      console.log(`    [${event.type}] ${event.detail}`);
    }

    // ============================================================
    // 7. History
    // ============================================================
    console.log("\n=== Run History ===");

    const history = await client.workflow.history(runId, { limit: 10 });
    for (const event of history) {
      console.log(`  [${event.timestamp}] ${event.type}: ${event.detail}`);
    }

    // ============================================================
    // 8. List runs
    // ============================================================
    console.log("\n=== List Runs ===");

    const runs = await client.workflow.listRuns({
      workflowName: "process-order",
      limit: 5,
    });
    for (const run of runs) {
      console.log(
        `  ${run.run_id} — ${run.workflow} — ${run.status} (${run.created_at})`
      );
    }

    // ============================================================
    // 9. Disable / Enable
    // ============================================================
    console.log("\n=== Disable / Enable ===");

    await client.workflow.disable("process-order");
    console.log("  Disabled process-order");

    await client.workflow.enable("process-order");
    console.log("  Re-enabled process-order");

    // ============================================================
    // 10. Cancel a run
    // ============================================================
    console.log("\n=== Cancel Run ===");

    const cancelRunId = await client.workflow.start(
      "process-order",
      JSON.stringify({ orderId: "ORD-9999" })
    );
    await client.workflow.cancel(cancelRunId, "User requested cancellation");
    console.log(`  Cancelled run: ${cancelRunId}`);

    // ============================================================
    // 11. Outcome-Based Routing
    //     review-order returns "approved", "rejected", or "needs_review"
    //     depending on order amount — workflow routes accordingly.
    //     Run sequentially to avoid action dispatch concurrency issues.
    // ============================================================
    console.log("\n=== Outcome-Based Routing ===");

    // Helper: start a run and wait for it to advance past "start"
    async function runOutcomeTest(
      label: string,
      amount: number,
      orderId: string,
      expectedStep: string
    ) {
      const runId = await client.workflow.start(
        "order-review",
        JSON.stringify({ orderId, amount })
      );
      console.log(`  ${label}: started ${runId}`);

      // Poll until past start step or 10s elapses
      const dl = Date.now() + 10000;
      let s = await client.workflow.status(runId);
      while (Date.now() < dl && s.current_step === "start") {
        await delay(500);
        s = await client.workflow.status(runId);
      }

      console.log(`  ${label}: step=${s.current_step} status=${s.status}`);
      if (s.current_step === expectedStep || s.status === "completed") {
        console.log(`  ✓ Routed correctly to ${s.current_step}`);
      } else {
        console.log(`  ⚠ Expected step '${expectedStep}' but got '${s.current_step}'`);
      }

      // Show history
      const hist = await client.workflow.history(runId, { limit: 10 });
      for (const event of hist) {
        console.log(`    [${event.type}] ${event.detail}`);
      }
    }

    // Test each outcome path sequentially
    await runOutcomeTest("Small  ($50) ", 50,  "ORD-SMALL", "fulfill");
    await runOutcomeTest("Large  ($750)", 750, "ORD-BIG",   "notify_rejection");
    await runOutcomeTest("Medium ($250)", 250, "ORD-MID",   "manual_review");

    console.log("\nDone ✓");
  } finally {
    await client.close();
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

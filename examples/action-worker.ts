/**
 * Example: High-level ActionWorker API usage with the Flo Node.js SDK
 *
 * This example demonstrates how to use the ActionWorker class to process actions.
 *
 * Usage:
 *   npx tsx examples/worker.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:9000
 *   - pnpm install && pnpm build
 */

import { ActionWorker, ActionContext, ActionResult, NonRetryableError } from "@floruntime/node";

// =============================================================================
// Data Types
// =============================================================================

interface OrderItem {
  sku: string;
  quantity: number;
  price: number;
}

interface OrderRequest {
  orderId: string;
  customerId: string;
  amount: number;
  items: OrderItem[];
}

interface NotificationRequest {
  userId: string;
  channel: string;
  message: string;
}

// =============================================================================
// Action Handlers
// =============================================================================

/**
 * Process an order - demonstrates long-running tasks with Touch.
 */
async function processOrder(ctx: ActionContext): Promise<Uint8Array> {
  const req = ctx.json<OrderRequest>();

  console.log(
    `Processing order ${req.orderId} for customer ${req.customerId} ` +
      `(amount: $${req.amount.toFixed(2)})`
  );

  // Simulate a long-running order processing task
  // For long tasks, periodically call touch() to extend the lease
  const items = req.items || [];

  for (const [i, item] of items.entries()) {
    console.log(
      `  Processing item ${i + 1}/${items.length}: ${item.sku} (qty: ${item.quantity})`
    );

    // Simulate work for each item
    await sleep(2000);

    // Extend the lease every few items to prevent timeout
    // This is critical for long-running tasks
    if ((i + 1) % 3 === 0) {
      try {
        await ctx.touch(30000);
        console.log(`  Extended lease for order ${req.orderId}`);
      } catch (e) {
        console.warn(`Warning: failed to extend lease: ${e}`);
      }
    }
  }

  // Return result
  return ctx.toBytes({
    orderId: req.orderId,
    status: "processed",
    processedBy: ctx.taskId,
  });
}

/**
 * Send a notification - demonstrates returning a plain object.
 * The worker auto-serializes it to JSON bytes.
 */
async function sendNotification(ctx: ActionContext) {
  const req = ctx.json<NotificationRequest>();

  console.log(
    `Sending ${req.channel} notification to user ${req.userId}: ${req.message}`
  );

  // Simulate sending notification
  await sleep(500);

  // Return a plain object — no need for ctx.toBytes()
  return {
    success: true,
    channel: req.channel,
    userId: req.userId,
  };
}

/**
 * Generate a report - demonstrates context usage.
 */
async function generateReport(ctx: ActionContext): Promise<Uint8Array> {
  const data = ctx.json<{ type?: string; dateRange?: string }>();
  const reportType = data.type || "summary";
  const dateRange = data.dateRange || "last_7_days";

  console.log(
    `Generating ${reportType} report for ${dateRange} ` +
      `(attempt ${ctx.attempt}, task ${ctx.taskId})`
  );

  // Simulate report generation with progress
  const totalSteps = 5;
  for (let step = 0; step < totalSteps; step++) {
    console.log(`  Report generation step ${step + 1}/${totalSteps}`);
    await sleep(1000);

    // Extend lease periodically
    if (step === 2) {
      await ctx.touch(30000);
    }
  }

  return ctx.toBytes({
    reportType,
    dateRange,
    generatedAt: ctx.createdAt.toString(),
    rows: 1500,
  });
}

/**
 * Simple health check action — plain object return.
 */
async function healthCheck(ctx: ActionContext) {
  return {
    status: "healthy",
    workerId: ctx.taskId,
    timestamp: ctx.createdAt.toString(),
  };
}

// =============================================================================
// Workflow Action Handlers (used by workflow-basic.ts)
// =============================================================================

/**
 * Validate an order — rejects orders over $2000 or missing orderId.
 * Returns a plain object (auto-serialized to JSON bytes).
 */
async function validateOrder(ctx: ActionContext) {
  const data = ctx.json<{ orderId?: string; amount?: number }>();
  console.log(`[validate-order] Validating order ${data.orderId ?? "unknown"} ($${data.amount ?? 0})`);
  await sleep(200);

  if (!data.orderId) {
    throw new NonRetryableError("missing orderId");
  }
  if ((data.amount ?? 0) > 2000) {
    throw new NonRetryableError(`order amount $${data.amount} exceeds $2000 limit`);
  }
  return { valid: true, orderId: data.orderId };
}

/**
 * Charge payment — rejects amounts over $1500 (simulates card decline).
 */
async function chargePayment(ctx: ActionContext): Promise<Uint8Array> {
  const data = ctx.json<{ orderId?: string; amount?: number }>();
  console.log(`[charge-payment] Charging $${data.amount?.toFixed(2)} for ${data.orderId}`);
  await sleep(300);

  if ((data.amount ?? 0) > 1500) {
    throw new NonRetryableError(`payment of $${data.amount} declined — exceeds $1500 limit`);
  }
  return ctx.toBytes({ charged: true, orderId: data.orderId, amount: data.amount });
}

/**
 * Ship an order — rejects orders with id ending in "-FAIL".
 */
async function shipOrder(ctx: ActionContext): Promise<Uint8Array> {
  const data = ctx.json<{ orderId?: string }>();
  console.log(`[ship-order] Shipping order ${data.orderId}`);
  await sleep(200);

  if (data.orderId?.endsWith("-FAIL")) {
    throw new NonRetryableError(`shipping failed for ${data.orderId} — address invalid`);
  }
  return ctx.toBytes({ shipped: true, orderId: data.orderId, trackingId: "TRK-42" });
}

/**
 * Validate an expense report.
 */
async function validateExpense(ctx: ActionContext): Promise<Uint8Array> {
  const data = ctx.json<{ expense?: string; amount?: number }>();
  console.log(`[validate-expense] Validating expense: ${data.expense} ($${data.amount})`);
  await sleep(200);
  return ctx.toBytes({ valid: true, expense: data.expense });
}

/**
 * Process an approved expense.
 */
async function processExpense(ctx: ActionContext): Promise<Uint8Array> {
  const data = ctx.json<{ expense?: string; amount?: number }>();
  console.log(`[process-expense] Processing expense: ${data.expense}`);
  await sleep(200);
  return ctx.toBytes({ processed: true, expense: data.expense });
}

// =============================================================================
// Outcome-Based Action Handlers (used by workflow-basic.ts outcome test)
// =============================================================================

/**
 * Review an order — returns a named outcome based on amount thresholds.
 *   amount < 100  → "approved"  (auto-approve small orders)
 *   amount >= 500 → "rejected"  (decline large orders)
 *   otherwise     → "needs_review" (manual review for mid-range)
 */
async function reviewOrder(ctx: ActionContext): Promise<ActionResult> {
  const data = ctx.json<{ orderId?: string; amount?: number }>();
  const amount = data.amount ?? 0;
  console.log(`[review-order] Reviewing order ${data.orderId ?? "unknown"} ($${amount})`);
  await sleep(200);

  if (amount < 100) {
    return ctx.result("approved", { orderId: data.orderId, decision: "auto-approved" });
  } else if (amount >= 500) {
    return ctx.result("rejected", { orderId: data.orderId, reason: "amount too high" });
  } else {
    return ctx.result("needs_review", { orderId: data.orderId, note: "manual review required" });
  }
}

/**
 * Fulfill an approved order — plain object return.
 */
async function fulfillOrder(ctx: ActionContext) {
  const data = ctx.json<{ orderId?: string }>();
  console.log(`[fulfill-order] Fulfilling order ${data.orderId}`);
  await sleep(200);
  return { fulfilled: true, orderId: data.orderId };
}

/**
 * Notify rejection to customer — plain object return.
 */
async function notifyRejection(ctx: ActionContext) {
  const data = ctx.json<{ orderId?: string; reason?: string }>();
  console.log(`[notify-rejection] Notifying rejection for ${data.orderId}: ${data.reason}`);
  await sleep(200);
  return { notified: true, orderId: data.orderId };
}

/**
 * Queue order for manual review — plain object return.
 */
async function manualReview(ctx: ActionContext) {
  const data = ctx.json<{ orderId?: string }>();
  console.log(`[manual-review] Queuing order ${data.orderId} for manual review`);
  await sleep(200);
  return { queued: true, orderId: data.orderId };
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  // Create action worker with configuration
  const worker = new ActionWorker({
    endpoint: process.env.FLO_ENDPOINT || "localhost:4453",
    namespace: process.env.FLO_NAMESPACE || "example",
    concurrency: 5,
    actionTimeoutMs: 300000, // 5 minutes
    logger: process.env.FLO_DEBUG === "1" || process.env.FLO_DEBUG === "true",
  });

  // Register action handlers
  worker.action("process-order", processOrder);
  worker.action("send-notification", sendNotification);
  worker.action("generate-report", generateReport);
  worker.action("health-check", healthCheck);

  // Workflow actions (used by workflow-basic.ts)
  worker.action("validate-order", validateOrder);
  worker.action("charge-payment", chargePayment);
  worker.action("ship-order", shipOrder);
  worker.action("validate-expense", validateExpense);
  worker.action("process-expense", processExpense);

  // Outcome-based actions (used by workflow-basic.ts outcome test)
  worker.action("review-order", reviewOrder);
  worker.action("fulfill-order", fulfillOrder);
  worker.action("notify-rejection", notifyRejection);
  worker.action("manual-review", manualReview);

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

  // Start worker (blocks until stopped)
  console.log("Starting worker...");
  try {
    await worker.start();
  } catch (e) {
    if (!shuttingDown) {
      console.error("Worker error:", e);
    }
  } finally {
    await worker.close();
    console.log("Worker shutdown complete");
  }
}

// Helper function
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main();

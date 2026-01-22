/**
 * Example: High-level Worker API usage with the Flo Node.js SDK
 *
 * This example demonstrates how to use the Worker class to process actions.
 *
 * Usage:
 *   npx tsx examples/worker.ts
 *
 * Prerequisites:
 *   - A running Flo server on localhost:9000
 *   - pnpm install && pnpm build
 */

import { Worker, ActionContext } from "@floruntime/node";

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

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
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
 * Send a notification - demonstrates simple action.
 */
async function sendNotification(ctx: ActionContext): Promise<Uint8Array> {
  const req = ctx.json<NotificationRequest>();

  console.log(
    `Sending ${req.channel} notification to user ${req.userId}: ${req.message}`
  );

  // Simulate sending notification
  await sleep(500);

  return ctx.toBytes({
    success: true,
    channel: req.channel,
    userId: req.userId,
  });
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
 * Simple health check action.
 */
async function healthCheck(ctx: ActionContext): Promise<Uint8Array> {
  return ctx.toBytes({
    status: "healthy",
    workerId: ctx.taskId,
    timestamp: ctx.createdAt.toString(),
  });
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  // Create worker with configuration
  const worker = new Worker({
    endpoint: process.env.FLO_ENDPOINT || "localhost:9000",
    namespace: process.env.FLO_NAMESPACE || "myapp",
    concurrency: 5,
    actionTimeoutMs: 300000, // 5 minutes
    debug: process.env.FLO_DEBUG === "1" || process.env.FLO_DEBUG === "true",
  });

  // Register action handlers
  worker.action("process-order", processOrder);
  worker.action("send-notification", sendNotification);
  worker.action("generate-report", generateReport);
  worker.action("health-check", healthCheck);

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

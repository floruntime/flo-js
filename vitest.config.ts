import { defineConfig } from "vitest/config";
import { resolve } from "path";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    globals: true,
  },
  resolve: {
    alias: {
      "@floruntime/core": resolve(__dirname, "packages/core/src/index.ts"),
      "@floruntime/node": resolve(__dirname, "packages/node/src/index.ts"),
      "@floruntime/web": resolve(__dirname, "packages/web/src/index.ts"),
    },
  },
});

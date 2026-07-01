import { defineConfig } from "vitest/config";

// Docker-backed integration config — SEPARATE from the unit `test` task.
//
// Deliberately standalone (not extending vitest.config.base.ts): the base
// `include` targets `src/**/*.test.ts`, and vitest's mergeConfig CONCATENATES
// array options, which would fold the Docker-free unit tests back into this
// run. Keeping this config self-contained guarantees a hard split:
//   - `pnpm test`             -> src/**/*.test.ts        (unit, no Docker)
//   - `pnpm test:integration` -> test/integration/**     (this file, Docker)
//
// The unit config can never pick these up because they live OUTSIDE src/.
export default defineConfig({
  test: {
    name: "@nats-kit/core (integration)",
    environment: "node",
    include: ["test/integration/**/*.integration.test.ts"],
    watch: false,
    // Real container pulls + reconnect windows are slow; bound generously so
    // the in-test waitUntil()s are what fail fast, not an outer vitest timeout.
    testTimeout: 120_000,
    hookTimeout: 180_000,
    // One container-heavy file at a time — avoids Docker daemon contention and
    // keeps log output readable.
    fileParallelism: false,
  },
});

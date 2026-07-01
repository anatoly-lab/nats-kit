// Shared vitest base config. Per-package configs extend / override this.
//
// STEP 1 skeleton: there are no test files yet, so `passWithNoTests` keeps the
// `test` turbo task (and CI) green until the real suites land alongside the
// business logic in the next extraction step.

import { defineConfig } from "vitest/config";

export const baseConfig = defineConfig({
  test: {
    // Node environment by default. Browser-ish packages override to happy-dom.
    environment: "node",

    // Discover *.test.ts and __tests__/**/*.test.ts only.
    include: ["src/**/*.test.ts", "src/**/__tests__/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**"],

    // Skeleton has no tests yet — don't fail the run on an empty suite.
    passWithNoTests: true,

    // Don't pollute test runs with watch mode in CI.
    watch: false,

    // Show failures fast.
    reporters: ["default"],

    // Coverage: opt-in via `--coverage`; this just standardizes the shape.
    coverage: {
      provider: "v8",
      reporter: ["text", "lcov"],
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/**/__tests__/**", "src/**/index.ts"],
    },
  },
});

export default baseConfig;

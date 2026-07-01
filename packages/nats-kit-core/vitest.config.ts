import { mergeConfig, defineConfig } from "vitest/config";

import baseConfig from "../../vitest.config.base.ts";

export default mergeConfig(
  baseConfig,
  defineConfig({
    test: {
      name: "@nats-kit/core",
    },
  }),
);

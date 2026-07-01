import { mergeConfig, defineConfig } from "vitest/config";

// The NestJS adapter needs the swc transform (legacy decorators +
// emitDecoratorMetadata) for its DI tests — see ../../vitest.config.nestjs.ts.
import nestjsConfig from "../../vitest.config.nestjs.ts";

export default mergeConfig(
  nestjsConfig,
  defineConfig({
    test: {
      name: "@nats-kit/nestjs",
    },
  }),
);

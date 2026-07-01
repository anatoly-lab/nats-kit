// Shared vitest config for the NestJS adapter package(s) only.
//
// Why this exists (and why ONLY the NestJS adapter extends it):
//   The NestJS adapter uses legacy TS decorators (`@Module`, `@Injectable`,
//   `@Inject`, …) plus `design:*` reflect-metadata for DI. As of vite 8.1 the
//   default test transform is oxc (rolldown), which — unlike the old esbuild
//   path — no longer lowers `experimentalDecorators`; it emits *native*
//   decorator syntax Node can't parse at test time, and it drops the
//   `emitDecoratorMetadata` output Nest's DI relies on. The framework-free
//   `@nats-kit/core` is decorator-free, so it keeps the faster oxc path.
//
//   Fix: make swc the SOLE `.ts` transform so it lowers decorators + emits
//   metadata. Two parts are both required on vite 8.1:
//     1. `oxc: false` — disable vite's built-in transform. unplugin-swc only
//        sets the legacy `esbuild: false`, which vite 8.1 has made INERT (it
//        now logs "set `oxc: false` instead"). Without this, oxc still runs —
//        and runs BEFORE the swc plugin — stripping type annotations first, so
//        swc's `decoratorMetadata` can no longer emit `design:paramtypes` and
//        Nest's type-based DI metadata is lost.
//     2. the swc plugin itself (the official NestJS-on-vitest recipe). The
//        legacy-decorator + metadata flags are set explicitly rather than left
//        to tsconfig auto-detection so the transform is correct regardless of
//        how the package's tsconfig resolves at test time.
//   Test-transform ONLY — production builds still go through tshy/tsc, this
//   plugin never touches `pnpm build`.

import swc from "unplugin-swc";
import { mergeConfig, defineConfig } from "vitest/config";

import baseConfig from "./vitest.config.base.ts";

export const nestjsConfig = mergeConfig(
  baseConfig,
  defineConfig({
    // Remove vite's default oxc transform; swc (below) becomes the sole .ts
    // transform. `esbuild: false` (what unplugin-swc sets) is inert on vite 8.1.
    oxc: false,
    plugins: [
      swc.vite({
        module: { type: "es6" },
        jsc: {
          target: "es2022",
          parser: { syntax: "typescript", decorators: true },
          transform: { legacyDecorator: true, decoratorMetadata: true },
        },
      }),
    ],
  }),
);

export default nestjsConfig;

// `ConfigurableModuleBuilder` factory for `NatsModule`.
//
// Nest 10/11 idiomatic pattern: declare the module-options shape, let the
// builder generate `forRoot` / `forRootAsync` (sync + async DI overloads,
// useFactory / useClass / useExisting) plus the `MODULE_OPTIONS_TOKEN` for
// injection. We rename the generated options token to a library-namespaced
// `NATS_OPTIONS` so consumers see `NATS_OPTIONS` everywhere, not Nest's
// generic `MODULE_OPTIONS_TOKEN`. `moduleName: "Nats"` makes the underlying
// provider token the deterministic string "NATS_MODULE_OPTIONS" instead of a
// random UUID, so DI errors that print the token stay readable (consumers
// still inject via the exported `NATS_OPTIONS` binding either way).
//
// `.setClassMethodName("forRoot")` flips the builder defaults from
// `register` / `registerAsync` to `forRoot` / `forRootAsync` — the convention
// used by `@nestjs/config`, `@nestjs/typeorm`, etc.
//
// `.setExtras({ isGlobal: true }, ...)` threads an extra `isGlobal` knob onto
// the generated methods (default `true`) that maps to the DynamicModule's
// `global` flag — the exact `@nestjs/config` recipe. The module is global by
// default (a NATS connection is an app-wide singleton), and a consumer can
// still opt out per-call with `forRoot({ config, isGlobal: false })`.
//
// See: https://docs.nestjs.com/fundamentals/dynamic-modules#configurable-module-builder

import { ConfigurableModuleBuilder } from "@nestjs/common";

import type { NatsModuleOptions } from "./nats.options.js";

export const {
  ConfigurableModuleClass: NatsConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN: NATS_OPTIONS,
} = new ConfigurableModuleBuilder<NatsModuleOptions>({ moduleName: "Nats" })
  .setClassMethodName("forRoot")
  .setExtras({ isGlobal: true }, (definition, extras) => ({
    ...definition,
    global: extras.isGlobal,
  }))
  .build();

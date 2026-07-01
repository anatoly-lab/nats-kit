// Module options for `NatsModule`.
//
// The adapter is a thin lifecycle wrapper around the framework-free
// `NatsConnectionRunner`. Its options mirror what the runner's constructor
// takes — a `NatsConfig` (validated/merged with defaults inside the runner,
// so a `Partial` is accepted) plus the optional `logger` / `telemetry` seams.
//
// DROP-IN NOTE: the former in-repo `@repo/nats` `NatsModule.forRoot(config)`
// took the config OBJECT directly. Here the config is wrapped as `{ config }`
// so the `logger` / `telemetry` seams have a home alongside it. That single
// wrapping is the one intended shape change for the re-consume swap.

import type { NatsConfig, NatsLogger, NatsTelemetry } from "@nats-kit/core";

export interface NatsModuleOptions {
  /**
   * NATS connection config. A full `NatsConfig` or a `Partial` — the runner
   * validates and merges it with `defaultNatsConfig` internally
   * (`NatsConfigSchema.parse`), so passing only the fields you care about is
   * fine.
   */
  config: NatsConfig | Partial<NatsConfig>;

  /**
   * Optional logger seam (design L1). Defaults to the core's console logger
   * when omitted.
   */
  logger?: NatsLogger;

  /**
   * Optional telemetry seam (design D7). Defaults to `noopTelemetry` when
   * omitted.
   */
  telemetry?: NatsTelemetry;
}

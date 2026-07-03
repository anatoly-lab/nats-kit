// Module options for `NatsModule`.
//
// The adapter is a thin lifecycle wrapper around the framework-free
// `NatsConnectionRunner`. Its options mirror what the runner's constructor
// takes — a `NatsConfig` (validated/merged with defaults inside the runner,
// so a `Partial` is accepted) plus the optional `logger` / `telemetry` seams.
//
// NOTE: the config is deliberately wrapped as `{ config }` rather than being
// the options object itself, so the `logger` / `telemetry` seams have a home
// alongside it.

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
   * Optional logger seam. Defaults to the core's console logger
   * when omitted.
   */
  logger?: NatsLogger;

  /**
   * Optional telemetry seam. Defaults to `noopTelemetry` when
   * omitted.
   */
  telemetry?: NatsTelemetry;
}

# @nats-kit/core

## 0.1.0

### Minor Changes

- Initial public release.

  `@nats-kit/core` — framework-agnostic NATS toolkit: `NatsConnectionRunner`
  lifecycle, `KvService` / `JetStreamService`, reconnect-resilient helpers
  (`runDurableConsumer`, `watchWithReconnect`, `subscribeWithReconnect`),
  config schema, telemetry seam, and logger abstraction. Zero framework
  dependencies.

  `@nats-kit/nestjs` — NestJS adapter: dynamic module (`forRoot` /
  `forRootAsync`), injectable `NatsService` lifecycle bridge, and
  `KvService` / `JetStreamService` providers, re-exporting core.

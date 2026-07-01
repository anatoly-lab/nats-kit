// Public surface of `@nats-kit/nestjs`.
//
// SINGLE IMPORT SURFACE (design §4.2): consumers import EVERYTHING from
// `@nats-kit/nestjs` and never need to reach into `@nats-kit/core` directly.
// The adapter's own module + injectable services come first; everything below
// is a 1:1 re-export of the core's public types/values (sourced from
// `@nats-kit/core`, which itself re-exports the `@nats-io/*` types), MINUS the
// two core class VALUES the adapter replaces with its `@Injectable`
// counterparts (`KvService`, `JetStreamService`) and the low-level
// `NatsConnectionRunner` (the `NatsService` lifecycle bridge owns it).

// ----- Adapter's own surface -----
export { NatsModule } from "./nats.module.js";
export { NatsService } from "./nats.service.js";
export { KvService } from "./kv.service.js";
export { JetStreamService } from "./jetstream.service.js";
export { NATS_OPTIONS } from "./nats.module-builder.js";
export type { NatsModuleOptions } from "./nats.options.js";

// TYPE-ONLY re-export of the core runner. `NatsService.getRunner()` returns a
// `NatsConnectionRunner`, so consumers must be able to name that type from the
// single import surface. Kept type-only ON PURPOSE: the barrel intentionally
// omits the core `NatsConnectionRunner` VALUE (the `NatsService` lifecycle
// bridge owns it), so a type-only export adds the name for annotations without
// re-introducing an ambiguous value double-export.
export type { NatsConnectionRunner } from "@nats-kit/core";

// Narrow connection interface the reconnect helpers accept. Re-exported so
// consumers can name it from the single import surface; both the runner and
// this adapter's `NatsService` structurally satisfy it.
export type { NatsConnectionLike } from "@nats-kit/core";

// ----- Re-exports from @nats-kit/core (single import surface) -----

// Configuration
export type { NatsConfig } from "@nats-kit/core";
export { NatsConfigSchema, defaultNatsConfig } from "@nats-kit/core";

// Connection status (value enum)
export { NatsConnectionStatus } from "@nats-kit/core";

// Errors
export { NatsNotConnectedError } from "@nats-kit/core";

// Logging + telemetry seams (design L1 / D7)
export type {
  NatsLogger,
  DurableConsumerLogger,
  NatsTelemetry,
} from "@nats-kit/core";
export { noopTelemetry } from "@nats-kit/core";

// Constants
export {
  DEFAULT_KV_TTL_MS,
  DEFAULT_STREAM_RETENTION,
  DEFAULT_ACK_WAIT,
  DEFAULT_MAX_ACK_PENDING,
} from "@nats-kit/core";

// KV reconnect-aware watch helper (+ types)
export { watchWithReconnect, WatchEventType } from "@nats-kit/core";
export type { WatchEvent, WatchEventTypeValue } from "@nats-kit/core";

// Core (non-JetStream) pub/sub helper (+ options)
export { subscribeWithReconnect } from "@nats-kit/core";
export type { SubscribeWithReconnectOptions } from "@nats-kit/core";

// JetStream durable-consumer helper (+ options)
export { runDurableConsumer } from "@nats-kit/core";
export type { RunDurableConsumerOptions } from "@nats-kit/core";

// Commonly used @nats-io/* types + values (re-exported by the core barrel, so
// consumers get them without a direct `@nats-io/*` dependency)
export type { KV, KvEntry, KvOptions, KvPutOptions } from "@nats-kit/core";
export type {
  ConsumerMessages,
  JsMsg,
  ConsumerConfig,
  StreamConfig,
  Subscription,
  Msg,
} from "@nats-kit/core";
export {
  RequestError,
  TimeoutError,
  NoRespondersError,
  DeliverPolicy,
  AckPolicy,
  RetentionPolicy,
  DiscardPolicy,
  StorageType,
} from "@nats-kit/core";

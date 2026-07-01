// Public surface of @nats-kit/core — the framework-free NATS toolkit.
//
// Mirrors the in-repo `@repo/nats` barrel MINUS the NestJS module, NatsCheck,
// domain stream/consumer presets, and NatsSubjects (all of which are framework-
// or product-specific and live elsewhere). The Nest lifecycle owner `NatsService`
// becomes the framework-free `NatsConnectionRunner`.

// Connection runner (was NatsService)
export { NatsConnectionRunner } from "./connection/nats-connection-runner.js";
export { NatsConnectionStatus } from "./connection/connection-status.js";

// Narrow connection interface the reconnect helpers accept (satisfied by both
// NatsConnectionRunner and the NestJS adapter's NatsService).
export type { NatsConnectionLike } from "./connection/nats-connection-like.js";

// Configuration
export type { NatsConfig } from "./config/nats.config.js";
export { NatsConfigSchema, defaultNatsConfig } from "./config/nats.config.js";

// Errors
export { NatsNotConnectedError } from "./errors/index.js";

// Logging seam (design L1)
export type { NatsLogger, DurableConsumerLogger } from "./logging/logger.types.js";

// Telemetry seam (design D7)
export type { NatsTelemetry } from "./telemetry/telemetry.types.js";
export { noopTelemetry } from "./telemetry/telemetry.types.js";

// Constants
export {
  DEFAULT_KV_TTL_MS,
  DEFAULT_STREAM_RETENTION,
  DEFAULT_ACK_WAIT,
  DEFAULT_MAX_ACK_PENDING,
} from "./constants/nats.constants.js";

// KV operations
export { KvService, watchWithReconnect, WatchEventType } from "./kv/index.js";
export type { WatchEvent, WatchEventTypeValue } from "./kv/index.js";

// Core (non-JetStream) pub/sub operations
export { subscribeWithReconnect } from "./core/index.js";
export type { SubscribeWithReconnectOptions } from "./core/index.js";

// JetStream operations
export { JetStreamService, runDurableConsumer } from "./jetstream/index.js";
export type { RunDurableConsumerOptions } from "./jetstream/index.js";

// Re-export commonly used NATS types (nats.js v3 modular packages)
export type { KV, KvEntry, KvOptions, KvPutOptions } from "@nats-io/kv";
export type {
  ConsumerMessages,
  JsMsg,
  ConsumerConfig,
  StreamConfig,
} from "@nats-io/jetstream";
export type { Subscription, Msg } from "@nats-io/transport-node";
// nats.js v3 typed request errors — used with `instanceof` to detect
// no-responders / timeout from a rejected `nc.request(...)` (replaces the
// v2 string error codes "503"/"TIMEOUT" that no longer exist in v3).
export {
  RequestError,
  TimeoutError,
  NoRespondersError,
} from "@nats-io/transport-node";
export {
  DeliverPolicy,
  AckPolicy,
  RetentionPolicy,
  DiscardPolicy,
  StorageType,
} from "@nats-io/jetstream";

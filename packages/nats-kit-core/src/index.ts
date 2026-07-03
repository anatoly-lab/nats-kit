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

// Re-export commonly used NATS types (nats.js v3 modular packages).
//
// Design rule: consumers must NEVER need a direct `@nats-io/*` dependency.
// Every @nats-io type reachable from a public signature of this package is
// re-exported here (type-only), and the values consumers need at runtime
// (headers builder, error classes for `instanceof`) are re-exported as values.
// `@nats-io/*` are regular deps (not peers) precisely so `instanceof` checks
// run against a single copy.

// KV: `KvStatus` is the return of `KvService.status()` / `kv.status()`.
export type { KV, KvEntry, KvOptions, KvPutOptions, KvStatus } from "@nats-io/kv";
export type {
  // Clients/handles returned by NatsConnectionRunner + JetStreamService
  JetStreamClient,
  JetStreamManager,
  Stream,
  Consumer,
  // Message/config types in helper + service signatures
  ConsumerMessages,
  ConsumeOptions,
  JsMsg,
  ConsumerConfig,
  StreamConfig,
  // Info/ack results returned by JetStreamService methods
  ConsumerInfo,
  StreamInfo,
  PubAck,
  JetStreamAccountStats,
} from "@nats-io/jetstream";
export type {
  // `NatsConnection` is the return of getConnection(); `MsgHdrs` pairs with
  // the `headers()` builder below. `WithRequired` appears in the
  // `createOrUpdateStream` parameter type.
  NatsConnection,
  Subscription,
  Msg,
  MsgHdrs,
  WithRequired,
} from "@nats-io/transport-node";
// `headers()` builds a `MsgHdrs` for publishing with headers — a VALUE export
// so consumers don't need `@nats-io/transport-node` themselves.
export { headers } from "@nats-io/transport-node";
// nats.js v3 typed request errors — used with `instanceof` to detect
// no-responders / timeout from a rejected `nc.request(...)` (replaces the
// v2 string error codes "503"/"TIMEOUT" that no longer exist in v3).
// `ClosedConnectionError` / `ConnectionError` are the connection-loss classes
// `withRetry` retries on (and rethrows once retries are exhausted).
export {
  RequestError,
  TimeoutError,
  NoRespondersError,
  ClosedConnectionError,
  ConnectionError,
} from "@nats-io/transport-node";
// `JetStreamApiError` is what `createOrUpdateConsumer` / `createOrUpdateStream`
// throw (and `runDurableConsumer` rejects with) when the server rejects a
// config — callers need it for `instanceof` + `.code`. `JetStreamApiCodes`
// maps the server err_codes for those `.code` checks.
export {
  JetStreamApiError,
  JetStreamApiCodes,
  DeliverPolicy,
  AckPolicy,
  RetentionPolicy,
  DiscardPolicy,
  StorageType,
} from "@nats-io/jetstream";

import { TIME_MS, TIME_NS } from "./time.constants.js";

/**
 * NATS-specific constants
 *
 * Common constants for NATS configuration. KV TTLs use milliseconds
 * because nats.js `KvOptions.ttl` is in ms (the lib internally
 * converts to nanoseconds via `nanos()` when configuring `max_age`).
 * Stream/consumer durations stay in nanoseconds because their config
 * fields are typed as `Nanos` and the lib does not convert them.
 */

/**
 * Default TTL for NATS KV buckets (2 minutes).
 * Default TTL for tunnel state in KV.
 *
 * Units: milliseconds (see file header).
 */
export const DEFAULT_KV_TTL_MS = 120 * TIME_MS.SECOND;

/**
 * Default stream retention (7 days in nanoseconds)
 * Used for user/session event streams
 */
export const DEFAULT_STREAM_RETENTION = 7 * TIME_NS.DAY;

/**
 * Default consumer ack wait time (30 seconds in nanoseconds)
 * How long to wait for message acknowledgment before redelivery
 */
export const DEFAULT_ACK_WAIT = 30 * TIME_NS.SECOND;

/**
 * Default max pending acks per consumer
 * Number of unacknowledged messages before flow control kicks in
 */
export const DEFAULT_MAX_ACK_PENDING = 100;

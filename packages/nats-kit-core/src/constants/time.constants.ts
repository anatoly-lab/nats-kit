/**
 * Generic time-unit constants (library-internal).
 *
 * Owned by the transport library so it stays self-contained and topology-
 * agnostic — it must not depend on any product/domain package. These are plain
 * unit multipliers, not domain topology.
 */

/**
 * Time constants in milliseconds.
 */
export const TIME_MS = {
  /** 1 second in milliseconds */
  SECOND: 1000,
  /** 1 minute in milliseconds (60 seconds) */
  MINUTE: 60 * 1000,
  /** 1 hour in milliseconds (60 minutes) */
  HOUR: 60 * 60 * 1000,
  /** 1 day in milliseconds (24 hours) */
  DAY: 24 * 60 * 60 * 1000,
} as const;

/**
 * Time constants in nanoseconds.
 *
 * NATS JetStream uses nanoseconds for time durations (TTL, max age, ack wait).
 */
export const TIME_NS = {
  /** 1 millisecond in nanoseconds */
  MILLISECOND: 1_000_000,
  /** 1 second in nanoseconds */
  SECOND: 1_000_000_000,
  /** 1 minute in nanoseconds (60 seconds) */
  MINUTE: 60 * 1_000_000_000,
  /** 1 hour in nanoseconds (60 minutes) */
  HOUR: 60 * 60 * 1_000_000_000,
  /** 1 day in nanoseconds (24 hours) */
  DAY: 24 * 60 * 60 * 1_000_000_000,
  /** 1 week in nanoseconds (7 days) */
  WEEK: 7 * 24 * 60 * 60 * 1_000_000_000,
} as const;

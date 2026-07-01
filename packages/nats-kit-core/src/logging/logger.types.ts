/**
 * Logger seam for the NATS core (design L1).
 *
 * A minimal, duck-typed logger contract satisfied by both a NestJS `Logger`
 * and pino-style loggers (`logger.error({ err }, "message")`). Hoisted here so
 * every core module (connection runner, KV/JetStream services, and the
 * durable-consumer / kv-watcher / subscribe-with-reconnect helpers) imports the
 * SAME logger type from one place — no JetStream-module → core cross-import.
 *
 * `debug` is optional: not every consumer logger implements it, and callers use
 * optional-chaining (`logger.debug?.(...)`) so it degrades gracefully.
 */
export interface NatsLogger {
  log(message: unknown, ...optionalParams: unknown[]): void;
  warn(message: unknown, ...optionalParams: unknown[]): void;
  error(message: unknown, ...optionalParams: unknown[]): void;
  debug?(message: unknown, ...optionalParams: unknown[]): void;
}

/**
 * Backwards-compatible alias for the original name this contract shipped under
 * inside `jetstream/durable-consumer.ts`. Kept so existing option types
 * (`RunDurableConsumerOptions.logger`) and consumers keep a familiar name.
 */
export type DurableConsumerLogger = NatsLogger;

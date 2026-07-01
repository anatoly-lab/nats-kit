/* eslint-disable no-console -- the default logger is intentionally console-based; consumers inject their own (Nest Logger / pino) to route elsewhere. */
import { type NatsLogger } from "./logger.types.js";

/**
 * Default {@link NatsLogger} used by {@link NatsConnectionRunner} when the
 * caller does not inject one. Intentionally console-based and prefixed so a
 * bare (framework-free) consumer still sees connection lifecycle output. The
 * NestJS adapter overrides this with a Nest `Logger`.
 */
export function createConsoleLogger(prefix: string): NatsLogger {
  const tag = `[${prefix}]`;
  return {
    log: (message, ...rest) => console.log(tag, message, ...rest),
    warn: (message, ...rest) => console.warn(tag, message, ...rest),
    error: (message, ...rest) => console.error(tag, message, ...rest),
    debug: (message, ...rest) => console.debug(tag, message, ...rest),
  };
}

import { type NatsLogger } from "../logging/logger.types.js";
import { type NatsTelemetry } from "./telemetry.types.js";

/**
 * Invoke a consumer-provided telemetry hook defensively.
 *
 * Telemetry sinks are injected by the consumer; a throwing sink must never
 * break the connection lifecycle or a consumer loop. This runs the hook inside
 * a try/catch, logs a swallowed throw via the optional logger (preferring
 * `debug`, falling back to `warn`, both optional-chained), and returns.
 *
 * Only the telemetry hook's OWN throw is swallowed — control flow, ordering,
 * and every other error path are unchanged. Callers pass the exact hook
 * invocation (with its optional-chaining preserved) via `invoke`.
 */
export function emitTelemetry(
  telemetry: NatsTelemetry,
  invoke: (telemetry: NatsTelemetry) => void,
  logger?: NatsLogger,
): void {
  try {
    invoke(telemetry);
  } catch (err) {
    (logger?.debug ?? logger?.warn)?.call(
      logger,
      { err },
      "NATS telemetry hook threw - ignored",
    );
  }
}

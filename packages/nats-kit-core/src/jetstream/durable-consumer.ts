import {
  JetStreamApiError,
  type ConsumeOptions,
  type ConsumerConfig,
  type ConsumerMessages,
  type JsMsg,
} from "@nats-io/jetstream";
import { type JetStreamService } from "./jetstream.service.js";
import { type NatsConnectionLike } from "../connection/nats-connection-like.js";
import { type NatsLogger } from "../logging/logger.types.js";
import { noopTelemetry, type NatsTelemetry } from "../telemetry/telemetry.types.js";
import { emitTelemetry } from "../telemetry/telemetry.util.js";

export interface RunDurableConsumerOptions {
  /** Stream to consume from (owned/created by another service). */
  stream: string;

  /**
   * Consumer configuration. `name` (or `durable_name`) must be set —
   * it is used both to create the consumer and to fetch it back.
   */
  consumerConfig: Partial<ConsumerConfig>;

  /**
   * Per-message handler.
   *
   * Disposition contract:
   * - The handler OWNS the success/skip disposition: it must `ack()`
   *   (handled) or `term()` (poison message, don't redeliver) every
   *   message it returns normally from.
   * - If the handler THROWS, the loop logs the error and `nak()`s the
   *   message (with `nakDelayMs`, if set) so JetStream redelivers it.
   */
  handler: (msg: JsMsg) => void | Promise<void>;

  /** Abort to stop consuming (graceful shutdown). */
  signal: AbortSignal;

  /** Required dependencies (mirrors watchWithReconnect taking natsService). */
  jetStreamService: JetStreamService;
  natsService: NatsConnectionLike;

  /** Optional logger for loop lifecycle and handler errors. */
  logger?: NatsLogger;

  /**
   * Optional telemetry sink. Defaults to {@link noopTelemetry}. The loop
   * emits `onConsume(subject, "nak")` when the handler throws (the only
   * disposition it observes — see the seam note in the loop body).
   */
  telemetry?: NatsTelemetry;

  /** Passed through to `consumer.consume()` (e.g. `{ max_messages: 100 }`). */
  consumeOptions?: ConsumeOptions;

  /**
   * Delay before re-creating the consumer after an error or an
   * unexpected loop exit. Default: 5000ms.
   */
  reconnectDelayMs?: number;

  /**
   * Optional delay (ms) passed to `msg.nak()` when the handler throws.
   * Omit for immediate redelivery per the consumer's backoff policy.
   */
  nakDelayMs?: number;

  /** Passed through to `waitForStream` on every (re)connect attempt. */
  streamWaitTimeoutMs?: number;
  streamWaitRetryIntervalMs?: number;
}

const DEFAULT_RECONNECT_DELAY_MS = 5000;

/**
 * Server err_codes that mean the CONSUMER CONFIG ITSELF was rejected —
 * a permanent error that no amount of retrying fixes (not exported by
 * `JetStreamApiCodes` in @nats-io/jetstream 3.4.0, hence the local constant):
 * - 10012 `JSConsumerCreateErrF` — wraps consumer config validation
 *   failures, including every immutable-field rejection ("deliver policy
 *   can not be updated", "ack policy can not be updated", ...).
 *
 * Deliberately NOT here:
 * - 10013 `JSConsumerNameExistErr`: per the official Go client mapping it is
 *   a CREATE-path "name already in use" code, i.e. transient from this
 *   loop's perspective (createOrUpdateConsumer resolves it internally; a
 *   stray one on the next iteration self-heals). Classifying it fatal would
 *   permanently halt a healthy consumer on a clustered create race — worse
 *   than the visible retry the loop does instead.
 * - 10014/10149 (consumer missing — transient, the loop recreates it) and
 *   anything that isn't a JetStreamApiError (connection-class errors —
 *   transient by definition).
 */
const FATAL_CONSUMER_CONFIG_CODES: readonly number[] = [10012];

function isConsumerConfigRejected(error: unknown): boolean {
  return (
    error instanceof JetStreamApiError &&
    FATAL_CONSUMER_CONFIG_CODES.includes(error.code)
  );
}

/**
 * runDurableConsumer
 *
 * Resilient JetStream durable-consumer loop with automatic reconnect.
 * The JetStream counterpart of `watchWithReconnect` (KV): callers get
 * hardened reconnect/abort/redelivery plumbing once, instead of
 * re-implementing it per consumer.
 *
 * Each (re)connect iteration:
 * 1. `natsService.waitForReady()` — wait for a live NATS connection
 * 2. `jetStreamService.waitForStream(stream)` — wait for the stream owner
 * 3. `createOrUpdateConsumer` — create the durable if missing, otherwise
 *    apply `consumerConfig` to it (so config changes take effect on deploy)
 * 4. `consumer.consume()` and iterate, dispatching each message to `handler`
 * 5. On error or unexpected loop exit: wait `reconnectDelayMs`, go to 1
 *
 * Aborting `signal` stops the in-flight `consume()` iterator and exits
 * the loop; the returned promise resolves once shutdown completes.
 *
 * FATAL path: if the server rejects the consumer config itself (e.g. an
 * immutable field like deliver_policy/ack_policy was changed on an existing
 * durable — `JetStreamApiError` err_code 10012), the loop does NOT
 * retry: retrying a permanent config rejection would silently halt message
 * processing in an infinite throw-sleep-retry loop. Instead the returned
 * promise REJECTS with the server's error (its message names the offending
 * field). Callers should surface that rejection — it means the config needs
 * a fix + redeploy (or the existing consumer must be deleted). Transient
 * errors (consumer/stream not found, connection loss) are still retried.
 *
 * Usage:
 * ```typescript
 * const abortController = new AbortController();
 *
 * const done = runDurableConsumer({
 *   stream: "SUBSCRIPTIONS",
 *   consumerConfig,
 *   handler: (msg) => this.handleMessage(msg), // ack/term inside, throw => nak
 *   signal: abortController.signal,
 *   jetStreamService,
 *   natsService,
 *   logger,
 * });
 *
 * // To stop consuming:
 * abortController.abort();
 * await done;
 * ```
 */
export async function runDurableConsumer(
  options: RunDurableConsumerOptions,
): Promise<void> {
  const {
    stream,
    consumerConfig,
    handler,
    signal,
    jetStreamService,
    natsService,
    logger,
    telemetry = noopTelemetry,
    consumeOptions,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
    nakDelayMs,
    streamWaitTimeoutMs,
    streamWaitRetryIntervalMs,
  } = options;

  const consumerName = consumerConfig.name ?? consumerConfig.durable_name;
  if (!consumerName) {
    throw new Error(
      "runDurableConsumer: consumerConfig must set name or durable_name",
    );
  }

  const logContext = { stream, consumer: consumerName };

  let messages: ConsumerMessages | null = null;

  // Stop the in-flight consume iterator so the for-await loop exits
  // promptly on shutdown.
  const stopMessages = () => {
    messages?.stop();
    messages = null;
  };
  signal.addEventListener("abort", stopMessages);

  try {
    while (!signal.aborted) {
      try {
        // `signal.aborted` is re-checked after EVERY awaited setup step below:
        // an abort that fires mid-await runs the abort listener while
        // `messages` is still null (stopMessages is a no-op), and the event
        // never fires again — without these re-checks the loop would proceed
        // to consume() AFTER shutdown and park in the for-await forever on an
        // idle stream. `break` runs the inner finally (stopMessages) and then
        // exits the while directly.

        // Wait for a live connection before touching JetStream.
        await natsService.waitForReady();
        if (signal.aborted) break;

        // Wait for the stream owner (no-op when the stream exists). The
        // signal makes the poll abort-aware; on abort it RESOLVES early
        // without the stream existing — covered by the re-check below.
        await jetStreamService.waitForStream(stream, {
          timeout: streamWaitTimeoutMs,
          retryInterval: streamWaitRetryIntervalMs,
          signal,
        });
        if (signal.aborted) break;

        // Idempotent: creates the consumer or applies the config to the
        // existing one. A server-side config rejection propagates to the
        // catch below and is classified as FATAL.
        await jetStreamService.createOrUpdateConsumer(stream, consumerConfig);
        if (signal.aborted) break;

        const consumer = await jetStreamService.getConsumer(
          stream,
          consumerName,
        );
        if (signal.aborted) break;

        messages = await consumer.consume(consumeOptions);
        // Last re-check AFTER `messages` is assigned: an abort during the
        // consume() await itself is the one window the listener still misses.
        if (signal.aborted) break;

        for await (const msg of messages) {
          if (signal.aborted) break;

          try {
            await handler(msg);
            // telemetry seam: successful ack/term dispositions are owned by the
            // handler (it calls `msg.ack()` / `msg.term()` itself), so the loop
            // cannot observe them without wrapping `msg` — which would be more
            // invasive than warranted. Only the nak-on-throw outcome below is
            // observable at loop level.
          } catch (error) {
            logger?.error(
              { err: error, ...logContext },
              "Error processing JetStream message - nak for redelivery",
            );
            msg.nak(nakDelayMs);
            emitTelemetry(
              telemetry,
              (t) => t.onConsume?.(msg.subject, "nak"),
              logger,
            );
          }
        }

        // Iterator ended without abort => connection/consumer was lost.
        if (!signal.aborted) {
          logger?.warn(
            logContext,
            "JetStream consumer loop exited unexpectedly - will reconnect",
          );
        }
      } catch (error) {
        if (signal.aborted) {
          logger?.log(
            logContext,
            "Consumer aborted (expected during shutdown)",
          );
          return;
        }
        // A consumer-config rejection is PERMANENT: retrying would loop
        // forever while silently processing nothing. Reject the returned
        // promise so the caller can surface it (config fix + redeploy).
        if (isConsumerConfigRejected(error)) {
          logger?.error(
            { err: error, ...logContext },
            "JetStream consumer config rejected by server - fatal, not retrying",
          );
          throw error;
        }
        logger?.error(
          { err: error, ...logContext },
          "JetStream consumer error - will reconnect",
        );
      } finally {
        stopMessages();
      }

      if (signal.aborted) {
        return;
      }

      await abortableSleep(reconnectDelayMs, signal);
    }
  } finally {
    stopMessages();
    signal.removeEventListener("abort", stopMessages);
  }
}

/**
 * Sleep that resolves early when the signal aborts.
 */
function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const onAbort = () => {
      clearTimeout(timeout);
      resolve();
    };

    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

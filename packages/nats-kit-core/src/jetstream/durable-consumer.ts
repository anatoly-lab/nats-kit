import {
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
   * Optional telemetry sink (D7). Defaults to {@link noopTelemetry}. The loop
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
 * 3. `createOrUpdateConsumer` — idempotently (re)create the durable consumer
 * 4. `consumer.consume()` and iterate, dispatching each message to `handler`
 * 5. On error or unexpected loop exit: wait `reconnectDelayMs`, go to 1
 *
 * Aborting `signal` stops the in-flight `consume()` iterator and exits
 * the loop; the returned promise resolves once shutdown completes.
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
        // Wait for a live connection before touching JetStream.
        await natsService.waitForReady();

        // Wait for the stream owner (no-op when the stream exists).
        await jetStreamService.waitForStream(stream, {
          timeout: streamWaitTimeoutMs,
          retryInterval: streamWaitRetryIntervalMs,
        });

        // Idempotent: gets the existing consumer or creates it.
        await jetStreamService.createOrUpdateConsumer(stream, consumerConfig);

        const consumer = await jetStreamService.getConsumer(
          stream,
          consumerName,
        );
        messages = await consumer.consume(consumeOptions);

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

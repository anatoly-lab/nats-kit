import { type Msg, type Subscription } from "@nats-io/transport-node";
import { type NatsConnectionLike } from "../connection/nats-connection-like.js";
import { waitForReconnectOrAbort } from "../connection/wait-for-reconnect.js";
import { type NatsLogger } from "../logging/logger.types.js";

/**
 * Options for {@link subscribeWithReconnect}.
 */
export interface SubscribeWithReconnectOptions {
  /** Core (non-JetStream) subject to subscribe to. May be a wildcard. */
  subject: string;

  /**
   * NATS connection used for `waitForReady()`, `getConnection()` and the
   * `onReconnect()` signal. Mirrors `runDurableConsumer`/`watchWithReconnect`,
   * which also take the connection by parameter.
   */
  natsService: NatsConnectionLike;

  /**
   * Per-message handler. Receives the raw `Msg` so request-reply callers can
   * call `msg.respond(...)`.
   *
   * Disposition contract: Core NATS has no ack/nak. A handler that THROWS is
   * logged and the loop CONTINUES with the next message — one bad publish never
   * tears down the subscription.
   */
  handler: (msg: Msg) => void | Promise<void>;

  /** Abort to stop subscribing and exit the loop (graceful shutdown). */
  signal: AbortSignal;

  /** Optional logger for loop lifecycle and handler errors. */
  logger?: NatsLogger;

  /**
   * Optional NATS queue-group. Set for load-balanced (one-of-N) delivery;
   * omit for broadcast (every subscriber receives every message).
   */
  queue?: string;

  /**
   * Optional fixed delay (ms) inserted after a subscription ends and before
   * waiting for the next reconnect. Default 0 — resubscription is driven by
   * `onReconnect()`, matching the existing hand-rolled Core loops.
   */
  reconnectDelayMs?: number;
}

const DEFAULT_RECONNECT_DELAY_MS = 0;

/**
 * subscribeWithReconnect
 *
 * Resilient NATS **Core** (non-JetStream) subscription loop with automatic
 * reconnect — the Core counterpart of `runDurableConsumer` (JetStream) and
 * `watchWithReconnect` (KV). Callers get hardened wait-for-ready / resubscribe /
 * abort plumbing once, instead of re-implementing it per subscription.
 *
 * Each (re)subscribe iteration:
 * 1. `natsService.waitForReady()` — wait for a live NATS connection
 * 2. `nc.subscribe(subject[, { queue }])` — (re)create the Core subscription
 * 3. iterate, dispatching every message to `handler`; a throwing handler is
 *    logged and the loop continues (Core has no ack)
 * 4. when the iterator ends without abort (connection lost / subscription
 *    closed), wait for the next `onReconnect()` tick, then resubscribe
 *
 * Aborting `signal` unsubscribes the in-flight subscription, stops the
 * for-await loop, and resolves the returned promise — even while waiting for a
 * reconnect (the wait is abortable, so a mid-life abort never strands the
 * loop). If the runner stops first (its reconnect subject completes), the loop
 * also ends cleanly — a stopped runner can never signal another reconnect.
 * Run one independent call per subject (each with its own `AbortController`)
 * to manage many subscriptions concurrently.
 *
 * Usage:
 * ```typescript
 * const abortController = new AbortController();
 *
 * const done = subscribeWithReconnect({
 *   subject: "quota.*.updated",
 *   natsService,
 *   handler: (msg) => this.handleMessage(msg),
 *   signal: abortController.signal,
 *   logger,
 * });
 *
 * // To stop subscribing:
 * abortController.abort();
 * await done;
 * ```
 */
export async function subscribeWithReconnect(
  options: SubscribeWithReconnectOptions,
): Promise<void> {
  const {
    subject,
    natsService,
    handler,
    signal,
    logger,
    queue,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
  } = options;

  const logContext = queue ? { subject, queue } : { subject };

  let subscription: Subscription | null = null;

  // Stop the in-flight subscription so the for-await loop exits promptly on
  // shutdown (mirrors runDurableConsumer's `stopMessages`).
  const stopSubscription = () => {
    subscription?.unsubscribe();
    subscription = null;
  };
  signal.addEventListener("abort", stopSubscription);

  try {
    // Loop until aborted — or until the runner stops first: a completed
    // `onReconnect()` subject is detected as a TERMINAL outcome by the
    // reconnect wait below, so a runner.stop() that beats the consumer's
    // abort ends the loop cleanly instead of spinning against a dead runner.
    while (!signal.aborted) {
      try {
        // Wait for a live connection before subscribing.
        await natsService.waitForReady();

        const nc = natsService.getConnection();
        subscription = queue
          ? nc.subscribe(subject, { queue })
          : nc.subscribe(subject);

        // The abort listener may have fired while awaiting waitForReady()
        // above — when `subscription` was still null and stopSubscription a
        // no-op — and it never fires again. Re-check now that the
        // subscription exists so shutdown can't park the for-await below on
        // a live subscription forever (the inner finally unsubscribes it).
        if (signal.aborted) break;

        for await (const msg of subscription) {
          if (signal.aborted) break;

          try {
            await handler(msg);
          } catch (error) {
            // A single bad publish must not tear down the subscription.
            logger?.error(
              { err: error, ...logContext },
              "Error handling NATS Core message - continuing",
            );
          }
        }

        // Iterator ended without abort => connection/subscription was lost.
        if (!signal.aborted) {
          logger?.warn(
            logContext,
            "NATS Core subscription ended - will resubscribe on reconnect",
          );
        }
      } catch (error) {
        if (signal.aborted) break;
        logger?.error(
          { err: error, ...logContext },
          "NATS Core subscription error - will resubscribe on reconnect",
        );
      } finally {
        stopSubscription();
      }

      if (signal.aborted) break;

      // Optional fixed backoff before waiting for reconnect (default 0).
      if (reconnectDelayMs > 0) {
        await abortableSleep(reconnectDelayMs, signal);
        if (signal.aborted) break;
      }

      // Wait for the next reconnect tick before resubscribing. Abortable, so a
      // mid-life abort exits immediately instead of stranding until the next
      // reconnect.
      const outcome = await waitForReconnectOrAbort(natsService, signal);
      if (outcome === "completed") {
        // The reconnect subject completed: the runner stopped and can never
        // signal another reconnect. Treating this like a reconnect would
        // hot-spin (waitForReady settles instantly, subscribe throws, and the
        // completed subject "signals" synchronously — a microtask-speed loop
        // at 100% CPU). Terminal: exit the loop cleanly.
        logger?.log(logContext, "NATS runner stopped - ending subscription loop");
        return;
      }
    }
  } finally {
    stopSubscription();
    signal.removeEventListener("abort", stopSubscription);
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

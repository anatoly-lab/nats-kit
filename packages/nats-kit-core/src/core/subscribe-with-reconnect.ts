import { type Msg, type Subscription } from "@nats-io/transport-node";
import { type NatsConnectionLike } from "../connection/nats-connection-like.js";
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
 * loop). Run one independent call per subject (each with its own
 * `AbortController`) to manage many subscriptions concurrently.
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
    // Clean shutdown relies on the consumer being aborted BEFORE the
    // `onReconnect()` subject completes: a framework typically destroys the
    // consumer services (which own the AbortController) before the runner
    // completes the subject, so `signal.aborted` is already true here.
    while (!signal.aborted) {
      try {
        // Wait for a live connection before subscribing.
        await natsService.waitForReady();

        const nc = natsService.getConnection();
        subscription = queue
          ? nc.subscribe(subject, { queue })
          : nc.subscribe(subject);

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
      // reconnect (or shutdown completing the subject).
      await waitForReconnectOrAbort(natsService, signal);
    }
  } finally {
    stopSubscription();
    signal.removeEventListener("abort", stopSubscription);
  }
}

/**
 * Resolve on the next `onReconnect()` emission, on abort, or when the
 * reconnect subject completes (shutdown) — never hangs.
 */
function waitForReconnectOrAbort(
  natsService: NatsConnectionLike,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }

  return new Promise<void>((resolve) => {
    let settled = false;
    // Holder so the callbacks can unsubscribe even when the subject emits
    // `complete` SYNCHRONOUSLY during `.subscribe()` (a completed RxJS subject
    // does), i.e. before the `subscription` assignment below would otherwise
    // have run. `let` (not `const`) is required precisely because `settle` reads
    // this holder before the single assignment; a `const` initialised at the
    // subscribe call would be in the TDZ during a synchronous `complete`.
    // eslint-disable-next-line prefer-const
    let subscription: { unsubscribe(): void } | undefined;

    // Function declarations (hoisted) so the two can reference each other
    // without a temporal-dead-zone hazard.
    function settle(): void {
      if (settled) return;
      settled = true;
      subscription?.unsubscribe();
      signal.removeEventListener("abort", onAbort);
      resolve();
    }
    function onAbort(): void {
      settle();
    }

    signal.addEventListener("abort", onAbort, { once: true });

    subscription = natsService.onReconnect().subscribe({
      next: settle,
      error: settle,
      complete: settle, // subject completed on shutdown
    });

    // If the subject completed synchronously above, `settle` already ran while
    // `subscription` was still undefined; unsubscribe the now-assigned holder.
    if (settled) {
      subscription.unsubscribe();
    }
  });
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

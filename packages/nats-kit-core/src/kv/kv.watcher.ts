import { type KV, type KvEntry, type KvWatchEntry } from "@nats-io/kv";
import { type QueuedIterator } from "@nats-io/transport-node";
import { type NatsConnectionLike } from "../connection/nats-connection-like.js";
import { waitForReconnectOrAbort } from "../connection/wait-for-reconnect.js";
import { type NatsLogger } from "../logging/logger.types.js";

/**
 * Watch event type constants
 */
export const WatchEventType = {
  CLEAR: "clear",
  READY: "ready",
  EVENT: "event",
  ERROR: "error",
} as const;

export type WatchEventTypeValue =
  (typeof WatchEventType)[keyof typeof WatchEventType];

/**
 * Watch event types
 */
export type WatchEvent<T> =
  | { type: typeof WatchEventType.CLEAR } // Clear cache on reconnect
  | { type: typeof WatchEventType.READY } // Initial delivery complete
  | { type: typeof WatchEventType.EVENT; data: T } // New KV change
  | { type: typeof WatchEventType.ERROR; error: Error }; // Transform error

/**
 * watchWithReconnect
 *
 * Async generator that watches a NATS KV bucket with automatic reconnect handling.
 *
 * Features:
 * - Proper lifecycle management with AbortSignal
 * - Automatic cleanup in finally block
 * - Emits 'clear' event on reconnect to trigger cache invalidation
 * - Emits 'ready' event after initial delivery
 * - Transforms KV entries using provided function
 * - Ends cleanly (generator returns) when the runner stops — a completed
 *   reconnect subject can never emit again, so waiting would strand forever
 *
 * Usage:
 * ```typescript
 * const abortController = new AbortController();
 *
 * for await (const event of watchWithReconnect(
 *   kv,
 *   natsService,
 *   (entry) => transformEntry(entry),
 *   abortController.signal
 * )) {
 *   if (event.type === WatchEventType.CLEAR) {
 *     // Clear local cache - reconnected
 *   } else if (event.type === WatchEventType.READY) {
 *     // Initial delivery complete
 *   } else if (event.type === WatchEventType.ERROR) {
 *     // Handle transform error
 *   } else {
 *     // Process event.data
 *   }
 * }
 *
 * // To stop watching:
 * abortController.abort();
 * ```
 *
 * @param kv - KV bucket to watch
 * @param natsService - NATS connection for reconnect events
 * @param transform - Function to transform KV entries
 * @param signal - AbortSignal for cancellation
 * @param logger - Optional logger; watch failures (permissions, deleted
 *   bucket, ...) are logged at error level instead of being swallowed
 * @yields Watch events (clear, ready, or data)
 */
export async function* watchWithReconnect<T>(
  kv: KV,
  natsService: NatsConnectionLike,
  transform: (entry: KvEntry) => T,
  signal?: AbortSignal,
  logger?: NatsLogger,
): AsyncGenerator<WatchEvent<T>> {
  // v3 `kv.watch()` yields `KvWatchEntry` (a `KvEntry` plus `isUpdate`), so the
  // iterator is typed accordingly; `transform` still accepts a `KvEntry` since
  // `KvWatchEntry` is assignable to it.
  let currentWatcher: QueuedIterator<KvWatchEntry> | null = null;

  // The KV interface only exposes the bucket name via the async `status()`;
  // the concrete v3 `Bucket` implementation carries it as a plain `bucket`
  // property. Read it defensively for log context only.
  const maybeBucket = (kv as unknown as { bucket?: unknown }).bucket;
  const bucketName = typeof maybeBucket === "string" ? maybeBucket : "unknown";

  // Cleanup function
  const cleanup = () => {
    if (currentWatcher) {
      currentWatcher.stop();
      currentWatcher = null;
    }
  };

  // Register signal handler
  signal?.addEventListener("abort", cleanup);

  try {
    while (!signal?.aborted) {
      let initialDeliveryComplete = false; // Reset per reconnect

      try {
        // Signal that cache should be cleared (reconnect scenario)
        yield { type: WatchEventType.CLEAR };

        // Clean up previous watcher before creating new one
        cleanup();

        // Start watching bucket
        currentWatcher = await kv.watch();

        for await (const entry of currentWatcher) {
          if (signal?.aborted) break;

          try {
            const data = transform(entry);
            yield { type: WatchEventType.EVENT, data };

            // NATS KV watch delivers all existing keys first
            // delta === 0 means we've caught up to current state
            if (!initialDeliveryComplete && entry.delta === 0) {
              initialDeliveryComplete = true;
              yield { type: WatchEventType.READY };
            }
          } catch (transformError) {
            // Yield error event instead of logging to console
            yield {
              type: WatchEventType.ERROR,
              error:
                transformError instanceof Error
                  ? transformError
                  : new Error(String(transformError)),
            };
          }
        }

        // If we exit normally (not aborted), connection was lost
        if (!signal?.aborted) {
          cleanup();
          // Wait for the reconnect event before retrying. "completed" is
          // TERMINAL — the runner stopped and can never emit again — so end
          // the generator cleanly (the consumer's for-await just finishes)
          // instead of crashing it (firstValueFrom used to reject with an
          // opaque rxjs EmptyError here).
          const outcome = await waitForReconnectOrAbort(natsService, signal);
          if (outcome === "completed") return;
        }
      } catch (error) {
        // Surface real watch failures (permissions, deleted bucket, ...) —
        // a bare catch here used to swallow them without a trace. Retry
        // behavior is unchanged: park until the next reconnect, re-watch.
        logger?.error(
          { err: error, bucket: bucketName },
          "KV watch failed - will retry on reconnect",
        );
        cleanup();
        if (!signal?.aborted) {
          // Same terminal semantics as the normal-exit path above.
          const outcome = await waitForReconnectOrAbort(natsService, signal);
          if (outcome === "completed") return;
        }
      }
    }
  } finally {
    cleanup();
    signal?.removeEventListener("abort", cleanup);
  }
}

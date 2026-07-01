import { type KV, type KvEntry, type KvWatchEntry } from "@nats-io/kv";
import { type QueuedIterator } from "@nats-io/transport-node";
import { firstValueFrom } from "rxjs";
import { type NatsConnectionLike } from "../connection/nats-connection-like.js";

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
 * @yields Watch events (clear, ready, or data)
 */
export async function* watchWithReconnect<T>(
  kv: KV,
  natsService: NatsConnectionLike,
  transform: (entry: KvEntry) => T,
  signal?: AbortSignal,
): AsyncGenerator<WatchEvent<T>> {
  // v3 `kv.watch()` yields `KvWatchEntry` (a `KvEntry` plus `isUpdate`), so the
  // iterator is typed accordingly; `transform` still accepts a `KvEntry` since
  // `KvWatchEntry` is assignable to it.
  let currentWatcher: QueuedIterator<KvWatchEntry> | null = null;

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
          // Wait for reconnect event before retrying
          await firstValueFrom(natsService.onReconnect());
        }
      } catch {
        cleanup();
        if (!signal?.aborted) {
          // Wait for reconnect event before retrying
          await firstValueFrom(natsService.onReconnect());
        }
      }
    }
  } finally {
    cleanup();
    signal?.removeEventListener("abort", cleanup);
  }
}

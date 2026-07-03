import { type Observable } from "rxjs";

/**
 * Outcome of {@link waitForReconnectOrAbort}:
 *
 * - `"reconnect"`: the reconnect subject emitted — a new connection is up and
 *   the caller's loop should re-subscribe / re-watch.
 * - `"aborted"`: the caller's signal aborted — graceful shutdown of the loop.
 * - `"completed"`: the reconnect subject completed (or errored) — the runner
 *   stopped. TERMINAL: a completed/errored subject can never emit again, so
 *   waiting on it again is meaningless — and because a completed RxJS subject
 *   re-signals `complete` SYNCHRONOUSLY to every new subscriber, a loop that
 *   treats this like a reconnect hot-spins at microtask speed. Callers must
 *   exit their loop.
 */
export type ReconnectWaitOutcome = "reconnect" | "aborted" | "completed";

/**
 * Resolve on the next `onReconnect()` emission, on abort, or when the
 * reconnect subject completes (runner shutdown) — never hangs — and report
 * WHICH of the three happened so callers can tell a real reconnect apart from
 * a terminal shutdown (see {@link ReconnectWaitOutcome}).
 *
 * Shared by `subscribeWithReconnect` (Core) and `watchWithReconnect` (KV) so
 * both loops get identical terminal-completion semantics.
 */
export function waitForReconnectOrAbort(
  natsService: { onReconnect(): Observable<void> },
  signal?: AbortSignal,
): Promise<ReconnectWaitOutcome> {
  if (signal?.aborted) {
    return Promise.resolve("aborted");
  }

  return new Promise<ReconnectWaitOutcome>((resolve) => {
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
    function settle(outcome: ReconnectWaitOutcome): void {
      if (settled) return;
      settled = true;
      subscription?.unsubscribe();
      signal?.removeEventListener("abort", onAbort);
      resolve(outcome);
    }
    function onAbort(): void {
      settle("aborted");
    }

    signal?.addEventListener("abort", onAbort, { once: true });

    subscription = natsService.onReconnect().subscribe({
      next: () => settle("reconnect"),
      // An errored subject is exactly as terminal as a completed one (it can
      // never emit again), so both map to "completed".
      error: () => settle("completed"),
      complete: () => settle("completed"),
    });

    // If the subject completed synchronously above, `settle` already ran while
    // `subscription` was still undefined; unsubscribe the now-assigned holder.
    if (settled) {
      subscription.unsubscribe();
    }
  });
}

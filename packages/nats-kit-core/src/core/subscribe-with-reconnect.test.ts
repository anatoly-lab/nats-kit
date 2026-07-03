/**
 * Ported from the in-repo PR4 regression test for `subscribeWithReconnect`,
 * adapted to vitest and the `natsService` option name.
 *
 * THE BUG the helper fixes: a hand-rolled Core subscriber that subscribes once
 * and, when the iterator ends (a drop / background-retry reconnect), simply
 * returns — never resubscribing, so messages silently stop.
 *
 * THE FIX: `subscribeWithReconnect` wraps subscribe/iterate in a resilient loop
 * that waits for the next `onReconnect()` tick and resubscribes.
 */
import { describe, it, expect, vi } from "vitest";
import { Subject } from "rxjs";
import { subscribeWithReconnect } from "./subscribe-with-reconnect.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import type { Msg } from "@nats-io/transport-node";

/** Controllable async-iterable stand-in for a NATS Core `Subscription`. */
class FakeSubscription {
  private queue: Msg[] = [];
  private resolveNext: ((r: IteratorResult<Msg>) => void) | null = null;
  private ended = false;

  push(msg: Msg): void {
    if (this.ended) return;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: msg, done: false });
    } else {
      this.queue.push(msg);
    }
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: undefined as unknown as Msg, done: true });
    }
  }

  unsubscribe(): void {
    this.end();
  }

  [Symbol.asyncIterator](): AsyncIterator<Msg> {
    return {
      next: (): Promise<IteratorResult<Msg>> => {
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false });
        }
        if (this.ended) {
          return Promise.resolve({
            value: undefined as unknown as Msg,
            done: true,
          });
        }
        return new Promise((resolve) => {
          this.resolveNext = resolve;
        });
      },
    };
  }
}

async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

function makeMsg(payload: string): Msg {
  return {
    subject: "session.kick",
    data: new TextEncoder().encode(payload),
  } as unknown as Msg;
}

function makeRunner(reconnect$: Subject<void>, connection: unknown) {
  return {
    waitForReady: vi.fn().mockResolvedValue(undefined),
    getConnection: vi.fn(() => connection),
    onReconnect: vi.fn(() => reconnect$.asObservable()),
  } as unknown as NatsConnectionRunner;
}

describe("subscribeWithReconnect", () => {
  it("resubscribes after a drop+reconnect so messages keep flowing", async () => {
    const reconnect$ = new Subject<void>();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    const natsService = makeRunner(reconnect$, connection);

    const handled: string[] = [];
    const decoder = new TextDecoder();
    const handler = vi.fn((msg: Msg) => {
      handled.push(decoder.decode(msg.data));
    });

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler,
      signal: abort.signal,
    });

    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    subscriptions[0]!.push(makeMsg("first"));
    await tick();
    expect(handled).toEqual(["first"]);

    // Drop: iterator ends. No abort → loop parks waiting for reconnect.
    subscriptions[0]!.end();
    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    // Reconnect → resubscribe on a fresh subscription.
    reconnect$.next();
    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(2);

    subscriptions[1]!.push(makeMsg("second"));
    await tick();
    expect(handled).toEqual(["first", "second"]);

    abort.abort();
    await expect(done).resolves.toBeUndefined();
  });

  it("logs and continues when a handler throws (Core has no ack)", async () => {
    const reconnect$ = new Subject<void>();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    const natsService = makeRunner(reconnect$, connection);

    const handled: string[] = [];
    const decoder = new TextDecoder();
    const handler = vi.fn((msg: Msg) => {
      const text = decoder.decode(msg.data);
      if (text === "boom") throw new Error("handler failure");
      handled.push(text);
    });
    const logger = {
      log: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    };

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler,
      signal: abort.signal,
      logger,
    });

    await tick();
    subscriptions[0]!.push(makeMsg("boom"));
    await tick();
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    subscriptions[0]!.push(makeMsg("after-boom"));
    await tick();
    expect(handled).toEqual(["after-boom"]);

    abort.abort();
    await expect(done).resolves.toBeUndefined();
  });

  it("treats an already-completed onReconnect() as terminal: loop ends, no resubscribe, no TDZ crash", async () => {
    const reconnect$ = new Subject<void>();
    reconnect$.complete();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    const natsService = makeRunner(reconnect$, connection);

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler: vi.fn(),
      signal: abort.signal,
    });

    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    // Drop → the wait observes the completed subject SYNCHRONOUSLY (the TDZ
    // guard) and reports it as TERMINAL: the loop exits WITHOUT abort and
    // never resubscribes. (The old behavior — treating complete like a
    // reconnect tick — resubscribed here and hot-spun once the runner was
    // stopped for real.)
    subscriptions[0]!.end();
    await expect(done).resolves.toBeUndefined();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);
    expect(abort.signal.aborted).toBe(false);
  });

  it("exits without spinning when the runner stops while the loop is alive and the signal is NOT aborted", async () => {
    const reconnect$ = new Subject<void>();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    const natsService = makeRunner(reconnect$, connection);

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler: vi.fn(),
      signal: abort.signal,
    });

    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    // runner.stop(): the reconnect subject completes, then the live
    // subscription's iterator ends (connection closed).
    reconnect$.complete();
    subscriptions[0]!.end();

    // The loop must EXIT cleanly — not resubscribe against a dead runner in a
    // microtask-speed spin. Bounded: attempts do not grow after completion.
    await expect(done).resolves.toBeUndefined();
    await tick(10);
    expect(connection.subscribe).toHaveBeenCalledTimes(1);
  });

  it("does not park in for-await when abort fires while awaiting waitForReady", async () => {
    const reconnect$ = new Subject<void>();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    let resolveReady!: () => void;
    const natsService = {
      waitForReady: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            resolveReady = resolve;
          }),
      ),
      getConnection: vi.fn(() => connection),
      onReconnect: vi.fn(() => reconnect$.asObservable()),
    } as unknown as NatsConnectionRunner;

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler: vi.fn(),
      signal: abort.signal,
    });

    await tick();
    // Abort fires while waitForReady is pending — the abort listener runs
    // while `subscription` is still null (a no-op) and never fires again.
    abort.abort();
    resolveReady();

    // The post-subscribe re-check must exit the loop instead of parking the
    // for-await on a live subscription forever.
    await expect(done).resolves.toBeUndefined();
  });

  it("resolves without resubscribing when aborted while parked in the reconnect wait", async () => {
    const reconnect$ = new Subject<void>();
    const subscriptions: FakeSubscription[] = [];
    const connection = {
      subscribe: vi.fn(() => {
        const sub = new FakeSubscription();
        subscriptions.push(sub);
        return sub;
      }),
    };
    const natsService = makeRunner(reconnect$, connection);

    const abort = new AbortController();
    const done = subscribeWithReconnect({
      subject: "session.kick",
      natsService,
      handler: vi.fn(),
      signal: abort.signal,
    });

    await tick();
    subscriptions[0]!.end();
    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    // Abort WITHOUT a reconnect: the abortable wait resolves and the loop exits
    // without resubscribing.
    abort.abort();
    await expect(done).resolves.toBeUndefined();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);
  });
});

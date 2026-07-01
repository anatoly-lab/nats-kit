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
    subject: "tunnel.kick",
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
      subject: "tunnel.kick",
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
      subject: "tunnel.kick",
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

  it("handles an already-completed onReconnect() during the reconnect wait (no TDZ)", async () => {
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
      subject: "tunnel.kick",
      natsService,
      handler: vi.fn(),
      signal: abort.signal,
    });

    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(1);

    // Drop → helper subscribes to the already-completed subject; `complete`
    // fires synchronously and the wait resolves WITHOUT throwing (TDZ guard).
    subscriptions[0]!.end();
    await tick();
    expect(connection.subscribe).toHaveBeenCalledTimes(2);

    abort.abort();
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
      subject: "tunnel.kick",
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

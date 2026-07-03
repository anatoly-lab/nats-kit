import { describe, it, expect, vi } from "vitest";
import { runDurableConsumer } from "./durable-consumer.js";
import type { JetStreamService } from "./jetstream.service.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import type { JsMsg } from "@nats-io/jetstream";

/** Controllable async-iterable stand-in for `consumer.consume()`. */
class FakeMessages {
  private queue: JsMsg[] = [];
  private resolveNext: ((r: IteratorResult<JsMsg>) => void) | null = null;
  private ended = false;
  stopCalled = false;

  push(msg: JsMsg): void {
    if (this.ended) return;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: msg, done: false });
    } else {
      this.queue.push(msg);
    }
  }

  stop(): void {
    this.stopCalled = true;
    this.ended = true;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: undefined as unknown as JsMsg, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<JsMsg> {
    return {
      next: (): Promise<IteratorResult<JsMsg>> => {
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false });
        }
        if (this.ended) {
          return Promise.resolve({
            value: undefined as unknown as JsMsg,
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

function makeMsg(subject: string) {
  return {
    subject,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
  };
}

async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

function makeDeps(messages: FakeMessages) {
  const jetStreamService = {
    waitForStream: vi.fn().mockResolvedValue(undefined),
    createOrUpdateConsumer: vi.fn().mockResolvedValue({}),
    getConsumer: vi.fn().mockResolvedValue({
      consume: vi.fn().mockResolvedValue(messages),
    }),
  } as unknown as JetStreamService;
  const natsService = {
    waitForReady: vi.fn().mockResolvedValue(undefined),
  } as unknown as NatsConnectionRunner;
  return { jetStreamService, natsService };
}

describe("runDurableConsumer", () => {
  it("throws if the consumer config has no name/durable_name", async () => {
    const messages = new FakeMessages();
    const { jetStreamService, natsService } = makeDeps(messages);
    await expect(
      runDurableConsumer({
        stream: "S",
        consumerConfig: {},
        handler: vi.fn(),
        signal: new AbortController().signal,
        jetStreamService,
        natsService,
      }),
    ).rejects.toThrow(/must set name or durable_name/);
  });

  it("dispatches messages to the handler; handler owns ack/term", async () => {
    const messages = new FakeMessages();
    const { jetStreamService, natsService } = makeDeps(messages);

    const handled: string[] = [];
    const handler = vi.fn((msg: JsMsg) => {
      handled.push(msg.subject);
      msg.ack();
    });

    const abort = new AbortController();
    const done = runDurableConsumer({
      stream: "S",
      consumerConfig: { name: "c" },
      handler,
      signal: abort.signal,
      jetStreamService,
      natsService,
    });

    await tick();
    const m1 = makeMsg("subj.one");
    messages.push(m1 as unknown as JsMsg);
    await tick();
    expect(handled).toEqual(["subj.one"]);
    expect(m1.ack).toHaveBeenCalledTimes(1);

    abort.abort();
    await expect(done).resolves.toBeUndefined();
    expect(messages.stopCalled).toBe(true);
  });

  it("naks and emits onConsume(subject, 'nak') telemetry when the handler throws", async () => {
    const messages = new FakeMessages();
    const { jetStreamService, natsService } = makeDeps(messages);
    const telemetry = { onConsume: vi.fn() };

    const handler = vi.fn(() => {
      throw new Error("processing failed");
    });

    const abort = new AbortController();
    const done = runDurableConsumer({
      stream: "S",
      consumerConfig: { name: "c" },
      handler,
      signal: abort.signal,
      jetStreamService,
      natsService,
      telemetry,
      nakDelayMs: 250,
    });

    await tick();
    const m = makeMsg("subj.bad");
    messages.push(m as unknown as JsMsg);
    await tick();

    expect(m.nak).toHaveBeenCalledWith(250);
    expect(telemetry.onConsume).toHaveBeenCalledWith("subj.bad", "nak");
    // The loop keeps running after a nak (subscription not torn down).
    const m2 = makeMsg("subj.next");
    messages.push(m2 as unknown as JsMsg);
    await tick();
    expect(handler).toHaveBeenCalledTimes(2);

    abort.abort();
    await expect(done).resolves.toBeUndefined();
  });

  it("returns promptly when abort fires during waitForStream, without ever calling consume()", async () => {
    let resolveWaitForStream!: () => void;
    const consume = vi.fn().mockResolvedValue(new FakeMessages());
    const createOrUpdateConsumer = vi.fn().mockResolvedValue({});
    const jetStreamService = {
      // Controllable hang: the loop is parked here when the abort fires.
      waitForStream: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            resolveWaitForStream = resolve;
          }),
      ),
      createOrUpdateConsumer,
      getConsumer: vi.fn().mockResolvedValue({ consume }),
    } as unknown as JetStreamService;
    const natsService = {
      waitForReady: vi.fn().mockResolvedValue(undefined),
    } as unknown as NatsConnectionRunner;

    const abort = new AbortController();
    const done = runDurableConsumer({
      stream: "S",
      consumerConfig: { name: "c" },
      handler: vi.fn(),
      signal: abort.signal,
      jetStreamService,
      natsService,
    });

    await tick();
    // Abort while parked in waitForStream: the abort listener runs while
    // `messages` is null (stopMessages is a no-op) and never fires again.
    abort.abort();
    // The (abort-aware) waitForStream resolves early; the loop must bail
    // BEFORE creating/consuming instead of starting to consume post-shutdown.
    resolveWaitForStream();

    await expect(done).resolves.toBeUndefined();
    expect(createOrUpdateConsumer).not.toHaveBeenCalled();
    expect(consume).not.toHaveBeenCalled();
  });

  it("stops the fresh message iterator when abort fires between consumer setup and consume() resolving", async () => {
    const messages = new FakeMessages();
    let resolveConsume!: (m: FakeMessages) => void;
    const consume = vi.fn(
      () =>
        new Promise<FakeMessages>((resolve) => {
          resolveConsume = resolve;
        }),
    );
    const jetStreamService = {
      waitForStream: vi.fn().mockResolvedValue(undefined),
      createOrUpdateConsumer: vi.fn().mockResolvedValue({}),
      getConsumer: vi.fn().mockResolvedValue({ consume }),
    } as unknown as JetStreamService;
    const natsService = {
      waitForReady: vi.fn().mockResolvedValue(undefined),
    } as unknown as NatsConnectionRunner;

    const abort = new AbortController();
    const done = runDurableConsumer({
      stream: "S",
      consumerConfig: { name: "c" },
      handler: vi.fn(),
      signal: abort.signal,
      jetStreamService,
      natsService,
    });

    await tick();
    expect(consume).toHaveBeenCalledTimes(1);
    // Abort while consume() is in flight — the last window the abort listener
    // misses (`messages` still null when it runs).
    abort.abort();
    resolveConsume(messages);

    // The post-assignment re-check must stop the fresh iterator and exit
    // instead of parking the for-await on an idle stream forever.
    await expect(done).resolves.toBeUndefined();
    expect(messages.stopCalled).toBe(true);
  });
});

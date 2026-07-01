import { describe, it, expect, vi } from "vitest";
import { Subject } from "rxjs";
import { watchWithReconnect, WatchEventType } from "./kv.watcher.js";
import type { WatchEvent } from "./kv.watcher.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import type { KV, KvEntry } from "@nats-io/kv";

/** Controllable async-iterable stand-in for a `kv.watch()` QueuedIterator. */
class FakeWatcher {
  private queue: KvEntry[] = [];
  private resolveNext: ((r: IteratorResult<KvEntry>) => void) | null = null;
  private ended = false;
  stopCalled = false;

  push(entry: KvEntry): void {
    if (this.ended) return;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: entry, done: false });
    } else {
      this.queue.push(entry);
    }
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: undefined as unknown as KvEntry, done: true });
    }
  }

  stop(): void {
    this.stopCalled = true;
    this.end();
  }

  [Symbol.asyncIterator](): AsyncIterator<KvEntry> {
    return {
      next: (): Promise<IteratorResult<KvEntry>> => {
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false });
        }
        if (this.ended) {
          return Promise.resolve({
            value: undefined as unknown as KvEntry,
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

function entry(key: string, delta: number): KvEntry {
  return { key, delta } as unknown as KvEntry;
}

async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

describe("watchWithReconnect", () => {
  it("emits CLEAR, then EVENT(s), then READY on delta 0, and re-CLEARs after reconnect", async () => {
    const reconnect$ = new Subject<void>();
    const watchers: FakeWatcher[] = [];
    const kv = {
      watch: vi.fn(async () => {
        const w = new FakeWatcher();
        watchers.push(w);
        return w;
      }),
    } as unknown as KV;
    const natsService = {
      onReconnect: vi.fn(() => reconnect$.asObservable()),
    } as unknown as NatsConnectionRunner;

    const abort = new AbortController();
    const events: WatchEvent<string>[] = [];

    const consume = (async () => {
      for await (const ev of watchWithReconnect(
        kv,
        natsService,
        (e: KvEntry) => e.key,
        abort.signal,
      )) {
        events.push(ev);
      }
    })();

    await tick();
    // First: CLEAR yielded before the watch begins.
    expect(events[0]).toEqual({ type: WatchEventType.CLEAR });

    watchers[0]!.push(entry("a", 1));
    await tick();
    watchers[0]!.push(entry("b", 0)); // delta 0 → READY after the EVENT
    await tick();

    const types = events.map((e) => e.type);
    expect(types).toContain(WatchEventType.EVENT);
    expect(types).toContain(WatchEventType.READY);
    // READY comes after the b EVENT.
    const eventDatas = events
      .filter((e) => e.type === WatchEventType.EVENT)
      .map((e) => (e as { data: string }).data);
    expect(eventDatas).toEqual(["a", "b"]);

    // Simulate a connection drop: the watcher iterator ends → the generator
    // waits for onReconnect(), then yields a fresh CLEAR and re-watches.
    watchers[0]!.end();
    await tick();
    reconnect$.next();
    await tick();
    expect(kv.watch).toHaveBeenCalledTimes(2);
    const clears = events.filter((e) => e.type === WatchEventType.CLEAR);
    expect(clears.length).toBeGreaterThanOrEqual(2);

    abort.abort();
    await tick();
    await consume;
    expect(watchers[watchers.length - 1]!.stopCalled).toBe(true);
  });

  it("yields an ERROR event when the transform throws, without tearing down", async () => {
    const reconnect$ = new Subject<void>();
    const watchers: FakeWatcher[] = [];
    const kv = {
      watch: vi.fn(async () => {
        const w = new FakeWatcher();
        watchers.push(w);
        return w;
      }),
    } as unknown as KV;
    const natsService = {
      onReconnect: vi.fn(() => reconnect$.asObservable()),
    } as unknown as NatsConnectionRunner;

    const abort = new AbortController();
    const events: WatchEvent<string>[] = [];
    const consume = (async () => {
      for await (const ev of watchWithReconnect(
        kv,
        natsService,
        (e: KvEntry) => {
          if (e.key === "bad") throw new Error("transform boom");
          return e.key;
        },
        abort.signal,
      )) {
        events.push(ev);
      }
    })();

    await tick();
    watchers[0]!.push(entry("bad", 1));
    await tick();
    watchers[0]!.push(entry("good", 5));
    await tick();

    const errorEvents = events.filter((e) => e.type === WatchEventType.ERROR);
    expect(errorEvents).toHaveLength(1);
    const goodEvents = events
      .filter((e) => e.type === WatchEventType.EVENT)
      .map((e) => (e as { data: string }).data);
    expect(goodEvents).toEqual(["good"]);

    abort.abort();
    await tick();
    await consume;
  });
});

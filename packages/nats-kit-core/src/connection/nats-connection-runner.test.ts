import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock the two @nats-io modules the runner touches at runtime: `connect` (dial)
// and `jetstream` (client factory). Everything else the runner imports (zod
// config, status enum, error, logging, telemetry) is real.
const { connectMock, jetstreamMock } = vi.hoisted(() => ({
  connectMock: vi.fn(),
  jetstreamMock: vi.fn(() => ({})),
}));
vi.mock("@nats-io/transport-node", () => ({ connect: connectMock }));
vi.mock("@nats-io/jetstream", () => ({ jetstream: jetstreamMock }));

import { NatsConnectionRunner } from "./nats-connection-runner.js";
import { NatsConnectionStatus } from "./connection-status.js";
import { NatsNotConnectedError } from "../errors/index.js";
import type { NatsLogger } from "../logging/logger.types.js";

// ---- test doubles -------------------------------------------------------

interface StatusEvent {
  type: "disconnect" | "reconnecting" | "reconnect" | "error" | "ldm";
  error?: Error;
}

/** Pushable async-iterable stand-in for `nc.status()`. */
class FakeStatus {
  private queue: StatusEvent[] = [];
  private resolveNext: ((r: IteratorResult<StatusEvent>) => void) | null = null;
  private ended = false;

  push(ev: StatusEvent): void {
    if (this.ended) return;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: ev, done: false });
    } else {
      this.queue.push(ev);
    }
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve({ value: undefined as unknown as StatusEvent, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<StatusEvent> {
    return {
      next: (): Promise<IteratorResult<StatusEvent>> => {
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false });
        }
        if (this.ended) {
          return Promise.resolve({
            value: undefined as unknown as StatusEvent,
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

class FakeConnection {
  statusStream = new FakeStatus();
  closed = false;
  drainCalled = false;
  closeCalled = false;
  getServer(): string {
    return "nats://fake:4222";
  }
  status(): FakeStatus {
    return this.statusStream;
  }
  isClosed(): boolean {
    return this.closed;
  }
  async drain(): Promise<void> {
    this.drainCalled = true;
  }
  async close(): Promise<void> {
    this.closeCalled = true;
    this.closed = true;
    // Closing ends the status iterator (as the real client does), so the
    // background monitor loop terminates and `stop()` can await it.
    this.statusStream.end();
  }
}

class Deferred<T> {
  promise: Promise<T>;
  resolve!: (value: T) => void;
  reject!: (err: unknown) => void;
  constructor() {
    this.promise = new Promise<T>((res, rej) => {
      this.resolve = res;
      this.reject = rej;
    });
  }
}

const silentLogger: NatsLogger = {
  log: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

/** Flush pending microtasks so the background status monitor advances. */
async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

beforeEach(() => {
  connectMock.mockReset();
  jetstreamMock.mockReset();
  jetstreamMock.mockReturnValue({});
});

afterEach(() => {
  vi.useRealTimers();
});

describe("NatsConnectionRunner — lifecycle", () => {
  it("start() connects, marks Connected, and waitForReady resolves; onConnect fires once", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    const connects: number[] = [];
    runner.onConnect().subscribe(() => connects.push(1));

    await runner.start();

    expect(runner.isConnected()).toBe(true);
    expect(runner.getStatus()).toBe(NatsConnectionStatus.Connected);
    await expect(runner.waitForReady()).resolves.toBeUndefined();
    expect(connects).toHaveLength(1);

    await runner.stop();
    expect(conn.drainCalled).toBe(true);
    expect(conn.closeCalled).toBe(true);
    expect(runner.getStatus()).toBe(NatsConnectionStatus.Closed);
  });

  it("mints a fresh ready promise on disconnect so a later waitForReady awaits the NEXT connect", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const telemetry = { onReconnect: vi.fn() };
    const runner = new NatsConnectionRunner(
      {},
      { logger: silentLogger, telemetry },
    );

    await runner.start();
    await runner.waitForReady(); // settles the initial ready promise

    conn.statusStream.push({ type: "disconnect" });
    await tick();
    expect(runner.isConnected()).toBe(false);

    // Fresh pending promise → an explicit small timeout REJECTS instead of
    // resolving instantly off the stale (already-settled) promise.
    await expect(runner.waitForReady(20)).rejects.toBeInstanceOf(
      NatsNotConnectedError,
    );

    conn.statusStream.push({ type: "reconnect" });
    await tick();
    expect(runner.isConnected()).toBe(true);
    expect(telemetry.onReconnect).toHaveBeenCalledTimes(1);
    await expect(runner.waitForReady()).resolves.toBeUndefined();

    await runner.stop();
  });

  it("fires connectSubject + reconnectSubject (and onReconnect telemetry) on a background-retry first connect (attempt > 1)", async () => {
    vi.useFakeTimers();
    const conn = new FakeConnection();
    connectMock
      .mockRejectedValueOnce(new Error("dial failed"))
      .mockResolvedValueOnce(conn);
    const telemetry = { onReconnect: vi.fn(), onError: vi.fn() };
    const runner = new NatsConnectionRunner(
      {},
      { logger: silentLogger, telemetry },
    );

    const connects: number[] = [];
    const reconnects: number[] = [];
    runner.onConnect().subscribe(() => connects.push(1));
    runner.onReconnect().subscribe(() => reconnects.push(1));

    await runner.start(); // attempt 1 fails → schedules retry
    expect(runner.isConnected()).toBe(false);
    expect(telemetry.onError).toHaveBeenCalledWith("connect", expect.any(Error));

    await vi.advanceTimersByTimeAsync(2000); // retry (attempt 2) succeeds
    expect(runner.isConnected()).toBe(true);
    // connectSubject fires on the (background-retry) first connect...
    expect(connects.length).toBeGreaterThanOrEqual(1);
    // ...and because attempt > 1, reconnectSubject + onReconnect telemetry too.
    expect(reconnects).toHaveLength(1);
    expect(telemetry.onReconnect).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
    await runner.stop();
  });

  it("closes an orphaned late-arriving connection when the outer timeout wins the race", async () => {
    vi.useFakeTimers();
    const orphan = new FakeConnection();
    const deferred = new Deferred<FakeConnection>();
    connectMock
      .mockReturnValueOnce(deferred.promise) // attempt 1: resolves LATE (past outer timeout)
      .mockReturnValue(new Promise(() => {})); // any retry: hang
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    const startP = runner.start();
    // dial timeout defaults to 10_000 → outer bound 15_000; advance past it.
    await vi.advanceTimersByTimeAsync(15000);
    await startP;
    expect(runner.isConnected()).toBe(false);

    // The late connection finally resolves; the leak-guard must close it.
    deferred.resolve(orphan);
    await Promise.resolve();
    await Promise.resolve();
    expect(orphan.closeCalled).toBe(true);

    vi.useRealTimers();
    await runner.stop();
  });

  it("re-fires connectSubject on the 'reconnect' status case", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    const connects: number[] = [];
    runner.onConnect().subscribe(() => connects.push(1));

    await runner.start();
    expect(connects).toHaveLength(1); // first connect

    // Drive a `reconnect` status event; the monitorStatus reconnect branch
    // re-signals connectSubject (alongside resolveReady + reconnectSubject) so
    // one-time-per-connection setup re-runs after a reconnect.
    conn.statusStream.push({ type: "reconnect" });
    await tick();

    expect(runner.isConnected()).toBe(true);
    // Regression guard: if the reconnect branch stops calling
    // connectSubject.next(), this stays at 1.
    expect(connects).toHaveLength(2);

    await runner.stop();
  });

  it("stop() rejects an unsettled readyPromise so in-flight waitForReady callers unblock", async () => {
    // connect never succeeds → readyPromise stays pending (never connected).
    connectMock.mockRejectedValue(new Error("dial failed"));
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    await runner.start(); // attempt fails, schedules retry, ready still pending
    expect(runner.isConnected()).toBe(false);

    // A caller awaiting readiness with an explicit (hard-reject) timeout. The
    // shutdown path must reject the unsettled ready with the SHUTDOWN reason,
    // not let this hang until the 1000ms timeout fires.
    const assertion = expect(runner.waitForReady(1000)).rejects.toThrow(
      /shutting down/i,
    );

    await runner.stop();
    await assertion;
  });
});

describe("NatsConnectionRunner — waitForReady bounds", () => {
  it("resolves-degraded on the DEFAULT timeout (no explicit arg)", async () => {
    vi.useFakeTimers();
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    let resolved = false;
    const p = runner.waitForReady().then(() => {
      resolved = true;
    });

    await vi.advanceTimersByTimeAsync(29999);
    expect(resolved).toBe(false);
    await vi.advanceTimersByTimeAsync(1); // hits DEFAULT_READY_TIMEOUT_MS (30_000)
    await p;
    expect(resolved).toBe(true);
  });

  it("REJECTS on an explicit timeout", async () => {
    vi.useFakeTimers();
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });

    const assertion = expect(runner.waitForReady(500)).rejects.toBeInstanceOf(
      NatsNotConnectedError,
    );
    await vi.advanceTimersByTimeAsync(500);
    await assertion;
  });
});

describe("NatsConnectionRunner — guarded / required / withRetry", () => {
  it("guarded SKIPS (onSkip, returns undefined) when not connected", async () => {
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });
    const op = vi.fn();
    const onSkip = vi.fn();

    const result = await runner.guarded(op, { onSkip });

    expect(result).toBeUndefined();
    expect(op).not.toHaveBeenCalled();
    expect(onSkip).toHaveBeenCalledTimes(1);
  });

  it("required THROWS NatsNotConnectedError when not connected", async () => {
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });
    await expect(runner.required(() => 1)).rejects.toBeInstanceOf(
      NatsNotConnectedError,
    );
  });

  it("guarded / required RUN the op when connected", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });
    await runner.start();

    expect(await runner.guarded(() => "g")).toBe("g");
    expect(await runner.required(() => "r")).toBe("r");

    await runner.stop();
  });

  it("withRetry RETRIES a connection-shaped error then succeeds", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });
    await runner.start();

    let calls = 0;
    const op = vi.fn(() => {
      calls += 1;
      if (calls === 1) throw new Error("connection closed");
      return "ok";
    });

    const result = await runner.withRetry(op, {
      maxRetries: 3,
      baseDelayMs: 1,
    });
    expect(result).toBe("ok");
    expect(op).toHaveBeenCalledTimes(2);

    await runner.stop();
  });

  it("withRetry does NOT retry a non-connection error", async () => {
    const conn = new FakeConnection();
    connectMock.mockResolvedValueOnce(conn);
    const runner = new NatsConnectionRunner({}, { logger: silentLogger });
    await runner.start();

    const op = vi.fn(() => {
      throw new Error("business rule violation");
    });

    await expect(
      runner.withRetry(op, { maxRetries: 3, baseDelayMs: 1 }),
    ).rejects.toThrow("business rule violation");
    expect(op).toHaveBeenCalledTimes(1);

    await runner.stop();
  });
});

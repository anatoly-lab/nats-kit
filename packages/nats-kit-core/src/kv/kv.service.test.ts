import { describe, it, expect, vi, beforeEach } from "vitest";
import { Subject } from "rxjs";

// Mock the KV manager: `new Kvm(js).create(name, opts)` returns a fresh fake
// bucket. `createMock` lets us count (re)fetches.
const { createMock } = vi.hoisted(() => ({
  createMock: vi.fn(),
}));
vi.mock("@nats-io/kv", () => ({
  Kvm: class {
    create(name: string, opts?: unknown) {
      return createMock(name, opts);
    }
  },
}));

import { KvService } from "./kv.service.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import type { NatsLogger } from "../logging/logger.types.js";

const silentLogger: NatsLogger = {
  log: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

function makeRunner(reconnect$: Subject<void>) {
  return {
    getLogger: () => silentLogger,
    waitForReady: vi.fn().mockResolvedValue(undefined),
    getJetStream: vi.fn(() => ({})),
    onReconnect: () => reconnect$.asObservable(),
  } as unknown as NatsConnectionRunner;
}

beforeEach(() => {
  createMock.mockReset();
  createMock.mockImplementation((name: string) => ({ bucket: name }));
});

describe("KvService — bucket cache", () => {
  it("caches a bucket by name (single fetch across repeated getBucket calls)", async () => {
    const reconnect$ = new Subject<void>();
    const svc = new KvService(makeRunner(reconnect$));
    svc.start();

    const a1 = await svc.getBucket("tunnels");
    const a2 = await svc.getBucket("tunnels");

    expect(a1).toBe(a2);
    expect(createMock).toHaveBeenCalledTimes(1);

    svc.stop();
  });

  it("invalidates the cache on reconnect so the next getBucket re-fetches", async () => {
    const reconnect$ = new Subject<void>();
    const svc = new KvService(makeRunner(reconnect$));
    svc.start();

    await svc.getBucket("tunnels");
    expect(createMock).toHaveBeenCalledTimes(1);

    // NATS reconnected — the start() subscription clears the cache.
    reconnect$.next();

    await svc.getBucket("tunnels");
    expect(createMock).toHaveBeenCalledTimes(2);

    svc.stop();
  });

  it("clearCache(name) forces a re-fetch of that bucket only", async () => {
    const reconnect$ = new Subject<void>();
    const svc = new KvService(makeRunner(reconnect$));
    svc.start();

    await svc.getBucket("tunnels");
    await svc.getBucket("quota");
    expect(createMock).toHaveBeenCalledTimes(2);

    svc.clearCache("tunnels");
    await svc.getBucket("tunnels"); // re-fetch
    await svc.getBucket("quota"); // still cached
    expect(createMock).toHaveBeenCalledTimes(3);

    svc.stop();
  });

  it("get() normalizes a thrown not-found into null", async () => {
    const reconnect$ = new Subject<void>();
    const svc = new KvService(makeRunner(reconnect$));
    const bucket = {
      get: vi.fn().mockRejectedValue(new Error("no message found")),
    } as unknown as Parameters<KvService["get"]>[0];

    await expect(svc.get(bucket, "missing")).resolves.toBeNull();
  });
});

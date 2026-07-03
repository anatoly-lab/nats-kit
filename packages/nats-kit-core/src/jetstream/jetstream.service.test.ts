import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock only `jetstreamManager` (the factory getManager() calls); the rest of
// the module (JetStreamApiError, JetStreamApiCodes, types) stays REAL, spread
// from the actual import.
const { jetstreamManagerMock } = vi.hoisted(() => ({
  jetstreamManagerMock: vi.fn(),
}));
vi.mock("@nats-io/jetstream", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  jetstreamManager: jetstreamManagerMock,
}));

import { JetStreamApiError } from "@nats-io/jetstream";
import { JetStreamService } from "./jetstream.service.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import { NatsNotConnectedError } from "../errors/index.js";
import type { NatsLogger } from "../logging/logger.types.js";

/** Real JetStreamApiError carrying a server err_code (matched via `.code`). */
function apiError(errCode: number, description: string): JetStreamApiError {
  return new JetStreamApiError({
    code: 400,
    err_code: errCode,
    description,
  });
}

const silentLogger: NatsLogger = {
  log: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

function makeRunner(opts?: {
  connected?: boolean;
  jetstreamOptions?: { domain?: string; apiPrefix?: string };
}) {
  const connection = { fake: "connection" };
  const runner = {
    getLogger: () => silentLogger,
    isConnected: () => opts?.connected ?? true,
    getJetStream: vi.fn(() => ({})),
    getConnection: vi.fn(() => connection),
    getJetStreamOptions: vi.fn(() => opts?.jetstreamOptions),
  } as unknown as NatsConnectionRunner;
  return { runner, connection };
}

beforeEach(() => {
  jetstreamManagerMock.mockReset();
  jetstreamManagerMock.mockResolvedValue({});
});

describe("JetStreamService — getManager", () => {
  it("passes the runner's connection AND its JetStream options through", async () => {
    const jetstreamOptions = { domain: "hub", apiPrefix: "$JS.hub.API" };
    const { runner, connection } = makeRunner({ jetstreamOptions });
    const svc = new JetStreamService(runner);

    await svc.getManager();

    // Regression guard: dropping the options would silently point admin
    // operations at the default domain/apiPrefix instead of the configured one.
    expect(jetstreamManagerMock).toHaveBeenCalledWith(
      connection,
      jetstreamOptions,
    );
  });

  it("passes undefined options when no JetStream config is set", async () => {
    const { runner, connection } = makeRunner();
    const svc = new JetStreamService(runner);

    await svc.getManager();

    expect(jetstreamManagerMock).toHaveBeenCalledWith(connection, undefined);
  });

  it("throws NatsNotConnectedError (without dialing the manager) when disconnected", async () => {
    const { runner } = makeRunner({ connected: false });
    const svc = new JetStreamService(runner);

    await expect(svc.getManager()).rejects.toBeInstanceOf(
      NatsNotConnectedError,
    );
    expect(jetstreamManagerMock).not.toHaveBeenCalled();
  });
});

describe("JetStreamService — createOrUpdateConsumer", () => {
  const consumerInfo = { name: "c", config: { durable_name: "c" } };

  function makeService(consumers: {
    update: ReturnType<typeof vi.fn>;
    add: ReturnType<typeof vi.fn>;
  }) {
    const { runner } = makeRunner();
    jetstreamManagerMock.mockResolvedValue({ consumers });
    return new JetStreamService(runner);
  }

  it("creates the consumer (with the passed config) when it does not exist", async () => {
    const update = vi
      .fn()
      .mockRejectedValue(apiError(10014, "consumer not found"));
    const add = vi.fn().mockResolvedValue(consumerInfo);
    const svc = makeService({ update, add });

    const config = { durable_name: "c", max_deliver: 5 };
    const info = await svc.createOrUpdateConsumer("S", config);

    expect(add).toHaveBeenCalledWith("S", config);
    expect(info).toBe(consumerInfo);
  });

  it("updates the existing consumer — the passed config is applied, not discarded", async () => {
    const update = vi.fn().mockResolvedValue(consumerInfo);
    const add = vi.fn();
    const svc = makeService({ update, add });

    const config = { durable_name: "c", max_deliver: 7 };
    const info = await svc.createOrUpdateConsumer("S", config);

    expect(update).toHaveBeenCalledWith("S", "c", config);
    expect(add).not.toHaveBeenCalled();
    // Same return shape as the create path.
    expect(info).toBe(consumerInfo);
  });

  it("falls through to create ONCE when the consumer is deleted between info and update (10149)", async () => {
    const update = vi
      .fn()
      .mockRejectedValue(apiError(10149, "consumer does not exist"));
    const add = vi.fn().mockResolvedValue(consumerInfo);
    const svc = makeService({ update, add });

    const info = await svc.createOrUpdateConsumer("S", { name: "c" });

    expect(update).toHaveBeenCalledTimes(1);
    expect(add).toHaveBeenCalledTimes(1);
    expect(info).toBe(consumerInfo);
  });

  it("falls through to update ONCE when the consumer is created concurrently (add → 10148)", async () => {
    const update = vi
      .fn()
      .mockRejectedValueOnce(apiError(10014, "consumer not found"))
      .mockResolvedValueOnce(consumerInfo);
    const add = vi
      .fn()
      .mockRejectedValue(apiError(10148, "consumer already exists"));
    const svc = makeService({ update, add });

    const info = await svc.createOrUpdateConsumer("S", { name: "c" });

    // update → not found → add → already exists → update. No loop.
    expect(update).toHaveBeenCalledTimes(2);
    expect(add).toHaveBeenCalledTimes(1);
    expect(info).toBe(consumerInfo);
  });

  it("throws (without touching the server) when config has neither name nor durable_name", async () => {
    const update = vi.fn();
    const add = vi.fn();
    const svc = makeService({ update, add });

    await expect(svc.createOrUpdateConsumer("S", {})).rejects.toThrow(
      /must set name or durable_name/,
    );
    expect(update).not.toHaveBeenCalled();
    expect(add).not.toHaveBeenCalled();
  });

  it("propagates an immutable-field rejection (10012) from update without creating", async () => {
    const rejection = apiError(10012, "deliver policy can not be updated");
    const update = vi.fn().mockRejectedValue(rejection);
    const add = vi.fn();
    const svc = makeService({ update, add });

    await expect(
      svc.createOrUpdateConsumer("S", { durable_name: "c" }),
    ).rejects.toBe(rejection);
    expect(add).not.toHaveBeenCalled();
  });
});

describe("JetStreamService — waitForStream abort", () => {
  it("resolves promptly (well under the poll interval) when the signal aborts mid-poll", async () => {
    vi.useFakeTimers();
    try {
      const { runner } = makeRunner();
      const svc = new JetStreamService(runner);
      // "stream not found" drives the retry path via the message fallback of
      // isStreamNotFound, so the poll parks in its inter-poll sleep.
      const info = vi.fn().mockRejectedValue(new Error("stream not found"));
      jetstreamManagerMock.mockResolvedValue({ streams: { info } });

      const abort = new AbortController();
      let resolved = false;
      const p = svc
        .waitForStream("S", { retryInterval: 1000, signal: abort.signal })
        .then(() => {
          resolved = true;
        });

      // First poll: not found → sleeping for the retry interval.
      await vi.advanceTimersByTimeAsync(0);
      expect(info).toHaveBeenCalledTimes(1);
      expect(resolved).toBe(false);

      // Abort mid-sleep: must resolve WITHOUT advancing the 1000ms interval
      // (the sleep is listener-cancelled) and without polling again.
      abort.abort();
      await vi.advanceTimersByTimeAsync(0);
      expect(resolved).toBe(true);
      expect(info).toHaveBeenCalledTimes(1);
      await p;
    } finally {
      vi.useRealTimers();
    }
  });

  it("returns immediately without polling when the signal is already aborted", async () => {
    const { runner } = makeRunner();
    const svc = new JetStreamService(runner);
    const info = vi.fn();
    jetstreamManagerMock.mockResolvedValue({ streams: { info } });

    const abort = new AbortController();
    abort.abort();

    await svc.waitForStream("S", { signal: abort.signal });
    expect(info).not.toHaveBeenCalled();
  });
});

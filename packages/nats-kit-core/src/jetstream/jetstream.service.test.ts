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

import { JetStreamService } from "./jetstream.service.js";
import type { NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import { NatsNotConnectedError } from "../errors/index.js";
import type { NatsLogger } from "../logging/logger.types.js";

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

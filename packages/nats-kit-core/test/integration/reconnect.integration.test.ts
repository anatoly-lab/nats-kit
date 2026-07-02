// THE priority integration test: prove the extracted NatsConnectionRunner
// survives a real nats-server drop and reconnects to the SAME address —
// WITHOUT recreating the runner. This is the highest-risk part of the
// framework-free extraction, so it runs against a real server (Testcontainers)
// rather than a mocked status stream.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { StartedTestContainer } from "testcontainers";

import { JetStreamService, type NatsConnectionRunner } from "../../src/index.js";
import {
  makeRunner,
  startNatsContainer,
  testLogger,
  waitUntil,
} from "./support/nats-container.js";

describe("NatsConnectionRunner reconnect resilience (real nats-server)", () => {
  let container: StartedTestContainer;
  let runner: NatsConnectionRunner;

  beforeAll(async () => {
    container = await startNatsContainer();
    runner = makeRunner(container, testLogger);
  });

  afterAll(async () => {
    await runner?.stop();
    await container?.stop();
  });

  it(
    "reconnects to the SAME host:port after a real NATS drop, without recreating the runner",
    async () => {
      const js = new JetStreamService(runner);

      // 1. Connect and prove JetStream actually works over the live connection.
      await runner.start();
      await waitUntil(() => runner.isConnected(), {
        timeout: 30_000,
        message: "runner never connected to NATS",
      });
      const accountBefore = await js.getAccountInfo();
      expect(accountBefore).toBeDefined();

      // 2 + 3. Bounce the SAME container (restart preserves the published host
      // port). Kick restart() off concurrently so we can observe the runner's
      // status monitor flip to disconnected DURING the down window — not just
      // after it. While the container is down the runner is unambiguously
      // disconnected (docker stop + start + boot spans well over a second), so
      // the 100ms poll catches the false state reliably.
      const bounce = container.restart();
      await waitUntil(() => runner.isConnected() === false, {
        timeout: 30_000,
        message:
          "runner never observed the NATS drop (isConnected stayed true through the bounce)",
      });

      // 4. NATS is back on the SAME address. The SAME runner instance must
      // reconnect on its own (reconnection is always infinite) — no new runner,
      // no new config. Generous timeout: reconnect is not instant.
      await bounce;
      await waitUntil(() => runner.isConnected() === true, {
        timeout: 30_000,
        message: "runner never reconnected after NATS came back",
      });

      // JetStream works again over the reconnected connection — the reconnect
      // proof, end to end.
      const accountAfter = await js.getAccountInfo();
      expect(accountAfter).toBeDefined();
    },
    120_000,
  );
});

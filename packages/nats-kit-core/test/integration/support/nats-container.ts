// Shared harness for the Docker-backed integration suite.
//
// Everything here talks to a REAL nats-server running in a throwaway container
// (Testcontainers), driving the framework-free core classes exactly as an
// application would. Kept out of `src/` on purpose so the default unit `test`
// glob (`src/**/*.test.ts`) never discovers these Docker-dependent files.

import {
  GenericContainer,
  Wait,
  type StartedTestContainer,
} from "testcontainers";

import {
  NatsConnectionRunner,
  type NatsConfig,
  type NatsLogger,
} from "../../../src/index.js";

/** Client port nats-server listens on inside the image. */
const NATS_CONTAINER_PORT = 4222;

/** Fixed host port so a container restart reuses the SAME host:port the client is pinned to. Uncommon value avoids clashing with a local nats-server on the default 4222. */
const NATS_HOST_PORT = 44222;

/**
 * Start a real nats-server (JetStream enabled) in a throwaway container.
 *
 * - Image `nats:2`, command `-js` turns on JetStream (needed for streams + KV).
 * - We bind a FIXED host port (`NATS_HOST_PORT` -> container `NATS_CONTAINER_PORT`)
 *   precisely SO THAT the reconnect test's `restart()` — a `docker restart` of the
 *   SAME container — comes back on the identical host:port, keeping the
 *   already-connected client's frozen server URL valid. A random host port would
 *   be re-allocated on restart (notably on CI runners), leaving the client
 *   pointed at a dead port it could never find. `getMappedPort(4222)` still
 *   resolves this binding for building the initial URL.
 * - The official nats image is minimal (no shell), so `Wait.forListeningPorts`
 *   — which execs a probe INSIDE the container — is not usable; wait on the
 *   server's readiness LOG line instead.
 */
export async function startNatsContainer(): Promise<StartedTestContainer> {
  return new GenericContainer("nats:2")
    .withCommand(["-js"])
    .withExposedPorts({ container: NATS_CONTAINER_PORT, host: NATS_HOST_PORT })
    .withWaitStrategy(Wait.forLogMessage(/Server is ready/))
    // First run has to pull the image; give it room.
    .withStartupTimeout(120_000)
    .start();
}

/** Build the `nats://host:port` client URL for a started container. */
export function natsUrl(container: StartedTestContainer): string {
  return `nats://${container.getHost()}:${container.getMappedPort(
    NATS_CONTAINER_PORT,
  )}`;
}

/**
 * Construct a runner pointed at the container's mapped host:port.
 *
 * Reconnect is tuned snappy (500ms wait, infinite attempts) so the
 * drop -> recover cycle is fast and reliable rather than flaky.
 */
export function makeRunner(
  container: StartedTestContainer,
  logger?: NatsLogger,
): NatsConnectionRunner {
  const config: Partial<NatsConfig> = {
    servers: [natsUrl(container)],
    name: "nats-kit-integration",
    connection: { timeout: 5_000 },
    reconnect: {
      reconnectTimeWait: 500,
    },
  };
  return new NatsConnectionRunner(config, logger ? { logger } : undefined);
}

export interface WaitUntilOptions {
  /** Hard bound in ms; the poll rejects with `message` on expiry. */
  timeout: number;
  /** Poll interval in ms (default 100). */
  interval?: number;
  /** Error message thrown on timeout. */
  message?: string;
}

/**
 * Poll `predicate` until it returns true or `timeout` elapses. Reconnect is not
 * instant, so the whole suite leans on this instead of fixed sleeps (which
 * flake): it returns the instant the condition holds and fails loudly if it
 * never does.
 */
export async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  options: WaitUntilOptions,
): Promise<void> {
  const { timeout, interval = 100, message } = options;
  const start = Date.now();
  for (;;) {
    if (await predicate()) return;
    if (Date.now() - start >= timeout) {
      throw new Error(
        message ?? `waitUntil: condition not met within ${timeout}ms`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

/**
 * Console-backed logger so a developer watching the integration run can SEE the
 * disconnect/reconnect lifecycle. (`no-console` is off for test files.)
 */
export const testLogger: NatsLogger = {
  log: (...args: unknown[]) => console.log("[nats]", ...args),
  warn: (...args: unknown[]) => console.warn("[nats]", ...args),
  error: (...args: unknown[]) => console.error("[nats]", ...args),
  debug: (...args: unknown[]) => console.debug("[nats]", ...args),
};

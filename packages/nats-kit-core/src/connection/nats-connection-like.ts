import { type NatsConnectionRunner } from "./nats-connection-runner.js";

/**
 * NatsConnectionLike
 *
 * The narrow subset of the NATS connection surface that the reconnect helpers
 * (`runDurableConsumer`, `subscribeWithReconnect`, `watchWithReconnect`) depend
 * on:
 *
 * - `waitForReady()`  — wait for a live connection before touching NATS
 *   (`runDurableConsumer`, `subscribeWithReconnect`)
 * - `getConnection()` — obtain the raw connection to (re)subscribe on
 *   (`subscribeWithReconnect`)
 * - `onReconnect()`   — reconnect signal used to re-drive the loops
 *   (`subscribeWithReconnect`, `watchWithReconnect`)
 *
 * Declared as a `Pick` from {@link NatsConnectionRunner} so it stays in lockstep
 * with the runner's signatures. Both the framework-free `NatsConnectionRunner`
 * and the NestJS adapter's `NatsService` (which delegates these methods 1:1)
 * structurally satisfy it, so a helper caller may pass either without change.
 */
export type NatsConnectionLike = Pick<
  NatsConnectionRunner,
  "waitForReady" | "getConnection" | "onReconnect"
>;

import {
  jetstreamManager,
  JetStreamApiError,
  JetStreamApiCodes,
} from "@nats-io/jetstream";
import {
  type JetStreamClient,
  type JetStreamManager,
  type StreamConfig,
  type ConsumerConfig,
  type Consumer,
  type Stream,
} from "@nats-io/jetstream";
import { type WithRequired } from "@nats-io/transport-node";
import { type NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import { NatsNotConnectedError } from "../errors/index.js";
import { type NatsLogger } from "../logging/logger.types.js";
import { emitTelemetry } from "../telemetry/telemetry.util.js";

/**
 * JetStreamService
 *
 * Provides JetStream stream and consumer management operations.
 *
 * Key features:
 * - Create/update streams
 * - Create/update consumers
 * - Get stream/consumer instances
 * - Stream and consumer info/status
 *
 * Note: All operations require NATS to be connected. Operations will throw
 * NatsNotConnectedError if NATS is not available. Callers should check
 * runner.isConnected() before calling, or handle the error gracefully.
 *
 * Usage:
 * ```typescript
 * // Create stream (will throw if NATS not connected)
 * await jsService.createOrUpdateStream({
 *   name: 'USERS',
 *   subjects: ['user.updated', 'user.deleted'],
 * });
 *
 * // Get consumer
 * const consumer = await jsService.getConsumer('USERS', 'tunnel-user-123');
 * const messages = await consumer.consume();
 * ```
 */
export class JetStreamService {
  private readonly logger: NatsLogger;

  constructor(private readonly runner: NatsConnectionRunner) {
    this.logger = runner.getLogger();
  }

  /**
   * Get JetStream client
   * @throws NatsNotConnectedError if NATS is not connected
   */
  getClient(): JetStreamClient {
    if (!this.runner.isConnected()) {
      throw new NatsNotConnectedError();
    }
    return this.runner.getJetStream();
  }

  /**
   * Get JetStream manager for admin operations
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getManager(): Promise<JetStreamManager> {
    // Ensure NATS is connected (mirrors getClient()'s guard).
    this.getClient();
    // Pass the runner's JetStream options so admin operations target the same
    // domain/apiPrefix as the runner's JetStream client.
    return await jetstreamManager(
      this.runner.getConnection(),
      this.runner.getJetStreamOptions(),
    );
  }

  /**
   * Create or update a stream
   *
   * @param config - Stream configuration
   * @returns Stream info
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async createOrUpdateStream(
    config: WithRequired<Partial<StreamConfig>, "name">,
  ) {
    const jsm = await this.getManager();
    try {
      // Try to update existing stream
      const info = await jsm.streams.update(config.name, config);
      this.logger.log(`Updated stream: ${config.name}`);
      return info;
    } catch (error) {
      // Only create if stream not found, otherwise rethrow
      if (this.isStreamNotFound(error)) {
        const info = await jsm.streams.add(config);
        this.logger.log(`Created stream: ${config.name}`);
        return info;
      }
      throw error;
    }
  }

  /**
   * Get stream by name
   *
   * @param streamName - Name of the stream
   * @returns Stream instance
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getStream(streamName: string): Promise<Stream> {
    const jsm = await this.getManager();
    return await jsm.streams.get(streamName);
  }

  /**
   * Delete a stream
   *
   * @param streamName - Name of the stream to delete
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async deleteStream(streamName: string): Promise<boolean> {
    const jsm = await this.getManager();
    const deleted = await jsm.streams.delete(streamName);
    this.logger.log(`Deleted stream: ${streamName}`);
    return deleted;
  }

  /**
   * Get stream info
   *
   * @param streamName - Name of the stream
   * @returns Stream information
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getStreamInfo(streamName: string) {
    const jsm = await this.getManager();
    return await jsm.streams.info(streamName);
  }

  /**
   * Create or update a consumer
   *
   * Semantics: update-if-exists, create-if-missing. When the consumer already
   * exists, `jsm.consumers.update()` fetches the server config and MERGES
   * `config` over it before sending (nats.js 3.4.0 behavior), so passing
   * immutable fields with UNCHANGED values is fine — only fields present in
   * `config` are (re)applied. NATS rejects changes to immutable fields
   * (durable_name, deliver_policy, ack_policy, replay_policy, start
   * sequence/time, ...) with a `JetStreamApiError` naming the offending field
   * (server err_code 10012); that error propagates to the caller.
   *
   * Note: nats.js 3.4.0 also exposes `ConsumerApiAction.CreateOrUpdate` via
   * `jsm.consumers.add(stream, cfg, { action })`, but that sends `config` as
   * the FULL consumer config without the merge — omitted fields would be
   * treated as changes back to server defaults. The update-then-create dance
   * below keeps the merge semantics for partial configs.
   *
   * @param streamName - Name of the stream
   * @param config - Consumer configuration; MUST set `name` or `durable_name`
   * @returns Consumer info (same shape from both the create and update paths)
   * @throws Error if config sets neither `name` nor `durable_name`
   * @throws NatsNotConnectedError if NATS is not connected
   * @throws JetStreamApiError if the server rejects the config (e.g. an
   *   immutable field changed on an existing consumer)
   */
  async createOrUpdateConsumer(
    streamName: string,
    config: Partial<ConsumerConfig>,
  ) {
    // `||` (not `??`): the lib itself normalizes an empty-string `name` to
    // undefined, so `{ name: "", durable_name: "x" }` must pick the durable.
    const consumerName = config.name || config.durable_name;
    if (!consumerName) {
      // Guard: without a name this used to fall through to
      // `js.consumers.get(stream, undefined)` downstream, which silently
      // creates an ORDERED consumer instead of failing.
      throw new Error(
        "createOrUpdateConsumer: config must set name or durable_name",
      );
    }

    const jsm = await this.getManager();

    // Update-first mirrors createOrUpdateStream. update() does info() +
    // merge + action:"update"; a missing consumer surfaces as
    // ConsumerNotFound (10014, from its internal info()) or — when the
    // consumer is deleted between that info() and the update request — as
    // "consumer does not exist" (10149). Both mean: fall through to create.
    try {
      const info = await jsm.consumers.update(
        streamName,
        consumerName,
        config,
      );
      this.logger.log(`Updated consumer: ${consumerName}`);
      return info;
    } catch (error) {
      if (!this.isConsumerMissing(error)) {
        throw error;
      }
    }

    try {
      const info = await jsm.consumers.add(streamName, config);
      this.logger.log(`Created consumer: ${consumerName}`);
      return info;
    } catch (error) {
      // Race: consumer created concurrently between the failed update and
      // this add (action:"create" rejects an existing consumer with a
      // different config). Fall through to a single update — no loop; a
      // second failure propagates.
      if (!this.isConsumerAlreadyExists(error)) {
        throw error;
      }
      const info = await jsm.consumers.update(
        streamName,
        consumerName,
        config,
      );
      this.logger.log(`Updated consumer (created concurrently): ${consumerName}`);
      return info;
    }
  }

  /**
   * Get consumer instance
   *
   * @param streamName - Name of the stream
   * @param consumerName - Name of the consumer
   * @returns Consumer instance
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getConsumer(
    streamName: string,
    consumerName: string,
  ): Promise<Consumer> {
    const js = this.getClient();
    return await js.consumers.get(streamName, consumerName);
  }

  /**
   * Delete a consumer
   *
   * @param streamName - Name of the stream
   * @param consumerName - Name of the consumer to delete
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async deleteConsumer(
    streamName: string,
    consumerName: string,
  ): Promise<boolean> {
    const jsm = await this.getManager();
    const deleted = await jsm.consumers.delete(streamName, consumerName);
    this.logger.log(
      `Deleted consumer: ${consumerName} from stream ${streamName}`,
    );
    return deleted;
  }

  /**
   * Get consumer info
   *
   * @param streamName - Name of the stream
   * @param consumerName - Name of the consumer
   * @returns Consumer information
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getConsumerInfo(streamName: string, consumerName: string) {
    const jsm = await this.getManager();
    return await jsm.consumers.info(streamName, consumerName);
  }

  /**
   * Publish a message to JetStream
   *
   * @param subject - Subject to publish to
   * @param data - Message data (string or Uint8Array)
   * @param options - Publish options (msgID for deduplication)
   * @returns Publish acknowledgment
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async publish(
    subject: string,
    data: string | Uint8Array,
    options?: { msgID?: string },
  ) {
    const js = this.getClient();
    const payload =
      typeof data === "string" ? new TextEncoder().encode(data) : data;

    // telemetry seam (onPublish): records the publish attempt for this subject.
    emitTelemetry(
      this.runner.getTelemetry(),
      (t) => t.onPublish?.(subject),
      this.logger,
    );

    try {
      return await js.publish(subject, payload, {
        msgID: options?.msgID,
      });
    } catch (error) {
      this.logger.error(
        `Failed to publish to ${subject}: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  /**
   * Get JetStream account info (useful for health checks)
   *
   * @returns Account information including stream/consumer counts
   * @throws NatsNotConnectedError if NATS is not connected
   */
  async getAccountInfo() {
    const jsm = await this.getManager();
    return await jsm.getAccountInfo();
  }

  /**
   * Wait for a stream to exist (for consumers that don't own the stream)
   *
   * Polls stream info until the stream exists or timeout is reached.
   * Useful when a consumer needs to wait for a producer to create the stream.
   *
   * @param streamName - Name of the stream to wait for
   * @param options - timeout in ms (default 30000), retryInterval in ms
   *   (default 1000), and an optional AbortSignal
   * @returns Promise that resolves when stream exists, rejects on timeout.
   *   Abort semantics: when `options.signal` aborts, the poll RESOLVES early
   *   (it does not throw) — mirroring the loops' abortable sleeps — and the
   *   stream may NOT exist. Callers passing a signal must re-check
   *   `signal.aborted` before doing further work (runDurableConsumer does).
   * @throws NatsNotConnectedError if NATS is not connected
   *
   * @example
   * ```typescript
   * // Wait for USERS stream to be created by another service
   * await jsService.waitForStream('USERS', { timeout: 60000 });
   * const consumer = await jsService.getConsumer('USERS', 'my-consumer');
   * ```
   */
  async waitForStream(
    streamName: string,
    options?: {
      timeout?: number;
      retryInterval?: number;
      signal?: AbortSignal;
    },
  ): Promise<void> {
    const timeout = options?.timeout ?? 30000;
    const retryInterval = options?.retryInterval ?? 1000;
    const signal = options?.signal;
    const startTime = Date.now();

    while (true) {
      // Checked at loop top AND honored by the abortable inter-poll sleep, so
      // a shutdown never has to sit out a (default 30s) poll it will discard.
      if (signal?.aborted) {
        this.logger.debug?.(
          `waitForStream(${streamName}) aborted - returning early`,
        );
        return;
      }

      try {
        await this.getStreamInfo(streamName);
        this.logger.log(`Stream ${streamName} is ready`);
        return;
      } catch (error) {
        // Re-throw connection errors immediately
        if (error instanceof NatsNotConnectedError) {
          throw error;
        }

        if (!this.isStreamNotFound(error)) {
          // Unexpected error, rethrow
          throw error;
        }

        const elapsed = Date.now() - startTime;
        if (elapsed >= timeout) {
          throw new Error(
            `Timeout waiting for stream ${streamName} to exist after ${elapsed}ms`,
            { cause: error },
          );
        }

        this.logger.debug?.(
          `Stream ${streamName} not found, retrying in ${retryInterval}ms (elapsed: ${elapsed}ms)`,
        );

        await abortableSleep(retryInterval, signal);
      }
    }
  }

  /**
   * Detect a JetStream "stream not found" error.
   *
   * nats.js v3 throws a typed `StreamNotFoundError` (subclass of
   * `JetStreamApiError`) carrying server err_code 10059
   * (`JetStreamApiCodes.StreamNotFound`). The specific subclass isn't part of
   * the public export surface, so we match the publicly-exported base class by
   * code. The message-string check is kept only as a defensive fallback for
   * any path that surfaces a plain Error with the server description.
   */
  private isStreamNotFound(error: unknown): boolean {
    if (
      error instanceof JetStreamApiError &&
      error.code === JetStreamApiCodes.StreamNotFound
    ) {
      return true;
    }
    return (
      error instanceof Error &&
      error.message.toLowerCase().includes("stream not found")
    );
  }

  /**
   * Detect a JetStream "consumer not found" error.
   *
   * v3 throws a typed `ConsumerNotFoundError` (subclass of `JetStreamApiError`)
   * carrying server err_code 10014 (`JetStreamApiCodes.ConsumerNotFound`); see
   * `isStreamNotFound` for why we match the base class by code with a
   * message-string fallback.
   */
  private isConsumerNotFound(error: unknown): boolean {
    if (
      error instanceof JetStreamApiError &&
      error.code === JetStreamApiCodes.ConsumerNotFound
    ) {
      return true;
    }
    return (
      error instanceof Error &&
      error.message.toLowerCase().includes("consumer not found")
    );
  }

  /**
   * Detect "the consumer is gone" for the create-or-update flow: either
   * ConsumerNotFound (10014, from `update()`'s internal info()) or
   * "consumer does not exist" (10149 `JSConsumerDoesNotExist`, returned when
   * an action:"update" request races a delete). 10149 is not part of the
   * `JetStreamApiCodes` export surface, hence the local constant.
   */
  private isConsumerMissing(error: unknown): boolean {
    return (
      this.isConsumerNotFound(error) ||
      (error instanceof JetStreamApiError &&
        error.code === JS_CONSUMER_DOES_NOT_EXIST)
    );
  }

  /**
   * Detect "a consumer with this name already exists" from an
   * action:"create" request (verified against nats-server errors.json; none
   * of these codes are exported by `JetStreamApiCodes` in 3.4.0):
   * - 10148 `JSConsumerAlreadyExists` — create raced a concurrent create and
   *   the existing config differs
   * - 10013 `JSConsumerNameExistErr` — name-registration race / older servers
   * - 10105 `JSConsumerExistingActiveErr` — durable exists and is active
   */
  private isConsumerAlreadyExists(error: unknown): boolean {
    return (
      error instanceof JetStreamApiError &&
      (CONSUMER_ALREADY_EXISTS_CODES as readonly number[]).includes(error.code)
    );
  }
}

// Server err_codes used by createOrUpdateConsumer's race handling; these are
// real nats-server codes (server/errors.json) that @nats-io/jetstream 3.4.0
// does NOT include in its `JetStreamApiCodes` export.
const JS_CONSUMER_DOES_NOT_EXIST = 10149;
const CONSUMER_ALREADY_EXISTS_CODES = [10148, 10013, 10105] as const;

/**
 * Sleep that resolves early when the signal aborts (listener-cancelled timer,
 * listener cleaned up on the timer path). Mirrors the module-private helper in
 * `durable-consumer.ts` — kept local rather than promoted to a shared util
 * because the signal is optional here and the helper is ~15 lines.
 */
function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (!signal) {
      setTimeout(resolve, ms);
      return;
    }
    if (signal.aborted) {
      resolve();
      return;
    }

    const onAbort = () => {
      clearTimeout(timeout);
      resolve();
    };

    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

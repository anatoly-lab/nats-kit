import { Kvm } from "@nats-io/kv";
import { type KV, type KvEntry, type KvOptions, type KvPutOptions } from "@nats-io/kv";
import { type Subscription } from "rxjs";
import { type NatsConnectionRunner } from "../connection/nats-connection-runner.js";
import { type NatsLogger } from "../logging/logger.types.js";

/**
 * KvService
 *
 * Provides NATS KV bucket operations with centralized caching and reconnect handling.
 *
 * Key operations:
 * - getBucket: Get a cached bucket instance (invalidated on reconnect)
 * - put: Set key with optional TTL (bucket-level TTL applies by default)
 * - get: Get key value
 * - delete: Delete key
 * - watch: Watch bucket for changes (use with kv.watcher.ts for reconnect handling)
 *
 * Caching behavior:
 * - Bucket instances are cached by name
 * - Cache is cleared when NATS reconnects (stale bucket references would fail)
 * - First getBucket() call with options creates the bucket; subsequent calls return cached instance
 * - Different options for same bucket name: first caller wins (bucket already exists on NATS server)
 *
 * Lifecycle: `start()` subscribes to reconnect events (cache invalidation);
 * `stop()` unsubscribes. A framework adapter drives those (formerly the NestJS
 * `onModuleInit` / `onModuleDestroy` hooks).
 *
 * Usage:
 * ```typescript
 * const kv = await kvService.getBucket('sessions');
 * await kv.put('user-123', JSON.stringify({ sessionId: 'abc' }));
 * const entry = await kv.get('user-123');
 * ```
 */
export class KvService {
  private readonly logger: NatsLogger;

  /**
   * Bound (ms) for the per-operation readiness wait in fetchAndCacheBucket().
   * Short + rejecting so a bucket fetch with NATS down fails fast rather than
   * stalling on waitForReady()'s 30s graceful default.
   */
  private static readonly FETCH_BUCKET_READY_TIMEOUT_MS = 5000;

  /** Cached bucket instances by name */
  private readonly bucketCache = new Map<string, KV>();

  /** Pending bucket requests to prevent duplicate fetches during concurrent calls */
  private readonly pendingRequests = new Map<string, Promise<KV>>();

  /** Subscription to NATS reconnect events */
  private reconnectSubscription?: Subscription;

  constructor(private readonly runner: NatsConnectionRunner) {
    this.logger = runner.getLogger();
  }

  /**
   * Lifecycle hook - subscribe to reconnect events (was NestJS `onModuleInit`).
   *
   * Intentionally does NOT await waitForReady(): gating boot on a NATS
   * connection deadlocks the init phase if NATS never comes up (the original
   * silent-hang bug). Bucket access is lazy — fetchAndCacheBucket() waits for
   * readiness per-operation (now bounded) — so init returns immediately and KV
   * simply degrades until NATS is available.
   */
  start(): void {
    // Subscribe to reconnect events to invalidate cached buckets
    this.reconnectSubscription = this.runner.onReconnect().subscribe(() => {
      const bucketCount = this.bucketCache.size;
      const pendingCount = this.pendingRequests.size;
      this.bucketCache.clear();
      this.pendingRequests.clear(); // Clear pending to force re-fetch with fresh connection
      this.logger.log(
        `NATS reconnected - KV bucket cache invalidated count=${bucketCount} pending=${pendingCount}`,
      );
    });
  }

  /**
   * Lifecycle hook - cleanup subscriptions (was NestJS `onModuleDestroy`).
   */
  stop(): void {
    this.reconnectSubscription?.unsubscribe();
  }

  /**
   * Get a KV bucket (cached by name, invalidated on reconnect)
   *
   * @param bucketName - Name of the bucket
   * @param options - Optional bucket configuration (TTL, replicas, etc.)
   *                  Only applies on first call - bucket already exists on subsequent calls
   * @returns KV bucket instance
   */
  async getBucket(
    bucketName: string,
    options?: Partial<KvOptions>,
  ): Promise<KV> {
    // Check cache first
    const cached = this.bucketCache.get(bucketName);
    if (cached) {
      if (options && Object.keys(options).length > 0) {
        this.logger.warn(
          `Returning cached KV bucket '${bucketName}' - provided options ignored. ` +
            `Call clearCache('${bucketName}') first if you need different options.`,
        );
      }
      return cached;
    }

    // Check if request already in flight (prevents duplicate fetches during concurrent calls)
    const pending = this.pendingRequests.get(bucketName);
    if (pending) {
      return pending;
    }

    // Create new request and track it
    const request = this.fetchAndCacheBucket(bucketName, options);
    this.pendingRequests.set(bucketName, request);

    try {
      return await request;
    } finally {
      this.pendingRequests.delete(bucketName);
    }
  }

  /**
   * Internal method to fetch bucket from NATS and cache it
   */
  private async fetchAndCacheBucket(
    bucketName: string,
    options?: Partial<KvOptions>,
  ): Promise<KV> {
    // Short bounded wait on the per-operation path: with NATS down, fail fast
    // (~5s, REJECTS via the explicit-timeout branch) instead of stalling on the
    // 30s default. Callers (watchers / request handlers) retry or degrade.
    await this.runner.waitForReady(KvService.FETCH_BUCKET_READY_TIMEOUT_MS);
    // v3 removed `js.views.kv()`. Use the KV manager (`Kvm`) instead,
    // constructed over the runner's JetStream client (not the raw connection)
    // so KV admin operations inherit the configured domain/apiPrefix.
    // `Kvm.create()` creates the bucket if missing and otherwise binds to the
    // existing one — preserving the previous create-if-missing semantics
    // (whereas `Kvm.open()` would fail if the bucket does not yet exist).
    const kvm = new Kvm(this.runner.getJetStream());
    const kv = await kvm.create(bucketName, options);

    // Cache for future calls
    this.bucketCache.set(bucketName, kv);
    this.logger.debug?.(`KV bucket cached bucket=${bucketName}`);

    return kv;
  }

  /**
   * Clear cached bucket(s)
   *
   * Use this when you need to re-fetch a bucket with different options,
   * or to force a fresh bucket reference.
   *
   * @param bucketName - Specific bucket to clear, or undefined to clear all
   */
  clearCache(bucketName?: string): void {
    if (bucketName) {
      const deleted = this.bucketCache.delete(bucketName);
      this.pendingRequests.delete(bucketName);
      if (deleted) {
        this.logger.debug?.(`KV bucket cache cleared bucket=${bucketName}`);
      }
    } else {
      const count = this.bucketCache.size;
      this.bucketCache.clear();
      this.pendingRequests.clear();
      this.logger.debug?.(`KV bucket cache cleared count=${count}`);
    }
  }

  /**
   * Put a value into a KV bucket
   *
   * @param bucket - KV bucket instance
   * @param key - Key to set
   * @param value - Value (typically JSON string)
   * @param options - Optional put options
   * @returns Sequence number of the stored value
   */
  async put(
    bucket: KV,
    key: string,
    value: string,
    options?: KvPutOptions,
  ): Promise<number> {
    return await bucket.put(key, value, options);
  }

  /**
   * Get a value from a KV bucket
   *
   * @param bucket - KV bucket instance
   * @param key - Key to get
   * @returns KV entry or null if not found
   */
  async get(bucket: KV, key: string): Promise<KvEntry | null> {
    // In nats.js v3, KV `get()` returns null for a missing key (the underlying
    // streams.getMessage swallows JetStreamApiError/NoMessageFound and returns
    // null), so the common not-found case never reaches the catch. The catch is
    // kept as a defensive fallback: if any backend ever surfaces not-found as a
    // thrown error, match its message and normalize to null.
    try {
      return await bucket.get(key);
    } catch (error) {
      if (
        error instanceof Error &&
        (error.message.includes("no message found") ||
          error.message.toLowerCase().includes("not found"))
      ) {
        return null;
      }
      throw error;
    }
  }

  /**
   * Delete a key from a KV bucket
   *
   * @param bucket - KV bucket instance
   * @param key - Key to delete
   */
  async delete(bucket: KV, key: string): Promise<void> {
    await bucket.delete(key);
  }

  /**
   * Check if a key exists in a KV bucket
   *
   * @param bucket - KV bucket instance
   * @param key - Key to check
   * @returns true if key exists, false otherwise
   */
  async exists(bucket: KV, key: string): Promise<boolean> {
    const entry = await this.get(bucket, key);
    return entry !== null;
  }

  /**
   * Purge a key from a KV bucket (remove all history)
   * Use delete() for normal deletion, purge() only for cleanup
   *
   * @param bucket - KV bucket instance
   * @param key - Key to purge
   */
  async purge(bucket: KV, key: string): Promise<void> {
    await bucket.purge(key);
  }

  /**
   * Get bucket status (number of keys, storage, etc.)
   *
   * @param bucket - KV bucket instance
   * @returns Bucket status information
   */
  async status(bucket: KV) {
    return await bucket.status();
  }

  /**
   * List all keys in a KV bucket
   *
   * Drains the underlying `bucket.keys(filter)` async iterator into an array.
   * Use for read-side enumeration (e.g. admin listing of active sessions).
   * Note: this materializes every matching key in memory, so it is intended
   * for buckets with a bounded key count (live connection state), not for
   * unbounded high-cardinality buckets.
   *
   * @param bucket - KV bucket instance
   * @param filter - Optional subject filter(s) (e.g. "foo.>")
   * @returns Array of key names
   */
  async keys(bucket: KV, filter?: string | string[]): Promise<string[]> {
    const result: string[] = [];
    const iter = await bucket.keys(filter);
    for await (const key of iter) {
      result.push(key);
    }
    return result;
  }
}

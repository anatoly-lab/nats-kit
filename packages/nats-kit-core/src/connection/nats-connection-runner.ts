import { readFileSync } from "node:fs";
import { Subject, type Observable } from "rxjs";
import {
  connect,
  credsAuthenticator,
  nkeyAuthenticator,
  ClosedConnectionError,
  ConnectionError,
  type NatsConnection,
  type NodeConnectionOptions,
} from "@nats-io/transport-node";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import {
  NatsConfigSchema,
  defaultNatsConfig,
  type NatsConfig,
} from "../config/nats.config.js";
import { NatsConnectionStatus } from "./connection-status.js";
import { NatsNotConnectedError } from "../errors/index.js";
import { type NatsLogger } from "../logging/logger.types.js";
import { createConsoleLogger } from "../logging/console-logger.js";
import { noopTelemetry, type NatsTelemetry } from "../telemetry/telemetry.types.js";
import { emitTelemetry } from "../telemetry/telemetry.util.js";

/**
 * NatsConnectionRunner
 *
 * Owns the NATS connection lifecycle (framework-free extraction of the former
 * `NatsService`). Instead of NestJS lifecycle hooks it exposes explicit
 * `start()` / `stop()` methods; a framework adapter drives those.
 *
 * Features:
 * - Automatic connection on `start()` (resilient - doesn't crash on failure)
 * - Status monitoring (disconnect, reconnect, errors)
 * - waitForReady() pattern for dependent services (BOUNDED - resolves or rejects
 *   within a timeout, never hangs forever)
 * - Graceful shutdown with drain
 * - Observable status events for reconnect handling
 * - Background retry on initial connection failure
 *
 * Usage:
 * ```typescript
 * const runner = new NatsConnectionRunner(config, { logger, telemetry });
 * await runner.start();
 * await runner.waitForReady();
 * const js = runner.getJetStream();
 * // ...
 * await runner.stop();
 * ```
 */
export class NatsConnectionRunner {
  private readonly config: NatsConfig;
  private readonly logger: NatsLogger;
  private readonly telemetry: NatsTelemetry;

  // Connection state - optional until connected (Issue #6)
  private nc?: NatsConnection;
  private js?: JetStreamClient;

  // Status observables for reconnect handling
  private readonly reconnectSubject = new Subject<void>();
  private readonly disconnectSubject = new Subject<void>();
  // Emits on the FIRST successful connect (and on every reconnect), so
  // dependent modules can run one-time setup (e.g. JetStream stream creation)
  // that the `reconnect` status event never fires for on a background-retry
  // first connect.
  private readonly connectSubject = new Subject<void>();
  private statusMonitorPromise?: Promise<void>;

  // Ready guard for dependent services (Issue #6)
  private readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (err: Error) => void;
  // Whether the current readyPromise has already settled (resolved or
  // rejected). Used to know when to mint a fresh pending promise on
  // disconnect so a later waitForReady() waits for the NEXT connect.
  private readySettled = false;

  /**
   * Default bound (ms) applied to waitForReady() when called with no explicit
   * timeout. On expiry it RESOLVES (with a warning) rather than rejecting, so
   * boot-path callers degrade gracefully instead of hanging forever or
   * crashing the init phase. Slightly larger than the connect timeout so a
   * healthy-but-slow first connect still wins the race.
   */
  private static readonly DEFAULT_READY_TIMEOUT_MS = 30000;

  private currentStatus: NatsConnectionStatus =
    NatsConnectionStatus.Disconnected;

  // Background retry state
  private retryTimeout?: ReturnType<typeof setTimeout>;
  private isShuttingDown = false;
  private connectAttempt = 0;
  // True while a connect() dial is in flight. Together with `this.nc` this
  // makes attemptConnection idempotent: a second start() (or a start() racing
  // a background retry) must never open a second connection — the first would
  // leak live, since nothing would ever close it.
  private connectInFlight = false;

  constructor(
    config: NatsConfig | Partial<NatsConfig>,
    deps?: { logger?: NatsLogger; telemetry?: NatsTelemetry },
  ) {
    // Validate/merge with defaults inside the runner (matches how the Nest
    // module fed config previously). `parse` is idempotent for an already-valid
    // NatsConfig, so passing a fully-resolved config is fine too.
    this.config = NatsConfigSchema.parse({ ...defaultNatsConfig, ...config });
    this.logger = deps?.logger ?? createConsoleLogger("NatsConnectionRunner");
    this.telemetry = deps?.telemetry ?? noopTelemetry;
    this.readyPromise = this.createReadyPromise();
  }

  /**
   * The logger this runner was constructed with. Exposed so dependent core
   * services (KvService, JetStreamService) can share the same logger without
   * re-injecting it.
   */
  getLogger(): NatsLogger {
    return this.logger;
  }

  /**
   * The telemetry sink this runner was constructed with. Exposed so dependent
   * core services (JetStreamService) and helpers can emit through the same sink.
   */
  getTelemetry(): NatsTelemetry {
    return this.telemetry;
  }

  /**
   * Create a fresh pending readyPromise and capture its settle callbacks.
   * Wrapping resolve/reject lets us track settlement and (on disconnect) mint
   * a new pending promise so a later waitForReady() waits for the NEXT connect
   * instead of returning instantly off a stale resolved promise.
   */
  private createReadyPromise(): Promise<void> {
    this.readySettled = false;
    return new Promise<void>((resolve, reject) => {
      this.resolveReady = () => {
        this.readySettled = true;
        resolve();
      };
      this.rejectReady = (err: Error) => {
        this.readySettled = true;
        reject(err);
      };
    });
  }

  /**
   * Start the connection lifecycle (was the NestJS `onModuleInit`).
   * Attempts to establish a NATS connection. On failure, starts background
   * retry instead of throwing.
   *
   * Idempotent: a second start() while connected — or while a dial is already
   * in flight — logs a warning and returns without dialing (a second live
   * connection would leak, since nothing would ever close it).
   */
  async start(): Promise<void> {
    await this.attemptConnection();
  }

  /**
   * Attempt to connect to NATS. On failure, schedules a retry.
   * Does NOT throw - the runner stays up regardless of NATS availability.
   */
  private async attemptConnection(): Promise<void> {
    if (this.isShuttingDown) {
      return;
    }

    // Idempotency guard: if a connection already exists or a dial is in
    // flight, do NOT dial again — a duplicate start() (or a start() racing a
    // background retry that already fired) would leak the first connection.
    if (this.nc !== undefined || this.connectInFlight) {
      this.logger.warn(
        "NATS connection already established or dial in flight - ignoring duplicate connect attempt",
      );
      return;
    }
    this.connectInFlight = true;

    // A user start() can also race a SCHEDULED (not yet fired) background
    // retry: cancel the pending timer so it can't double-dial after this
    // attempt settles. (A timer that already fired is covered by the
    // in-flight guard above.)
    if (this.retryTimeout) {
      clearTimeout(this.retryTimeout);
      this.retryTimeout = undefined;
    }

    this.connectAttempt += 1;
    const dialTimeout = this.config.connection?.timeout ?? 10000;
    // nats.js v3 runs DNS resolution OUTSIDE its own dial timer, so a hanging
    // resolve phase can block `connect()` past `timeout`. Race the whole
    // connect against an outer bound so the boot path can never strand here.
    const outerTimeout = dialTimeout + 5000;

    try {
      this.logger.log(
        {
          servers: this.config.servers,
          attempt: this.connectAttempt,
          timeoutMs: dialTimeout,
        },
        "Connecting to NATS...",
      );

      const connectionOptions: NodeConnectionOptions = {
        servers: this.config.servers,
        name: this.config.name,
        user: this.config.user,
        pass: this.config.pass,
        token: this.config.token,
        // Timeout for initial connection attempt (prevents hanging indefinitely)
        timeout: dialTimeout,
        // Infinite by contract: the runner keeps the connection alive until
        // stop(). Finite attempts would leave a dead client behind (the
        // status iterator ends and nothing re-dials).
        maxReconnectAttempts: -1,
        reconnectTimeWait: this.config.reconnect?.reconnectTimeWait ?? 2000,
        // Note: maxPayload is a server-side setting, not a client connection option
        // The client automatically respects the server's max_payload setting
      };

      // Authenticator-based auth. The schema refinement guarantees at most one
      // auth method is configured, so a simple if/else cannot mask anything.
      if (this.config.credsFile !== undefined) {
        const credsFile = this.config.credsFile;
        // Thunk form: the client invokes it on every (re)connect handshake, so
        // the file is re-read each time and rotated creds are picked up
        // without a restart. A throw from the thunk during a RECONNECT
        // handshake closes the client terminally, so a transient read failure
        // (non-atomic creds rotation, brief unreadability) falls back to the
        // last successfully read bytes instead of propagating.
        let lastGoodCreds: Buffer | undefined;
        connectionOptions.authenticator = credsAuthenticator(() => {
          try {
            lastGoodCreds = readFileSync(credsFile);
            return lastGoodCreds;
          } catch (error) {
            if (lastGoodCreds === undefined) {
              // First read: nothing to fall back to. On initial connect this
              // rejects connect() and the background retry takes over.
              throw error;
            }
            this.logger.warn(
              `Failed to re-read creds file, using last good credentials: ${error instanceof Error ? error.message : String(error)}`,
            );
            return lastGoodCreds;
          }
        });
      } else if (this.config.nkeySeed !== undefined) {
        connectionOptions.authenticator = nkeyAuthenticator(
          new TextEncoder().encode(this.config.nkeySeed),
        );
      }

      // Add TLS if configured. ca/cert/key each accept a filesystem path OR an
      // inline PEM string — PEM content is detected by its "-----BEGIN" marker
      // and mapped to the inline field, everything else to the *File field.
      if (this.config.tls?.enabled) {
        const { ca, cert, key, rejectUnauthorized } = this.config.tls;
        const tls: NonNullable<NodeConnectionOptions["tls"]> = {
          // The node transport merges the user tls object into the options it
          // hands to node's tls.connect() (verified in transport-node's
          // node_transport.js), so this is an intentional documented
          // pass-through that takes effect there.
          rejectUnauthorized,
        };
        if (ca !== undefined) {
          if (ca.includes("-----BEGIN")) tls.ca = ca;
          else tls.caFile = ca;
        }
        if (cert !== undefined) {
          if (cert.includes("-----BEGIN")) tls.cert = cert;
          else tls.certFile = cert;
        }
        if (key !== undefined) {
          if (key.includes("-----BEGIN")) tls.key = key;
          else tls.keyFile = key;
        }
        connectionOptions.tls = tls;
      }

      const nc = await this.connectWithOuterTimeout(
        connectionOptions,
        outerTimeout,
      );

      // stop() may have completed while the dial was in flight: it saw no
      // `this.nc`, had nothing to close, and returned. Adopting this
      // connection now would leak it live forever (maxReconnectAttempts: -1
      // means it never closes itself and keeps the process alive). Close it
      // best-effort and bail WITHOUT assigning state or starting the monitor
      // — same rationale as the late-arrival guard in connectWithOuterTimeout.
      if (this.isShuttingDown) {
        this.logger.warn(
          "NATS connect resolved during shutdown - closing fresh connection",
        );
        try {
          await nc.close();
        } catch (error) {
          this.logger.warn(
            `Error closing connection opened during shutdown: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
        return;
      }

      this.nc = nc;
      this.js = jetstream(nc, this.config.jetstream);

      this.currentStatus = NatsConnectionStatus.Connected;

      // Start status monitoring in background
      this.statusMonitorPromise = this.monitorStatus().catch((error) => {
        this.logger.error(`Status monitor crashed: ${error.message}`);
      });

      this.resolveReady();
      this.logger.log(
        { server: nc.getServer(), attempt: this.connectAttempt },
        "NATS connected",
      );

      // Signal first-connect (and every later (re)connect routed here) so
      // dependent modules can run one-time setup. The library's `reconnect`
      // status event does NOT fire for a background-retry first connect.
      this.connectSubject.next();

      // If this is a RE-established connection after we were down (i.e. a
      // background-retry connect, attempt > 1), the library's `reconnect`
      // status event also won't fire — yet `onReconnect()`-based fallbacks
      // (mcp-quota resync, kv.watcher cache-invalidation) only resume off
      // that signal and would otherwise stay stranded until some unrelated
      // future reconnect. Drive reconnectSubject too so they un-strand. Those
      // consumers are idempotent (resync / cache-invalidation), so the extra
      // emission alongside a later genuine `reconnect` is harmless.
      if (this.connectAttempt > 1) {
        this.reconnectSubject.next();
        emitTelemetry(this.telemetry, (t) => t.onReconnect?.(), this.logger);
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      // Visible at warn level WITH the server list + attempt number so a CI
      // connect failure is diagnosable from normal logs.
      this.logger.warn(
        {
          err,
          servers: this.config.servers,
          attempt: this.connectAttempt,
        },
        "Failed to connect to NATS, will retry in background",
      );
      emitTelemetry(
        this.telemetry,
        (t) => t.onError?.("connect", err),
        this.logger,
      );

      // Schedule retry instead of crashing
      this.scheduleRetry();
    } finally {
      this.connectInFlight = false;
    }
  }

  /**
   * Race connect() against an outer timeout. v3's DNS resolve phase runs
   * outside the library's dial timer, so this guards against a hang there.
   *
   * Connection-leak guard: if the outer timer wins the race, the underlying
   * connect() may still resolve LATER (the slow-DNS case this targets). That
   * late connection is never assigned to `this.nc` and — with
   * maxReconnectAttempts:-1 — would never close itself, while scheduleRetry()
   * opens a second one. So we hold the connect promise and, when the timer
   * wins, attach a handler that closes the orphaned connection if/when it
   * eventually arrives.
   */
  private async connectWithOuterTimeout(
    options: NodeConnectionOptions,
    timeoutMs: number,
  ): Promise<NatsConnection> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    const connectPromise = connect(options);
    try {
      return await Promise.race([
        connectPromise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            timedOut = true;
            reject(
              new Error(
                `NATS connect timed out after ${timeoutMs}ms (servers=${this.config.servers.join(
                  ",",
                )})`,
              ),
            );
          }, timeoutMs);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
      // If the timer won, ensure a late-arriving connection gets closed rather
      // than orphaned. (If connect() won, this is the same already-resolved
      // promise we returned — closing it would be wrong, so only attach when
      // the timeout fired.)
      if (timedOut) {
        connectPromise
          .then((nc) => {
            void nc.close();
          })
          .catch(() => {});
      }
    }
  }

  /**
   * Schedule a background retry for NATS connection
   */
  private scheduleRetry(): void {
    if (this.isShuttingDown) {
      return;
    }

    const retryDelay = this.config.reconnect?.reconnectTimeWait ?? 2000;
    this.currentStatus = NatsConnectionStatus.Reconnecting;

    this.retryTimeout = setTimeout(() => {
      this.attemptConnection().catch((error) => {
        this.logger.error(
          { err: error },
          "Background connection attempt failed",
        );
      });
    }, retryDelay);

    this.logger.log(`NATS reconnection scheduled in ${retryDelay}ms`);
  }

  /**
   * Stop the connection lifecycle (was the NestJS `onModuleDestroy`).
   * Gracefully closes the NATS connection using drain.
   */
  async stop(): Promise<void> {
    this.logger.log("Shutting down NATS connection...");
    this.isShuttingDown = true;

    // Clear any pending retry
    if (this.retryTimeout) {
      clearTimeout(this.retryTimeout);
      this.retryTimeout = undefined;
    }

    // Reject any unsettled readyPromise so in-flight waitForReady() callers
    // unblock during shutdown instead of stranding until process exit. Attach a
    // no-op catch so a rejection with no current waiter doesn't surface as an
    // unhandledRejection.
    if (!this.readySettled) {
      this.readyPromise.catch(() => {});
      this.rejectReady(
        new NatsNotConnectedError("NATS shutting down before connection"),
      );
    }

    // Complete observables
    this.reconnectSubject.complete();
    this.disconnectSubject.complete();
    this.connectSubject.complete();

    // Drain and close connection. These are TWO independent steps: a drain
    // that times out (NATS unreachable at shutdown) must NOT skip close().
    // close() is what terminates the nc.status() async iterator, so skipping
    // it would leave monitorStatus() — and the statusMonitorPromise awaited
    // below — hanging forever.
    if (this.nc && !this.nc.isClosed()) {
      // Best-effort drain: race against a timeout so a down server can't hang
      // shutdown. On failure just log it and fall through to close().
      let drainTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        const DRAIN_TIMEOUT_MS = 10000;

        // Race drain against timeout to prevent hanging on shutdown
        await Promise.race([
          this.nc.drain(),
          new Promise((_, reject) => {
            drainTimer = setTimeout(
              () => reject(new Error("Drain timeout")),
              DRAIN_TIMEOUT_MS,
            );
          }),
        ]);
      } catch (error) {
        this.logger.warn(
          `Error draining NATS connection: ${error instanceof Error ? error.message : String(error)}`,
        );
      } finally {
        // clearTimeout (not unref) for portability: unref() is Node-specific,
        // and without clearing, a CLEAN shutdown would leave this timer live
        // holding the process open for up to the full drain timeout.
        if (drainTimer) clearTimeout(drainTimer);
      }

      // Unconditional close: ALWAYS attempt this regardless of the drain
      // outcome so the status iterator terminates and shutdown can complete.
      try {
        await this.nc.close();
        this.currentStatus = NatsConnectionStatus.Closed;
        this.logger.log("NATS connection closed gracefully");
      } catch (error) {
        this.logger.error(
          `Error closing NATS connection: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }

    // Wait for status monitor to complete
    if (this.statusMonitorPromise) {
      await this.statusMonitorPromise;
    }
  }

  /**
   * Monitor NATS connection status and emit events
   * Runs in background until connection is closed
   */
  private async monitorStatus(): Promise<void> {
    const nc = this.nc;
    if (!nc) return;

    try {
      for await (const status of nc.status()) {
        switch (status.type) {
          case "disconnect":
            this.currentStatus = NatsConnectionStatus.Disconnected;
            this.logger.warn(`NATS disconnected from ${nc.getServer()}`);
            // Mint a fresh pending readyPromise (only if the previous one
            // already settled) so a waitForReady() issued while disconnected
            // waits for the NEXT (re)connect instead of resolving instantly.
            if (this.readySettled) {
              this.readyPromise = this.createReadyPromise();
            }
            this.disconnectSubject.next();
            break;

          case "reconnecting":
            this.currentStatus = NatsConnectionStatus.Reconnecting;
            this.logger.log("NATS attempting to reconnect...");
            break;

          case "reconnect":
            this.currentStatus = NatsConnectionStatus.Connected;
            this.logger.log(`NATS reconnected to ${nc.getServer()}`);
            // Resolve the (possibly freshly-minted) ready promise for waiters.
            this.resolveReady();
            this.reconnectSubject.next();
            emitTelemetry(this.telemetry, (t) => t.onReconnect?.(), this.logger);
            // Also drive the connect signal so first-time setup that missed
            // the original connect still runs after a reconnect.
            this.connectSubject.next();
            break;

          case "error":
            this.logger.error(`NATS error: ${status.error.message}`);
            emitTelemetry(
              this.telemetry,
              (t) => t.onError?.("status", status.error),
              this.logger,
            );
            break;

          case "ldm":
            // Lame Duck Mode: the SERVER is draining for maintenance and the
            // client will migrate to another server — informational, not a
            // data-loss condition (that's "slowConsumer" below).
            this.logger.warn(
              "NATS server entering lame duck mode - connection will migrate to another server",
            );
            break;

          case "slowConsumer":
            this.logger.warn(
              "NATS slow consumer detected - messages may be lost",
            );
            break;
        }
      }
    } catch (error) {
      this.logger.error(
        `Error in NATS status monitor: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

    // The status iterator only ends (or throws) once the underlying
    // connection closed. If that wasn't stop()'s doing, the close was
    // terminal (auth expiry, server-forced close) and nothing else re-dials.
    await this.handleUnexpectedClose(nc);
  }

  /**
   * Handle the status iterator ending WITHOUT stop() being called — a
   * terminal close (auth expiry, server-forced close, ...). Without this the
   * runner would be a zombie: stale Connected status, a finished monitor, and
   * nothing ever re-dialing. Logs the close cause (the only place that
   * diagnostic exists), flips to Disconnected with the same ready-promise
   * dance as the "disconnect" status case, drops the dead connection object,
   * and schedules a background re-dial — so "stay connected until stop()"
   * holds for ALL close causes.
   */
  private async handleUnexpectedClose(nc: NatsConnection): Promise<void> {
    if (this.isShuttingDown) {
      return; // normal shutdown: stop() owns the close
    }

    // At this point closed() resolves immediately with the close cause
    // (or void when the server gave none).
    let reason: Error | void;
    try {
      reason = await nc.closed();
    } catch (error) {
      reason = error instanceof Error ? error : new Error(String(error));
    }
    // stop() may have started while awaiting closed(); from here the
    // shutdown path owns all state, so don't disturb it.
    if (this.isShuttingDown) {
      return;
    }

    const err =
      reason instanceof Error
        ? reason
        : new Error("NATS connection closed without a cause");
    this.logger.error(
      { err },
      "NATS connection closed unexpectedly - scheduling reconnect",
    );
    emitTelemetry(this.telemetry, (t) => t.onError?.("close", err), this.logger);

    this.currentStatus = NatsConnectionStatus.Disconnected;
    // Same dance as the "disconnect" status case: a waitForReady() issued
    // from here on must wait for the NEXT connect.
    if (this.readySettled) {
      this.readyPromise = this.createReadyPromise();
    }
    this.disconnectSubject.next();

    // The connection object is dead: drop the references so getConnection()/
    // getJetStream() fail fast instead of handing out a corpse, and so the
    // re-dial below passes attemptConnection's idempotency guard. Guarded to
    // never clobber a (theoretical) newer connection.
    if (this.nc === nc) {
      this.nc = undefined;
      this.js = undefined;
    }

    // Re-dial with a fresh connection (guards isShuttingDown internally).
    this.scheduleRetry();
  }

  /**
   * Wait for NATS to be ready before using.
   * Call this in dependent services if they require NATS.
   *
   * BOUNDED by design — never awaits the underlying readyPromise unbounded:
   *
   * - With an explicit `timeoutMs`: races the connect against a timer that
   *   REJECTS (NatsNotConnectedError) on expiry. Use this where you want a
   *   hard failure (e.g. retry loops that catch + back off).
   *
   * - With NO argument (the common boot case): falls back to
   *   DEFAULT_READY_TIMEOUT_MS and RESOLVES (with a warning) on expiry rather
   *   than rejecting, so a NATS-down boot degrades gracefully (per-operation
   *   guards / lazy bucket fetch still apply) instead of hanging the init
   *   phase forever or crashing it.
   *
   * @param timeoutMs Optional hard timeout. Omit for graceful default behavior.
   */
  async waitForReady(timeoutMs?: number): Promise<void> {
    // Already connected: resolve immediately regardless of timer.
    if (this.isConnected()) {
      return;
    }

    const rejectOnTimeout = timeoutMs !== undefined;
    const effectiveTimeout =
      timeoutMs ?? NatsConnectionRunner.DEFAULT_READY_TIMEOUT_MS;

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<void>((resolve, reject) => {
      timer = setTimeout(() => {
        if (rejectOnTimeout) {
          reject(
            new NatsNotConnectedError(
              `waitForReady timed out after ${effectiveTimeout}ms`,
            ),
          );
        } else {
          this.logger.warn(
            `waitForReady timed out after ${effectiveTimeout}ms - continuing in degraded mode (NATS not connected)`,
          );
          resolve();
        }
      }, effectiveTimeout);
    });

    try {
      await Promise.race([this.readyPromise, timeoutPromise]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  /**
   * Get JetStream client for stream/consumer operations
   *
   * @throws Error if not connected - did you call waitForReady()?
   */
  getJetStream(): JetStreamClient {
    if (!this.js) {
      throw new Error(
        "JetStream not initialized - did you call waitForReady()?",
      );
    }
    return this.js;
  }

  /**
   * The JetStream options (domain/apiPrefix) this runner was configured with.
   * Exposed so dependent services can construct JetStream managers/clients
   * bound to the same domain and API prefix as the runner's own client.
   */
  getJetStreamOptions(): NatsConfig["jetstream"] {
    return this.config.jetstream;
  }

  /**
   * Get raw NATS connection for Core NATS operations (pub/sub)
   *
   * @throws Error if not connected - did you call waitForReady()?
   */
  getConnection(): NatsConnection {
    if (!this.nc) {
      throw new Error("NATS not connected - did you call waitForReady()?");
    }
    return this.nc;
  }

  /**
   * Check if NATS is currently connected and ready for operations
   *
   * This checks the tracked connection status rather than just the connection
   * object state. Returns true only when the connection is actively usable.
   *
   * Note: During disconnect/reconnect cycles, this will return false even though
   * the connection object exists. This is intentional - operations will fail
   * during these states, so callers should handle appropriately.
   */
  isConnected(): boolean {
    return this.currentStatus === NatsConnectionStatus.Connected;
  }

  /**
   * Get current connection status
   */
  getStatus(): NatsConnectionStatus {
    return this.currentStatus;
  }

  /**
   * Observable that emits when NATS reconnects
   * Use this to trigger state resync in dependent services
   */
  onReconnect(): Observable<void> {
    return this.reconnectSubject.asObservable();
  }

  /**
   * Observable that emits on EVERY successful (re)connect, including the FIRST
   * connect established via background retry (which emits no `reconnect` status
   * event). Use this for one-time-per-connection setup such as JetStream
   * stream creation. Combine with `isConnected()` to also handle the case
   * where the connection is already up when you subscribe.
   */
  onConnect(): Observable<void> {
    return this.connectSubject.asObservable();
  }

  /**
   * Observable that emits when NATS disconnects.
   *
   * LEVEL, not edge: one outage can emit more than once (the client's
   * "disconnect" status fires first; if the close then turns out to be
   * terminal, handleUnexpectedClose emits again as it hands off to the
   * re-dial path). Subscribers must treat emissions idempotently rather
   * than counting or pairing them with reconnects.
   */
  onDisconnect(): Observable<void> {
    return this.disconnectSubject.asObservable();
  }

  /**
   * Execute a NATS operation with connection guard.
   * Use for fire-and-forget operations where failure is acceptable.
   * If not connected, calls onSkip callback and returns undefined.
   *
   * @param operation - The NATS operation to execute
   * @param options - Optional configuration with onSkip callback
   * @returns The operation result or undefined if skipped
   */
  async guarded<T>(
    operation: () => T | Promise<T>,
    options?: { onSkip?: () => void },
  ): Promise<T | undefined> {
    if (!this.isConnected()) {
      // telemetry seam: a guard SKIP is not an error (NATS is simply down and
      // the caller opted into fire-and-forget), so no onError is emitted here.
      options?.onSkip?.();
      return undefined;
    }
    return await operation();
  }

  /**
   * Execute a NATS operation with required connection.
   * Use for operations that MUST succeed or throw.
   * If not connected, throws NatsNotConnectedError immediately.
   *
   * @param operation - The NATS operation to execute
   * @returns The operation result
   * @throws NatsNotConnectedError if not connected
   */
  async required<T>(operation: () => T | Promise<T>): Promise<T> {
    if (!this.isConnected()) {
      throw new NatsNotConnectedError();
    }
    return await operation();
  }

  /**
   * Execute a NATS operation with retry on transient failures.
   * Retries on connection-loss errors (NatsNotConnectedError,
   * ClosedConnectionError, ConnectionError) with exponential backoff.
   * Does NOT retry on application-level errors (those propagate).
   * Deliberately does NOT retry on TimeoutError either: a timed-out request
   * may already have had side effects server-side, so retrying is an
   * at-least-once decision that belongs to the caller.
   *
   * @param operation - The NATS operation to execute
   * @param options - Optional retry configuration
   * @param options.maxRetries - Maximum number of retries (default: 3)
   * @param options.baseDelayMs - Base delay in milliseconds for exponential backoff (default: 100)
   * @returns The operation result
   * @throws NatsNotConnectedError if connection cannot be established after retries
   * @throws Error if the operation fails with a non-retryable error
   */
  async withRetry<T>(
    operation: () => T | Promise<T>,
    options?: { maxRetries?: number; baseDelayMs?: number },
  ): Promise<T> {
    const maxRetries = options?.maxRetries ?? 3;
    const baseDelayMs = options?.baseDelayMs ?? 100;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (!this.isConnected()) {
          throw new NatsNotConnectedError();
        }
        return await operation();
      } catch (error) {
        // Only retry on connection-related errors (typed v3 error classes;
        // message matching would be brittle and would miss/false-match).
        const isRetryable =
          error instanceof NatsNotConnectedError ||
          error instanceof ClosedConnectionError ||
          error instanceof ConnectionError;

        if (!isRetryable || attempt === maxRetries) {
          throw error;
        }

        // Exponential backoff with jitter
        const delay =
          baseDelayMs * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5);
        this.logger.debug?.(
          `NATS operation failed, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${maxRetries})`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    // TypeScript satisfaction - unreachable
    throw new NatsNotConnectedError();
  }

  /**
   * Subscribe to reconnect events and register a resync callback.
   * The callback is invoked every time NATS reconnects.
   * Returns a cleanup function to unsubscribe.
   *
   * @param callback - Function to call on reconnect (can be async)
   * @returns Cleanup function to unsubscribe from reconnect events
   */
  onReconnectResync(callback: () => void | Promise<void>): () => void {
    const subscription = this.onReconnect().subscribe(async () => {
      try {
        await callback();
      } catch (error) {
        this.logger.error({ err: error }, "Reconnect resync callback failed");
      }
    });

    return () => subscription.unsubscribe();
  }
}

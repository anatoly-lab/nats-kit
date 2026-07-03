// `@Injectable` lifecycle bridge around the framework-free
// `NatsConnectionRunner` core.
//
// Lifecycle bridging:
//   - constructor:       builds a `NatsConnectionRunner` from the injected
//                        `NATS_OPTIONS` (config + optional logger/telemetry
//                        seams). No connection is opened here.
//   - OnModuleInit:      `runner.start()` — begins the connection lifecycle
//                        (resilient; retries in the background on failure).
//   - OnModuleDestroy:   `runner.stop()` — graceful drain + close.
//
// The full public API of the runner is delegated 1:1 so injection sites need
// nothing but this class
// (waitForReady / getConnection / getJetStream / isConnected / getStatus /
// guarded / required / withRetry / the observable seams). `getRunner()` is the
// one addition — it exposes the underlying runner so the `KvService` /
// `JetStreamService` wrappers can construct their core counterparts from it.

import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";
import { type Observable } from "rxjs";

import {
  NatsConnectionRunner,
  type NatsConnectionStatus,
} from "@nats-kit/core";

import { NATS_OPTIONS } from "./nats.module-builder.js";
import { type NatsModuleOptions } from "./nats.options.js";

@Injectable()
export class NatsService implements OnModuleInit, OnModuleDestroy {
  private readonly runner: NatsConnectionRunner;

  constructor(@Inject(NATS_OPTIONS) options: NatsModuleOptions) {
    this.runner = new NatsConnectionRunner(options.config, {
      logger: options.logger,
      telemetry: options.telemetry,
    });
  }

  onModuleInit(): Promise<void> {
    return this.runner.start();
  }

  onModuleDestroy(): Promise<void> {
    return this.runner.stop();
  }

  /**
   * The underlying framework-free runner. Used by the adapter's `KvService` /
   * `JetStreamService` wrappers to build their core counterparts, and
   * available to advanced consumers who want the runner directly.
   */
  getRunner(): NatsConnectionRunner {
    return this.runner;
  }

  // ----- Delegated public API (1:1 with the runner) -----

  waitForReady(timeoutMs?: number): Promise<void> {
    return this.runner.waitForReady(timeoutMs);
  }

  getConnection(): ReturnType<NatsConnectionRunner["getConnection"]> {
    return this.runner.getConnection();
  }

  getJetStream(): ReturnType<NatsConnectionRunner["getJetStream"]> {
    return this.runner.getJetStream();
  }

  isConnected(): boolean {
    return this.runner.isConnected();
  }

  getStatus(): NatsConnectionStatus {
    return this.runner.getStatus();
  }

  guarded<T>(
    operation: () => T | Promise<T>,
    options?: { onSkip?: () => void },
  ): Promise<T | undefined> {
    return this.runner.guarded(operation, options);
  }

  required<T>(operation: () => T | Promise<T>): Promise<T> {
    return this.runner.required(operation);
  }

  withRetry<T>(
    operation: () => T | Promise<T>,
    options?: { maxRetries?: number; baseDelayMs?: number },
  ): Promise<T> {
    return this.runner.withRetry(operation, options);
  }

  onConnect(): Observable<void> {
    return this.runner.onConnect();
  }

  onReconnect(): Observable<void> {
    return this.runner.onReconnect();
  }

  onDisconnect(): Observable<void> {
    return this.runner.onDisconnect();
  }

  onReconnectResync(callback: () => void | Promise<void>): () => void {
    return this.runner.onReconnectResync(callback);
  }
}

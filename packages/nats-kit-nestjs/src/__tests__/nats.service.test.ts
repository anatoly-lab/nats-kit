// Lifecycle-bridge + delegation tests for the adapter services.
//
// What we pin here:
//   - Nest itself drives the lifecycle bridge: `moduleRef.init()` ->
//     `runner.start()`, `moduleRef.close()` -> `runner.stop()`. Real hook
//     dispatch (not manual `onModuleInit()` calls) — Nest duck-types on the
//     METHOD (`isFunction(instance.onModuleInit)`), so this catches a renamed
//     or removed hook method (the `implements` clause is erased at runtime).
//   - A call on the adapter `NatsService` reaches the underlying runner
//     (delegation), and `getRunner()` exposes it.
//   - The `KvService` wrapper drives the core `start()` / `stop()` via its
//     Nest lifecycle hooks.
//   - A call on the adapter `JetStreamService` reaches the underlying runner's
//     JetStream client (delegation through the inherited core method).
//   - The `KvService` Nest lifecycle hooks drive a REAL subscription to the
//     runner's reconnect signal (the cache-invalidation seam), and tear it
//     down on destroy.
//
// The runner is stubbed on its prototype throughout — no real nats-server /
// docker, no socket ever opened.

import "reflect-metadata";

import { Test } from "@nestjs/testing";
import {
  KvService as CoreKvService,
  NatsConnectionRunner,
  NatsConnectionStatus,
} from "@nats-kit/core";
import { Subject } from "rxjs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { JetStreamService } from "../jetstream.service.js";
import { KvService } from "../kv.service.js";
import { NatsModule } from "../nats.module.js";
import { type NatsModuleOptions } from "../nats.options.js";
import { NatsService } from "../nats.service.js";

const options: NatsModuleOptions = { config: {} };

let startSpy: ReturnType<typeof vi.spyOn>;
let stopSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  startSpy = vi
    .spyOn(NatsConnectionRunner.prototype, "start")
    .mockResolvedValue(undefined);
  stopSpy = vi
    .spyOn(NatsConnectionRunner.prototype, "stop")
    .mockResolvedValue(undefined);
});

afterEach(() => {
  vi.restoreAllMocks();
});

async function buildNatsService(): Promise<{
  nats: NatsService;
  close: () => Promise<void>;
}> {
  const moduleRef = await Test.createTestingModule({
    imports: [NatsModule.forRoot(options)],
  }).compile();
  return {
    nats: moduleRef.get(NatsService),
    close: () => moduleRef.close(),
  };
}

describe("NatsService lifecycle bridge", () => {
  // Driven through Nest's real hook dispatch (`moduleRef.init()` /
  // `moduleRef.close()`), not manual `onModuleInit()` calls — so this fails
  // if the hook METHODS are renamed or removed. (Nest duck-types on the
  // method, not the `implements` clause, which is erased at runtime.)
  it("moduleRef.init() starts the runner and moduleRef.close() stops it", async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [NatsModule.forRoot(options)],
    }).compile();

    // compile() only builds the graph; no lifecycle hook has fired yet.
    expect(startSpy).not.toHaveBeenCalled();

    await moduleRef.init();
    expect(startSpy).toHaveBeenCalledTimes(1);

    await moduleRef.close();
    expect(stopSpy).toHaveBeenCalledTimes(1);
  });
});

describe("NatsService delegation", () => {
  it("delegates read calls to the runner and exposes it via getRunner()", async () => {
    const isConnectedSpy = vi
      .spyOn(NatsConnectionRunner.prototype, "isConnected")
      .mockReturnValue(true);
    const getStatusSpy = vi
      .spyOn(NatsConnectionRunner.prototype, "getStatus")
      .mockReturnValue(NatsConnectionStatus.Connected);

    const { nats, close } = await buildNatsService();

    expect(nats.isConnected()).toBe(true);
    expect(isConnectedSpy).toHaveBeenCalled();

    expect(nats.getStatus()).toBe(NatsConnectionStatus.Connected);
    expect(getStatusSpy).toHaveBeenCalled();

    expect(nats.getRunner()).toBeInstanceOf(NatsConnectionRunner);

    await close();
  });

  it("delegates waitForReady(timeoutMs) to the runner", async () => {
    const waitSpy = vi
      .spyOn(NatsConnectionRunner.prototype, "waitForReady")
      .mockResolvedValue(undefined);

    const { nats, close } = await buildNatsService();

    await nats.waitForReady(1234);
    expect(waitSpy).toHaveBeenCalledWith(1234);

    await close();
  });
});

describe("KvService lifecycle wrapper", () => {
  // Same real-dispatch rule as the NatsService bridge test: Nest's
  // `moduleRef.init()` / `moduleRef.close()` must invoke the hooks.
  it("drives the core KvService start()/stop() via Nest lifecycle hooks", async () => {
    const kvStart = vi
      .spyOn(CoreKvService.prototype, "start")
      .mockReturnValue(undefined);
    const kvStop = vi
      .spyOn(CoreKvService.prototype, "stop")
      .mockReturnValue(undefined);

    const moduleRef = await Test.createTestingModule({
      imports: [NatsModule.forRoot(options)],
    }).compile();

    expect(kvStart).not.toHaveBeenCalled();
    await moduleRef.init();
    expect(kvStart).toHaveBeenCalledTimes(1);

    await moduleRef.close();
    expect(kvStop).toHaveBeenCalledTimes(1);
  });

  // Stronger than the call-count test above: pins that the adapter's Nest
  // lifecycle hooks drive a REAL subscription to the runner's reconnect signal
  // (the cache-invalidation seam the core `start()` wires up), and tear it down
  // on destroy. Uses a mock runner whose `onReconnect()` is a real rxjs
  // `Subject`, so no `@nats-io/kv` client is touched — see the report note on
  // why the full re-fetch assertion isn't feasible cleanly at the adapter
  // level (core resolves to built dist; its transitive `@nats-io/kv` import
  // isn't reachable to mock from here).
  it("subscribes to the runner reconnect signal on init and unsubscribes on destroy", () => {
    const reconnect$ = new Subject<void>();
    const silentLogger = {
      log: () => {},
      warn: () => {},
      error: () => {},
      debug: () => {},
    };
    const mockRunner = {
      getLogger: () => silentLogger,
      onReconnect: () => reconnect$,
    } as unknown as NatsConnectionRunner;
    const fakeNats = {
      getRunner: () => mockRunner,
    } as unknown as NatsService;

    const kv = new KvService(fakeNats);

    // No subscription until the Nest init hook drives the core `start()`.
    expect(reconnect$.observed).toBe(false);

    kv.onModuleInit();
    expect(reconnect$.observed).toBe(true);
    // Emitting the reconnect signal runs the (real) cache-invalidation callback
    // without throwing — the seam is live.
    expect(() => reconnect$.next()).not.toThrow();

    kv.onModuleDestroy();
    expect(reconnect$.observed).toBe(false);
  });
});

describe("JetStreamService delegation", () => {
  it("delegates publish() through to the runner's JetStream client", async () => {
    const publishAck = { seq: 1, stream: "EVENTS", duplicate: false };
    const jsPublish = vi.fn().mockResolvedValue(publishAck);

    // getClient() guards on isConnected() then hands back getJetStream().
    vi.spyOn(NatsConnectionRunner.prototype, "isConnected").mockReturnValue(
      true,
    );
    const getJetStreamSpy = vi
      .spyOn(NatsConnectionRunner.prototype, "getJetStream")
      .mockReturnValue({ publish: jsPublish } as unknown as ReturnType<
        NatsConnectionRunner["getJetStream"]
      >);

    const moduleRef = await Test.createTestingModule({
      imports: [NatsModule.forRoot(options)],
    }).compile();
    const js = moduleRef.get(JetStreamService);

    const ack = await js.publish("evt.subject", "hello", { msgID: "m1" });

    expect(ack).toBe(publishAck);
    expect(getJetStreamSpy).toHaveBeenCalled();
    expect(jsPublish).toHaveBeenCalledTimes(1);

    const [subject, payload, opts] = jsPublish.mock.calls[0] as [
      string,
      Uint8Array,
      { msgID?: string },
    ];
    expect(subject).toBe("evt.subject");
    // core.publish() TextEncoder-encodes string payloads before handing off.
    expect(new TextDecoder().decode(payload)).toBe("hello");
    expect(opts).toEqual({ msgID: "m1" });

    await moduleRef.close();
  });
});

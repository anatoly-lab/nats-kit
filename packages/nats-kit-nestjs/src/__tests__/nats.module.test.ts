// Wiring tests for `NatsModule.forRoot` / `forRootAsync`.
//
// What we pin here:
//   - `forRoot(options)` returns a `@Global` DynamicModule that provides +
//     exports the three services and binds `NATS_OPTIONS` to the passed value.
//   - `forRootAsync({ useFactory, inject })` runs the factory at compile()
//     time with injected deps, and the resolved options are reachable via
//     `NATS_OPTIONS`.
//
// No real nats-server / docker: the `NatsConnectionRunner` lifecycle
// (`start` / `stop`) is stubbed on its prototype so DI resolution never opens
// a socket.

import "reflect-metadata";

import { Injectable, Module } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import {
  JetStreamService as CoreJetStreamService,
  KvService as CoreKvService,
  NatsConnectionRunner,
} from "@nats-kit/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// The barrel-exported (public single-import-surface) service classes. The
// barrel-substitution test below asserts DI hands consumers THESE — the
// lifecycle-managed adapter subclasses — not the raw core classes.
import {
  JetStreamService as BarrelJetStreamService,
  KvService as BarrelKvService,
} from "../index.js";
import { JetStreamService } from "../jetstream.service.js";
import { KvService } from "../kv.service.js";
import { NATS_OPTIONS } from "../nats.module-builder.js";
import { NatsModule } from "../nats.module.js";
import { type NatsModuleOptions } from "../nats.options.js";
import { NatsService } from "../nats.service.js";

const options: NatsModuleOptions = {
  config: { servers: ["nats://localhost:4222"] },
};

beforeEach(() => {
  // Neutralize the connection lifecycle so nothing dials NATS during DI.
  vi.spyOn(NatsConnectionRunner.prototype, "start").mockResolvedValue(
    undefined,
  );
  vi.spyOn(NatsConnectionRunner.prototype, "stop").mockResolvedValue(undefined);
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("NatsModule.forRoot()", () => {
  it("returns a @Global DynamicModule for NatsModule", () => {
    const dm = NatsModule.forRoot(options);
    expect(dm.module).toBe(NatsModule);
    // `setExtras({ isGlobal: true }, …)` maps to the DynamicModule `global` flag.
    expect(dm.global).toBe(true);
  });

  it("respects an explicit isGlobal: false override", () => {
    const dm = NatsModule.forRoot({ ...options, isGlobal: false });
    expect(dm.global).toBe(false);
  });

  it("provides + exports the three services and binds NATS_OPTIONS", async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [NatsModule.forRoot(options)],
    }).compile();

    expect(moduleRef.get(NatsService)).toBeInstanceOf(NatsService);
    expect(moduleRef.get(KvService)).toBeInstanceOf(KvService);
    expect(moduleRef.get(JetStreamService)).toBeInstanceOf(JetStreamService);

    // The options token is injected and holds exactly what we passed.
    const resolved = moduleRef.get<NatsModuleOptions>(NATS_OPTIONS);
    expect(resolved).toStrictEqual(options);

    await moduleRef.close();
  });

  it("resolves the barrel-exported adapter subclasses, not the core classes", async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [NatsModule.forRoot(options)],
    }).compile();

    // Resolving by the PUBLIC barrel token must yield the adapter subclass
    // instance (the lifecycle-managed one), never the raw core class. This
    // locks the barrel-substitution invariant: if the barrel ever re-exported
    // a core VALUE in place of the adapter class, this `get()` would look up an
    // unprovided token and throw — or return the wrong class.
    const kv = moduleRef.get(BarrelKvService);
    const js = moduleRef.get(BarrelJetStreamService);

    // Adapter subclasses extend the core classes, so `instanceof` the core
    // class holds — the distinguishing check is the exact constructor.
    expect(kv).toBeInstanceOf(CoreKvService);
    expect(kv.constructor).toBe(BarrelKvService);
    expect(kv.constructor).not.toBe(CoreKvService);

    expect(js).toBeInstanceOf(CoreJetStreamService);
    expect(js.constructor).toBe(BarrelJetStreamService);
    expect(js.constructor).not.toBe(CoreJetStreamService);

    // The barrel export and the module's provider token are the same class.
    expect(BarrelKvService).toBe(KvService);
    expect(BarrelJetStreamService).toBe(JetStreamService);

    await moduleRef.close();
  });
});

// A DI dependency for the async factory below.
@Injectable()
class FakeConfigService {
  servers(): string[] {
    return ["nats://factory:4222"];
  }
}

@Module({
  providers: [FakeConfigService],
  exports: [FakeConfigService],
})
class FakeConfigModule {}

describe("NatsModule.forRootAsync()", () => {
  it("invokes useFactory with injected deps and binds the resolved options", async () => {
    const factory = vi.fn(
      (cfg: FakeConfigService): NatsModuleOptions => ({
        config: { servers: cfg.servers() },
      }),
    );

    const moduleRef = await Test.createTestingModule({
      imports: [
        FakeConfigModule,
        NatsModule.forRootAsync({
          imports: [FakeConfigModule],
          inject: [FakeConfigService],
          useFactory: factory,
        }),
      ],
    }).compile();

    expect(factory).toHaveBeenCalledTimes(1);
    expect(factory).toHaveBeenCalledWith(expect.any(FakeConfigService));

    const resolved = moduleRef.get<NatsModuleOptions>(NATS_OPTIONS);
    expect(resolved.config).toEqual({ servers: ["nats://factory:4222"] });

    // Services still resolve off the async-provided options.
    expect(moduleRef.get(NatsService)).toBeInstanceOf(NatsService);

    await moduleRef.close();
  });
});

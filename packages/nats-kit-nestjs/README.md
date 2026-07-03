# `@nats-kit/nestjs`

NestJS adapter for [`@nats-kit/core`](https://www.npmjs.com/package/@nats-kit/core).
The core is framework-free; this package adds the two things NestJS apps need
on top:

- **DI wiring** — a `forRoot` / `forRootAsync` dynamic module and injectable
  `NatsService` / `KvService` / `JetStreamService`.
- **Lifecycle bridging** — the core runner's `start()` / `stop()` are driven
  by Nest's `OnModuleInit` / `OnModuleDestroy`, so the connection comes up and
  drains with your application.

The core surface (helpers, types, enums, error classes) is re-exported here —
one import surface, no direct dependency on `@nats-kit/core` or `@nats-io/*`
needed. The one deliberate exception: `NatsConnectionRunner` is type-only —
instances come from `NatsService.getRunner()`, not `new`.

## Install

```bash
npm install @nats-kit/nestjs
```

Peer dependencies (a Nest 11 app has these already):

- `@nestjs/common` `^11.0.0`
- `@nestjs/core` `^11.0.0`
- `reflect-metadata` `>=0.2.0`
- `rxjs` `^7.8.0`

Requires Node `>=22.22.0`.

## Quickstart

Static config:

```typescript
import { Module } from "@nestjs/common";
import { NatsModule } from "@nats-kit/nestjs";

@Module({
  imports: [
    NatsModule.forRoot({
      config: {
        servers: ["nats://localhost:4222"],
        name: "my-service",
      },
    }),
  ],
})
export class AppModule {}
```

Async config (the usual production pattern, e.g. from `ConfigService`):

```typescript
import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { NatsModule } from "@nats-kit/nestjs";

@Module({
  imports: [
    ConfigModule.forRoot(),
    NatsModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        config: {
          servers: config.get<string>("NATS_SERVERS")!.split(","),
          credsFile: config.get<string>("NATS_CREDS_FILE"),
        },
      }),
    }),
  ],
})
export class AppModule {}
```

The options object is `{ config, logger?, telemetry? }` — `config` is a
(partial) `NatsConfig`, validated and merged with defaults by the core; the
optional `logger` / `telemetry` seams are documented in the
[core README](https://www.npmjs.com/package/@nats-kit/core).

Then inject the services anywhere:

```typescript
import { Injectable } from "@nestjs/common";
import { NatsService, JetStreamService, KvService } from "@nats-kit/nestjs";

@Injectable()
export class OrdersService {
  constructor(
    private readonly nats: NatsService,
    private readonly js: JetStreamService,
    private readonly kv: KvService,
  ) {}

  async publishOrder(order: Order) {
    await this.js.publish("orders.created", JSON.stringify(order));
  }
}
```

## `isGlobal`

The module is **global by default** (a NATS connection is an app-wide
singleton): configure it once at the root and every feature module can inject
the services without importing `NatsModule` again. Pass
`forRoot({ config, isGlobal: false })` to actually scope it — the opt-out is
honored (there is no stray `@Global()` decorator overriding it); with
`isGlobal: false` only modules that import `NatsModule` see the providers.

## What's injectable

| Token | What it is |
| --- | --- |
| `NatsService` | Lifecycle bridge around the core `NatsConnectionRunner`. Delegates its full API 1:1 (`waitForReady`, `getConnection`, `getJetStream`, `isConnected`, `getStatus`, `guarded`/`required`/`withRetry`, `onConnect`/`onReconnect`/`onDisconnect`, ...) plus `getRunner()` for the raw runner. |
| `JetStreamService` | The core JetStream service (streams/consumers/publish), DI-constructed over the shared connection. |
| `KvService` | The core KV service (bucket cache + reconnect invalidation), with the cache lifecycle wired to Nest hooks. |
| `NATS_OPTIONS` | The resolved module options — `@Inject(NATS_OPTIONS)` if you need them. |

## Lifecycle

- **Startup**: the connection is established in `onModuleInit`, so it is up
  before your consumers' own init hooks run (Nest initializes imported
  modules first).
- **Shutdown**: the connection is drained + closed in `onModuleDestroy`,
  after dependent modules have destroyed — in-flight consumers get a clean
  drain. (Enable Nest shutdown hooks: `app.enableShutdownHooks()`.)
- **NATS down at boot**: bootstrap is *bounded* by the connect timeout
  (`connection.timeout`, default 10s, plus a small outer margin) — the app
  **boots degraded and retries in the background** rather than crashing or
  hanging. `waitForReady()` in dependent services behaves the same bounded
  way (see the core README for the resolve-vs-reject semantics).

## Health probes

The kit deliberately never gives up or crashes the process — reconnection is
infinite by contract. Give-up/alerting policy is *yours*, and the natural
place for it is a health indicator. With
[Terminus](https://docs.nestjs.com/recipes/terminus):

```typescript
import { Injectable } from "@nestjs/common";
import { HealthIndicatorService } from "@nestjs/terminus";
import { NatsService } from "@nats-kit/nestjs";

@Injectable()
export class NatsHealthIndicator {
  constructor(
    private readonly nats: NatsService,
    private readonly health: HealthIndicatorService,
  ) {}

  check(key = "nats") {
    const indicator = this.health.check(key);
    return this.nats.isConnected()
      ? indicator.up({ status: this.nats.getStatus() })
      : indicator.down({ status: this.nats.getStatus() });
  }
}
```

Wire that into your readiness endpoint so an orchestrator stops routing
traffic while NATS is down. For event-driven reactions (metrics, alerts),
subscribe to `nats.onDisconnect()` / `nats.onReconnect()` — note
`onDisconnect()` is level- not edge-triggered (one outage can emit more than
once), so treat emissions idempotently.

## Single import surface

Import **everything** from `@nats-kit/nestjs`: the helpers
(`runDurableConsumer`, `subscribeWithReconnect`, `watchWithReconnect`), config
schema, constants, all re-exported `@nats-io/*` types (`JsMsg`,
`ConsumerConfig`, `NatsConnection`, ...), the policy enums, `headers()`, and
the error classes (`JetStreamApiError`, `NatsNotConnectedError`, ...). You
never need to depend on `@nats-kit/core` or `@nats-io/*` directly.

## License

MIT

# `@nats-kit/core`

Framework-free NATS connection lifecycle + JetStream/KV helpers, built on the
nats.js v3 modular packages (`@nats-io/transport-node`, `@nats-io/jetstream`,
`@nats-io/kv`, all `^3.4.0`). No framework dependencies — the NestJS wiring
lives in the sibling [`@nats-kit/nestjs`](https://www.npmjs.com/package/@nats-kit/nestjs)
adapter.

What you get over raw nats.js:

- **`NatsConnectionRunner`** — owns the connection lifecycle: resilient
  `start()` (never crashes on a down server, retries in the background),
  bounded `waitForReady()`, status observables, graceful drain + close on
  `stop()`, and self-healing after terminal closes.
- **`JetStreamService`** — stream/consumer management with real
  create-*or-update* semantics.
- **`runDurableConsumer`** / **`subscribeWithReconnect`** /
  **`watchWithReconnect`** — hardened consume/subscribe/watch loops
  (reconnect, abort, redelivery) you'd otherwise re-implement per consumer.
- **`KvService`** — KV bucket access with caching and reconnect invalidation.

## Install

```bash
npm install @nats-kit/core rxjs
```

`rxjs` (`^7.8.0`) is a peer dependency (status events are RxJS observables).
The `@nats-io/*` packages are regular dependencies — do **not** install them
yourself; everything you need from them (types, `headers()`, error classes) is
re-exported from `@nats-kit/core`, and a single installed copy is what makes
`instanceof` checks on nats.js errors reliable. Requires Node `>=22.22.0`.

## Quickstart

```typescript
import { NatsConnectionRunner } from "@nats-kit/core";

const runner = new NatsConnectionRunner({
  servers: ["nats://localhost:4222"],
});

await runner.start(); // resolves even if NATS is down (background retry)
await runner.waitForReady(); // bounded — see connection semantics below

const nc = runner.getConnection(); // NatsConnection (Core pub/sub)
const js = runner.getJetStream(); // JetStreamClient

// ... use nc / js ...

await runner.stop(); // drain + close
```

The constructor accepts a `Partial<NatsConfig>` — it is validated and merged
with defaults internally (zod). Invalid config (e.g. two auth methods) throws
at construction time.

## Configuration (`NatsConfig`)

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `servers` | `string[]` | `["nats://localhost:4222"]` | Server URLs. |
| `name` | `string?` | — | Connection name (shows up in server logs). |
| `user` / `pass` | `string?` | — | Basic auth. Must be provided **together**. |
| `token` | `string?` | — | Token auth. |
| `nkeySeed` | `string?` | — | Inline NKey seed (trimmed — trailing env-var whitespace won't kill the handshake). |
| `credsFile` | `string?` | — | Path to a `.creds` file (JWT + NKey). **Re-read on every (re)connect handshake**, so rotated credentials are picked up without a restart; transient read failures fall back to the last good bytes. |
| `tls.enabled` | `boolean` | `false` | Enable TLS. |
| `tls.ca` / `tls.cert` / `tls.key` | `string?` | — | Each accepts a **filesystem path or an inline PEM string** (detected by `-----BEGIN` content). |
| `tls.rejectUnauthorized` | `boolean` | `true` | Verify the server certificate. |
| `jetstream.domain` | `string?` | — | JetStream domain. Applied to the JetStream client **and** manager/KV admin operations. |
| `jetstream.apiPrefix` | `string?` | — | Custom JS API prefix (default `$JS.API`). |
| `connection.timeout` | `number` | `10000` | Initial dial timeout (ms). Also raced against an outer bound (`timeout + 5000`) because v3 DNS resolution runs outside the library's dial timer. |
| `reconnect.reconnectTimeWait` | `number` | `2000` | Wait between reconnect attempts (ms). |
| `maxPayload` | `number?` | — | Documentation-only: max payload is configured server-side and the client respects it automatically. |

**Auth is mutually exclusive**: at most one of `user`/`pass`, `token`,
`nkeySeed`, `credsFile` — the schema rejects configs with more than one
(previously-silent misconfiguration now fails loudly at parse time).

**Reconnection is always infinite.** There is no max-attempts knob — the
runner's contract is "stay connected until `stop()`". Give-up policy (e.g.
exit after being down too long) belongs to the application, driven by the
status events below.

## Connection semantics

**`waitForReady(timeoutMs?)` is bounded by design** — it never hangs forever:

- With an **explicit** `timeoutMs`: **rejects** with `NatsNotConnectedError` on
  expiry. Use where you want a hard failure (retry loops that catch + back
  off).
- With **no argument** (the common boot case): falls back to a 30s bound and
  **resolves with a warning** on expiry, so a NATS-down boot degrades
  gracefully instead of hanging or crashing the init phase.

**Status observables** (RxJS, `Observable<void>`):

- `onConnect()` — every successful (re)connect, **including the first connect
  established via background retry** (which emits no library `reconnect`
  event). Use for one-time-per-connection setup, e.g. stream creation.
- `onReconnect()` — connection re-established; use to resync state
  (`onReconnectResync(cb)` is a convenience wrapper).
- `onDisconnect()` — **level, not edge**: one outage can emit more than once
  (the client's `disconnect` status fires first; a terminal close emits again
  on hand-off to the re-dial path). Treat emissions idempotently — don't count
  them or pair them with reconnects.

`getStatus()` returns the current `NatsConnectionStatus`
(`connected` / `disconnected` / `reconnecting` / `closed` / ...);
`isConnected()` is the boolean shortcut.

**Terminal closes self-heal.** If the server force-closes the connection (auth
expiry, ...), the runner logs the close cause, flips to disconnected, drops
the dead connection, and re-dials with a fresh one — "stay connected until
`stop()`" holds for all close causes.

**Operation guards**: `guarded(op)` (skip when disconnected — fire-and-forget),
`required(op)` (throw `NatsNotConnectedError` when disconnected), and
`withRetry(op, { maxRetries, baseDelayMs })` (retry connection-loss errors —
`NatsNotConnectedError`, `ClosedConnectionError`, `ConnectionError` — with
exponential backoff; `TimeoutError` is deliberately **not** retried since a
timed-out request may already have had server-side effects).

## JetStream

```typescript
import { JetStreamService } from "@nats-kit/core";

const jsService = new JetStreamService(runner);

await jsService.createOrUpdateStream({
  name: "ORDERS",
  subjects: ["orders.>"],
});
```

`createOrUpdateStream` / `createOrUpdateConsumer` are update-if-exists,
create-if-missing. Consumer updates **merge your config over the server's**
(nats.js 3.4.0 `consumers.update()` behavior), so passing immutable fields
with *unchanged* values is fine — only the fields you pass are (re)applied.
Changing an immutable field (`deliver_policy`, `ack_policy`, ...) on an
existing consumer is rejected by the server with a `JetStreamApiError`
(err_code 10012) naming the offending field; that error propagates.

### `runDurableConsumer`

A resilient durable-consumer loop: waits for the connection and the stream,
creates/updates the consumer, consumes, and reconnects on transient failures.

```typescript
import { runDurableConsumer, AckPolicy } from "@nats-kit/core";

const ac = new AbortController();

const done = runDurableConsumer({
  stream: "ORDERS",
  consumerConfig: { durable_name: "billing", ack_policy: AckPolicy.Explicit },
  handler: async (msg) => {
    await processOrder(msg.json());
    msg.ack();
  },
  signal: ac.signal,
  jetStreamService: jsService,
  natsService: runner,
});

// shutdown:
ac.abort();
await done;
```

**Handler disposition contract**: the handler owns the success/skip
disposition — it must `ack()` (handled) or `term()` (poison message, don't
redeliver) every message it returns normally from. If the handler **throws**,
the loop logs the error and `nak()`s the message (with `nakDelayMs`, if set)
so JetStream redelivers it.

**Fatal path — surface the rejection.** If the server rejects the consumer
config itself (an immutable-field change on an existing durable, err_code
10012), the loop does **not** retry — the returned promise rejects with the
server's error. Retrying a permanent config rejection would silently halt
message processing forever, so callers must surface that rejection: it means
the config needs a fix + redeploy (or the existing consumer must be deleted).
Transient errors (consumer/stream not found, connection loss) are still
retried after `reconnectDelayMs` (default 5000ms).

For Core (non-JetStream) subjects there is the analogous
`subscribeWithReconnect({ subject, handler, signal, natsService })` — no
ack/nak; a throwing handler is logged and the loop continues.

## KV

```typescript
import { KvService, watchWithReconnect, WatchEventType } from "@nats-kit/core";

const kvService = new KvService(runner);
kvService.start(); // subscribes to reconnect events (cache invalidation)

const bucket = await kvService.getBucket("sessions", { ttl: 120_000 });
await kvService.put(bucket, "user-123", JSON.stringify({ active: true }));
```

`getBucket` creates the bucket if missing and **caches instances by name**;
the cache is invalidated on reconnect (stale bucket references would fail).
Options only apply on the first call for a given name — use
`clearCache(name)` to re-fetch with different options.

### `watchWithReconnect`

An async generator over a KV bucket that survives reconnects:

```typescript
const ac = new AbortController();

for await (const event of watchWithReconnect(bucket, runner, transform, ac.signal)) {
  switch (event.type) {
    case WatchEventType.CLEAR: /* reconnected — clear local cache */ break;
    case WatchEventType.READY: /* initial delivery complete */ break;
    case WatchEventType.EVENT: /* event.data — a transformed entry */ break;
    case WatchEventType.ERROR: /* event.error — transform threw */ break;
  }
}
```

- `CLEAR` is emitted at the start of every (re)watch cycle — invalidate any
  local cache built from previous events.
- `READY` fires once the initial key delivery catches up — **including
  immediately for an empty bucket** (an empty bucket delivers no entries, so
  "buffer until READY" consumers would otherwise hang on first boot).
- The generator **ends cleanly when the runner stops** (a stopped runner can
  never reconnect) — the consumer's `for await` just finishes.

## Logger & telemetry seams

Both are constructor-injected on `NatsConnectionRunner` and default to safe
built-ins:

```typescript
const runner = new NatsConnectionRunner(config, { logger, telemetry });
```

- `NatsLogger` — a minimal duck-typed contract (`log`/`warn`/`error`, optional
  `debug`) satisfied by a NestJS `Logger` and pino-style loggers alike. The
  default is a prefixed **console** logger.
- `NatsTelemetry` — all-optional hooks (`onPublish`, `onConsume`,
  `onReconnect`, `onError`) with **no** OpenTelemetry import; implement it
  with real OTel and inject it. Defaults to `noopTelemetry`. A throwing sink
  never breaks the connection lifecycle.

## Types

Every `@nats-io/*` type reachable from this package's public API is
re-exported — `NatsConnection`, `JetStreamClient`, `JetStreamManager`,
`Stream`, `Consumer`, `ConsumerConfig`, `StreamConfig`, `JsMsg`, `Msg`, `KV`,
`KvEntry`, ... — plus the runtime values you need: `headers()` (publish with
headers), the policy enums (`AckPolicy`, `DeliverPolicy`, ...), and the error
classes for `instanceof` checks (`JetStreamApiError`, `RequestError`,
`TimeoutError`, `NoRespondersError`, ...). Import everything from
`@nats-kit/core`; never from `@nats-io/*` directly.

This README covers the shape of the API; the JSDoc on each export covers the
depth (edge cases, race notes, exact semantics).

## License

MIT

# @nats-kit/nestjs

## 0.2.0

### Minor Changes

- 8c24492: Correctness pass over config wiring, the NestJS module, and the connection lifecycle.

  **Breaking config changes (@nats-kit/core)**

  - Removed `reconnect.maxReconnectAttempts` and `reconnect.maxReconnectTimeWait`. Reconnection is always infinite — the runner's contract is "stay connected until `stop()`"; give-up policy belongs to the application (drive it from status events, e.g. a health probe). Every finite attempts value previously produced a zombie runner, and `maxReconnectTimeWait` mapped to no nats.js v3 option.
  - Removed the `nkey` field (it duplicated `nkeySeed` and was never wired). Use `nkeySeed`.
  - The schema now rejects configs with more than one auth method (`user`/`pass`, `token`, `nkeySeed`, `credsFile`), and `user`/`pass` must come together. Previously extra methods were silently dropped.

  **Fixed: config fields that were validated but silently ignored (@nats-kit/core)**

  - `credsFile` and `nkeySeed` now actually authenticate (previously the client connected with no auth). The creds file is re-read on every (re)connect handshake, so rotated credentials are picked up without a restart, with a last-good fallback for transient read failures.
  - `tls.ca`/`cert`/`key` accept a filesystem path or an inline PEM string as documented; `tls.rejectUnauthorized` is honored.
  - JetStream `domain`/`apiPrefix` now reach `jetstreamManager()` and KV admin operations (previously only publishes targeted the domain while admin calls went to `$JS.API`).
  - `withRetry` retries on real nats.js v3 error classes (`ClosedConnectionError`, `ConnectionError`) instead of v2-era message strings that never matched — retry was silently inert. `TimeoutError` is deliberately not retried.

  **Fixed: shutdown races and terminal closes (@nats-kit/core)**

  - `stop()` during an in-flight connect no longer leaks a live connection.
  - A terminal close (auth expiry, server-forced) no longer leaves a zombie runner: the close cause is logged and the runner re-dials with a fresh connection.
  - `subscribeWithReconnect` exits cleanly when the runner stops first instead of hot-spinning at 100% CPU; `watchWithReconnect` ends its generator cleanly instead of throwing an rxjs `EmptyError` into the consumer.
  - `runDurableConsumer` honors aborts during setup (`waitForStream` is now abort-aware), so a shutdown abort can no longer strand a consumer.
  - `start()` is idempotent (a duplicate call warns instead of leaking a second connection); clean shutdown no longer holds the process open on a stray drain timer; `ldm` status is logged correctly as lame duck mode.

  **Changed: consumer and KV watch semantics (@nats-kit/core)**

  - `createOrUpdateConsumer` now does what its name says: create if missing, **update if it exists** (previously the passed config was silently discarded when the consumer already existed, so config changes never reached the server). Updates merge over the server config, so passing immutable fields with unchanged values is safe. The method now throws if the config sets neither `name` nor `durable_name` (previously this could silently create an ordered consumer).
  - `runDurableConsumer` treats a server rejection of the consumer config itself (immutable-field change, err_code 10012) as **fatal**: its promise rejects with the server's error naming the offending field, instead of retrying forever and silently halting message processing. Surface that rejection — it means the config needs a fix (or the server-side consumer must be deleted). Transient errors are still retried.
  - `watchWithReconnect` now emits `READY` immediately for an **empty bucket** (previously "buffer until READY" consumers hung on first boot until the first put), and `READY` is no longer delayed when the transform throws on the final catch-up entry.

  **Added: complete type surface + real documentation (both packages)**

  - Both barrels now re-export every `@nats-io/*` type reachable from a public signature (`NatsConnection`, `JetStreamClient`, `JetStreamManager`, `Stream`, `Consumer`, `ConsumeOptions`, `ConsumerInfo`, `StreamInfo`, `PubAck`, `KvStatus`, `MsgHdrs`, ...) plus the runtime values consumers need: `headers()`, `JetStreamApiError` + `JetStreamApiCodes` (for `instanceof` + `.code` checks on the new fatal rejection), and `ClosedConnectionError`/`ConnectionError`. Consumers never need a direct `@nats-io/*` dependency.
  - The placeholder READMEs are replaced with real documentation: full config reference, connection/JetStream/KV semantics, NestJS module usage, and health-probe guidance.

  **Fixed (@nats-kit/nestjs)**

  - `forRoot({ isGlobal: false })` now actually scopes the module (a stray `@Global()` decorator made the opt-out a silent no-op).
  - `NATS_OPTIONS` is exported from the module, so `@Inject(NATS_OPTIONS)` works in consumer modules as documented.

  **Packaging (both packages)**

  - Node `engines` floor relaxed from `>=22.22.0` to `>=22.0.0`.
  - `@nats-kit/nestjs` now supports NestJS 10 and 11 (`@nestjs/common` / `@nestjs/core` peer range widened to `^10.0.0 || ^11.0.0`; tested against 11).
  - `@nats-kit/nestjs` depends on `@nats-kit/core` via a caret range instead of an exact pin, so npm can dedupe a single core copy (keeps `instanceof` / enum identity intact).
  - `"sideEffects": false` for tree-shaking in bundled consumers.
  - `./package.json` added to the `exports` map (tooling can `require.resolve('@nats-kit/*/package.json')`).
  - `CHANGELOG.md` ships in the npm tarball; added `keywords` / `homepage` / `bugs` npm metadata.

### Patch Changes

- Updated dependencies [8c24492]
  - @nats-kit/core@0.2.0

## 0.1.0

### Minor Changes

- Initial public release.

  `@nats-kit/core` — framework-agnostic NATS toolkit: `NatsConnectionRunner`
  lifecycle, `KvService` / `JetStreamService`, reconnect-resilient helpers
  (`runDurableConsumer`, `watchWithReconnect`, `subscribeWithReconnect`),
  config schema, telemetry seam, and logger abstraction. Zero framework
  dependencies.

  `@nats-kit/nestjs` — NestJS adapter: dynamic module (`forRoot` /
  `forRootAsync`), injectable `NatsService` lifecycle bridge, and
  `KvService` / `JetStreamService` providers, re-exporting core.

### Patch Changes

- Updated dependencies
  - @nats-kit/core@0.1.0

# `@nats-kit/core`

Framework-free core of the `@nats-kit/*` family — a toolkit for NATS
JetStream + KV built on `@nats-io/jetstream`, `@nats-io/kv`, and
`@nats-io/transport-node`. No framework dependencies: NestJS wiring lives in
the sibling `@nats-kit/nestjs` adapter.

> **STATUS: STEP 1 skeleton.** This package currently exports only a version
> placeholder. The real core (connection runner, KV service, JetStream service,
> helpers) is extracted in a later step.

## Peer dependencies

- `rxjs` `^7.8.0`

## Build

Dual CJS + ESM via [tshy](https://github.com/isaacs/tshy):

```bash
pnpm --filter @nats-kit/core build
```

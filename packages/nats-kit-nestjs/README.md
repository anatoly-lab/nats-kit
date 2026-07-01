# `@nats-kit/nestjs`

NestJS adapter for [`@nats-kit/core`](../nats-kit-core). The only package in the
family allowed to import `@nestjs/*` and `reflect-metadata` — it wraps the
framework-free core in a Nest dynamic module + injectable services.

> **STATUS: STEP 1 skeleton.** This package currently re-exports the core
> version placeholder. The real adapter (`forRoot` / `forRootAsync` module,
> injectable NATS services) is extracted in a later step.

## Peer dependencies

- `@nestjs/common` `^11.0.0`
- `@nestjs/core` `^11.0.0`
- `reflect-metadata` `>=0.2.0`
- `rxjs` `^7.8.0`

## Build

Dual CJS + ESM via [tshy](https://github.com/isaacs/tshy):

```bash
pnpm --filter @nats-kit/nestjs build
```

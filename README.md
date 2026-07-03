# nats-kit

Monorepo for the `@nats-kit/*` library family — a toolkit for NATS
JetStream + KV built on the nats.js v3 modular packages (`@nats-io/*`), with a
framework-free core and a thin NestJS adapter.

## Packages

| Package | Directory | Role |
| --- | --- | --- |
| [`@nats-kit/core`](packages/nats-kit-core) | `packages/nats-kit-core` | Framework-free core: connection lifecycle runner, JetStream/KV services, resilient consume/subscribe/watch helpers. No NestJS. |
| [`@nats-kit/nestjs`](packages/nats-kit-nestjs) | `packages/nats-kit-nestjs` | NestJS adapter: `forRoot`/`forRootAsync` dynamic module + injectable services. The only package allowed to import `@nestjs/*`. |

Both packages ship **dual CJS + ESM** builds via
[tshy](https://github.com/isaacs/tshy) and version in **lockstep** (see
`fixed` in `.changeset/config.json`). Each package's README is its npm landing
page and documents its API.

## Toolchain

- **Package manager:** pnpm (the `packageManager` field is authoritative)
- **Node:** `>=22.22.0` (`.nvmrc`)
- **Monorepo runner:** Turborepo
- **Build:** tshy (dual `dist/esm` + `dist/commonjs`)
- **Language:** TypeScript (`NodeNext`, strict)
- **Tests:** Vitest (unit + Testcontainers-based integration suites)
- **Versioning/publish:** Changesets (`fixed` lockstep group)

## Development

```bash
pnpm install          # wire the workspace
pnpm build            # turbo run build
pnpm typecheck        # turbo run typecheck (--force)
pnpm lint             # turbo run lint
pnpm test             # turbo run test (unit)
pnpm ci               # lint + typecheck + test + build

# Integration tests (require Docker; spin up a NATS server via Testcontainers)
pnpm --filter @nats-kit/core test:integration
```

## Releasing

Changesets drive versioning:

```bash
pnpm changeset          # describe the change + pick a semver bump
pnpm version-packages   # consume changesets, bump versions, write CHANGELOGs
```

Publishing is tag-triggered and lockstep — see
`.github/workflows/npm-publish.yml`.

## License

MIT — see [LICENSE](LICENSE).

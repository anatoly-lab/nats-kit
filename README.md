# nats-kit

Monorepo for the `@nats-kit/*` library family — a framework-agnostic toolkit
for NATS JetStream + KV, with a thin NestJS adapter.

> **STATUS: STEP 1 skeleton.** This repo currently contains only the monorepo
> tooling and two placeholder packages. No business logic has been extracted
> yet — the real NATS core (connection runner, KV service, JetStream service,
> helpers) moves in a later step.

## Packages

| Package            | Directory                  | Role                                                                 |
| ------------------ | -------------------------- | ------------------------------------------------------------------- |
| `@nats-kit/core`   | `packages/nats-kit-core`   | Framework-free core. NATS JetStream/KV over `@nats-io/*`. No NestJS. |
| `@nats-kit/nestjs` | `packages/nats-kit-nestjs` | NestJS adapter. The only package allowed to import `@nestjs/*`.      |

Both packages ship **dual CJS + ESM** builds via [tshy](https://github.com/isaacs/tshy)
and version in **lockstep** (see `fixed` in `.changeset/config.json`).

## Toolchain

- **Package manager:** pnpm `11.6.0` (`packageManager` field is authoritative)
- **Node:** `>=22.22.0` (`.nvmrc` pins `22.22`)
- **Monorepo runner:** Turborepo `2.10.0`
- **Build tool:** tshy `4.1.3` (dual `dist/esm` + `dist/commonjs`)
- **Language:** TypeScript `6.0.3` (`NodeNext`, strict)
- **Tests:** Vitest `4.1.9` (no suites yet)
- **Versioning/publish:** Changesets (`fixed` lockstep group)

## Common commands

```bash
pnpm install        # wire the workspace
pnpm build          # turbo run build (dual CJS+ESM for both packages)
pnpm typecheck      # turbo run typecheck
pnpm lint           # turbo run lint
pnpm test           # turbo run test (passWithNoTests until suites land)
pnpm changeset      # describe a change + choose a semver bump
pnpm version-packages  # consume changesets, bump versions, write CHANGELOGs
```

## Publishing

Publishing is tag-triggered and lockstep — see `.github/workflows/npm-publish.yml`.
Before the first publish works, a human must create the `@nats-kit` npm org and
configure npmjs.org trusted-publisher (OIDC) bindings for both packages.

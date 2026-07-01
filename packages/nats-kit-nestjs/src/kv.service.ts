// `@Injectable` KV wrapper.
//
// SHAPE CHOICE — thin SUBCLASS of the core `KvService` (aliased
// `CoreKvService`). The core class already exposes the exact public API
// consumers use (getBucket / put / get / delete / exists / purge / status /
// keys / clearCache); extending it inherits every method verbatim, so this is
// a guaranteed drop-in with ZERO delegation boilerplate to drift. All the
// adapter layers on is (1) a Nest DI constructor that builds the core instance
// from the shared `NatsConnectionRunner` (via `NatsService.getRunner()`), and
// (2) the Nest lifecycle hooks that drive the core's `start()` / `stop()` —
// the reconnect-driven bucket-cache invalidation that was formerly wired to
// the NestJS `onModuleInit` / `onModuleDestroy` directly.
//
// (Delegation would mean re-declaring ~9 methods and risking drift; a factory
// provider would lose the ergonomic class-token injection consumers rely on.)
//
// The core class is imported under an alias so its name doesn't collide with
// this adapter's exported `KvService`.

import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";

import { KvService as CoreKvService } from "@nats-kit/core";

import { NatsService } from "./nats.service.js";

@Injectable()
export class KvService
  extends CoreKvService
  implements OnModuleInit, OnModuleDestroy
{
  // `@Inject(NatsService)` names the token explicitly so the class reference is
  // used as a VALUE (not an erasable type-only import) — required under
  // `verbatimModuleSyntax`, and clearer than relying on decorator-metadata.
  constructor(@Inject(NatsService) nats: NatsService) {
    super(nats.getRunner());
  }

  onModuleInit(): void {
    // Subscribe to reconnect events (bucket-cache invalidation).
    this.start();
  }

  onModuleDestroy(): void {
    // Unsubscribe.
    this.stop();
  }
}

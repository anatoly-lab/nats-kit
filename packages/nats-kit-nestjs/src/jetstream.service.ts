// `@Injectable` JetStream wrapper.
//
// SHAPE CHOICE — thin SUBCLASS of the core `JetStreamService` (aliased
// `CoreJetStreamService`), for the same reasons as `KvService`: the core class
// already exposes the full public API (createOrUpdateStream / getStream /
// deleteStream / createOrUpdateConsumer / getConsumer / publish /
// waitForStream / …), so extending inherits it verbatim — a guaranteed drop-in
// with no delegation to drift. Unlike KV, JetStream has NO reconnect-driven
// lifecycle state, so there are no `OnModuleInit` / `OnModuleDestroy` hooks to
// bridge here — only the DI constructor that builds the core instance from the
// shared `NatsConnectionRunner`.
//
// The core class is imported under an alias so its name doesn't collide with
// this adapter's exported `JetStreamService`.

import { Inject, Injectable } from "@nestjs/common";

import { JetStreamService as CoreJetStreamService } from "@nats-kit/core";

import { NatsService } from "./nats.service.js";

@Injectable()
export class JetStreamService extends CoreJetStreamService {
  // See `KvService` for why the token is named explicitly with `@Inject`.
  constructor(@Inject(NatsService) nats: NatsService) {
    super(nats.getRunner());
  }
}

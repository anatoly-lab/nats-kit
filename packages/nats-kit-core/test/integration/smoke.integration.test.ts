// Minimal harness-validation smoke: prove the container harness + core wiring
// actually round-trip against a real nats-server. Deliberately small — the full
// integration matrix (see the TODO list in the extraction notes) is out of
// scope here; this only guards that the happy paths connect, consume, and
// KV-round-trip so a broken harness fails loudly and early.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { StartedTestContainer } from "testcontainers";

import {
  AckPolicy,
  JetStreamService,
  KvService,
  runDurableConsumer,
  type NatsConnectionRunner,
} from "../../src/index.js";
import {
  makeRunner,
  startNatsContainer,
  testLogger,
  waitUntil,
} from "./support/nats-container.js";

describe("nats-kit core happy-path smoke (real nats-server)", () => {
  let container: StartedTestContainer;
  let runner: NatsConnectionRunner;

  beforeAll(async () => {
    container = await startNatsContainer();
    runner = makeRunner(container, testLogger);
    await runner.start();
    await waitUntil(() => runner.isConnected(), {
      timeout: 30_000,
      message: "runner never connected to NATS",
    });
  });

  afterAll(async () => {
    await runner?.stop();
    await container?.stop();
  });

  it(
    "runDurableConsumer receives a published JetStream message",
    async () => {
      const js = new JetStreamService(runner);
      const STREAM = "SMOKE";

      await js.createOrUpdateStream({ name: STREAM, subjects: ["smoke.>"] });

      const received: string[] = [];
      const abort = new AbortController();
      const done = runDurableConsumer({
        stream: STREAM,
        consumerConfig: {
          durable_name: "smoke-consumer",
          ack_policy: AckPolicy.Explicit,
          filter_subject: "smoke.>",
        },
        handler: (msg) => {
          received.push(new TextDecoder().decode(msg.data));
          msg.ack();
        },
        signal: abort.signal,
        jetStreamService: js,
        natsService: runner,
        logger: testLogger,
      });

      try {
        await js.publish("smoke.hello", "hello-nats-kit");
        await waitUntil(() => received.length > 0, {
          timeout: 30_000,
          message: "durable consumer never received the published message",
        });
        expect(received).toContain("hello-nats-kit");
      } finally {
        abort.abort();
        await done;
      }
    },
    120_000,
  );

  it(
    "KvService put/get round-trips through a real KV bucket",
    async () => {
      const kv = new KvService(runner);
      kv.start();
      try {
        const bucket = await kv.getBucket("smoke_kv");
        await kv.put(bucket, "greeting", "hello-kv");
        const entry = await kv.get(bucket, "greeting");
        expect(entry?.string()).toBe("hello-kv");
      } finally {
        kv.stop();
      }
    },
    60_000,
  );
});

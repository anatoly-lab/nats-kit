import { describe, it, expect } from "vitest";
import {
  NatsConfigSchema,
  defaultNatsConfig,
} from "./nats.config.js";

describe("NatsConfigSchema", () => {
  it("applies defaults for an empty object", () => {
    const cfg = NatsConfigSchema.parse({});
    expect(cfg.servers).toEqual(["nats://localhost:4222"]);
    // Nested groups are optional and only defaulted when present; top-level
    // server default is always applied.
    expect(cfg.name).toBeUndefined();
  });

  it("fills nested defaults when a group is provided partially", () => {
    const cfg = NatsConfigSchema.parse({ reconnect: { reconnectTimeWait: 5000 } });
    expect(cfg.reconnect).toEqual({
      maxReconnectAttempts: -1,
      reconnectTimeWait: 5000,
      maxReconnectTimeWait: 30000,
    });
  });

  it("defaults tls.enabled=false and rejectUnauthorized=true when tls present", () => {
    const cfg = NatsConfigSchema.parse({ tls: {} });
    expect(cfg.tls).toEqual({ enabled: false, rejectUnauthorized: true });
  });

  it("merges the runner's { ...defaultNatsConfig, ...config } shape", () => {
    // Mirrors what NatsConnectionRunner does internally.
    const merged = NatsConfigSchema.parse({
      ...defaultNatsConfig,
      servers: ["nats://a:4222", "nats://b:4222"],
      name: "svc",
    });
    expect(merged.servers).toEqual(["nats://a:4222", "nats://b:4222"]);
    expect(merged.name).toBe("svc");
    expect(merged.connection).toEqual({ timeout: 10000 });
    expect(merged.reconnect?.reconnectTimeWait).toBe(2000);
  });

  it("rejects a non-string in servers", () => {
    expect(() => NatsConfigSchema.parse({ servers: [123] })).toThrow();
  });
});

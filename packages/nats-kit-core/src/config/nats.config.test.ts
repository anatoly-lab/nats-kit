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
    const cfg = NatsConfigSchema.parse({ reconnect: {} });
    expect(cfg.reconnect).toEqual({
      reconnectTimeWait: 2000,
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

describe("NatsConfigSchema — auth mutual exclusion", () => {
  it("accepts a single auth method", () => {
    expect(() => NatsConfigSchema.parse({ token: "t" })).not.toThrow();
    expect(() =>
      NatsConfigSchema.parse({ user: "u", pass: "p" }),
    ).not.toThrow();
    expect(() => NatsConfigSchema.parse({ nkeySeed: "SUA..." })).not.toThrow();
    expect(() =>
      NatsConfigSchema.parse({ credsFile: "/etc/nats/svc.creds" }),
    ).not.toThrow();
  });

  it("rejects two auth methods, naming what was provided", () => {
    expect(() =>
      NatsConfigSchema.parse({ token: "t", nkeySeed: "SUA..." }),
    ).toThrow(/token, nkeySeed/);
    expect(() =>
      NatsConfigSchema.parse({ user: "u", pass: "p", credsFile: "x.creds" }),
    ).toThrow(/user\/pass, credsFile/);
  });

  it("rejects user without pass (and pass without user)", () => {
    expect(() => NatsConfigSchema.parse({ user: "u" })).toThrow(
      /user and pass must be provided together/,
    );
    expect(() => NatsConfigSchema.parse({ pass: "p" })).toThrow(
      /user and pass must be provided together/,
    );
  });

  it("trims whitespace around nkeySeed (env-var paste hygiene)", () => {
    // A trailing newline would throw inside the auth handshake — terminal on
    // reconnect — so the schema strips it before it can reach the wire.
    const cfg = NatsConfigSchema.parse({ nkeySeed: "  SUA...\n" });
    expect(cfg.nkeySeed).toBe("SUA...");
  });

  it("strips the removed `nkey` field (use nkeySeed instead)", () => {
    // `nkey` duplicated nkeySeed and was never wired downstream; the schema no
    // longer knows it, so zod strips it like any unknown key.
    const cfg = NatsConfigSchema.parse({ nkey: "SUA..." });
    expect("nkey" in cfg).toBe(false);
  });
});

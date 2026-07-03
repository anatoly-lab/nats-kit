import { z } from "zod";

/**
 * NATS Configuration Schema
 *
 * Validates NATS connection configuration including authentication and TLS options.
 * Security options (nkeySeed, TLS) are optional in schema but recommended for production.
 *
 * Authentication options (at most one, enforced by the schema):
 * - user/pass: Basic authentication
 * - token: Token-based authentication
 * - nkeySeed: Inline NKey seed string (recommended for production)
 * - credsFile: Path to .creds file containing JWT + NKey
 *
 * Note: Actual NATS server permissions (which service can read/write what) are
 * configured server-side, not in application code.
 */
export const NatsConfigSchema = z.object({
  /** NATS server URLs (e.g., ['nats://localhost:4222']) */
  servers: z.array(z.string()).default(["nats://localhost:4222"]),

  /** Connection name for debugging (appears in server logs) */
  name: z.string().optional(),

  // Authentication options (at most one, enforced by the refinement below)
  /** Basic auth username */
  user: z.string().optional(),
  /** Basic auth password */
  pass: z.string().optional(),
  /** Token-based authentication */
  token: z.string().optional(),
  /** Inline NKey seed string (recommended for production). Trimmed: trailing
   * whitespace (typical env-var paste) would otherwise throw inside the auth
   * handshake, which nats.js treats as a terminal close on reconnect. */
  nkeySeed: z.string().trim().optional(),
  /** Path to .creds file (contains JWT + NKey) */
  credsFile: z.string().optional(),

  // TLS settings (recommended for production)
  tls: z
    .object({
      /** Enable TLS */
      enabled: z.boolean().default(false),
      /** CA certificate: filesystem path or inline PEM string (detected by "-----BEGIN" content) */
      ca: z.string().optional(),
      /** Client certificate for mTLS: filesystem path or inline PEM string (detected by "-----BEGIN" content) */
      cert: z.string().optional(),
      /** Client private key for mTLS: filesystem path or inline PEM string (detected by "-----BEGIN" content) */
      key: z.string().optional(),
      /** Verify server certificate (default: true) */
      rejectUnauthorized: z.boolean().default(true),
    })
    .optional(),

  // JetStream settings
  jetstream: z
    .object({
      /** JetStream domain (for multi-tenancy) */
      domain: z.string().optional(),
      /** Custom API prefix (default: $JS.API) */
      apiPrefix: z.string().optional(),
    })
    .optional(),

  // Connection settings
  connection: z
    .object({
      /**
       * Timeout for initial connection attempt in milliseconds.
       * If the server is unreachable, connect() will fail after this timeout.
       * Default: 10000 (10 seconds)
       */
      timeout: z.number().default(10000),
    })
    .optional(),

  // Reconnect settings. Reconnection is always infinite — the runner's
  // contract is "stay connected until stop()". Give-up policy (e.g. exit the
  // process after being disconnected too long) belongs to the application,
  // driven by connection status events.
  reconnect: z
    .object({
      /** Initial reconnect wait time in milliseconds */
      reconnectTimeWait: z.number().default(2000),
    })
    .optional(),

  /**
   * Maximum payload size in bytes for NATS messages.
   * Recommended: 4MB when transporting large payloads (e.g. embedded images/documents)
   * Note: This is a documentation field only. The actual max_payload is
   * configured server-side in NATS, and the client automatically respects it.
   * This field exists to document the expected server configuration.
   */
  maxPayload: z.number().optional(),
}).superRefine((config, ctx) => {
  // Auth methods are mutually exclusive: the runner wires exactly one of them
  // into the connection options, so configuring several would silently drop
  // all but one. Fail loudly at parse time instead.
  if ((config.user === undefined) !== (config.pass === undefined)) {
    ctx.addIssue({
      code: "custom",
      message:
        "user and pass must be provided together (basic auth requires both)",
    });
  }
  const provided = [
    config.user !== undefined || config.pass !== undefined
      ? "user/pass"
      : undefined,
    config.token !== undefined ? "token" : undefined,
    config.nkeySeed !== undefined ? "nkeySeed" : undefined,
    config.credsFile !== undefined ? "credsFile" : undefined,
  ].filter((method): method is string => method !== undefined);
  if (provided.length > 1) {
    ctx.addIssue({
      code: "custom",
      message: `At most one authentication method may be configured, got: ${provided.join(", ")}`,
    });
  }
});

export type NatsConfig = z.infer<typeof NatsConfigSchema>;

/**
 * Default NATS configuration
 * Values used when not specified
 */
export const defaultNatsConfig: Partial<NatsConfig> = {
  servers: ["nats://localhost:4222"],
  connection: {
    timeout: 10000, // 10 seconds for initial connection
  },
  reconnect: {
    reconnectTimeWait: 2000,
  },
};

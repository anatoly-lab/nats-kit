import { z } from "zod";

/**
 * NATS Configuration Schema
 *
 * Validates NATS connection configuration including authentication and TLS options.
 * Security options (nkey, TLS) are optional in schema but recommended for production.
 *
 * Authentication options (use one):
 * - user/pass: Basic authentication
 * - token: Token-based authentication
 * - nkey/nkeySeed: NKey authentication (recommended for production)
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

  // Authentication options (use one)
  /** Basic auth username */
  user: z.string().optional(),
  /** Basic auth password */
  pass: z.string().optional(),
  /** Token-based authentication */
  token: z.string().optional(),
  /** NKey seed (recommended for production) */
  nkey: z.string().optional(),
  /** Alternative: inline NKey seed string */
  nkeySeed: z.string().optional(),
  /** Path to .creds file (contains JWT + NKey) */
  credsFile: z.string().optional(),

  // TLS settings (recommended for production)
  tls: z
    .object({
      /** Enable TLS */
      enabled: z.boolean().default(false),
      /** CA certificate (path or PEM string) */
      ca: z.string().optional(),
      /** Client certificate for mTLS (path or PEM string) */
      cert: z.string().optional(),
      /** Client private key for mTLS (path or PEM string) */
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

  // Reconnect settings
  reconnect: z
    .object({
      /** Max reconnect attempts (-1 = infinite) */
      maxReconnectAttempts: z.number().default(-1),
      /** Initial reconnect wait time in milliseconds */
      reconnectTimeWait: z.number().default(2000),
      /** Max reconnect wait time in milliseconds */
      maxReconnectTimeWait: z.number().default(30000),
    })
    .optional(),

  /**
   * Maximum payload size in bytes for NATS messages.
   * Recommended: 4MB (for large MCP payloads with images/documents)
   * Note: This is a documentation field only. The actual max_payload is
   * configured server-side in NATS, and the client automatically respects it.
   * This field exists to document the expected server configuration.
   */
  maxPayload: z.number().optional(),
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
    maxReconnectAttempts: -1,
    reconnectTimeWait: 2000,
    maxReconnectTimeWait: 30000,
  },
};

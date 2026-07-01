/**
 * Shared connection-status type for the NATS connection runner.
 */

/**
 * NATS connection status events
 */
export enum NatsConnectionStatus {
  /** Initial state before connection attempt */
  Disconnected = "disconnected",
  /** Currently disconnecting */
  Disconnecting = "disconnecting",
  /** Currently connected */
  Connected = "connected",
  /** Attempting to reconnect after disconnection */
  Reconnecting = "reconnecting",
  /** Connection permanently closed */
  Closed = "closed",
}

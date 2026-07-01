/**
 * Thrown when a required NATS operation fails due to disconnection.
 * Callers can catch this to return appropriate HTTP status (503 Service Unavailable).
 */
export class NatsNotConnectedError extends Error {
  constructor(message = "NATS is not connected") {
    super(message);
    this.name = "NatsNotConnectedError";
  }
}

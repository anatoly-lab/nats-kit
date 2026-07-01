/**
 * Telemetry seam (design D7).
 *
 * Library-owned, all-optional, with NO OpenTelemetry import. The consumer
 * implements this with real OTel (spans/metrics) and injects it; the default is
 * {@link noopTelemetry}. Mirrors the {@link NatsLogger} dependency-inversion
 * precedent — the core stays vendor-neutral and dependency-light.
 *
 * All methods are optional so the surface can grow (add `onKvPut?`, span
 * context, etc.) without a breaking change.
 */
export interface NatsTelemetry {
  onPublish?(subject: string): void;
  onConsume?(subject: string, outcome: "ack" | "nak" | "term"): void;
  onReconnect?(): void;
  onError?(op: string, err: unknown): void;
}

/** No-op telemetry: the default when the consumer injects nothing. */
export const noopTelemetry: NatsTelemetry = {};

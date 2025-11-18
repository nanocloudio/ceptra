# CEPtra Wire Header Mapping

Clustor transports already expose a stable set of envelopes (`Why*`,
`ThrottleEnvelope`, Explain metadata) so CEPtra avoids inventing new schemas.
Instead, it annotates the existing headers with a small number of fields that
downstream systems use to steer routing or enrich analytics. This note ties each
field back to the Clustor schema and documents how ingress/egress adapters map
them into Kafka/gRPC headers.

## `DERIVED`

- `ceptra::DerivedEmission` inserts the `DERIVED` flag into the mutable header
  set before the payload is handed back to the Clustor transport stack.
- On the wire this lands inside `Why.flags` alongside the standard
  `TRANSIENT_*`/`PERMANENT_*` reasons. No schema changes are required—Clustor
  simply sees an additional flag value.
- Ingress adapters copy the value into the `ceptra.flags` metadata key when
  publishing to Kafka or gRPC so downstream consumers can enforce policies
  (“route DERIVED events only to canary sinks”).
- Egress consumers (for example, Explain or the SDKs) should treat the flag as a
  pure annotation: it is not a new error code, merely a signal that the payload
  was emitted by a derived rule instead of the primary ingest path.

## `lane_bitmap`

- Aggregation stages carry a 64-bit bitmap that records which `lane_domains`
  fired for the current evaluation. The bit ordering mirrors the sorted order
  of the user-declared lane labels.
- The value is injected into `Why.lane_bitmap` before the payload leaves the
  node. Nothing about the Clustor encoding changes—the field already exists and
  CEPtra just populates it.
- Kafka/gRPC adapters publish the bitmap as the `ceptra.lane_bitmap` header so
  batchers can route events to the correct per-lane downstream systems (for
  example, fan-out per `carrier_id` or `facility_id`).
- Operators can decode the bitmap into lane names by consulting the active
  bundle’s `lane_domains` map. The helper `tools/bundle_deployer` already emits
  the sorted order used on the wire.

## `flags`

- Besides `DERIVED`, CEPtra reuses the existing `flags` map for late-event
  markers (`LATE`) and replay inversions (`RETRACT`). These values are not
  CEP-specific schemas—they are the same strings described in Clustor §0.3 and
  appendix E.
- Egress adapters surface them unchanged via the `ceptra.flags` header, and
  SDKs include the last-seen set in `AppendTelemetry` so operators can correlate
  retries with gating predicates.

## How SDKs and operators should use the fields

- **Ingestion** – when the Kafka source pulls payloads from a channel that has
  been wired into a `source.*` phase, it passes the `lane_bitmap` and `flags`
  back through the exact same header keys (`why_lane_bitmap`, `why_flags`). This
  makes ingestion idempotent: CEPtra simply forwards the metadata back to
  Clustor unchanged.
- **Egress** – the SDKs expose the `flags`/`lane_bitmap` values via their normal
  telemetry exports. Operators can correlate `DERIVED` or `LATE` with specific
  partitions without needing to decode the Clustor wire protocol.
- **Analytics** – systems that archive derived events should store the flags and
  lane bitmaps next to the payload. Replaying a bundle or running a new canary
  against historical data therefore has access to the same metadata the
  real-time system saw.

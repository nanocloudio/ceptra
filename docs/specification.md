# CEPtra – Unified System Specification
> **Dependency Banner:** CEPtra is layered on the Clustor Consensus Core; all replication, durability, wire, and control-plane semantics are inherited from Clustor §§0–16 and App.A–E. Where language in this document conflicts with Clustor, §0 of the Clustor spec governs and CEPtra only defines additive CEP behavior. Mapping tokens (for example, §3.3, App.C) follow the Clustor reference verbatim.

Version: Draft 1.0  
Language: Rust (no GC runtime)  
Deployment: Single-binary, self-arranging Kubernetes StatefulSet

---

## Table of Contents
0. Dependency & Conformance  
1. Overview  
2. Partitioning & Routing  
3. Event Model  
4. Replication & Consistency  
5. Durability & Acknowledgment Policy  
6. Storage Layout  
7. Active Processor (AP)  
8. In-Memory Aggregation Structures  
9. Rule Layer & Meta-Aggregation  
10. Retention & Compaction  
11. Consistency, Idempotency & Recovery  
12. Fault Tolerance & Failover  
13. Time Semantics & Late Data Policy  
14. PromQL-Style Definition Surface  
15. Hot Reload & Canary Protocol  
16. Control Plane – Durability & Epochs  
17. Backpressure & Flow Control  
18. Schema Evolution & Replay Compatibility  
19. Storage Format Details  
20. Checkpointing  
21. Threading & Concurrency Model  
22. Security Model  
23. Configuration Parameters & Operational Controls  
24. Telemetry & Observability  
25. Disaster Recovery & Cluster Recovery  
26. Deployment & Bootstrap (Kubernetes)  
27. Summary of Guarantees
28. Interoperability Notes

---

## 0  Dependency & Conformance

### 0.1 Layering Statement
[Informative] CEPtra executes entirely on Clustor’s Raft partition groups, durability profiles, wire contracts, and manifest pipeline. All references to `raft_log_index`, `commit_index`, ReadIndex, `strict_fallback`, Group-Fsync, or ControlPlaneRaft artifacts point directly to the normative text in Clustor §§0–16. CEPtra extends the substrate only with workload-specific compute semantics (event parsing, panes/lanes, aggregators, CEL/PromQL bindings, CEP checkpoints, and client credit hints).

[Informative] Terminology introduced later in this document (for example, `commit_epoch_ticks`, `commit_rate_state`, or `DedupCapacityUpdate`) is scoped strictly to CEPtra’s compute layer. These names never alter Clustor’s §2.2 vocabulary table, wire enums, or manifest schemas unless and until they are formally registered through the Clustor term-registry process.

### 0.2 Dependency Matrix
| Topic | Clustor Source | CEP Scope |
|-------|----------------|-----------|
| Replication & consistency | §3 (Raft partition groups, leases/ReadIndex), §0.5 (strict_fallback truth table) | CEPtra consumes the provided sequencing and readiness signals; no overrides exist. |
| Durability & ACK policy | §6 (Strict vs Group-Fsync), §3.4 (ACK contract) | CEPtra only advertises which durability mode the workload selected; behavior must remain inside the active Clustor profile. |
| Snapshots & state transfer | §8 | CEP checkpoints (§20) are additive compute-state manifests layered atop Clustor snapshots. |
| Storage layout, encryption, log repair | §§5–6, §9 | WAL and encryption formats follow Clustor exactly; CEPtra tracks only higher-level compute metadata. |
| Control plane, epochs, readiness gates | §11 (ControlPlaneRaft, activation barriers, config epochs) | CEP adds AP-specific readiness predicates and lane-domain validation; all epoch management is Clustor-driven. |
| Wire catalog, error codes, telemetry registry | §0.3, §14, App.C–E | CEPtra reuses the same frozen catalogs and wide-int JSON/ChunkedList rules; CEP-specific envelopes appear only as application payloads. |
| CEP-unique behavior | §§2–3, 7–10, 12–15, 17–25, 27 | Event model, lane budgets, aggregator semantics, CEL/PromQL compilation, watermarking, late-data policy, checkpoints, and flow hints remain defined here. |

### 0.3 Terminology, Units, and Metrics
[Deprecation] Terminology mirrors Clustor §2.2: this document refers to **Raft partition groups (RPGs)**, Strict/Group-Fsync durability, ReadIndex, DurableOnly, and strict_fallback using those names exclusively. Telemetry now follows Clustor’s `_ms` millisecond suffix for any exported time unit. Legacy `_seconds` metrics continue to emit READ-ONLY data for one release and are listed below for compatibility tracking.

| Deprecated `_seconds` Metric | Preferred `_ms` Metric | Notes |
|------------------------------|------------------------|-------|
| `ap_watermark_ts_seconds` | `ap_watermark_ts_ms` | Partition and per-metric watermark emissions |
| `ap_event_lateness_seconds` | `ap_event_lateness_ms` | Distribution of lateness deltas |
| `wal_dirty_epoch_age_seconds` | `wal_dirty_epoch_age_ms` | Derived from Clustor durability guardrails |
| `wal_fsync_latency_seconds` | `wal_fsync_latency_ms` | Includes Strict and Group-Fsync fences |
| `ap_apply_latency_seconds` | `ap_apply_latency_ms` | Apply worker latency |
| `replication_lag_seconds` (+ `_p95`) | `replication_lag_ms` (+ `_p95`) | Mirrors Clustor §14 gauges |
| `apply_lag_seconds` | `apply_lag_ms` | Commit→apply backlog |
| `cp_reconcile_duration_seconds` | `cp_reconcile_duration_ms` | ControlPlaneRaft reconcile loops |
| `security_cert_expiry_seconds` | `security_cert_expiry_ms` | Time until mTLS material expires |
| `security_kms_latency_seconds` / `wal_kms_block_seconds` | `security_kms_latency_ms` / `wal_kms_block_ms` | KMS/AEAD timings |
| `definition_window_default_seconds` | `definition_window_default_ms` | Stage hint observability |
| `config_reload_duration_seconds` | `config_reload_duration_ms` | Config apply latency |
| `cert_expiry_seconds` | `cert_expiry_ms` | `/readyz` readiness gate |

[Normative] All configuration knobs expressed in seconds now specify millisecond integers (for example, `checkpoint_age_threshold_ms`). Operators should migrate dashboards and alerting rules to the `_ms` series; Clustor spec-lint enforces this naming from the next release onward.
[Deprecation] The legacy `_seconds` metrics above are read-only aliases that mirror the corresponding Clustor `_ms` gauges; they exist solely for compatibility during the transition window and will be removed after one release without changing the underlying measurement.
[Normative] Each `_ms` metric enumerated here is registered in `telemetry_catalog.json` and in the Clustor App.C `json_wide_int_catalog` list so that `json_wide_int_catalog_test` continues to verify millisecond durations are emitted as decimal strings. Exporters MUST populate these metrics as wide-int strings, not binary floats.
[Deprecation] Legacy `_seconds` metrics remain `read_only = true` entries inside the telemetry catalog and MUST reuse the `_ms` counter source by dividing by 1 000 at presentation time; implementations may not compute or emit fresh `_seconds` values from live clocks.

### 0.4 Spec-Lint, Wire & Catalog Compliance
[Normative] Every non-heading paragraph, blockquote, and list item in this document carries an explicit `[Normative]`, `[Operational]`, `[Informative]`, or `[Deprecation]` tag. Clustor spec-lint requires `clause_tag_coverage = 1.0` for every manifest entry; edits MUST preserve those tags so clause hashing remains stable across releases.
[Normative] `docs/specification.md` is recorded inside `manifests/ceptra_manifest.json` with `{ path, sha256, clause_tag_coverage = 1.0 }` and the manifest is chained into `consensus_core_manifest.json` next to the primary Clustor spec entries. A CEPtra change is therefore inseparable from a Clustor change: spec-lint refuses to advance the manifest hash tree when this file’s digest or clause coverage does not match the recorded value.
[Normative] `manifests/ceptra_manifest.json` enumerates the shared artifacts that govern CEPtra’s wires: `wire_catalog.json`, `wire_error_catalog.json`, `wire_envelope_catalog.json`, `telemetry_catalog.json`, and `term_registry.json`. Any CEPtra-visible wire, envelope, or telemetry addition MUST land in those catalogs—and update their hashes inside the manifest—before the prose references it. CEPtra does not mint parallel catalogs.
[Informative] CEPtra continues to reuse Clustor’s wire catalog, envelope schemas, and error code registry (§0.3, App.C–E). Application payloads such as CEP-derived alerts ride on Clustor transports unchanged, and CEP-only identifiers remain confined to payload bodies. Wide-int JSON encoding, ChunkedList framing, and error taxonomy therefore remain governed by the same generators and self-tests as Clustor.
[Operational] Contributors MUST run `spec_lint --manifest consensus_core_manifest.json` (or the corresponding CI check) before merging changes; builds that skip the manifest refresh are flagged as “out-of-band” and may not be promoted.

### 0.5 Term Registry Coupling
[Normative] Any runtime noun, enum, or readiness state that appears on Clustor-wide telemetry, `/readyz`, Explain bundles, control plane RPCs, or ingest responses MUST be registered in Clustor §2.2 with a `term_id` and mirrored in `term_registry.json`. CEPtra implementers MUST NOT emit a CEP-only symbol on those shared channels until `term_registry_check` passes for the new entry.
[Informative] CEP-private states such as `SHADOW_READY`, `SHADOW_FAILED`, `ceptra_finalized_horizon_stall`, and other CEP-only readiness or dedup classifications remain confined to CEPtra telemetry bundles (for example, `ceptra_shadow_state`, `ceptra_ready_partitions_ratio`). When CEPtra references a Clustor readiness primitive it reuses the exact Clustor names—`shadow_apply_state ∈ {Pending, Replaying, Ready, Expired}`, `ActivationBarrier`, `warmup_ready_ratio`—and never substitutes CEP-local labels inside those shared schemas.
[Normative] These CEP-local states MUST NOT leak into Clustor’s telemetry bundle, Explain schema, or wire contracts unless they have first been promoted to the shared registry and added to the Clustor spec.
[Operational] If CEPtra instrumentation or Explain generators require a CEP-private symbol in a Clustor-owned schema, the owning team must (1) land the registry entry and manifest hash updates in Clustor, (2) reference the canonical name in this document, and (3) delete any CEP-private aliases so that spec-lint enforces a single vocabulary across both stacks.

### 0.6 Conformance Statement
[Normative] Clustor §0 governs conflict resolution, manifests, and spec-lint coverage for both the control plane and worker binaries. CEPtra bundles, checkpoints, and telemetry must pass the same manifest validation and do not introduce alternate schema roots. All subsequent references in this document assume those base guarantees and describe only the additional CEP processing semantics layered on top.

---

## 1  Overview

[Informative] CEPtra is a high-throughput, low-latency in-memory complex event processor designed to track, link, and aggregate heterogeneous event streams (for example, IoT telemetry, security signals, or transactional feeds) in real time.

* [Informative] **Target throughput:** ≥ 10 000 events / s per active partition  
* [Informative] **Target p99 latency:** ≤ 5 ms end-to-end for accepted events  
* [Informative] **Consistency:** deterministic results for any given ordered sequence of events  
* [Informative] **Durability:** acknowledgment only after quorum replication  
* [Informative] **Restart time:** sub-second; no pause on single-replica loss  
* [Informative] **Hot-reload:** atomic cutover of rule and metric definitions with concurrent versioning  
* [Operational] **Deployment:** single Rust binary containing all roles (Control Plane, Replica, Active Processor, Control Agent) running as a Kubernetes StatefulSet.  
  [Informative] Each pod auto-selects roles via StatefulSet ordinal and peer discovery.

### 1.1 Primary Use Cases

- [Informative] **IoT fleet monitoring:** ingest per-device telemetry, apply mixed fast/baseline aggregation windows, and emit policy actions for overheating or authentication anomalies while preserving deterministic replay for audits.
- [Informative] **Security operations correlation:** fuse IDS, EDR, and identity streams into per-entity partitions, evaluate multi-signal rules in CEL, and route high-confidence incidents to downstream SOC tooling with quorum-backed durability.
- [Informative] **Telco network QoS assurance:** maintain rolling quantiles and baselines for cell and backhaul performance, trigger mitigations on jitter, packet loss, or latency regressions, and persist metrics for disaster recovery without sacrificing real-time latency.
- [Informative] **E-commerce supply chain visibility:** track warehouse scans, carrier updates, and inventory telemetry, compare short-term deviations against historical baselines, and emit remediation workflows when SLA breaches or stock shocks emerge.
- [Informative] **Connected gaming telemetry with cheat detection:** consolidate session metrics, matchmaking stats, and anti-cheat signals, compute per-cohort latency and win-rate deltas, and escalate suspicious patterns for live operations or automated countermeasures.

---

## 2  Partitioning and Routing

* [Informative] Deterministic partitioning by 64-bit hash on a logical key (e.g., device identifier, customer account, geography).  
* [Informative] Three replicas (`R0 R1 R2`) per partition per Clustor §11 placement policy.  
* [Informative] **Raft-based replication:** each partition is a 3-voter **Raft partition group (RPG)** per Clustor §2.2.  
  - [Informative] Client appends → RPG leader → replication to followers → quorum commit, as defined in Clustor §3.  
  - [Informative] Leader index (`log_index`) serves as `seqno` and follows the ReadIndex and lease rules in Clustor §3.3.  
  - [Informative] Follower catch-up and snapshot handling defer entirely to Clustor §8.  
* [Informative] **Durability acknowledgment:** leader ACKs client only after quorum commit inside the active Clustor durability profile (§6 / §3.4).  
* [Informative] **Event duplication** across partitions is allowed and expected.  
* [Informative] **Leader affinity:** AP colocated with the RPG leader by default; warm standby AP on followers, respecting Clustor lease ownership.

### Lane Budgets and Grouping
* [Informative] **Lane limits:** `LANE_MAX_PER_PARTITION = 64`; warning when active lanes `>= LANE_WARN_THRESHOLD = 48`.  
* [Normative] **Group-aware definitions:** `by(...)` is permitted only on labels whose domains are proven to have bounded cardinality within the configured lane budget. Bundles must declare limits (e.g., `facility_id <= 32`, `pod <= 8`) when the compiler cannot infer them. Fresh workloads without historical telemetry MUST include explicit `lane_domains.max_per_partition` declarations for every label whose bound cannot be proven statically; the control plane rejects the bundle otherwise. Labels whose true domain would exceed the cap must be folded into the partition key or otherwise sharded so that the per-partition bound remains ≤64.  
* [Operational] **Validation:** the control plane rejects bundles whose projected lane count may exceed the bound; operators receive diagnostics pointing at offending expressions. Grouping by the partition key is allowed but redundant and produces a single lane per partition.  
  [Normative] Workloads requiring higher cardinality must either encode that label into the partition key (reducing the residual per-partition bound to 1) or shard the workload across multiple partitions before compilation.
* [Informative] **Lane accounting scope:** lane admission is enforced per PromQL metric. For every metric that declares a `by(...)` set, the control plane projects the maximum per-label cardinality for that metric individually, then sums the contributions across the bundle (still capped at `LANE_MAX_PER_PARTITION`). A label that appears in multiple metrics therefore contributes only to the metrics whose grouping expressions actually reference it; safe usages do not inherit the maximum from unrelated metrics.
* [Operational] **Runtime overflow:** if live data would allocate a lane beyond the limit, lane creation freezes for that metric; existing lanes continue to update. Events whose grouping would require a new lane are not applied to that metric: the AP flags the event with `LANE_OVERFLOW`, increments the CEP-only metric `ceptra_definition_lane_overflow_total{part_id,metric}`, and records the same reason in audit logs. No fallback “overflow lane” or probabilistic bucketing is permitted, keeping behaviour deterministic and surfacing the misconfiguration explicitly for operators. Lanes are not reclaimed once allocated: lane membership is stable for the lifetime of the process, and `LANE_MAX_PER_PARTITION` is enforced as a hard ceiling on the total count of ever-allocated lanes, not just currently active ones.

[Informative] **Projection**
- [Informative] The control plane projects lane counts using declared `lane_domains.max_per_partition`, static analysis of label matchers, and optional 24 h P95 historical cardinality telemetry.  
- [Informative] Multi-label groupings multiply per-label maxima unless constrained by matchers.  
- [Informative] If projected lanes fall within `[LANE_WARN_THRESHOLD, LANE_MAX]`, the bundle is admitted with WARN; projections exceeding `LANE_MAX` are rejected with diagnostics.

### Client Append Interface
[Informative] *Transport & Security*
- [Informative] Clients connect to the Raft leader over mTLS (TLS 1.3) with mutual authentication; unauthorized calls are rejected before routing.
- [Informative] Client append RPCs run on gRPC over HTTP/2 with pipelined unary or streaming channels per partition leader. Every message carries the standardized envelope `{event_id, part_id, ts_ns, event_key_hash, schema_key, payload_ref}`.

[Informative] *Request semantics*
- [Informative] Exactly one logical event per message; batching is disabled. Multiple in-flight messages are permitted up to the advertised credit window on each streamed or pipelined channel.
- [Normative] Retries must reuse the same `event_id`; the leader treats duplicates as idempotent and returns the prior outcome.
- [Informative] Append responses include `{status, commit_index, credit_window_hint_events, credit_window_hint_bytes}`. These numeric hints advertise the next CEP-specific window size derived from §17 flow control, while the wire-level Clustor `credit_hint` field (the string enum `Recover|Hold|Shed` defined in §10.3) remains unchanged and authoritative for throttle semantics. Clients therefore continue to parse the Clustor field as before and may optionally opt into the numeric hints.

[Informative] *Error model*
- [Informative] **Transient failures** (`TRANSIENT_RETRY`, `TRANSIENT_REDIRECT`): leader change, quorum timeout, backpressure. Clients backoff with jitter and retry the same event after honoring the new credit window.
- [Normative] **Permanent failures** (`PERMANENT_SCHEMA`, `PERMANENT_EPOCH`, `PERMANENT_PAYLOAD`): schema blocked, stale epoch, validation failure. Clients must not retry; operators inspect the audit log.
- [Normative] **Durability fence** (`PERMANENT_DURABILITY`): deferred quorum fsync failed; partition is fenced in Strict mode until operators remediate storage and acknowledge the downgrade. Clients treat this as non-retriable and must pause append loops for the partition until the fence clears. The `PERMANENT_DURABILITY` string is emitted only via `cep_status_reason`; the Clustor `error_code` continues to carry whichever durability incident the substrate reported.
[Informative] All wire-visible `error_code` values continue to use the Clustor registry (§0.3). CEPtra surfaces the names above solely as an additional string field (`cep_status_reason`) inside the application payloads (and log/audit records) so that clients may distinguish CEP-specific causes without encountering undocumented numeric codes. The numeric `error_code` is always populated directly from the current Clustor `wire_error_catalog.json`; CEPtra never writes a new numeric ID into that slot.
[Normative] Each mapping in the table below references a documented Clustor identifier when one exists. If the desired name is absent from `wire_error_catalog.json`, CEPtra MUST map the situation onto an existing Clustor code (for example, `AppendDecision::Reject(Consistency)` or `ThrottleTransient`) and treat the CEP-specific label strictly as a `cep_status_reason`. Where this table notes “placeholder – requires Clustor registry entry,” the owning team MUST extend the Clustor registry (including manifest hash updates) before emitting the named code on the wire.
[Operational] The `cep_status_reason` string is purely additive: it never replaces the `error_code` enum and may be omitted entirely when the standard Clustor code is sufficient. Downstream systems that only understand the numeric code therefore continue to behave exactly as they do for non-CEP workloads.

| `cep_status_reason` | Clustor `error_code` (when available) |
|---------------------|---------------------------------------|
| `TRANSIENT_BACKPRESSURE` | `ThrottleTransient` |
| `TRANSIENT_WARMUP` | `ThrottleTransient` with `why.reason="warmup"`; dedicated `WarmupPending` would require a future Clustor registry entry |
| `TRANSIENT_RETRY` | `ThrottleTransient` |
| `TRANSIENT_REDIRECT` | `LeaderRedirect` |
| `PERMANENT_SCHEMA` | `SchemaRejected` (fallback `SchemaUnknown` when the registry entry is absent) |
| `PERMANENT_PAYLOAD` | `PayloadInvalid` |
| `PERMANENT_EPOCH` | `RoutingEpochMismatch` |
| `PERMANENT_DURABILITY` | `AppendDecision::Reject(Consistency)` today; placeholder – future `DurabilityFenceActive` entry must be added to the Clustor registry before a named code is used |
| `PERMANENT_RETRY_EXPIRED` | `AppendDecision::Reject(Consistency)` until a `RetryWindowExpired` code is registered |
| `ERR_UNKNOWN_SCHEMA` | `SchemaUnknown` |
| `ERR_SCHEMA_BLOCKED` | `SchemaBlocked` (placeholder – requires Clustor registry extension if not already present) |
| `ERR_STATE_UPGRADE_FAILED` | `AppendDecision::Reject(Consistency)`; placeholder – requires a `StateUpgradeFailed` registry entry to expose a named code |

[Informative] Implementations MAY extend the mapping table above only by allocating new numeric IDs through the Clustor registry process; CEPtra never unilaterally invents additional wire-visible codes. Every `PERMANENT_*` or `TRANSIENT_*` string in this section is therefore a CEP-local classification surfaced through `cep_status_reason` and logs rather than a shared enum.

[Informative] *Backpressure*
- [Informative] When the leader’s credit budget is exhausted it responds with `TRANSIENT_BACKPRESSURE`, sets the Clustor `credit_hint` to `Shed`, and lowers the numeric `{credit_window_hint_events, credit_window_hint_bytes}`; clients halve their in-flight window and retry after the indicated delay.
- [Informative] If `/readyz` reports the target partition as `NotReady`, or the overall healthy-partition ratio drops below the ≥99% (configurable) threshold, the endpoint rejects appends with `TRANSIENT_WARMUP`, signalling the caller to pause until readiness is restored.

[Informative] *Observability*
- [Informative] Every append request is logged with `{event_id, status, retry_count}`; aggregated metrics feed `client_window_size` and `reject_overload_total` in §17.7.

---

## 3  Event Model

| Field | Description |
|--------|-------------|
| `part_id` | Partition identifier |
| `raft_log_index` | Monotonic index within partition (log order) |
| `event_id` | Unique ID for retries of same logical event |
| `event_key_hash` | 128-bit SipHash-derived key fingerprint; collisions verified against canonical key |
| `ts_ns` | Event timestamp (UTC nanoseconds) |
| `schema_key` | Interned identifier of payload schema |
| `payload` | Normalized event attributes and metrics |
| `lane_bitmap` | Bitmask for lane predicates |
| `commit_epoch_ticks` | Leader-stamped monotonic tick used for dedup retention (§3.1) |
| `flags` | Metadata (LATE, OUT_OF_RANGE, etc.) |

### Commit Epoch Ticks & Throughput Estimator

* [Normative] `commit_epoch_granularity_ns = 1_000_000` (1 ms). The Raft leader samples `CLOCK_MONOTONIC_RAW` when acknowledging a batch, computes `tick = floor(now_ns / commit_epoch_granularity_ns)`, clamps it with `commit_epoch_ticks = max(tick, last_commit_epoch_tick)` (no forced `+1`), and persists that value into every WAL record in the batch. Multiple records acknowledged within the same millisecond therefore share identical ticks, preserving the coarse wall-clock semantics required by dedup retention. `last_commit_epoch_tick` (initially `0`) is updated after each append so the sequence remains monotone even if the clock stalls; consecutive batches may legally share the same tick.
  - [Normative] **Clock regression handling:** `CLOCK_MONOTONIC_RAW` is expected to be monotone, but if the platform reports a temporary negative delta (for example, during rare TSC recalibration), the implementation MUST clamp the sampled tick to `last_commit_epoch_tick` and emit `wal_clock_regression_total{part_id}` rather than panicking. The clamp preserves determinism while surfacing the anomaly for operators; forward progress resumes automatically once the clock reports ≥ the last tick.
* [Normative] The Active Processor tracks `commit_epoch_now = last_applied_commit_epoch_tick` and persists the pair `{commit_epoch_now, last_applied_index}` inside checkpoints. When Raft leadership changes, the new leader MUST seed `last_commit_epoch_tick` before acknowledging any writes by loading the greatest `commit_epoch_ticks` observed in durable state (checkpoint metadata if available, otherwise a bounded WAL scan of the open segment). This prevents the tick stream from regressing after failover and keeps every replica's eviction predicates that depend on `commit_epoch_now - commit_epoch_ticks` well-formed.
  - [Normative] Leaders MUST first consult the latest committed checkpoint header; if the checkpoint’s `commit_epoch_ticks` is older than the durable WAL head, they perform a bounded scan of the current WAL segment footer to capture any larger tick written after the checkpoint but before leadership transfer. This scan is limited to the open segment and therefore does not impact replay determinism.
  - [Normative] Implementation MUST perform the bounded WAL scan only after the substrate has completed its crash-repair/startup scrub pass on the open segment (per Clustor §9) and has re-established a trustworthy WAL tail. The scan must observe the highest `commit_epoch_ticks` among valid, post-repair records in the open segment, and it may examine only bytes whose AEAD/MAC/CRC validations have already succeeded. Any partially repaired or truncated region is excluded from the search so that `last_commit_epoch_tick` always reflects a durable, replayable tick.
  - [Normative] If the scan finds no valid record whose `commit_epoch_ticks` exceeds the checkpointed value, replicas MUST converge on the checkpoint’s `last_commit_epoch_tick` and treat it as authoritative, even if earlier versions observed a higher tick before the crash. This guarantees that every replica re-enters service with an identical `last_commit_epoch_tick`, preserving dedup-retention determinism.
* [Informative] Each partition maintains a deterministic throughput estimator `commit_rate_state = {ewma_per_sec, last_sample_tick, last_sample_index}` with `ewma_per_sec` initialised to `max_ingest_rate_per_partition` from the control plane. When applying new entries, if `delta_tick = commit_epoch_now - last_sample_tick > 0`, compute `delta_time_s = delta_tick × (commit_epoch_granularity_ns / 1e9)` and `sample_rate = (applied_index - last_sample_index) / delta_time_s`. Update `ewma_per_sec` using exponential smoothing with half-life `τ = 10 s`: `α = 1 - exp(-delta_time_s / τ)` and `ewma_per_sec = (1 - α) × ewma_per_sec + α × sample_rate`. Persist the updated state alongside checkpoints. When `delta_tick = 0`, defer the update until the next tick boundary to keep the estimator monotone. Estimator updates occur only when the AP applies an event whose `commit_epoch_ticks` is strictly greater than the previous sample tick, ensuring that every replica samples on the same WAL entries regardless of short-lived apply lag.  
  - [Operational] The control plane seeds `max_ingest_rate_per_partition` from operator policy and re-estimates it every 30 s by sampling `commit_rate_state.ewma_per_sec` across the partition’s replicas. The reconciler sets the new target to `ceil(1.5 × p95(ewma_per_sec, 5 min window))`, bounded by operator-configured `{min,max}` caps. Partitions whose local EWMA exceeds the control-plane target for two consecutive samples automatically emit `ceptra_dedup_rate_divergence_total{part_id}` and request an updated limit; the reconciler either accepts the higher value or issues a throttle so that dedup sizing assumptions remain valid. The 30 s sampling cadence is advisory only—no per-event decision reads wall-clock time—and any accepted change to `max_ingest_rate_per_partition` becomes effective only after the CP appends a replicated `DedupCapacityUpdate` marker and publishes a checkpoint that records the new bound.
* [Informative] Define `index_distance_for(window_s) = ceil(window_s × ewma_per_sec)` (minimum 1). The estimator relies solely on replicated ticks and indices, so replays deterministically reproduce the same distance.
[Normative] `commit_epoch_ticks` are replica-local metadata that exist purely to drive dedup retention, checkpoint scheduling, and throughput estimation. Implementations MUST NOT compare raw tick values across replicas for safety decisions, leases, or SLO enforcement; all cross-replica safety checks continue to rely on the Clustor lease and ReadIndex machinery guarded by `clock_guard`.
[Normative] `commit_epoch_ticks` therefore never participate in the Clustor lease election, ReadIndex approval, strict-fallback gate, or `clock_guard` calculations; those predicates remain governed solely by the substrate while CEPtra consumes the resulting signals.
[Normative] Every consumer of `commit_epoch_now` MUST express its predicates as differences relative to persisted floors—`commit_epoch_now - commit_epoch_ticks`, `commit_epoch_now - checkpoint_floor`, or the derived `index_distance_for(...)`. It is invalid to treat ticks as wall-clock timestamps or to evaluate absolute inequalities that could be invalidated when leadership restarts from a checkpoint floor.
[Informative] Because checkpoints persist `{commit_epoch_now, last_applied_index}`, a crash between checkpoint publication and the bounded WAL scan can legitimately roll `last_commit_epoch_tick` back to the checkpointed value. Dedup eviction, retention windows, and throughput estimation remain deterministic because they rely only on the persisted floor plus deltas rather than on any absolute notion of “current time”.

### Idempotency
[Informative] Per-event deduplication: A bounded, partition-local dedup table keyed by `event_id` prevents reprocessing of retries.  
[Informative] Structure: 1024 sharded lock-free cuckoo sets storing `(event_id → {raft_log_index, commit_epoch_ticks})`.  
[Normative] Shard assignment MUST be a deterministic, cluster-stable function of `event_id` alone. Implementations MUST derive the dedup shard index via a fixed hash function and key—for example `shard = SipHash-2-4(event_id, cluster_secret) mod 1024`—and MUST NOT change this mapping across upgrades or replays. This ensures that the same `event_id` always maps to the same shard on every replica and during every replay.
[Informative] Retention:
- [Normative] The control plane exposes `client_retry_window_s` (default 30 min) as the maximum interval that a client is permitted to retry an acknowledged `event_id`. Retries that arrive after this interval are rejected with `PERMANENT_RETRY_EXPIRED`, emit `ap_retry_expired_total{part_id}`, and generate an audit log entry `{event_id, part_id, received_ts_ns}`. Clients MUST treat this status as fatal and MUST NOT retry.  
- [Informative] Each dedup entry records the Raft index and the replicated `commit_epoch_ticks` captured at the time of acknowledgement.  
- [Informative] Define `retry_window_ticks = ceil(client_retry_window_s × 1_000)` (because each tick = 1 ms). An entry is eligible for eviction only when both conditions hold:  
  1. [Informative] `raft_log_index < finalized_horizon_index`; and  
  2. [Informative] `commit_epoch_now - commit_epoch_ticks ≥ retry_window_ticks`.  
- [Informative] The deterministic throughput estimator (§3.1) bounds index distance implicitly. To guarantee checkpoint replay safety, the AP also enforces `applied_index - raft_log_index ≥ index_distance_for(2 × checkpoint_full_interval)` before eviction.  
[Informative] The effective retention window is therefore `dedup_retention_window_s = max(client_retry_window_s, 2 × checkpoint_full_interval)` and is persisted in partition metadata.  
[Informative] Capacity planning: the control plane enforces `dedup_capacity_per_partition ≥ max(ceil(max_ingest_rate_per_partition × client_retry_window_s), index_distance_for(2 × checkpoint_full_interval))`. Checkpoints record the enforced capacity and the estimator state so replay uses the same bound.  
[Normative] `dedup_retention_window_s` and its derived `min_retain_index` feed directly into Clustor’s `compute_compaction_floor` routine (§9.1): CEPtra raises the substrate-owned `learner_slack_floor` to `min_retain_index`, waits for the standard SnapshotAuthorization + CompactionAuthAck handshakes, and never issues a standalone truncation command. WAL deletion therefore remains exclusively a Clustor decision even when CEPtra’s dedup horizon is the limiting factor.
[Normative] To keep the eviction predicate tractable, `checkpoint_full_interval` MUST remain inside the `[15s, 5m]` band already enforced for checkpoint scheduling (§20.3); configurations outside this window are rejected because they would either shrink the dedup horizon excessively or make the `index_distance_for(2 × checkpoint_full_interval)` guard unreasonably large.
[Operational] When operators shorten `checkpoint_full_interval` (still inside the permitted band) the Control Plane serializes the change through a replicated `CheckpointScheduleUpdate` marker before the new value takes effect. The marker records the updated `checkpoint_full_interval`, `dedup_retention_window_s`, and `min_retain_index`, ensuring every replica recomputes eviction predicates identically and the Clustor compaction floor (per §19) is raised before any truncation observes the smaller horizon.
[Operational] Whenever the reconciler raises `max_ingest_rate_per_partition`, it seals the current WAL segment with a `DedupCapacityUpdate` marker, applies the new capacity to all replicas atomically, and verifies that the in-memory table is resized before resuming client ACKs. Downward adjustments require operator approval and a quiescent window because they may force eviction; the control plane refuses to shrink capacity while occupancy > 70%.
[Normative] `DedupCapacityUpdate` markers MUST be appended only when no incremental checkpoint serialization is in flight so that every replica observes the marker→checkpoint ordering identically. The next checkpoint header MUST record the new `dedup_capacity_version`, allowing replay to reconcile capacity changes even if the marker and checkpoint land in different WAL segments. If a checkpoint is already running when the control plane approves a capacity raise, the leader waits for that checkpoint to publish before sealing the WAL and writing the marker.
[Normative] Eviction policy: entries are retired oldest-first once the two predicates above are satisfied; no entry that may still be referenced by replay or duplicate retries inside `client_retry_window_s` is removed. If the table occupancy reaches `dedup_capacity_per_partition` while eligible entries remain, the AP must drain them deterministically in log-index order. When the occupancy reaches the capacity and no further entries satisfy the eviction predicate, the leader raises `ceptra_dedup_capacity_exhausted_total{part_id}` and applies `TRANSIENT_BACKPRESSURE` to clients until either the horizon advances or the operator increases the configured capacity. The throttle is expressed with Clustor’s existing `WhyThrottle` envelope and `why.reason="ApplyBudget"` while `cep_status_reason="DEDUP_CAPACITY"` provides the CEP-local detail; no new throttle envelope names are introduced. No probabilistic or sampling eviction is permitted.  
[Informative] Scope: per partition. Duplicate `event_id`s in different partitions are allowed and not deduped.  
[Informative] Checkpoint persistence: replicas capture a deterministic snapshot of each dedup shard alongside checkpoints (§20). The serialized form orders entries by `(raft_log_index, event_id)` so restore + WAL replay reinstates identical dedup membership and eviction watermarks without scanning historical WAL beyond `dedup_retention_window_s`.  
[Informative] Last-event-wins per key: `last_seqno_by_key[event_key_hash]` enforces last-event-wins semantics.  
[Informative] Re-receiving the same `(part_id, event_id)` within `client_retry_window_s` → returns the prior outcome without re-apply.  
[Informative] An older event for the same `event_key_hash` after a newer `raft_log_index` → skipped.

[Informative] **Collision handling:** `event_key_hash` is produced via SipHash-2-4 keyed with a cluster secret and stored as a 128-bit `(hash_hi, hash_lo)` pair. Each `last_seqno_by_key` shard persists the canonical key identity alongside the hash: if the normalized partition key is ≤256 bytes the identity is stored inline; otherwise the shard stores the full byte string in an arena slot plus a SHA-256 digest. Updates only apply when the persisted identity matches byte-for-byte; any mismatch routes the lookup through the bounded spillover map that also stores the full key bytes. The validator emits an audit record (`ceptra_ap_key_hash_collision_total`) if a collision is detected, but last-write-wins semantics rely exclusively on the canonical identity rather than a truncated fingerprint, eliminating false matches.

### Finalized Horizon
[Informative] Each partition maintains a deterministic `finalized_horizon_index` and a per-metric `finalized_horizon_ts_ns` describing which panes are immutable for replay and eviction. All indices in this section use the Clustor §3.3 definitions for `applied_index`, `commit_index`, and ReadIndex sequencing:

1. [Informative] On every watermark update (§13.3), compute `finalizable_ts(metric) = WM_e(metric) - C(metric)` where `C(metric)` is the correction horizon chosen for that metric (`0` for DROP-only aggregators, default `1h` otherwise).  
2. [Informative] Mark all panes whose right edge ≤ `finalizable_ts(metric)` as finalized for that metric. The newest event’s Raft index among those panes becomes the candidate finalized index.  
3. [Informative] `finalized_horizon_index` is the minimum of those candidate indices across all metrics in the partition; it advances monotonically.  
4. [Informative] Persist both `finalized_horizon_index` and the per-metric `finalized_horizon_ts_ns` map in checkpoints (§20) so replay reproduces identical bounds.

[Operational] Operators surface `finalized_horizon_index` via `/readyz` metrics (`ap_finalized_panes_total`) and can monitor lag between the applied index and the finalized index to validate late-data policy configuration.

[Normative] **Horizon stall remediation:** If `ap_finalized_panes_total` or `finalized_horizon_index` remains flat for more than `2 × lateness_allowance_L` while `applied_index` advances, the Control Plane raises `ceptra_finalized_horizon_stall{part_id}` and throttles new appends once dedup occupancy exceeds 80% of `dedup_capacity_per_partition`. Operators must investigate late-data policy or correction horizons: either tighten `C(metric)` for the offending metrics or move them to DROP-only mode. After configuration change, the CP records an override audit entry and releases the throttle once the finalized horizon catches up. Runbook: (1) inspect `/readyz` for the stuck partition and identify metrics with high lateness via `ap_late_events_total{metric}`; (2) adjust lateness allowance or correction horizon via `/v1/config`; (3) verify `finalized_horizon_index` convergence before restoring prior durability/latency settings. This ensures dedup eviction proceeds without unbounded backlog.

[Normative] Metrics that require very large correction horizons (hours or longer) SHOULD be isolated into their own partitions or rule bundles so they do not hold back unrelated workloads. As a last resort, operators may disable the offending metric’s CEP definition entirely to allow `finalized_horizon_index` to advance; this bypass is preferable to globally relaxing lateness policies that would otherwise affect every metric in the partition.
[Normative] Ties are intentionally resolved pessimistically: `finalized_horizon_index` always tracks the slowest metric in the partition so that replay and eviction never overrun any metric’s correction horizon. Operators who want faster advancement for latency-sensitive metrics must place the long-horizon metrics in separate partitions or disable them explicitly.

### Integrity
* [Informative] CRC per block and segment per Clustor §9.  
* [Informative] No per-record checksum for efficiency.  
* [Informative] All payloads validated via schema registry and signature.

---

## 4  Replication and Consistency

[Informative] Replication, quorum commit, ReadIndex/lease handling, and strict_fallback behavior are inherited entirely from Clustor §3 (and §0.5 for the strict-fallback truth table). CEPtra does not redefine sequencing, fencing, or linearizability; every reference to `raft_log_index`, `commit_index`, `applied_index`, or ReadIndex elsewhere in this document refers back to those Clustor guarantees. RPG catch-up, follower repair, and deterministic replay therefore behave exactly as described in the substrate, and CEP-specific components (lanes, panes, aggregators) operate strictly on the committed log stream.

---

## 5  Durability and Acknowledgment Policy

[Informative] Durability modes, acknowledgment timing, dirty-epoch guardrails, and deferred fsync handling are governed entirely by Clustor §§6 and 3.4. CEPtra merely advertises which Clustor durability profile (Strict, Group-Fsync, or strict_fallback) is active per Raft partition group and reacts to Clustor-issued fences (for example, `PERMANENT_DURABILITY`) in §17’s throttles. When a partition enters strict_fallback states such as `LocalOnly` or `ProofPublished`, CEPtra applies the exact read/write restrictions defined in Clustor §0.5 and never attempts to override them. No additional ACK rules or write-barrier semantics are introduced here beyond the ones already enforced by Clustor.
[Normative] CEPtra’s durability and acknowledgment behaviour is entirely governed by Clustor §§3.4 and 6: the substrate decides when an append is acknowledged, when quorum fsync completes, and when strict_fallback applies. CEPtra may classify the outcome (for example, surfacing `PERMANENT_DURABILITY` as a `cep_status_reason`) but it MUST NOT alter ACK ordering, quorum requirements, or fsync sequencing.
[Normative] `PERMANENT_DURABILITY` and any CEP-visible “durability fence” status may be emitted only when the Clustor substrate reports `DurabilityFenceActive`, `strict_fallback_state ∈ {LocalOnly, ProofPublished}`, or one of the storage incidents enumerated in Clustor §0.5 Table 2. CEPtra MUST treat those states as hard readiness failures, stop acknowledging writes that would violate §3.4, and surface the precise Clustor incident code to operators. `PERMANENT_DURABILITY` is therefore purely a CEP-local label layered on top of the Clustor signals; CEP-specific text (for example, “dedup throttling”) can add classifications, but it MUST NOT invent a parallel ACK contract or new wire-level error code.
[Informative] CEPtra sometimes adds additional client-facing fence reasons (dedup capacity exhaustion, warmup gating, etc.), yet those reasons always *tighten* readiness relative to Clustor. Whenever Clustor reports the partition as not ready (`/readyz` false, `strict_fallback_state ∈ {LocalOnly, ProofPublished}`, `ControlPlaneUnavailable`), CEPtra automatically inherits that verdict and may only add detail in `cep_status_reason`; it never flips an ingest path to ready while Clustor says otherwise.

[Informative] `durability_mode` and `fsync_cadence_ms` in §23 are therefore thin control-plane aliases for the Clustor durability primitives:
- [Informative] `durability_mode=STRICT|GROUP_FSYNC` maps 1:1 onto Clustor’s DurabilityTransition entries; requests that conflict with the active hardware/profile guardrails (for example, attempting Group-Fsync on a profile that forbids it) are rejected by CP-Raft before they reach CEPtra.
- [Informative] `fsync_cadence_ms` is validated against (and compiled down to) Clustor’s `group_fsync.max_batch_ms`/`max_batch_bytes` ceilings. CEPtra never emits a cadence that exceeds the profile-specific limits defined in Clustor §6.2 or App.B, and the knob is ignored whenever Strict mode or strict_fallback is active.
[Informative] All durability proofs, lease guards, and strict-fallback transitions therefore continue to originate from the Clustor substrate; CEPtra only exposes friendlier configuration labels and surfaces the resulting state in telemetry.

---


## 6  Storage Layout

[Informative] WAL formatting, compression, encryption at rest, sparse indexing, crash repair, and snapshot transfer reuse Clustor §§5–6 and §9 exactly. CEPtra never mutates the on-disk layout or encryption domains; it only consumes the log via the APIs exposed by Clustor. All references to segments, checkpoints, or repair steps elsewhere in this document therefore rely on the Clustor implementation, while §20 describes the additional CEP-specific compute-state data layered on top.

---


## 7  Active Processor (AP)

* [Informative] Exactly one **AP leader** per partition, colocated with Raft leader.  
* [Informative] Reads committed stream directly from local WAL.  
* [Informative] Maintains in-memory:
  * [Informative] **Pane ring:** `M` panes × `L` lanes.  
  * [Informative] **Per-key map:** `last_seqno_by_key` for idempotency.  
  * [Informative] **Aggregator state:** associative monoids `{identity, ⊕, ⊖}`.  
  * [Informative] **Rolling windows:** pre-defined fixed horizons.

### Apply Logic
1. [Informative] Receive committed batch of `(log_index, event)` tuples.  
2. [Informative] For each event, consult the dedup table:  
   - [Informative] If `dedup.contains(event_id)` → treat as replay, skip apply, and emit prior status.  
   - [Informative] Otherwise insert `{event_id, log_index, commit_epoch_ticks}` into the shard corresponding to the event.  
3. [Informative] If `log_index > last_seqno_by_key[event_key_hash]`:  
   - [Informative] Apply aggregators for all relevant lanes/windows.  
   - [Informative] Update `last_seqno_by_key`.  
4. [Informative] If late relative to watermark → handle via policy (DROP or RETRACT).  
5. [Informative] Periodically checkpoint to persistent storage (see §20).

### Compute Guarantees
* [Informative] **Deterministic:** order by Raft index, pure functions only.  
* [Informative] **Idempotent:** skip if index ≤ recorded last_seqno.  
* [Informative] **Concurrent:** within partition, sequential; across partitions, parallel.

---

## 8  In-Memory Aggregation Structures

### Pane Ring
* [Informative] Base granularity `q` (e.g., 250 ms).  
* [Informative] Ring length `M = ceil(R_max / q)`.  
  * [Informative] `R_max = max(R_raw, max_window_horizon)` where `R_raw` is the raw-tier retention horizon in §10 and `max_window_horizon` is the longest rolling-window horizon declared for the partition. The ring therefore spans every pane that might be read before compaction or eviction.
* [Informative] Each pane is an array of lanes × aggregators.

### Lanes
* [Informative] Fixed predicates evaluated once per event.  
* [Informative] Stored as bitmask `lane_bitmap`.  
* [Informative] Compiler enforces lane budgets per §2; active lanes ≥48 emit warnings and lanes >64 are rejected.  
* [Informative] Examples: device_type, firmware_channel, facility_id (bounded domains only).

### Aggregators
* [Informative] Pure associative monoids implementing:
  - [Informative] `⊕(state, event)` add  
  - [Informative] `⊗(state_a, state_b)` merge (deterministic ordering)  
  - [Informative] `⊖(state, event)` remove (optional)  
  - [Informative] `identity()` neutral element  
* [Informative] Supported v1 families with native PromQL semantics: `sum`, `count`, `avg`, `min`, `max`, `rate`, `increase`, `count_over_time`, `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `quantile_over_time`, `topk`, `distinct`.
* [Informative] `quantile_over_time` uses a fixed-parameter KLL sketch; deterministic merge order guarantees rank error ≤1% at `p95`.  
  - [Informative] All compaction levels use a precomputed, deterministic split schedule; no random sampling or coin flips are permitted.  
  - [Informative] The sketch is seeded with the partition’s `part_id` and metric identifier to select lanes deterministically, and checkpoints persist the exact register buffers so replay and recovery reproduce identical quantiles bit-for-bit.  
* [Informative] `topk` relies on a bounded heap per lane with deterministic tiebreaks; `distinct` uses HyperLogLog++ (relative error ≤1.6%).  
  - [Informative] `distinct` hashes values with SipHash-2-4 keyed by the bundle signature digest; register promotion and sparse/dense transitions follow a fixed ordering with no randomness.  
  - [Informative] Both `topk` and `distinct` persist their serialized register/heap buffers inside checkpoints; replay never recomputes them from higher-level summaries, preserving determinism.
* [Informative] `quantile_over_time`, `topk`, and `distinct` are **non-retractable** and enforce DROP handling when late data would affect their panes.  
* [Informative] Aggregators without `⊖` are marked **non-retractable**; RETRACT policies devolve to DROP with audit records.

[Normative] **Invariant A\*:** All aggregations participating in rolling windows or compaction MUST form associative monoids with identity elements and deterministic merge. Compaction folds panes and mixes tiers (raw vs compacted) exclusively via monoid `⊕` and MUST yield bit-identical results to real-time rolling application.

[Informative] Examples:
- [Informative] `avg` implemented as monoid `(sum, count)` with identity `(0,0)` and merge = pairwise add; final value = `sum / count`.  
- [Informative] `rate`/`increase` defined over per-series monotonic counters with associative sufficient statistics per pane.  
- [Normative] Non-associative functions are prohibited from compaction and must be marked `NON-COMPACTABLE`; they remain queryable only from raw horizons.

[Informative] `avg`: implemented as the monoid `(sum: f64, count: u64)`. `⊕` = pairwise add; identity = `(0,0)`; retract applies `(⊖sum, ⊖count)`. The scalar is produced lazily as `sum / count`.

[Informative] KLL parameters: default `kll_k = 200` with deterministic compaction; target rank error ≤ 1% at `p95`. Configurable per metric set via `policy_version` metadata.  
[Informative] HyperLogLog++: `hll_p = 14` (16384 registers) with sparse mode enabled up to 3000 distincts; relative error ≤1.6%.  
[Informative] `topk`: default heap size `k ≤ 100`; ties broken deterministically by `(value, key_hash)`.  
[Informative] All structures expose hard memory caps per lane; breaching a cap freezes updates and emits `ceptra_definition_lane_overflow_total`.

### Fixed Windows
* [Informative] Pre-maintained rolling sums for horizons (e.g., 5 s, 30 s, 1 m, 1 h).  
* [Informative] `window[n] = Σ panes[i..i+n]`.

### Memory Bound
[Informative] `O(A × M × L)` where `A` = aggregators, `M` = panes, `L` = lanes.

[Informative] *All structures allocated from NUMA-local arenas; no GC.*  
[Informative] *Checkpointing and compaction free memory via `madvise(DONTNEED)` once sealed.*

---
## 9  Rule Layer and Meta-Aggregation

* [Informative] Rules combine raw events, side queries, and aggregates to produce **classification events**.
* [Operational] Rule outputs carry the standard event envelope plus the header flag `DERIVED`. By default they are forwarded only to their configured external sinks (`channel` URI); CEPtra does not loop them back into the ingestion path unless an operator explicitly binds that channel to a `source.*` phase under a different partition key.
* [Informative] Each rule set belongs to a **versioned DAG**; every emitted record carries `rule_version`.
* [Informative] Meta-aggregates (counts, ratios, threat scores) reuse the same pane/window logic.

[Normative] **Invariant R2:** Control Plane validation rejects bundles that consume any `channel` also produced by a rule in the same definition bundle. To build multi-pass pipelines, operators must declare a separate ingestion definition (new partition key or rule version) and fence the epochs explicitly, preventing accidental infinite feedback loops.

### CEL Execution Context (revised)

[Informative] Each metric binding in CEL refers to a single resolved time series per evaluation with these fields:
- [Informative] `value: f64` – scalar value of the resolved series at the pane/window boundary.  
- [Informative] `labels: map<string,string>` – present only when the originating PromQL expression declared a non-empty `by(...)` grouping set.  
- [Normative] `has_value: bool` – `false` when the metric is suppressed (for example, dropped late data on a non-retractable aggregator); CEL must test this before dereferencing `.value` in optional flows.  
- [Informative] `flags: set<string>` – declarative markers such as `NON_RETRACTABLE_DROPPED` that describe why a value is absent or adjusted.

[Informative] Bindings inherit deterministic contextual data:
- [Informative] Record: `payload`, `headers` (`{part_id, raft_log_index, schema_key, event_id, event_key_hash}`), and `partition_key`.  
- [Informative] Temporal: `window_start_ns`, `window_end_ns`, `watermark_ns`, `processing_time_ns`.  
- [Informative] Versioning: `rule_version`, `policy_version`.  
- [Informative] Helpers: `coalesce(a, b)`, `clamp(v, min, max)`, `safe_div(n, d, default)`.

[Normative] The CEL runtime purposely omits any wall-clock, pseudo-random, or external side-effect helpers so that rule output depends solely on the replicated event stream and the deterministic aggregates described earlier. User-authored CEL expressions MUST NOT import custom functions that would violate this determinism.
[Normative] Implementations MUST NOT expose blocking I/O, network lookups, KMS calls, or any other non-deterministic side effects to CEL; every helper available to rules must be a pure function of the replicated inputs enumerated above.

### Series Resolution

1) [Informative] **Materialization**
   - [Informative] If a PromQL expression omits `by(...)`, no labels are materialized; `.labels[...]` access is a compile-time error.  
   - [Informative] If `by(...)` is present, labels are materialized exactly for the declared set. Accessing labels outside that set is a compile-time error.

2) [Informative] **Resolution to one series**
   [Informative] a) *Lane binding:* when the triggering event matched a lane whose grouping set and labels align with the PromQL expression, use that series.  
   [Informative] b) *Payload join:* otherwise, if all labels in the grouping set are derivable deterministically from the event payload or headers, use the series whose labels match those values.  
   [Normative] c) *Ambiguity:* if multiple series match or required labels are missing, compilation fails unless the rule employs an explicit `select(metric, {label_map})` helper.

3) [Informative] **Comparability rule** – CEL may compare `.labels[...]` only between metrics whose PromQL grouping sets are identical. Violations raise compile-time errors.

4) [Informative] **Value availability** – `.value` is defined only after series resolution succeeds. Unresolved metrics reject the rule bundle at compile time.

[Informative] Helper: `select(name: string, labels: map<string,string>) -> MetricBinding`  
[Normative] Notes: `select` may only address a series within the same partition context; `labels` must cover the full grouping set.

[Informative] **Rule bundle contents**
- [Informative] Policy definition (PromQL selectors compiled per §14)
- [Informative] Lane definitions and declared label domains
- [Informative] Rule expressions
- [Informative] Version metadata (`def_version`, `policy_version`, allowed schema keys)
- [Informative] Signatures for integrity

### Derived Event Idempotency
[Informative] Every emitted rule record carries `derived_event_id = hash64(part_id, raft_log_index, rule_version, rule_id, channel)` so downstream sinks can deduplicate deterministically. This identifier references the Clustor §3 `raft_log_index` and remains stable under replay; CEPtra never reuses the tuple for a different outcome. Replays or re-evaluations therefore reproduce the same `derived_event_id`, preventing duplicate notifications even when downstream systems consume CEP-derived channels idempotently.

### Evaluation Cadence and Missing Labels
[Informative] Each incoming event drives one CEL evaluation regardless of how many lanes its predicates set inside `lane_bitmap`; aggregations referenced from CEL already reflect the per-lane series computed in §8, so there is no additional per-lane rule firing. If a binding requires labels that are expected from the payload (for example, `payload.device_tags["region"]`) yet the event omits them, compilation fails unless the rule guards access via `has_value` or `flags`. Runtime lookups therefore never introduce nondeterministic “missing label” semantics: they either produce a value (when the compiler proved it is always present) or the bundle is rejected.

---

## 10  Retention and Compaction

| Tier | Retention | Purpose |
|------|------------|----------|
| **Raw** | `R_raw` (e.g., 24 h) | Fine-grained replay |
| **Compacted** | `R_compact` (e.g., 12 mo) | Long-term aggregates |

[Informative] `R_raw` supplies the raw-tier horizon used in §8 to compute `R_max`.

### Compaction procedure
1. [Informative] Fold raw panes over `[T–24h, T)` using associative `⊕`.
2. [Informative] Write compacted record.
3. [Informative] Once confirmed durable, mark old panes `<T–24h` reusable.

[Normative] **Invariant A\*:** Compaction only folds panes via associative monoids with deterministic identity and merge. Mixing raw and compacted tiers uses monoid `⊕` exclusively and MUST reproduce the same result as real-time rolling application.

[Informative] A metric is eligible for compaction only if its aggregator declares `associative = true` and provides a deterministic `merge` implementation.

[Informative] **Exact switchover:** queries spanning the boundary use both tiers deterministically. The tier boundary at time `T` is defined as raw data covering `[T−24h, T)` (left-closed, right-open) and compacted data covering `(-∞, T−24h]`. Joins across the boundary evaluate panes as left-closed, right-open ranges so the sample at `T−24h` appears exactly once.

---

## 11  Consistency, Idempotency, and Recovery

[Informative] Replay ordering, checkpoint loading, ReadIndex validation, and idempotent re-apply behavior are provided by Clustor §8. CEPtra’s dedup tables, per-key maps, finalized horizon, and derived-event semantics build on those guarantees (see §§3, 8–10, 20) without altering the underlying Raft recovery logic. Any references to `PERMANENT_*` codes or replay horizons in this document therefore assume the Clustor behavior and add only the CEP-specific compute rules layered above it.

---


## 12  Fault Tolerance and Failover

| Component | Failure Behaviour |
|------------|------------------|
| **Single replica** | Raft quorum = 2; writes continue; AP leader unaffected. |
| **Two replicas** | No quorum → writes paused until recovery. |
| **Leader pod** | Follower elected in 150–300 ms; AP warm standby assumes role immediately. |
| **Follower pod** | Rebuild from snapshot. |
| **CP pod** | Other CP-Raft voters continue; management APIs only temporarily degraded. |
| **Power loss** | Quorum replication guarantees committed data safety per mode. |

### Active Processor (AP) assignment
* [Informative] Default priority `[R0 > R1 > R2]`.
* [Informative] **Warm standby**: follower AP applies all committed entries through WAL head silently so promotion is instantaneous.
* [Informative] **Cold standby**: loads latest checkpoint + replay (< 1 s typical).

### Self-healing
* [Informative] StatefulSet restart → pod re-joins cluster, replays WAL, resumes role.
* [Informative] All reconciliation done automatically by embedded Control Plane.
---
## 13  Time Semantics and Late Data Policy

### 13.1 Definitions
- [Informative] **Event time (`ts_ns`)** – UTC nanoseconds within the event payload; drives windowing.  
- [Informative] **Processing time** – Monotonic node clock (for guards and latency).  
- [Informative] **Sequence time** – Raft log index; authoritative for replay and rule version selection.

[Informative] **Invariant T1:** Windowing uses event time; rule versioning uses sequence time.

---

### 13.2 Windowing Model
- [Informative] Fixed pane size `q` (default 250 ms).
- [Informative] Window set `{W}` = multiples of `q` (e.g., 5s, 30s, 1m, 1h).
- [Informative] `pane(ts) = floor(ts / q)`.
- [Informative] Each lane maintains rolling windows = sum of recent panes.
- [Informative] Event-time windows are left-closed and right-open; events exactly at the upper bound belong to the following pane.

---

### 13.3 Watermarks
[Informative] Each AP computes a per-partition watermark `WM_e`:

```
candidate = max_event_ts_seen - L
guarded   = now_ns() - G
WM_e_next = min(candidate, guarded)
WM_e = max(WM_e_prev, WM_e_next)
```

[Informative] where:
- [Informative] `L` lateness allowance (default 2s)
- [Informative] `G` wall-clock guard (default 200ms)

[Informative] `WM_e` is monotonic, deterministic under replay.

[Informative] The guarded term uses the partition's monotonic clock, but every evaluation persists the sampled floor `wm_guard_floor_ns` alongside checkpoints (§20). During replay we restore that floor and re-apply the same min/max sequence, so the resulting watermark matches the original even though `now_ns()` advances.

[Informative] `L` and `G` are hot, per-partition knobs managed by the control plane. Defaults (`L=2s`, `G=200ms`) suit moderate jitter. High-jitter pipelines may increase `L` to 5–10 s while keeping `G` near 200 ms to absorb bursts; strict-latency workloads typically keep `L ≤ 1s` and reduce `G` toward 100 ms to minimize guard delay.

[Informative] Deterministic guard replay:
- [Informative] Every time `now_ns() - G` would increase the guard term, the AP captures the new value in `wm_guard_floor_ns` and tags it with the WAL index that triggered the update (`wm_guard_floor_index`). Subsequent evaluations reuse `guarded = wm_guard_floor_ns` until a later index advances the tag.  
- [Informative] Checkpoints persist both fields. On restore the AP resumes with the recorded pair and suppresses further guard increases until replay has processed beyond `wm_guard_floor_index`, ensuring the guard sequence remains identical even though `now_ns()` during recovery may jump ahead.  
- [Informative] When a guard update occurs after the checkpoint (new WAL data), the same tag is appended to the deterministic state so later replays repeat it at the identical index.
[Normative] Implementations MUST treat `{wm_guard_floor_ns, wm_guard_floor_index}` as the *only* state derived from wall-clock sampling; after restore, every watermark and late-event decision depends exclusively on `{WAL entries, checkpointed guard floors}` and never on the live clock.
[Normative] During replay (checkpoint load or WAL catch-up) the implementation MUST NOT invoke `now_ns()` inside the watermark computation; it MUST reuse the persisted guard floors until the system advances beyond the recorded indices and live ingestion resumes.
[Normative] The guard-floor tuples are CEP-only metadata and MUST NOT be written into Clustor-owned caches or guards (for example, `controlplane.cache_*`, `clock_guard`, lease state, or ReadIndex tables); Clustor primitives remain the sole source of truth for those safety predicates.
[Informative] Watermark guards rely on the node-local monotonic clock solely for forward progress hints and never feed Clustor’s lease, ReadIndex, or `clock_guard` safety predicates. Cross-node safety therefore remains governed entirely by the Clustor substrate even if the local monotonic clock drifts.

[Informative] Overrides:
- [Informative] `L` (lateness allowance) and `G` (guard) may be overridden per metric via `metric_overrides{L?, G?}`.  
- [Informative] When a metric override exists, its watermark is `WM_e(metric) = min(max_event_ts_seen(metric) - L(metric), now_ns() - G(metric))` and remains monotone for that metric.  
- [Normative] When per-metric overrides are active, each metric with a distinct `G(metric)` maintains its own guard floor pair `{wm_guard_floor_ns(metric), wm_guard_floor_index(metric)}`. Guard updates and checkpoint persistence for these floors MUST be tracked independently per metric while still using the same underlying monotonic clock. Replay MUST restore and re-apply each metric’s guard-floor sequence so that every `WM_e(metric)` is reproduced exactly.
- [Informative] When only `L` is overridden, the metric inherits the partition-level `G` and therefore shares the partition’s guard-floor timeline; a distinct guard floor is allocated only when `G(metric)` differs from the global guard.
- [Informative] Export both `ap_watermark_ts_ms` (partition) and `ap_watermark_ts_ms{metric="<name>"}` whenever overrides are active.

[Informative] After each watermark evaluation, compute `finalizable_ts(metric) = WM_e(metric) - C(metric)` using the metric’s configured correction horizon `C`. Panes whose right edge lies at or before `finalizable_ts(metric)` become finalized for that metric, contributing to the monotone `finalized_horizon_index` defined in §3. Finalization decisions are strictly deterministic functions of the watermark sequence and correction horizons so replay yields the same immutable window.

---

### 13.4 Late-Event Policies

| Policy | Description |
|---------|-------------|
| **DROP** | Ignore events ≤ WM_e; emit audit record to “late” partition. |
| **RETRACT** | Apply inverse `⊖` if aggregator supports it, within correction horizon `C` (default 1h). Non-retractable aggregators devolve to DROP with an audit record. |

[Informative] Outside `C`, fallback to DROP.

[Informative] **Invariant T2:** Finalized panes immutable under DROP; RETRACT uses logged inverse ops, deterministic under replay.

[Informative] Aggregators flagged as non-retractable—`quantile_over_time`, `topk`, and `distinct`—always use DROP when late data would affect them. Quantile panes additionally reject late updates touching finalized ranges to preserve the published error bound.

[Informative] Late Data for Non-Retractable Aggregators:
- [Informative] An event “affects” a non-retractable aggregate when its event time maps to a pane at or before the finalized horizon timestamp `finalized_horizon_ts_ns[metric]`.  
- [Informative] Such events are dropped ONLY for the affected non-retractable aggregators; retractable aggregators continue according to policy.  
- [Informative] Emit `ap_late_events_total{metric=<name>, reason="NON_RETRACTABLE_FINALIZED"}` and record `{event_id, raft_log_index}` in the audit log.
- [Informative] Even when DROP is selected, the event still updates `last_seqno_by_key` so idempotency semantics remain intact; the system merely skips mutating the non-retractable accumulator. Any key-hash collision checks (for example, spillover map lookups) still run and continue to increment `ceptra_ap_key_hash_collision_total` as they would for non-late events.

[Normative] Rule authors MUST treat bindings that include non-retractable metrics as conditionally sparse because DROP decisions apply per-aggregator. The CEL compiler exposes `binding.has_value` and `binding.flags` so downstream logic can gate comparisons or fall back when a metric was elided by lateness, preventing inadvertent skew between retractable and non-retractable inputs.
[Normative] As a corollary, CEL expressions that reference any non-retractable metric MUST check `has_value` (or examine `flags`) before dereferencing `.value`, even when the metric rarely drops late data, because the same event may simultaneously update retractable metrics and be dropped for the non-retractable ones.

---

### 13.5 Admission Checks
* [Informative] `max_future_skew` = +5s → reject events too far ahead.
* [Informative] `max_past_skew` = −30min → allowed; handled by late policy.
* [Informative] Violations → side-channel with reason.

[Informative] Admission checks run before lateness evaluation: events that pass `max_future_skew` and `max_past_skew` are still subject to the watermark derived from `L` and `G`.

[Informative] Rejection outcome:
- [Normative] `max_future_skew` violations are rejected permanently with side-channel audit; clients MUST NOT retry.  
- [Informative] `max_past_skew` violations pass to lateness policy unchanged.

[Informative] `max_past_skew` therefore never causes hard rejection; such events flow through the DROP/RETRACT policy described earlier, preserving determinism while still surfacing lateness telemetry.

---

### 13.6 Checkpoints and Replay
[Informative] Checkpoints persist `WM_e`, `max_event_ts_seen`, open/closed panes, `finalized_horizon_index`, and `finalized_horizon_ts_ns`.  
[Informative] Replay recomputes identical `WM_e` given same WAL sequence.

---

### 13.7 Metrics
- [Informative] `ap_watermark_ts_ms`
- [Informative] `ap_event_lateness_ms`
- [Informative] `ap_late_events_total{reason}`
- [Informative] `ap_finalized_panes_total`
- [Informative] `ap_retractions_total`

---

## 14  PromQL Definition Surface

[Informative] Definition bundles express metric extractions using native PromQL selectors. Each expression is compiled into an associative monoid with deterministic merge semantics for the Active Processor.

### 14.1 Expression Form
[Informative] CEPtra accepts the native PromQL subset shown below:

```
aggregation := agg_op grouping? '(' agg_args ')'
agg_op      := sum | count | avg | min | max | topk | distinct
agg_args    := selector | scalar ',' selector
range_op    := rate | increase | count_over_time | avg_over_time
               | sum_over_time | min_over_time | max_over_time
query       := aggregation
             | range_op '(' selector ')'
             | quantile_over_time '(' scalar ',' selector ')'
grouping    := by '(' label (',' label)* ')'
selector    := metric_name label_matchers? range_selector?
range_selector := '[' duration ']'
scalar      := number_literal
```

[Normative] Scalar arithmetic and cross-series expressions (for example, `sum(x) / count(y)` or `sum(a) - sum(b)`) are intentionally excluded from this layer; multi-series math must be expressed in the CEL stage (§9) so that deterministic evaluation order and lane budgets remain enforceable.

[Informative] Selectors use standard PromQL label matchers. Range selectors (`[Δ]`) are mandatory for range functions and optional elsewhere; bracket syntax follows PromQL duration literals (`5m`, `30s`, `1h`). `scalar` follows the PromQL numeric literal grammar. `without(...)` is not supported. Validation enforces that `topk` forms supply exactly one leading scalar argument and that other aggregators omit it; `quantile_over_time` likewise requires a scalar probability followed by the selector.

[Informative] Label matchers that would match unbounded cardinalities (for example, `{hostname=~".*"}`) are permitted only when the resulting series projection can still be proven to honor the declared lane-domain bounds from §2; otherwise the compiler rejects the definition even if the matcher looks trivially true.

[Normative] **Invariant A\*:** All aggregations used for rolling windows or compaction MUST be expressible as associative monoids with identity elements and deterministic merge. Compaction folds panes and mixes tiers solely through monoid `⊕` and MUST match real-time evaluation exactly.

### 14.2 Function Semantics
* [Informative] `sum`, `count`, `avg`, `min`, `max` – associative/retractable when an inverse exists; typically paired with `by(...)` to control lane cardinality.  
* [Informative] `rate`, `increase` – incremental counter primitives over range vectors.  
* [Informative] `count_over_time`, `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time` – sliding-window reductions across panes.  
* [Informative] `quantile_over_time` – KLL sketch per §8 with ≤1% rank error at `p95` (scalar probability as first argument).  
* [Informative] `topk` – deterministic heap of the highest `k` samples (scalar `k` as first argument).  
* [Informative] `distinct` – HyperLogLog++ cardinality estimate.  

[Operational] `distinct` is a CEPtra-specific extension to PromQL. It accepts the same selector grammar (optional `by(...)`) but the compiler verifies that every grouped label has a declared bound (§2). Generated bundles carry the HLL++ state described in §8 and advertise an approximate error of ≤1.6% at 99% confidence. Control Plane validation emits diagnostics for downstream targets that cannot interpret `distinct`, prompting operators to choose an alternate aggregator.

[Informative] `quantile_over_time`, `topk`, and `distinct` are non-retractable; late data for these aggregations is handled via DROP (§13.4).

[Informative] Counter Semantics (normative):
- [Informative] `rate` and `increase` operate on strictly monotonic per-series counters.  
- [Informative] Counter resets (monotonicity violations where `delta < 0`) restart the accumulator from the new sample.  
- [Informative] Negative deltas unrelated to resets (for example, clock skew) are discarded for that pane.  
- [Informative] Windows require at least two samples; otherwise the result is `NaN` and downstream monoids ignore it.  
- [Informative] Samples older than twice the range selector are skipped for the current evaluation.

[Informative] Associativity for Compaction:
- [Informative] `rate`/`increase` maintain `(Δ, duration, reset_count)` per pane; merges are associative and finalization emits the scalar.  
- [Normative] `count_over_time`, `sum_over_time`, `min_over_time`, `max_over_time`, `avg_over_time` maintain sufficient statistics (`sum`, `count`, `min`, `max` as required).  
- [Operational] `quantile_over_time`, `topk`, and `distinct` supply deterministic merges for their sketches/heaps. These functions are `NON-RETRACTABLE` and only compactable when panes combine via their defined merge operators; late data policy §13.4 applies.

### 14.3 Grouping, Lane Admission, and Label Availability
[Normative] `by(...)` clauses create lanes within a partition and materialize the corresponding labels map for CEL. Only labels with bounded cardinality approved by the Control Plane are accepted under `LANE_MAX_PER_PARTITION = 64`. Definition bundles must declare domain bounds for each grouped label (for example, `gateway_id ≤ 32 per partition`, `az ≤ 6`). If the natural cardinality of a label would exceed the cap, the bundle must partition on that label (yielding a bound of 1) or otherwise bucket it so the residual per-partition bound remains ≤64. Label availability rule: if a query omits `by(...)`, no labels map is present for that metric in CEL. Comparability rule: CEL may only compare `labels[...]` between metrics whose PromQL expressions declare identical grouping sets. Violations are rejected at compile time with diagnostics.
[Informative] For ungrouped aggregations (`by(...)` omitted), the control plane still evaluates the selector’s full label matcher set to ensure that every implicitly matched label has a bounded domain. Filters that reference unbounded labels without grouping on them are therefore rejected even if the label does not appear in `by(...)`, preventing lane-budget bypasses.

### 14.4 Examples
```
sum_over_time(power_draw_watts{site="plant-7"}[5m])
avg by (zone)(avg_over_time(device_temperature_c{state="ONLINE"}[15m]))
count_over_time(device_auth_failures_total{result="DENIED"}[1h])
quantile_over_time(0.95, network_latency_seconds{link_state="UP"}[30s])
increase(logon_failures_total{realm="corp"}[10m])
```

### 14.5 Translation Note
[Informative] Bundles authored with the draft `fn over Δ (metric)` form translate directly: `fn over 5m (x{...})` ↔ `fn(x{...}[5m])`. Aggregations without `over` become instant selectors without a range (`sum(metric{...})`).

---

### 14.6 Stage Option Defaults
[Informative] Definition bundles may supply per-stage hints consistent with the PromQL surface:
- [Normative] `aggregate.promql` stages accept an optional `window` duration that declares the default range vector expected by the enclosed queries. Every referenced PromQL expression MUST still specify its own explicit range selector; the compiler verifies the selectors align with the declared `window` (for example, equal to the stage window or a documented multiple) and rejects mismatches. The hint is persisted for observability (`definition_window_default_ms`) but does not alter query semantics.
- [Informative] The stage-level hint is informational only; execution always relies on the explicit range selectors present in each expression.
[Informative] The Control Plane validates stage hints during bundle admission to ensure they neither expand lane cardinality nor bypass the monoid requirements defined earlier in this section.

---

## 15  Hot Reload and Canary Protocol

[Normative] Hot reload follows the Clustor §11.5 distribution → activation pipeline. Clustor’s ControlPlaneRaft owns bundle publication, `DEFINE_ACTIVATE` barriers, and readiness gates; CEPtra layers on the additional requirement that shadow AP state reaches the WAL head and that all PromQL/CEL bindings validate per the lane-domain budget before the barrier may commit. The steps below therefore describe only the CEP-specific predicates that must be satisfied before the Clustor barrier is allowed to advance.

### Concepts
* [Informative] `def_version` – integer version of metrics and rule bundle.
* [Informative] `barrier_index` – Raft log index at which new version activates.

### Phases
1. [Informative] **Distribute:** CP publishes bundle `V+1`.  
2. [Informative] **Prepare:** APs compile, report `PREPARED(V+1)`.  
3. [Informative] **Warm (shadow apply, non-blocking):** APs evaluate `V+1` in shadow mode while continuing to ingest and serve with `V`. Readiness is evaluated per partition using the Clustor fields `shadow_apply_state ∈ {Pending, Replaying, Ready, Expired}`, `warmup_ready_ratio`, and `partition_ready_ratio`. A partition is considered CEP-ready only when Clustor already reports `shadow_apply_state=Ready` *and* CEPtra’s additional checks (apply lag, lane validation, dedup horizon, etc.) succeed; CEPtra exposes this finer classification solely via CEP-namespaced metrics such as `ceptra_shadow_state` and never rewrites the Clustor fields. The pod remains `Ready` if ≥99% (configurable) of its partitions meet both the Clustor and CEP predicates.  
4. [Operational] **Commit:** CP (via leader) appends the Clustor §11.5 `DEFINE_ACTIVATE{def_version=V+1}` entry only after both (a) the Clustor ActivationBarrier predicate is satisfied (per §11.5) and (b) every targeted partition’s Clustor state reports `shadow_apply_state=Ready` while CEPtra’s additive per-partition checks also pass (or the operator-approved readiness quorum, default 100%, is met). The CP refuses to advance while any partition remains behind the WAL head or in shadow compile failure, surfacing `ceptra_definition_warmup_pending{part_id}` and holding the barrier in a pending state within the CEP-only extensions.  
   [Normative] If a partition remains in a CEP-classified `SHADOW_FAILED` or `SHADOW_TIMEOUT` state (exposed only via CEP metrics/logs) for longer than an operator-configurable grace window (default 10 minutes), the Control Plane MUST require an explicit operator action: either (a) abort the activation for the affected bundle and roll back to the previous `def_version`, or (b) force-exclude the failing partitions from the activation scope and mark them `NotReady` until the underlying error is resolved. These CEP-local labels never replace the Clustor `shadow_apply_state`; the CP MUST NOT auto-advance the activation barrier past a partition in persistent shadow failure without such an explicit override.
   [Informative] The CEP-specific override therefore becomes an additional boolean input to the ActivationBarrier gate: `DefineActivate` is appended only when `clustor_barrier_ready && cep_shadow_override_ok`.
   - [Informative] Entries `≤ barrier_index` → process with V.  
   - [Informative] Entries `> barrier_index` → process with V+1.  
   - [Informative] Atomic swap after processing `≤ barrier_index`.  
5. [Informative] **Canary clients:** may pin `rule_version` during validation.  
[Informative] Per-partition readiness exposure: The `/readyz` endpoint includes a `partitions` map with readiness for each partition and diagnostic reasons when any remain `NotReady`.  
6. [Informative] **Decommission:** after grace window, remove old definitions.

[Informative] During checkpoint recovery or WAL catch-up, each partition uses the same readiness gate; append requests targeting partitions still warming emit `TRANSIENT_WARMUP` until their replay reaches the durable head (§24.6).

[Informative] Guarantees atomic cutover and deterministic replay.

[Informative] All of the readiness predicates above are encoded using Clustor’s native `WarmupReadiness` records and ActivationBarrier plumbing; CEPtra’s `/readyz` endpoint simply relays the same partition-level state with additional CEP-specific reasons. No parallel readiness mechanism exists, so the Clustor barrier continues to provide the authoritative view of which partitions may activate a new `def_version`.

---

## 16  Control Plane: Durability, Epochs, and Coordination

[Operational] All ControlPlaneRaft behavior—including epochs, joint-consensus transitions, durability policy orchestration, activation barriers, reconcilers, and certificate issuance—is defined by Clustor §11. CEPtra consumes those APIs to enforce lane-domain validation (§2), bundle admission (§14), readiness gates (§15, §24), and operator overrides; it does not introduce an alternate control plane or epoch state machine. References to CP actions elsewhere in this document therefore cite the Clustor sections directly and describe only the additive CEP readiness predicates.

---


## 17  Backpressure and Flow Control

### 17.1 Objectives
[Informative] Maintain bounded latency, memory, and deterministic throughput under load or replica lag.

[Informative] All throttle statuses and `Why*` envelopes reuse Clustor §10.3 and App.D semantics. The wire-level `credit_hint` string emitted by Clustor’s PID controller (`Recover|Hold|Shed`) remains untouched and authoritative; CEPtra layers *additional* numeric hints and never mutates the substrate contract. Leaders therefore continue to honor every Clustor throttle proof, lease guard, and PID bucket exactly as implemented in the substrate.

[Informative] **Clustor PID integration** – CEPtra’s numeric hints operate strictly inside the Clustor credit buckets:
```
effective_window_events = min(credit_window_hint_events,
                              entry_credits_available_events)
effective_window_bytes  = min(credit_window_hint_bytes,
                              byte_credits_available_bytes)
```
[Normative] Leaders MUST enforce the effective windows above when deciding whether to accept new appends, ensuring they never authorize more in-flight work than the substrate permits. Clients MUST treat CEPtra’s numeric hints as advisory and continue to interpret the Clustor `credit_hint` string to understand the PID controller’s state.
[Normative] CEPtra computes `credit_window_hint_*` strictly as a function of the PID output (`credit_hint`, `entry_credits_available_events`, `byte_credits_available_bytes`) and MUST NOT mutate PID gains, buckets, integrators, or the transmitted `credit_hint` string. Hints may never exceed the currently available credits; the Clustor PID controller remains the single authority for flow-control state.

### 17.2 Flow Boundaries
| Stage | Mechanism | Feedback Target |
|--------|------------|----------------|
| Client → Leader | Credit window & backoff | Client SDK |
| Leader → Followers | Raft replication credits | Raft sender |
| Leader → AP | Commit→apply queue | Raft apply loop |
| AP → Downstream | Emit queue capacity | AP internal |
| AP → Control Plane | Lag/health metrics | CP reconciler |

---

### 17.3 Client-side Credit Window
[Informative] Leaders recompute the advisory `{credit_window_hint_events, credit_window_hint_bytes}` every 200 ms:

```
base_events    = max_inflight_events
base_bytes     = max_inflight_bytes
headroom       = clamp(1 - queue_utilization(commit_apply_queue), 0, 1)
follower_factor = f(replication_lag_ms_p95)
credit_window_hint_events =
    ceil(base_events * (0.5 + 0.5 * headroom) * follower_factor)
credit_window_hint_bytes =
    ceil(base_bytes  * (0.5 + 0.5 * headroom) * follower_factor)
credit_window_hint_events =
    min(credit_window_hint_events,
        max(1, floor(credit_window_hint_bytes / max(1, avg_event_bytes))))
```

[Operational] `max_inflight_events` and `max_inflight_bytes` come from the partition's flow-control profile stored in CP-Raft (`partition_flow{max_events,max_bytes}`). Defaults are `max_inflight_events = 2048` and `max_inflight_bytes = 16 MiB`. Operators may hot-patch these values via `/v1/config`; the CP propagates new limits to leaders within one reconcile cycle, and the next credit-hint tick applies them. The same profile also constrains client SDKs during bootstrap so their initial window never exceeds the server cap.

[Informative] `avg_event_bytes` is a moving average over the last second of commits and is used to derive client-level conversions between events and bytes. The numeric hints are never lower than `{1 event, avg_event_bytes}` and are communicated on every append response and heartbeat alongside (not instead of) the Clustor `credit_hint` string field.

[Informative] The follower factor is piecewise on lag measured in milliseconds:
```
f(lag_ms) = 1.00  if lag_ms ≤ 50
          = 0.75  if 50 < lag_ms ≤ 150
          = 0.50  if 150 < lag_ms ≤ 400
          = 0.25  otherwise
```
[Normative] Implementations may optionally smooth the follower factor (for example, via a short EWMA) before applying it to the numeric hints so that client windows do not oscillate sharply when lag hovers near a boundary; any smoothing must remain deterministic and bounded so replicas compute identical hints.

[Informative] Client SDK behaviour:
- [Informative] Initial window = `min(1000 events, 10 MiB)`.
- [Informative] On overload, timeout, or `TRANSIENT_BACKPRESSURE`: halve the current window and apply jittered backoff (20–1000 ms).
- [Informative] On receiving updated `{credit_window_hint_events, credit_window_hint_bytes}`, raise the cap to `min(current * 1.25, hint)` after a 500 ms dwell; reductions apply immediately. Clients continue to interpret the Clustor `credit_hint` string to understand Recover/Hold/Shed state transitions.
- [Informative] On `PERMANENT_DURABILITY` (a CEP-local label emitted only when Clustor reports a durability incident), halt retries for the affected partition, surface the alert (`wal_durability_fence_active{part_id}`), and resume only after readiness reports the fence cleared or the Control Plane marks the partition healthy.
- [Informative] The effective limit is enforced on both event count and bytes per the min() rule above; whichever saturates first pauses new appends.

[Informative] This contract keeps commit→apply headroom while adapting to follower lag.

[Informative] `TRANSIENT_BACKPRESSURE`, `TRANSIENT_WARMUP`, and `PERMANENT_*` responses reuse the existing Clustor §10.3/App.D throttle envelopes; CEPtra only decides when to emit them, populates `why.reason`/`cep_status_reason` with CEP-specific context such as “warmup” or “dedup capacity”, and does not mint new status-code or envelope names.
[Normative] CEPtra does not implement an independent PID controller or auto-tuner. Leaders simply read the existing Clustor `credit_hint`/PID state (and the `pid_auto_tuner` feature flag when enabled) and compute CEP-specific numeric hints that remain bounded by the PID’s Recover/Hold/Shed state. Any future PID changes must land in Clustor first so the substrate and CEPtra stay in lockstep.

[Normative] References to Clustor `Why*` envelopes (for example, `WhyThrottle`, `WhyNotLeader`, `WhyDiskBlocked`) use the canonical App.D schema; CEPtra MUST NOT introduce new envelope names or status-code enums. When CEPtra needs to express a new motive it does so via the `why.reason` field inside the existing envelope plus, optionally, the `cep_status_reason` string described earlier.
[Normative] Every response still populates the Clustor `ingest_status_code` enum and App.D `Why*` envelope, ensuring the PID controller, clients, and `/readyz` consumers observe the canonical signals. CEPtra’s `cep_status_reason` string and numeric credit hints are supplemental and may not contradict the substrate fields.

[Informative] Outlier protection:
- [Informative] Enforce `max_event_bytes` to prevent a single payload from consuming the entire credit window; violations return `PERMANENT_PAYLOAD`.  
  - [Operational] `max_event_bytes` defaults to 1 MiB per partition. Operators may override it via `/v1/config` (`partition_flow{max_event_bytes}`), bounded to `[64 KiB, 8 MiB]` by the control plane.  
  - [Informative] The reconciler stages updates by broadcasting the new limit in the numeric hint fields for one dwell (500 ms) before enforcement flips, giving clients time to shrink batches deterministically.  
- [Informative] When `avg_event_bytes` volatility exceeds 3× over a 1 s window, clamp numeric hint growth to 10% per dwell.

---

### 17.4 Raft Internal Flow Control
* [Informative] Per-follower inflight byte credit (default 16 MB).  
* [Informative] Leader limits AppendEntries accordingly.  
* [Informative] Lag > 100 ms sustained → mark degraded; throttle new appends.

[Informative] **WRITE_THROTTLE:** when 2+ followers exceed 250 ms lag → reduce append rate by 50%.  
[Informative] **Severe (>1 s):** temporarily reject new appends.

---

### 17.5 Commit→Apply Queue
* [Informative] Lock-free ring buffer of 50 K events (≈ 20–40 MB).  
* [Informative] >75% → warn; >100% → pause ACKs.  
* [Informative] AP apply loop consumes in batches (256 events / 1 ms poll).  
* [Informative] AP sets `apply_lag_ms = (tail.commit_time - head.apply_time)` converted to milliseconds before publishing telemetry.

[Informative] ACK pause:
- [Normative] When Commit→Apply is full, leaders return `TRANSIENT_BACKPRESSURE` immediately rather than holding replies open. Clients MUST back off and shrink their windows.

---

### 17.6 AP Emission and Derived Events
* [Informative] Derived classifications sent through same credit protocol.  
* [Informative] Emit queue capped (20 K events).  
[Informative] Emit queue policy:
- [Informative] Per-channel options = `{drop_oldest | block_with_timeout | dead_letter}`.  
- [Informative] Default: `block_with_timeout = 200ms`, then `drop_oldest` with `ap_emit_drop_total{channel}` incremented.  
- [Informative] Channels may override to `dead_letter`.

[Informative] When downstream throttling persists (for example, NVMe throttling that stalls WAL fsync for seconds), ControlPlane policy may flip a channel to `drop_oldest` immediately to prevent emit backpressure from cascading to ingest. Derived events are always the first to shed load; primary ingress never drops, preserving deterministic replay at the expense of optional downstream notifications.

[Normative] DROP decisions for derived events (for example, `drop_oldest` or timeouts in `block_with_timeout`) are not persisted in any WAL or checkpoint; they are purely an emission-time behavior. On replay, the same derived events may be re-attempted for emission according to the then-current channel policy. Implementations MUST NOT encode emission outcomes (success, drop, timeout) into the replicated log, ensuring that CEP replay semantics remain a pure function of the input event stream and rule definitions.

[Operational] Operators MAY enable a replay-emission throttle that caps the number of derived events re-sent per partition after restart; exceeding the cap requires explicit operator acknowledgment before emission resumes. This guard prevents replay storms from overwhelming downstream systems while preserving deterministic ingest semantics.

---

### 17.7 Telemetry
- [Informative] `queue_depth{type}`
- [Informative] `replication_lag_ms`
- [Informative] `apply_lag_ms`
- [Informative] `ap_emit_queue_utilization`
- [Informative] `client_window_size`
- [Informative] `reject_overload_total`
- [Informative] `leader_credit_window_hint_events`
- [Informative] `leader_credit_window_hint_bytes`

---

### 17.8 Failure and Recovery
| Condition | Effect | Recovery |
|------------|--------|----------|
| Follower lag | Throttle writes | Auto clear |
| AP stall | Queue fills; pause ACKs | Drain after resume |
| Leader restart | Clients reconnect; re-sync credit | Automatic |
| Disk slowdown | Extend fsync cadence | Alert/operator action |

---

### 17.9 Implementation Notes
* [Informative] Lock-free ring buffers for all queues.  
* [Informative] Credit-hint timer runs every 200 ms from the Raft leader thread; hints respect the same dwell timers as other throttles.  
* [Informative] Throttles monotone: dwell ≥500 ms before switching states.  
* [Informative] Clients use jittered backoff 20–1000 ms on 429/OVERLOADED.

[Normative] Transport expectations: Client SDKs MUST use gRPC over HTTP/2 with streaming or pipelined unary messages, mutual TLS, and connection pooling per partition leader. One logical event per message; multiple outstanding messages are allowed up to the credit window. Minimum recommended initial concurrency: 256 in-flight messages or 10 MiB, whichever is reached first.

---

## 18  Schema Evolution and Replay Compatibility

### 18.1 Goals
[Informative] Deterministic replay; safe forward evolution of payloads, aggregators, and rules.

### 18.2 Payload Schemas
* [Informative] Format: Protobuf or FlatBuffers.
* [Informative] `schema_id = <format>:<major>.<minor>:<content_hash>`.
* [Informative] Registered via CP; mapped to `schema_key` (u64).

| Change | Safe | Requires Barrier |
|---------|------|------------------|
| Add optional field | Yes | No |
| Remove/rename field | No | Yes |
| Change type | No | Yes |

---

### 18.3 Policies
* [Informative] Versioned (`policy_version`).
* [Normative] Declare required fields, state layout, inverse availability.
* [Informative] Adding new aggregations within a policy is safe if they are not referenced by active rules.

---

### 18.4 Rule Bundles
* [Informative] Identified by `def_version`.
* [Informative] Contain `policy_version`, allowed schema keys, compiled functions.
* [Informative] Activation via barrier (`DEFINE_ACTIVATE`).

---

### 18.5 Event Headers
```
part_id, raft_log_index, ts_ns, event_id, event_key_hash,
schema_key, def_version_hint, lane_bitmap, flags
```

[Informative] `flags` includes `DERIVED` for rule emissions, `LATE` for late-policy handling, and `RETRACT` for inverse replays. Downstream consumers can key on `DERIVED` to enforce routing policies.

---

### 18.6 Processing Rules
* [Informative] Rule version chosen by Raft index vs activation barrier.
* [Informative] Schema determined by header `schema_key`.
* [Informative] Policy version chosen by rule bundle.

[Informative] Same event never processed by multiple rule versions.

---

### 18.7 Validation
[Informative] Pre-apply validators per schema:
- [Normative] Required field checks.
- [Informative] Range normalization.
- [Informative] Drop or rewrite invalids deterministically.

[Informative] Metrics: `ap_validate_fail_total{schema_id,reason}`.

---

### 18.8 Checkpoint Upgrade
* [Informative] Include `policy_version` and `ap_state_version`.  
* [Informative] On load, upgrade via aggregator `upgrade(from,to)` or replay from snapshot.

[Informative] Upgrade Decision:
- [Informative] If `policy_version(to) - policy_version(from) ≤ 1` and every aggregator exposes `upgrade(from, to)`, perform in-place upgrade during load.  
- [Informative] Otherwise replay from the latest full checkpoint bounded by `checkpoint_retained_fulls`; if the replay chain is incomplete, refuse readiness and emit `ERR_STATE_UPGRADE_FAILED`.

[Normative] Multiple policy upgrades MUST be staged sequentially: the Control Plane issues one `DEFINE_ACTIVATE` barrier per upgrade, waits for readiness, and only then admits the next version. If any replica fails the in-place upgrade (for example, because the aggregator implementation cannot satisfy `upgrade(from,to)`), that replica falls back to replay from the last approved checkpoint before rejoining the quorum. Minority failures therefore impact only the affected replicas and do not block the rest of the fleet from progressing.

---

### 18.9 Registry
* [Informative] CP stores schema descriptors content-addressed.  
* [Informative] AP caches ≤256 schemas; lazy fetch on miss.  
* [Informative] Clients register new schemas before append.

---

### 18.10 Errors
| Error | Meaning |
|--------|---------|
| `ERR_UNKNOWN_SCHEMA` | Key not registered |
| `ERR_SCHEMA_BLOCKED` | Deprecated |
| `ERR_STATE_UPGRADE_FAILED` | Aggregator upgrade failed → replay fallback |
| `PERMANENT_DURABILITY` | CEP-local status reason indicating Clustor reported a durability fence; clients pause retries until the substrate clears the incident |

[Informative] These identifiers populate `cep_status_reason` (logs, metrics, and application payloads) while the wire-level Clustor `error_code` slot carries the documented numeric IDs—for example `SchemaUnknown`, `SchemaBlocked`, or a future `DurabilityFenceActive`. CEPtra never writes a new numeric code to the shared registry; downstream components that only inspect the Clustor `error_code` field therefore remain unaffected.

---

### 18.11 Invariants
- [Informative] Schema selection purely from header.  
- [Informative] Rule version purely from Raft index.  
- [Informative] Aggregator state deterministic; replay identical outputs.

---

## 19  Storage Format Details

[Normative] WAL segment layout, compression, encryption, integrity checks, and crash-repair procedures are defined entirely by Clustor §§5–6 and §9. CEPtra never mutates those formats; references throughout this document (for example, §§3, 17, and 20) simply assume the Clustor implementation and describe how CEP compute state consumes it. Operators should consult the Clustor spec for byte-level details, while CEPtra focuses solely on the compute-layer structures layered on top.

[Normative] WAL truncation decisions remain governed by Clustor §§5–6 and §9. CEPtra merely supplies an additional input—`min_retain_index`, derived from the active checkpoint chain plus dedup/finalized-horizon requirements—and feeds it into the existing `compute_compaction_floor(...)` routine. Implementations MUST raise the Clustor `learner_slack_floor` (per §9.1) to `max(learner_slack_floor, min_retain_index)` before running the standard `floor_effective = max(learner_slack_floor, min(quorum_applied_index, base_index))` logic. No parallel truncation engine exists; CEPtra simply ensures that the permanent Clustor floor already in the substrate respects the dedup/replay horizon.
[Informative] `min_retain_index` equals the smallest Raft index needed to satisfy (a) the most recent verified full+incremental checkpoint chain for the partition and (b) the dedup horizon implied by `dedup_retention_window_s` and `finalized_horizon_index`.
[Normative] Raising `learner_slack_floor` to `min_retain_index` is required even when the partition currently has zero Clustor learners; CEPtra is permitted to “fake” a learner floor solely to protect its dedup and checkpoint requirements, but it MUST do so through the existing Clustor API rather than by bypassing or re-implementing the compaction logic.
[Informative] The resulting `floor_effective` still flows through Clustor’s SnapshotAuthorization, CompactionAuthAck, nonce/key-epoch exchange, and WAL deletion rules; CEPtra never instructs the storage layer to drop bytes independently of those guards.

---


## 20  Checkpointing

[Informative] CEPtra checkpoints are additive compute-state artifacts layered on top of the Clustor §8 snapshot and WAL machinery. They serialize panes, dedup maps, and CEP-specific metadata only; Clustor remains solely responsible for WAL integrity, truncation, and replica snapshot shipping. Loading or streaming these checkpoints therefore requires the substrate snapshot to be present and never mutates the underlying Clustor manifests.
[Informative] Checkpoint scheduling is likewise subordinate to Clustor: CEPtra never triggers, delays, or bypasses the substrate’s own snapshot cadence, and any CEP-specific checkpoint timing only affects the compute-layer artifacts described here.
[Normative] CEPtra checkpoints are never sufficient proof to delete WAL bytes. Storage reclamation continues to require the Clustor snapshot manifest, SnapshotAuthorizationRecord, and CompactionAuthAck handshake; CEPtra tooling MUST NOT instruct truncation based solely on CEP checkpoints, even if the checkpoint contains every lane/pane for the target range.
[Informative] CEP-specific knobs such as `checkpoint_full_interval` and `checkpoint_incremental_interval` operate independently from Clustor’s `snapshot.max_interval` / `snapshot.log_bytes_target` cadence. Operators who want a coupled schedule must configure both stacks explicitly; CEPtra’s tighter range `[15 s, 5 m]` merely bounds dedup formulas and may cause WAL retention to exceed the Clustor minimum when CEPtra requires longer horizons.

### 20.1 Purpose
[Informative] Bounded recovery (<1 s typical) and deterministic restart.

### 20.2 Contents
[Informative] Header, open/closed panes, windows, dedup table shards, key-map shards, aggregator state, `WM_e`, `finalized_horizon_index`, `finalized_horizon_ts_ns{metric}`, config/version info, checksum, signature.

---

### 20.3 Cadence
- [Informative] `checkpoint_full_interval = 30s` or 50 M entries, whichever occurs first.  
- [Informative] `checkpoint_incremental_interval = 10s`; the scheduler caps chains at ≤3 incrementals between fulls.  
- [Informative] `checkpoint_bandwidth_cap = 40 MB/s` per AP, enforced via token bucket on serialization threads.  
- [Informative] `checkpoint_retained_fulls = 2`; older fulls and their incrementals are evicted once the retention window is satisfied.
- [Normative] Definition bundles may request per-stage overrides via `sink.checkpoint` options `{full_interval, incremental_interval}`. Overrides must remain within `[5s, 5m]` for incremental and `[15s, 5m]` for full checkpoints (configurable cluster-wide). The Control Plane enforces these bounds, records approved values in the partition’s config metadata, and rejects bundles that would violate the durability and bandwidth guardrails above.

---

### 20.4 Per-Key Map and Dedup Table
* [Informative] `last_seqno_by_key` sharded (default 1024).  
* [Informative] Each shard: sorted `(key_hash,last_seqno)` pairs.  
* [Informative] Dedup shards mirrored 1:1 with the key shards; each entry encodes `(event_id, raft_log_index, commit_epoch_ticks)` in deterministic order.  
* [Informative] Incremental checkpoints emit only changed keys and dedup entries; sealed shards are copied verbatim.  
* [Informative] Eviction for both structures is deterministic on `finalized_horizon_index` and `dedup_retention_window_s`.

---

### 20.5 Concurrency
* [Informative] Copy-on-write snapshot; AP continues on new buffers.  
* [Informative] Serialization in background thread.  
* [Informative] Publish manifest after successful fsync + rename.

---

### 20.6 Restore Path
1. [Informative] Locate latest full + incrementals.  
2. [Informative] Load header; validate checksum & signature.  
3. [Informative] Apply incrementals.  
4. [Informative] Set AP watermarks.  
5. [Informative] Resume from `applied_index + 1`.

[Informative] **Invariant C3:** restored state + replay = pre-checkpoint state exactly.

---

### 20.7 Failure Handling
[Informative] Partial write → discarded; last valid used.  
[Informative] Checksum fail → fallback to previous chain; corrupt incrementals are skipped and replay resumes from the last verified full + incremental sequence.

[Informative] Incremental chains are therefore applied in order until the first corruption; the loader stops at that point, runs replay from the highest successfully applied incremental, and guarantees that every retained checkpoint pair represents a contiguous WAL index range with no gaps.

[Normative] When an incremental checkpoint in the active chain is detected as corrupt and skipped, the affected partition MUST expose a readiness reason (for example, `"checkpoint_chain_degraded"`) in `/readyz.partitions[part_id].reasons` and increment a counter such as `checkpoint_chain_degraded_total{part_id}`. This condition MUST remain visible until a new, fully verified full checkpoint supersedes the degraded chain. Ingest and replay remain correct, but operators are explicitly informed that the checkpoint chain has fallen back to a shorter prefix.
[Informative] The degraded state affects readiness reporting only; ingestion and replay continue using the surviving checkpoint prefix plus WAL without additional throttling, and Clustor’s snapshot and compaction floors continue to govern truncation exactly as before.

---

### 20.8 Defaults
| Parameter | Default |
|------------|----------|
| `checkpoint_full_interval` | 30 s |
| `checkpoint_incremental_interval` | 10 s (≤3 chained) |
| `checkpoint_bandwidth_cap` | 40 MB/s/AP |
| `checkpoint_retained_fulls` | 2 |
| `checkpoint_max_age` | 24 h |

---
## 21  Threading and Concurrency Model

### 21.1 Objectives
- [Informative] Deterministic low latency (<10 ms p99).  
- [Informative] Partition isolation.  
- [Informative] No global locks or blocking GC (Rust runtime).  
- [Informative] NUMA-local memory access and predictable scheduling.

---

### 21.2 Roles (single binary)
[Informative] Each pod runs:
- [Informative] **CP module** – Control Plane Raft and API.  
- [Informative] **Raft partition group (RPG) module** – Per-partition Raft replica and WAL I/O.  
- [Informative] **AP module** – Apply and aggregation logic.  
- [Informative] **CA module** – Heartbeat, reconciliation, metrics.  

---

### 21.3 Thread Layout per Partition
| Thread | Responsibility | Binding |
|---------|----------------|----------|
| I/O Reactor | Raft RPCs, AppendEntries | pinned to NUMA node with NVMe queue |
| WAL Writer | Batch compress/write, fsync | same node as NVMe |
| Apply Worker | Decode & aggregate (Tokio current-thread runtime) | same node |
| Checkpoint Worker | Serialize frozen snapshot | shares core when idle |
| Telemetry Worker | Metrics/GC | shared pool |

[Informative] Typical load: one dedicated apply thread with shared I/O and checkpoint workers supports 10–50 active partitions per 32-core node while holding apply `p99 ≤ 5 ms`. At peak, a saturated partition may consume roughly three core-equivalents.

---

### 21.4 Queues
[Informative] All queues = lock-free ring buffers.

| Queue | Size | Backpressure |
|--------|------|--------------|
| Ingress | 64 K events | WRITE_THROTTLE |
| Replication | 16 MB/follower | credit protocol |
| Commit→Apply | 50 K | pause ACKs |
| Emit | 20 K | drop oldest |
| Checkpoint hand-off | 1 | swap snapshot |

---

### 21.5 Scheduling
- [Informative] RPG threads: epoll / io_uring.  
- [Informative] AP: Tokio single-thread executor co-located with the Apply Worker; each partition owns one current-thread runtime pinned to its apply core, ensuring the “single mutator thread” invariant while still allowing async I/O within that thread.
- [Informative] CP: async runtime, cooperative tasks.  
- [Informative] CA: lightweight async loop.

---

### 21.6 NUMA
- [Informative] Partition memory pinned to local node.  
- [Informative] NVMe IRQs steered to same CPU.  
- [Informative] NIC RSS hashed by `part_id` → local core.

---

### 21.7 Synchronisation
- [Informative] WAL: single writer, no locks.  
- [Informative] Apply: single thread.  
- [Informative] Checkpoint: read-only snapshot via COW.  
- [Informative] Key-map shards: atomic or per-shard spinlock.  
- [Informative] Metrics: atomic counters.

---

### 21.8 Failure & Watchdog
- [Informative] Queue overflow → backpressure → client throttle.  
- [Informative] Heartbeat counter per thread; >1 s stall triggers restart.  
- [Informative] Restart isolates to partition.

---

### 21.9 Memory Model
- [Informative] Partition-scoped arenas; freed only on teardown.  
- [Informative] Temporary buffers from per-thread slab.  
- [Informative] `madvise(DONTNEED)` after compaction.

---

### 21.10 Invariants
- [Informative] One mutator thread per partition.  
- [Informative] Bounded queues.  
- [Informative] No cross-partition locks.  
- [Informative] Checkpoint swaps atomic pointer exchange.  
- [Informative] Thread failure isolated.

---

## 22  Security Model

[Informative] Clustor §12 supplies the end-to-end security substrate (mTLS, AEAD at rest, KMS integration, seccomp profile, signed artifacts, and immutable audit logs). CEPtra does not change those guarantees and never introduces alternate certificate formats or crypto primitives; every security reference in this document therefore assumes the Clustor controls are in force.

### 22.1 Roles and RBAC
[Informative] CEP-specific access decisions are expressed through Clustor's existing `SecurityMaterial` objects; no alternate credential type is introduced. The additional logical roles used by CEPtra are:
- [Informative] `ingest` – append events to CEP partitions.
- [Informative] `observer` – read `/metrics`, `/readyz`, and replay checkpoints.
- [Operational] `operator` – manage definition bundles, lane-domain policies, and lateness knobs.
- [Informative] `admin` – full control-plane access, including enabling or disabling CEP workloads.

[Informative] These roles map onto the same Clustor token exchange (`SecurityMaterial` + manifests) and inherit the existing audit trails. CEPtra components simply tag their RPCs with the appropriate role requirement; Clustor enforces the decision.

### 22.2 Enrollment Workflow
[Normative] Node enrollment, certificate rotation, and key distribution follow Clustor §12.3. CEPtra only records which CEP workloads (bundles, lane domains, schema IDs) a node is permitted to host; those annotations live alongside the Clustor-issued node certificate and do not affect the issuance protocol. Any CEP-specific overrides must therefore be expressed as metadata fields that Clustor already signs and audits.

### 22.3 Telemetry
[Normative] Security telemetry reuses the Clustor metric registry with CEP-specific labels when needed. Key examples include `security_cert_expiry_ms`, `security_tls_handshake_failures_total`, `security_audit_events_total`, `security_kms_latency_ms`, `security_kms_unavailable_total`, and `wal_kms_block_ms`. These replace the historic `_seconds` forms per §0.3; dashboards should migrate accordingly.
[Normative] Each of the metrics above is registered (or aliased) inside `telemetry_catalog.json` and inherits the wide-int annotations required by Clustor’s App.C generators, ensuring the shared spec-self-tests (for example, `telemetry_catalog_fingerprint_test`) remain authoritative. CEPtra MUST NOT publish a security metric that bypasses the shared registry.

---


## 23  Configuration Parameters and Operational Controls

### 23.1 Principles
[Informative] Declarative, dynamic, versioned, auditable.

### 23.2 Hierarchy
[Informative] Cluster → Node → Partition → Definition → Test override.

### 23.3 Representation
[Informative] YAML / JSON served at `/v1/config`; validated against protobuf schema.

### 23.4 Propagation
1. [Operational] Operator PATCH → CP-Raft commit.  
2. [Informative] CA detects `version++`.  
3. [Informative] Local modules reload or bump epoch.

[Informative] Safe after all partitions ack `APPLIED(V)`.

### 23.5 Rollback
[Informative] `POST /v1/config/rollback?to=<V-1>` restores previous blob.

---

### 23.6 Hot vs Static
| Type | Reload |
|------|---------|
| `lateness_allowance_L`, `pane_size_q` | Hot |
| `durability_mode`, `fsync_cadence` | Reconfigure |
| `wal_path`, `numa_affinity` | Restart |

---

### 23.7 Environment
[Informative] `CEPTRA_HOME`, `CEPTRA_CONFIG`, `CEPTRA_NODE_ID`,  
[Informative] `CEPTRA_LOG_LEVEL`, `CEPTRA_TEST_MODE`.

---

### 23.8 Observability
[Informative] `config_version_current`, `config_reload_duration_ms`,  
[Informative] `config_epoch_bumps_total`, `config_validation_failures_total`.

---

### 23.9 Safety
- [Informative] Mixed versions within a partition forbidden.  
- [Informative] Epoch++ for topology changes.  
- [Informative] Checkpoints record `config_version`.  
- [Informative] Test overrides ignored in prod.

---

### 23.10 Example
```
PATCH /v1/config
{
  "partitions": { "17": { "lateness_allowance_ms": 5000 } }
}
```

---

### 23.11 Defaults
| Parameter | Default |
|------------|----------|
| `pane_size_q` | 250 ms |
| `lateness_allowance_L` | 2 s |
| `durability_mode` | GROUP_FSYNC (alias for Clustor Strict/Group-Fsync profiles) |
| `fsync_cadence_ms` | 5–10 (validated against `group_fsync.max_batch_ms`) |
| `checkpoint_interval_s` | 30 |
| `replica_count` | 3 |
| `audit_retention_months` | 12 |
| `dedup_retention_window_s` | Derived: `max(client_retry_window_s, 2 × checkpoint_full_interval)` |
| `dedup_capacity_per_partition` | Computed: `ceil(max_ingest_rate × dedup_retention_window_s)` |
| `client_retry_window_s` | 1800 |
| `max_event_bytes` | 1 MiB (enforced bounds `[64 KiB, 8 MiB]`) |
| `test_mode` | false |

---

## 24  Telemetry and Observability

### 24.1 Architecture
[Informative] Single binary exports:
- [Informative] `/metrics` Prometheus endpoint  
- [Informative] `/healthz` liveness  
- [Informative] `/readyz` readiness with per-partition detail  
- [Informative] `/trace` OTLP optional

[Informative] All modules share one registry: Clustor-substrate gauges retain their `clustor_*` prefix, while CEP-specific overlays emit under `ceptra_*`.

---

### 24.2 Metric Style
[Deprecation] Prometheus exposition, 250 ms update interval. Substrate gauges keep their Clustor-defined prefixes (for example, `clustor_replication_lag_ms`, `clustor_wal_fsync_latency_ms`) while CEP-specific overlays always use the `ceptra_*` namespace (for example, `ceptra_finalized_horizon_stall`, `ceptra_dedup_capacity_exhausted_total`). All latency/time metrics now use the `_ms` suffix per Clustor §14, and the legacy `_seconds` aliases listed in §0.3 remain read-only for one release to ease migration. Every new `_ms` field is registered as a wide-int JSON string in both `telemetry_catalog.json` and the Clustor App.C `json_wide_int_catalog`, and exporters MUST encode those values as decimal strings. Metrics that mirror substrate counters—such as `clustor_wal_fsync_latency_ms`—are simple aliases of the existing gauges; CEPtra does not redefine their semantics.

[Informative] Key examples:
- [Informative] Clustor-owned gauges forwarded verbatim: `clustor_replication_lag_ms`, `clustor_apply_lag_ms`, `clustor_wal_fsync_latency_ms`, `clustor_queue_depth{type}`, `wal_dirty_epoch_age_ms`, `wal_dirty_bytes_pending`, `wal_durability_fence_active{part_id}`.
- [Informative] CEP-specific overlays (`ceptra_*` namespace): `ceptra_ap_apply_latency_ms`, `ceptra_cp_reconcile_duration_ms`, `ceptra_security_cert_expiry_ms`, `ceptra_definition_lane_overflow_total`, `ceptra_ap_key_hash_collision_total`, `ceptra_dedup_capacity_exhausted_total`, `ceptra_finalized_horizon_stall{part_id}`, `ceptra_definition_warmup_pending{part_id}`, `ceptra_ready_partitions_ratio`, `ceptra_dedup_rate_divergence_total`.
[Normative] CEP-specific metrics carry the `ceptra_*` prefix, are registered under the CEPtra namespace inside `telemetry_catalog.json`, and do not appear in Clustor’s term/metric registry unless the owning team formally promotes them through the §2.2 process.

---

### 24.3 Tracing
[Informative] OpenTelemetry spans:
[Informative] `AppendEvent`, `Raft/replicate`, `WAL/fsync`, `AP/apply`, `AP/checkpoint`.  
[Informative] Sampling = 1 % default; controlled by CP.

---

### 24.4 Logging
[Informative] JSON lines with `ts`, `level`, `module`, `part_id`, `log_index`.  
[Informative] Rotation = 1 GiB × 10 files × 7 days.  
[Informative] `/v1/loglevel` dynamic adjustment.

---

### 24.5 Alerts
| Condition | Metric | Threshold |
|------------|---------|-----------|
| Follower lag | `replication_lag_ms` > 250 (p99) | Warn |
| Apply backlog | `apply_lag_ms` > 200 | Warn |
| Fsync slow | `wal_fsync_latency_ms` > 10 (p99) | Alert |
| Late rate | `ap_late_events_total` > 1 % | Alert |
| Cert expiry | `cert_expiry_ms` < 3 days | Alert |
| Durability fence | `wal_durability_fence_active{part_id}` > 0 for 30 s | Page storage on-call |
| Finalized horizon stall | `ceptra_finalized_horizon_stall{part_id}` > 0 for 5 min | Page streaming on-call |

[Informative] Threshold values for `replication_lag_ms`, `apply_lag_ms`, and `wal_fsync_latency_ms` are in milliseconds.

---

### 24.6 Probes and Per-Partition Readiness
[Informative] `/healthz`: `200` when the process is alive.  
[Normative] `/readyz`: CEPtra emits the exact Clustor §11.5 document schema (including `definition_bundle_id`, `activation_barrier_id`, `shadow_apply_state`, `warmup_ready_ratio`, `partition_ready_ratio`, `readiness_digest`, feature-gate metrics, ingest status codes, and throttle hints). CEPtra injects additional top-level and per-partition fields into the `/readyz` payload but MUST NOT remove or rename any fields mandated by Clustor §11.5. Existing Clustor fields (such as `definition_bundle_id`, `activation_barrier_id`, `shadow_apply_state`, `warmup_ready_ratio`, `partition_ready_ratio`, `readiness_digest`, and feature-gate telemetry) remain the single source of truth and are emitted verbatim.
The JSON fragment below illustrates the CEP-specific *extensions* layered onto the same object:
```
{
  "definition_bundle_id": "bundle-42",
  "activation_barrier_id": "barrier-9",
  "shadow_apply_state": "Ready",
  "warmup_ready_ratio": 1.0,
  "partition_ready_ratio": 1.0,
  "readiness_digest": "sha256:...",
  "... Clustor-required fields elided ...": true,
  "ready": true|false,
  "threshold": {"healthy_partition_ratio": 0.99},
  "reasons": ["cp_quorum", "insufficient_healthy_partitions", "..."],
  "partitions": {
    "<part_id>": {
      "ready": true|false,
      "reasons": ["warmup", "apply_lag", "raft_quorum", "cert_expiry", "checkpoint_age"],
      "metrics": {
        "apply_lag_ms": 120,
        "replication_lag_ms": 45
      }
      }
    }
    }
  }
}
```
[Informative] CEPtra’s keys (`ready`, `threshold`, `reasons`, `partitions`) are additional fields at the top level and under `partitions[...]`. Tooling that only understands the original Clustor schema therefore continues to parse `/readyz` unchanged because the canonical fields remain unmodified.

[Informative] A partition is marked `ready=true` inside the CEP extension when all underlying Clustor predicates already report readiness (per `partition_ready_ratio`, `WarmupReadiness`, and ingest status codes) *and* the CEP-specific thresholds below hold (values expressed in milliseconds):
[Informative] `raft_quorum_ok == true`.  
[Informative] `apply_lag_ms <= 200` (configurable).  
[Informative] `replication_lag_ms <= 250` (configurable).  
[Informative] Warmup complete for the active `def_version` (shadow caught up).  
[Informative] `cert_expiry_ms > 3d`.  
[Informative] `checkpoint_age <= 2 × checkpoint_full_interval`.

[Informative] Overall pod readiness: `true` if ≥99% (configurable) of partitions are ready; otherwise `false`. Expose `ceptra_partition_ready{part_id}` and `ceptra_ready_partitions_ratio`.
[Normative] The ≥99% gate is evaluated on top of the Clustor `partition_ready_ratio`: CEPtra MUST first ensure `partition_ready_ratio` satisfies the ActivationBarrier contract, then apply the CEP overlay to declare the pod `ready=true`. CEPtra never lowers the substrate readiness bar; it can only add stricter criteria or additional diagnostics.
[Normative] ActivationBarrier readiness thresholds remain per-bundle decisions owned entirely by Clustor §11.5. CEPtra’s pod-level `healthy_partition_ratio` is an additional deployment/SLO gate used by operators and automation; it MUST NOT be confused with, or substituted for, the ActivationBarrier verdict that governs `DefineActivate` commits.

[Operational] Whenever a CEP-specific predicate fails (for example, warmup lag or checkpoint age), the leader surfaces the same state through Clustor’s `ingest_status_code`/`ingest_status_reason` fields (`TRANSIENT_WARMUP`, `TRANSIENT_BACKPRESSURE`, etc.), so existing deployment automation and ActivationBarrier proofs remain authoritative for gating ingest.

[Informative] Defaults:

| Parameter | Default |
|-----------|---------|
| `healthy_partition_ratio` | 0.99 |
| `apply_lag_ms_threshold` | 200 |
| `replication_lag_ms_threshold` | 250 |
| `checkpoint_age_threshold` | `2 × checkpoint_full_interval` |
| `cert_expiry_ms_threshold` | 3 days |

[Normative] Clusters with heterogeneous partition weights MAY configure per-partition readiness priorities (for example, `critical=true`). Non-critical partitions can be excluded from the 99% ratio at operator discretion so that a single heavily skewed partition does not hold the entire pod unready; however, these partitions still report their local readiness state and MUST be remediated explicitly.

---

### 24.7 Dashboards
[Informative] Cluster overview, storage I/O, AP latency, lateness, checkpoint times, CP health, security expiry.

---

### 24.8 Testing
[Informative] `TEST/ExportMetricsSnapshot` → JSON dump for deterministic comparison.

---

### 24.9 Invariants
- [Informative] Metrics monotone or resettable.  
- [Informative] Non-blocking emission.  
- [Informative] Timestamps from injected clock (for replay).  
- [Informative] `ceptra_` namespace.

---
## 25  Disaster Recovery and Cluster Recovery

### 25.1 Objectives
- [Informative] **RPO:** 0 (Strict) / ≤ Group-Fsync cadence (Group-Fsync).  
- [Informative] **RTO:** single pod <1 min; multi-pod <10 min; regional DR <1 h.  
- [Informative] Deterministic, auditable recovery from any failure.

---

### 25.2 Failure Classes
| Class | Example | Effect | Recovery |
|--------|----------|---------|----------|
| F1 | Node/pod loss | RPGs elect new leader; AP failover | Auto |
| F2 | PVC loss | Follower rebuilds | Raft snapshot |
| F3 | Two replicas down | Writes paused | Operator restore quorum |
| F4 | CP quorum loss | Mgmt API down; data ok | CP restore |
| F5 | WAL corruption | Partial write | Local repair |
| F6 | Definition/security state loss | Missing bundles | Rehydrate |
| F7 | Regional failure | Site outage | Promote DR cluster |

---

### 25.3 Cold Start
1. [Informative] Binary starts; discovers peers via StatefulSet DNS.  
2. [Informative] Ordinals <M form CP-Raft quorum.  
3. [Informative] CP reconciles partition assignments.  
4. [Informative] RPGs replay WAL, repair open segment, load checkpoints, replay tail.  
5. [Informative] `/readyz` reports `ready=true` when ≥99% (configurable) of partitions meet quorum, apply lag, and auxiliary thresholds.

---

### 25.4 Node/Pod Loss
[Informative] Automatic Raft election <300 ms, AP warm standby takes over.  
[Informative] Traffic throttled during transition.

---

### 25.5 PVC Loss
[Informative] Follower receives snapshot; rebuilds WAL.  
[Informative] No data loss; quorum intact.

---

### 25.6 Two Replicas Down
[Informative] Writes halt immediately and leaders reject appends with `PERMANENT_EPOCH`. Recovery sequence:
1. [Operational] Operator replaces at least one failed replica; CP issues an epoch++ and drives the Raft joint-consensus change.
2. [Informative] Once both joint configurations commit, quorum resumes and clients may retry.

[Informative] Safety-first: never advance a minority. CP throttles to one membership change per node and surfaces progress via `cp_epoch_mismatch_total`.

---

### 25.7 CP Quorum Loss
[Informative] Data path unaffected.  
[Informative] Restore CP pods or snapshot; reconcile drift via partition state.

---

### 25.8 WAL Corruption
[Informative] Auto repair on boot: scan back to valid block, truncate, rebuild index.  
[Informative] Resume at last committed index.

---

### 25.9 Definition / Security State Loss
[Informative] Reload bundles, schema descriptors, keys from blob store and verify signatures.  
[Informative] Reissue certs via CP.

---

### 25.10 Regional DR
[Informative] Primary and warm DR clusters run with asynchronous WAL and checkpoint shipping. Cutover uses an idempotent sequence:
1. [Operational] **Fence primary:** CP issues `FreezePartitionGroup` (epoch fence), causing leaders to reject appends with `PERMANENT_EPOCH`. By default this step is operator-triggered; automation may invoke the same API.  
2. [Informative] **Verify replication:** ensure WAL/checkpoint shipping has delivered entries through the latest committed index (RPO bounded by the durability mode).  
3. [Informative] **Promote DR:** CP runs Raft joint consensus to convert DR observers into voters and increments the epoch.  
4. [Informative] **Cut ingress:** update DNS, load balancers, and client routing to point at the DR cluster; resume ingestion once `/readyz` reports the ≥99% healthy-partition threshold and the target partitions are `ready=true`.  
5. [Informative] **Reconcile primary:** when the primary site returns, reconcile epochs, replay missing WAL, and rejoin partitions as followers under the new epoch fence.

[Operational] RPO ≤ shipping interval; operational RTO typically 30–60 min.

---

### 25.11 Backups
| Type | Frequency | Retention |
|-------|------------|------------|
| CP-Raft snapshot | 60 s | 24 h |
| Checkpoints | 30 s full | 24 h |
| WAL archival | after compaction | `R_compact` |

[Informative] Nightly export to object store; quarterly restore drills.

---

### 25.12 Replacement and Rebalance
[Informative] New pod joins; CP assigns partitions; snapshot catch-up.  
[Informative] Rebalance by capacity weight; one partition at a time.

---

### 25.13 Catastrophic Recovery
[Informative] Recreate cluster → restore CP snapshot → replay checkpoints/WALs → resume traffic.

---

### 25.14 Health Gates
[Informative] Healthy partition: quorum, lag < thresholds, checkpoint age <2×interval, cert valid.  
[Informative] Cluster ready if ≥99% healthy.

---

### 25.15 Test Hooks
- [Informative] `InjectThreadDelay`, `DisableBackpressure`, `CorruptLastBlock`, `TimeShiftCertExpiry`.  
- [Informative] Deterministic tests using manual clock.

---

### 25.16 Invariants
- [Informative] DR-1: No appends without quorum.  
- [Informative] DR-2: Never rewind committed indices.  
- [Informative] DR-3: All restores verified by checksum/signature.  
- [Informative] DR-4: Epochs unique and consistent.  
- [Operational] DR-5: Operator runbooks deterministic.

---

## 26  Deployment and Bootstrap (Kubernetes)

### 26.1 Single Binary Model
[Informative] All modules linked; each pod self-identifies role(s).

---

### 26.2 Auto-Arrangement
1. [Informative] Pods discover peers via headless Service (`ceptra-0`, `ceptra-1`, ...).  
2. [Informative] First M ordinals form CP-Raft voters.  
3. [Informative] Others join as observers.  
4. [Informative] CP assigns partitions using consistent hashing and capacity weights.  
5. [Informative] RPGs and APs start automatically; CA module reports health.

---

### 26.3 Configuration
[Informative] Environment:
```
CLUSTER_NAME=prod
STATEFULSET_SIZE=9
CP_VOTERS=3
SERVICE_DNS=ceptra
```
[Informative] Optional flags: `--role=auto|force-cp|no-cp`, `--pki=/etc/ceptra/pki`.

---

### 26.4 Kubernetes Resources
[Informative] **StatefulSet**
- [Informative] Headless Service for DNS.  
- [Informative] PersistentVolumeClaim for `/var/lib/ceptra`.  
- [Informative] Anti-affinity across nodes.  
- [Informative] PodDisruptionBudget (minAvailable ≥ quorum).  
- [Informative] Liveness `/healthz`, readiness `/readyz` (per-partition readiness, pod ready when ≥99% of partitions pass checks).  
- [Informative] `preStop`: drain appends, fsync, checkpoint.

[Informative] **Rolling Update**
- [Informative] `maxUnavailable=1`.  
- [Informative] Leader transfer before termination.

---

### 26.5 Resources
- [Informative] NVMe PVC (ext4/xfs `noatime`).  
- [Informative] CPU pinning via requests/limits.  
- [Informative] Memory: per partition 1–2 GB.

[Informative] SLO Prerequisites (Kubernetes):
- [Informative] Guaranteed QoS: set CPU requests equal to limits and use topology manager policy `restricted` or `single-numa-node`.  
- [Informative] Enable the Kubernetes CPU Manager static policy and pin critical threads via the runtime.  
- [Informative] Align NVMe IRQs to the same NUMA node; document node labels/affinity so scheduling preserves placement.

---

### 26.6 Security Integration
- [Informative] Certs via Kubernetes Secret or CSI.  
- [Normative] mTLS internal; no sidecars required.  
- [Informative] Optional network policy for port 8080 (metrics).

### 26.7 Node Topology Awareness
[Normative] Clusters that mix hardware profiles (for example, single-socket and dual-socket nodes) MUST annotate nodes with their NUMA topology and configure the Control Plane’s placement policy to prefer homogeneous nodes for latency-critical partitions. This prevents inadvertent scheduling of an RPG on hardware whose memory/NVMe affinity cannot satisfy the NUMA assumptions in §21.

---

### 26.8 Observability
[Informative] Prometheus ServiceMonitor on `/metrics`.  
[Informative] Grafana dashboards bundled.  
[Informative] Logs collected via FluentBit or OTEL sidecar.

---

### 26.9 Bootstrap Sequence
1. [Informative] Pod starts → load config.  
2. [Informative] Discover peers.  
3. [Informative] Join CP-Raft or observer mode.  
4. [Informative] Wait for CP quorum.  
5. [Informative] Fetch assignment → start RPGs/APs.  
6. [Informative] Report ready.

---

### 26.10 Scaling
[Informative] Add pod → auto-discovers, joins cluster → CP rebalances partitions → epoch++ for affected.

---

### 26.11 Testing / Dev Mode
[Informative] `--role=standalone` → single-node all-in-one (no quorum).  
[Informative] `CEPTRA_TEST_MODE=true` enables all test RPCs and manual clock.

### 26.12 Kubernetes Readiness Integration
[Operational] Readiness probe targets `/readyz`. Use a custom `successThreshold` and `failureThreshold` consistent with the per-partition policy. Optionally add a `PodReadinessGate` backed by a `Condition` updated from `ceptra_ready_partitions_ratio`. For large partition counts, set `terminationGracePeriodSeconds` to allow shadow warmup on graceful rollouts and enable leader transfer `preStop` hook. Expose per-partition readiness metrics for HPA or custom controllers.

### 26.13 Warmup Expectations
[Normative] Cold-start readiness time scales roughly with `(number_of_partitions × checkpoint_chain_depth)`; operators running >500 partitions per pod or retaining deep checkpoint chains (>3 incrementals) SHOULD stagger assignments so that no pod is asked to warm every partition simultaneously. Empirically, a single partition with a full + two incrementals catches up in <1 s, while a pod with 400 partitions may require 30–45 s before `/readyz` clears the 99% gate. Control Plane throttles heed these dynamics by assigning at most one new partition per pod at a time, but fleet operators should still provision warm spares and avoid bulk rescheduling when checkpoints approach the configured `checkpoint_max_age`.

---

## 27  Summary of Guarantees

| Property | Guarantee |
|-----------|------------|
| **Ordering** | Total within partition by Raft index |
| **Consistency** | Deterministic results across replicas and replays |
| **Durability** | Quorum fsync or configured mode |
| **Availability** | Survives single replica loss |
| **Idempotency** | Last-event-wins; duplicates harmless |
| **Recoverability** | Sub-second restart with checkpoint replay |
| **Extensibility** | Associative monoid aggregators |
| **Atomic reload** | WAL barrier activates new rule versions |
| **Security** | mTLS, AES-GCM at rest, signed artifacts |
| **Auditability** | All control actions logged and signed |
| **Observability** | Unified metrics, logs, traces |
| **Deployability** | Single Rust binary, auto-arranging StatefulSet |
| **Determinism** | Identical output under replay, clock-invariant |
| **DR compliance** | RPO 0–fsync cadence, RTO ≤ defined thresholds |

---

## 28  Interoperability Notes

- [Informative] CEPtra adheres to Clustor’s wide-int JSON encoding, ChunkedList framing, and error-code registry (App.C–E). No alternative list framing, wire enums, or status codes are introduced; CEP-specific semantics live entirely inside application payloads.
- [Informative] All RPC envelopes, manifests, and credit/throttle signals use the frozen Clustor wire catalog. CEP-derived payloads ride on those transports without mutating the outer schema, ensuring drop-in compatibility with other Clustor workloads.
- [Informative] Spec-lint manifests and ControlPlaneRaft metadata remain those defined in Clustor §0 and §11. CEPtra extensions merely add typed payloads or labels and therefore interoperate cleanly with any tooling that already understands Clustor catalogs.

---

# End of Specification

---

[Informative] Example bundles below assume the workload partitions on any naturally unbounded labels (for example `facility_id` or `device_id`) so that the residual per-partition domain stays within `LANE_MAX_PER_PARTITION = 64`. Declared `lane_domains` reflect that sharding, and CEL rules explicitly guard `.value` access for non-retractable aggregators (for example, quantiles) using the binding’s `has_value` field per §13.4.

[Informative] bundle:
  [Informative] name: gaming_liveops_cheat_detection
  [Informative] lane_domains:
    [Informative] player_id: { max_per_partition: 32 }
  [Informative] def_version: 1
[Informative] workflow:
  [Informative] name: gaming_liveops_cheat_detection
  [Informative] phases:
    - [Informative] name: ingest_game_events
      [Informative] type: source.kafka
      [Informative] options:
        [Informative] connector: game_telemetry_kafka
        [Informative] decode: schema_registry
        [Informative] enrich_headers: true

    - [Informative] name: aggregate_fast_signals
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 15s
        [Informative] queries:
          [Informative] anti_cheat_flags:
            [Informative] expression: sum by (player_id)(
              [Informative] increase(anti_cheat_flags_total{queue="ranked"}[15s])
            [Informative] )
          [Informative] headshot_ratio_short:
            [Informative] expression: avg by (player_id)(
              [Informative] avg_over_time(player_headshot_ratio{queue="ranked"}[15s])
            [Informative] )
          [Informative] movement_anomaly_score:
            [Informative] expression: avg by (player_id)(
              [Informative] avg_over_time(player_movement_deviation_score{mode="live"}[15s])
            [Informative] )
          [Informative] matchmaking_latency_p95:
            [Informative] expression: quantile_over_time(
              [Informative] 0.95,
              [Informative] matchmaking_latency_seconds{queue="ranked"}[15s]
            [Informative] )

    - [Informative] name: aggregate_behavior_baseline
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 30m
        [Informative] queries:
          [Informative] anti_cheat_flag_baseline:
            [Informative] expression: avg by (player_id)(
              [Informative] avg_over_time(anti_cheat_flags_total{queue="ranked"}[30m])
            [Informative] )
          [Informative] headshot_ratio_baseline:
            [Informative] expression: avg by (player_id)(
              [Informative] avg_over_time(player_headshot_ratio{queue="ranked"}[30m])
            [Informative] )
          [Informative] movement_anomaly_baseline:
            [Informative] expression: avg by (player_id)(
              [Informative] avg_over_time(player_movement_deviation_score{mode="live"}[30m])
            [Informative] )

    - [Informative] name: evaluate_player_risk
      [Informative] type: classify.cel
      [Informative] options:
        [Informative] bindings:
          [Informative] fast: phase.aggregate_fast_signals.metrics
          [Informative] baseline: phase.aggregate_behavior_baseline.metrics
        [Informative] rules:
          - [Informative] name: potential_aimbot_user
            [Informative] when: |
              [Informative] fast["headshot_ratio_short"].labels["player_id"] == baseline["headshot_ratio_baseline"].labels["player_id"] &&
              [Informative] fast["headshot_ratio_short"].value >= baseline["headshot_ratio_baseline"].value + 0.35 &&
              [Informative] fast["anti_cheat_flags"].labels["player_id"] == fast["headshot_ratio_short"].labels["player_id"] &&
              [Informative] fast["anti_cheat_flags"].value >= 2
            [Informative] emit:
              [Informative] channel: kafkas://liveops.cheat_alerts
              [Informative] schema_key: suspected_aimbot_v1
              [Informative] payload:
                [Informative] player_id: fast["headshot_ratio_short"].labels["player_id"]
                [Informative] headshot_ratio: fast["headshot_ratio_short"].value
                [Informative] baseline_headshot_ratio: baseline["headshot_ratio_baseline"].value
                [Informative] anti_cheat_flags: fast["anti_cheat_flags"].value
                [Informative] session_id: payload.session_id

          - [Informative] name: speed_hack_candidate
            [Informative] when: |
              [Informative] fast["movement_anomaly_score"].labels["player_id"] == baseline["movement_anomaly_baseline"].labels["player_id"] &&
              [Informative] fast["anti_cheat_flags"].labels["player_id"] == fast["movement_anomaly_score"].labels["player_id"] &&
              [Informative] fast["movement_anomaly_score"].value > 1.5 * max(1, baseline["movement_anomaly_baseline"].value) &&
              [Informative] fast["anti_cheat_flags"].value >= 1 &&
              [Informative] fast["matchmaking_latency_p95"].has_value &&
              [Informative] fast["matchmaking_latency_p95"].value < 0.04
            [Informative] emit:
              [Informative] channel: grpcs://anti-cheat/QueueInvestigation
              [Informative] metadata:
                [Informative] severity: "CRITICAL"
              [Informative] payload:
                [Informative] player_id: fast["movement_anomaly_score"].labels["player_id"]
                [Informative] anomaly_score: fast["movement_anomaly_score"].value
                [Informative] baseline_score: baseline["movement_anomaly_baseline"].value
                [Informative] anti_cheat_flags: fast["anti_cheat_flags"].value
                [Informative] latency_p95_seconds: fast["matchmaking_latency_p95"].value

    - [Informative] name: retain_state
      [Informative] type: sink.checkpoint
      [Informative] options:
        [Informative] full_interval: 60s
        [Informative] incremental_interval: 20s
        [Informative] include:
          - [Informative] aggregate_fast_signals
          - [Informative] aggregate_behavior_baseline

---

[Informative] bundle:
  [Informative] name: soc_multi_signal
  [Informative] lane_domains:
    [Informative] entity_id: { max_per_partition: 48 }
    [Informative] host_id:   { max_per_partition: 32 }
  [Informative] def_version: 1
[Informative] workflow:
  [Informative] name: soc_multi_signal
  [Informative] phases:
    - [Informative] name: ingest_security_events
      [Informative] type: source.kafka
      [Informative] options:
        [Informative] connector: security_events_kafka
        [Informative] decode: schema_registry
        [Informative] lane_bitmap_from: payload.alert_lanes

    - [Informative] name: aggregate_short_window
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 60s
        [Informative] queries:
          [Informative] correlated_auth_failures:
            [Informative] expression: sum by (entity_id)(
              [Informative] increase(auth_failure_events_total{outcome="DENIED"}[60s])
            [Informative] )
          [Informative] malware_alert_burst:
            [Informative] expression: sum by (host_id)(
              [Informative] increase(malware_detected_total{severity=~"HIGH|CRITICAL"}[60s])
            [Informative] )
          [Informative] lateral_movement_signals:
            [Informative] expression: count by (host_id)(
              [Informative] count_over_time(lateral_movement_flags{stage="suspected"}[60s])
            [Informative] )

    - [Informative] name: aggregate_baseline_window
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 1h
        [Informative] queries:
          [Informative] auth_failure_baseline:
            [Informative] expression: avg by (entity_id)(
              [Informative] avg_over_time(auth_failure_events_total{outcome="DENIED"}[1h])
            [Informative] )
          [Informative] malware_alert_baseline:
            [Informative] expression: avg by (host_id)(
              [Informative] avg_over_time(malware_detected_total{severity=~"HIGH|CRITICAL"}[1h])
            [Informative] )

    - [Informative] name: evaluate_incidents
      [Informative] type: classify.cel
      [Informative] options:
        [Informative] bindings:
          [Informative] short: phase.aggregate_short_window.metrics
          [Informative] baseline: phase.aggregate_baseline_window.metrics
        [Informative] rules:
          - [Informative] name: credential_stuffing_cluster
            [Informative] when: |
              [Informative] short["correlated_auth_failures"].labels["entity_id"] == baseline["auth_failure_baseline"].labels["entity_id"] &&
              [Informative] short["correlated_auth_failures"].value >= 100 &&
              [Informative] short["correlated_auth_failures"].value > 3 * max(1, baseline["auth_failure_baseline"].value)
            [Informative] emit:
              [Informative] channel: grpcs://soc.escalations/CreateIncident
              [Informative] metadata:
                [Informative] severity: "HIGH"
                [Informative] playbook: "PB-CREDENTIAL-STUFFING"
              [Informative] payload:
                [Informative] entity_id: short["correlated_auth_failures"].labels["entity_id"]
                [Informative] burst_failures: short["correlated_auth_failures"].value
                [Informative] baseline_failures: baseline["auth_failure_baseline"].value
                [Informative] window_started_ns: window_start_ns
                [Informative] window_ended_ns: window_end_ns

          - [Informative] name: host_compromise_candidate
            [Informative] when: |
              [Informative] short["malware_alert_burst"].labels["host_id"] == baseline["malware_alert_baseline"].labels["host_id"] &&
              [Informative] short["lateral_movement_signals"].labels["host_id"] == short["malware_alert_burst"].labels["host_id"] &&
              [Informative] short["malware_alert_burst"].value >= 10 &&
              [Informative] short["lateral_movement_signals"].value >= 3
            [Informative] emit:
              [Informative] channel: kafkas://soc.alerts
              [Informative] schema_key: host_compromise_v1
              [Informative] payload:
                [Informative] host_id: short["malware_alert_burst"].labels["host_id"]
                [Informative] high_severity_alerts: short["malware_alert_burst"].value
                [Informative] baseline_alert_rate: baseline["malware_alert_baseline"].value
                [Informative] lateral_indicators: short["lateral_movement_signals"].value
                [Informative] correlation_id: payload.envelope_id

    - [Informative] name: publish_state
      [Informative] type: sink.checkpoint
      [Informative] options:
        [Informative] full_interval: 2m
        [Informative] incremental_interval: 40s
        [Informative] include:
          - [Informative] aggregate_short_window
          - [Informative] aggregate_baseline_window

---

[Informative] bundle:
  [Informative] name: ecommerce_supply_chain
  [Informative] lane_domains:
    [Informative] facility_id: { max_per_partition: 48 }
    [Informative] carrier_id:  { max_per_partition: 24 }
  [Informative] def_version: 1
[Informative] workflow:
  [Informative] name: ecommerce_supply_chain
  [Informative] phases:
    - [Informative] name: ingest_logistics_events
      [Informative] type: source.kafka
      [Informative] options:
        [Informative] connector: logistics_events_kafka
        [Informative] decode: schema_registry
        [Informative] enrich_headers: true

    - [Informative] name: aggregate_short_term_flow
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 10m
        [Informative] queries:
          [Informative] outbound_orders_rate:
            [Informative] expression: rate(
              [Informative] warehouse_orders_processed_total[10m]
            [Informative] )
          [Informative] outbound_orders_rate_by_facility:
            [Informative] expression: sum by (facility_id)(
              [Informative] rate(warehouse_orders_processed_total[10m])
            [Informative] )
          [Informative] delayed_departures:
            [Informative] expression: sum by (facility_id)(
              [Informative] increase(shipment_delay_events_total{reason="MISSED_DEPARTURE"}[10m])
            [Informative] )
          [Informative] carrier_exception_spike:
            [Informative] expression: sum by (carrier_id)(
              [Informative] increase(carrier_exception_events_total{severity=~"MAJOR|CRITICAL"}[10m])
            [Informative] )

    - [Operational] name: aggregate_operational_baseline
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 6h
        [Informative] queries:
          [Informative] outbound_orders_baseline:
            [Informative] expression: avg by (facility_id)(
              [Informative] avg_over_time(warehouse_orders_processed_total[6h])
            [Informative] )
          [Informative] carrier_exception_baseline:
            [Informative] expression: avg by (carrier_id)(
              [Informative] avg_over_time(carrier_exception_events_total{severity=~"MAJOR|CRITICAL"}[6h])
            [Informative] )

    - [Informative] name: evaluate_supply_risks
      [Informative] type: classify.cel
      [Informative] options:
        [Informative] bindings:
          [Informative] short: phase.aggregate_short_term_flow.metrics
          [Operational] baseline: phase.aggregate_operational_baseline.metrics
        [Informative] rules:
          - [Informative] name: fulfillment_sla_slip
            [Informative] when: |
              [Informative] short["outbound_orders_rate_by_facility"].labels["facility_id"] == baseline["outbound_orders_baseline"].labels["facility_id"] &&
              [Informative] short["outbound_orders_rate_by_facility"].value < 0.8 * max(1, baseline["outbound_orders_baseline"].value) &&
              [Informative] payload.facility_tags["priority"] == "HIGH"
            [Informative] emit:
              [Informative] channel: kafkas://ops.sla_alerts
              [Informative] schema_key: fulfillment_sla_slip_v1
              [Informative] payload:
                [Informative] facility_id: short["outbound_orders_rate_by_facility"].labels["facility_id"]
                [Informative] current_rate: short["outbound_orders_rate_by_facility"].value
                [Informative] baseline_rate: baseline["outbound_orders_baseline"].value
                [Informative] window_started_ns: window_start_ns

          - [Informative] name: carrier_disruption
            [Informative] when: |
              [Informative] short["carrier_exception_spike"].labels["carrier_id"] == baseline["carrier_exception_baseline"].labels["carrier_id"] &&
              [Informative] short["carrier_exception_spike"].value >= 5 &&
              [Informative] short["carrier_exception_spike"].value > 2 * max(1, baseline["carrier_exception_baseline"].value)
            [Informative] emit:
              [Informative] channel: grpcs://supply-chain/TriggerRemediation
              [Informative] metadata:
                [Informative] severity: "MAJOR"
              [Informative] payload:
                [Informative] carrier_id: short["carrier_exception_spike"].labels["carrier_id"]
                [Informative] exception_count: short["carrier_exception_spike"].value
                [Informative] baseline_count: baseline["carrier_exception_baseline"].value
                [Informative] impacted_orders: payload.metrics["orders_in_transit"]

    - [Informative] name: archive_state
      [Informative] type: sink.checkpoint
      [Informative] options:
        [Informative] full_interval: 15m
        [Informative] incremental_interval: 5m
        [Informative] include:
          - [Informative] aggregate_short_term_flow
          - [Operational] aggregate_operational_baseline

---

[Informative] bundle:
  [Informative] name: telco_qos_guardrails
  [Informative] lane_domains:
    [Informative] cell_id: { max_per_partition: 48 }
  [Informative] def_version: 1
[Informative] workflow:
  [Informative] name: telco_qos_guardrails
  [Informative] phases:
    - [Informative] name: ingest_radio_events
      [Informative] type: source.kafka
      [Informative] options:
        [Informative] connector: ran_metrics_kafka
        [Informative] decode: schema_registry
        [Informative] enrich_headers: true

    - [Informative] name: aggregate_cell_health_fast
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 30s
        [Informative] queries:
          [Informative] cell_latency_p95:
            [Informative] expression: quantile_over_time(
              [Informative] 0.95,
              [Informative] cell_round_trip_seconds{link_state="UP"}[30s]
            [Informative] )
          [Informative] cell_jitter_avg:
            [Informative] expression: avg by (cell_id)(
              [Informative] avg_over_time(cell_jitter_seconds[30s])
            [Informative] )
          [Informative] packet_loss_short:
            [Informative] expression: avg by (cell_id)(
              [Informative] avg_over_time(packet_loss_ratio{direction="downlink"}[30s])
            [Informative] )

    - [Informative] name: aggregate_cell_health_baseline
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 2h
        [Informative] queries:
          [Informative] cell_latency_baseline:
            [Informative] expression: avg by (cell_id)(
              [Informative] avg_over_time(cell_round_trip_seconds{link_state="UP"}[2h])
            [Informative] )
          [Informative] packet_loss_baseline:
            [Informative] expression: avg by (cell_id)(
              [Informative] avg_over_time(packet_loss_ratio{direction="downlink"}[2h])
            [Informative] )

    - [Informative] name: evaluate_qos_degradations
      [Informative] type: classify.cel
      [Informative] options:
        [Informative] bindings:
          [Informative] fast: phase.aggregate_cell_health_fast.metrics
          [Informative] baseline: phase.aggregate_cell_health_baseline.metrics
        [Informative] rules:
          - [Informative] name: latency_regression
            [Informative] when: |
              [Informative] fast["cell_latency_p95"].has_value &&
              [Informative] fast["cell_latency_p95"].labels["cell_id"] == baseline["cell_latency_baseline"].labels["cell_id"] &&
              [Informative] fast["cell_latency_p95"].value > baseline["cell_latency_baseline"].value + 0.02 &&
              [Informative] payload.cell_metadata["tier"] == "urban"
            [Informative] emit:
              [Informative] channel: kafkas://noc.latency_incidents
              [Informative] schema_key: cell_latency_regression_v1
              [Informative] payload:
                [Informative] cell_id: fast["cell_latency_p95"].labels["cell_id"]
                [Informative] latency_p95_seconds: fast["cell_latency_p95"].value
                [Informative] baseline_seconds: baseline["cell_latency_baseline"].value
                [Informative] sector: payload.cell_metadata["sector"]
                [Informative] observed_at_ns: watermark_ns

          - [Informative] name: packet_loss_spike
            [Informative] when: |
              [Informative] fast["packet_loss_short"].labels["cell_id"] == baseline["packet_loss_baseline"].labels["cell_id"] &&
              [Informative] fast["packet_loss_short"].value >= 2 * max(0.01, baseline["packet_loss_baseline"].value) &&
              [Informative] fast["cell_jitter_avg"].labels["cell_id"] == fast["packet_loss_short"].labels["cell_id"] &&
              [Informative] fast["cell_jitter_avg"].value > 0.005
            [Informative] emit:
              [Informative] channel: grpcs://noc/TriggerMitigation
              [Informative] metadata:
                [Informative] severity: "CRITICAL"
              [Informative] payload:
                [Informative] cell_id: fast["packet_loss_short"].labels["cell_id"]
                [Informative] packet_loss_ratio: fast["packet_loss_short"].value
                [Informative] jitter_seconds: fast["cell_jitter_avg"].value
                [Informative] baseline_loss: baseline["packet_loss_baseline"].value

    - [Informative] name: persist_state
      [Informative] type: sink.checkpoint
      [Informative] options:
        [Informative] full_interval: 5m
        [Informative] incremental_interval: 1m
        [Informative] include:
          - [Informative] aggregate_cell_health_fast
          - [Informative] aggregate_cell_health_baseline

---

[Informative] bundle:
  [Informative] name: iot_fleet_monitoring
  [Informative] lane_domains:
    [Informative] device_id: { max_per_partition: 64 }
    [Informative] gateway_id:{ max_per_partition: 32 }
    [Informative] zone:      { max_per_partition: 16 }
  [Informative] def_version: 1
[Informative] workflow:
  [Informative] name: iot_fleet_monitoring
  [Informative] phases:
    - [Informative] name: ingest_iot_events
      [Informative] type: source.kafka
      [Informative] options:
        [Informative] connector: sensors_kafka
        [Informative] decode: schema_registry
        [Informative] enrich_headers: true

    - [Informative] name: aggregate_fast_metrics
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 30s
        [Informative] queries:
          [Informative] device_temp_peak:
            [Informative] expression: max by (device_id)(
              [Informative] max_over_time(device_temperature_c{state="ONLINE"}[30s])
            [Informative] )
          [Informative] gateway_latency_p95:
            [Informative] expression: quantile_over_time(
              [Informative] 0.95,
              [Informative] network_latency_seconds{link_state="UP"}[30s]
            [Informative] )
          [Informative] auth_fail_short:
            [Informative] expression: sum by (gateway_id)(
              [Informative] increase(device_auth_failures_total{result="DENIED"}[30s])
            [Informative] )

    - [Informative] name: aggregate_baseline_metrics
      [Informative] type: aggregate.promql
      [Informative] options:
        [Informative] window: 15m
        [Informative] queries:
          [Informative] device_temp_baseline:
            [Informative] expression: avg by (zone)(
              [Informative] avg_over_time(device_temperature_c{state="ONLINE"}[15m])
            [Informative] )
          [Informative] auth_fail_baseline:
            [Informative] expression: avg by (gateway_id)(
              [Informative] avg_over_time(device_auth_failures_total{result="DENIED"}[15m])
            [Informative] )
          [Informative] gateway_latency_baseline:
            [Informative] expression: avg_over_time(
              [Informative] network_latency_seconds{link_state="UP"}[15m]
            [Informative] )

    - [Informative] name: evaluate_policies
      [Informative] type: classify.cel
      [Informative] options:
        [Informative] bindings:
          [Informative] fast: phase.aggregate_fast_metrics.metrics
          [Informative] baseline: phase.aggregate_baseline_metrics.metrics
        [Informative] rules:
          - [Informative] name: overheating_device
            [Informative] when: |
              [Informative] baseline["device_temp_baseline"].labels["zone"] == payload.location.zone &&
              [Informative] fast["device_temp_peak"].value > baseline["device_temp_baseline"].value + 5 &&
              [Informative] payload.device_tags["safetyCritical"] == true
            [Informative] emit:
              [Informative] channel: kafkas://iot.alerts
              [Informative] schema_key: device_overheat_v1
              [Informative] payload:
                [Informative] device_id: payload.device_id
                [Informative] zone: payload.location.zone
                [Informative] temp_peak_c: fast["device_temp_peak"].value
                [Informative] temp_baseline_c: baseline["device_temp_baseline"].value
                [Informative] observed_at_ns: watermark_ns

          - [Informative] name: authentication_threat
            [Informative] when: |
              [Informative] fast["auth_fail_short"].labels["gateway_id"] == baseline["auth_fail_baseline"].labels["gateway_id"] &&
              [Informative] fast["auth_fail_short"].value >= 20 &&
              [Informative] fast["auth_fail_short"].value > 1.5 * baseline["auth_fail_baseline"].value
            [Informative] emit:
              [Informative] channel: grpcs://security-ops/NotifyThreat
              [Informative] metadata:
                [Informative] severity: "CRITICAL"
              [Informative] payload:
                [Informative] gateway_id: fast["auth_fail_short"].labels["gateway_id"]
                [Informative] short_window_failures: fast["auth_fail_short"].value
                [Informative] baseline_failures: baseline["auth_fail_baseline"].value
                [Informative] threat_type: "UNUSUAL_AUTH_FAILURE_RATE"

          - [Informative] name: gateway_latency_regression
            [Informative] when: |
              [Informative] fast["gateway_latency_p95"].has_value &&
              [Informative] baseline["gateway_latency_baseline"].has_value &&
              [Informative] fast["gateway_latency_p95"].value > baseline["gateway_latency_baseline"].value + 0.015
            [Informative] emit:
              [Informative] channel: kafkas://iot.latency_alerts
              [Informative] schema_key: gateway_latency_regression_v1
              [Informative] payload:
                [Informative] partition_id: headers.part_id
                [Informative] gateway_latency_p95_seconds: fast["gateway_latency_p95"].value
                [Informative] baseline_latency_seconds: baseline["gateway_latency_baseline"].value
                [Informative] observed_at_ns: watermark_ns

    - [Informative] name: publish_state
      [Informative] type: sink.checkpoint
      [Informative] options:
        [Informative] full_interval: 30s
        [Informative] incremental_interval: 10s
        [Informative] include:
          - [Informative] aggregate_fast_metrics
          - [Informative] aggregate_baseline_metrics

# CEPtra – Unified System Specification
Version: Draft 1.0  
Language: Rust (no GC runtime)  
Deployment: Single-binary, self-arranging Kubernetes StatefulSet

---

## Table of Contents
1. Overview  
   1.1 Primary Use Cases  
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

---

## 1  Overview

CEPtra is a high-throughput, low-latency in-memory complex event processor designed to track, link, and aggregate heterogeneous event streams (for example, IoT telemetry, security signals, or transactional feeds) in real time.

* **Target throughput:** ≥ 10 000 events / s per active partition  
* **Target p99 latency:** ≤ 5 ms end-to-end for accepted events  
* **Consistency:** deterministic results for any given ordered sequence of events  
* **Durability:** acknowledgment only after quorum replication  
* **Restart time:** sub-second; no pause on single-replica loss  
* **Hot-reload:** atomic cutover of rule and metric definitions with concurrent versioning  
* **Deployment:** single Rust binary containing all roles (Control Plane, Replica, Active Processor, Control Agent) running as a Kubernetes StatefulSet.  
  Each pod auto-selects roles via StatefulSet ordinal and peer discovery.

### 1.1 Primary Use Cases

- **IoT fleet monitoring:** ingest per-device telemetry, apply mixed fast/baseline aggregation windows, and emit policy actions for overheating or authentication anomalies while preserving deterministic replay for audits.
- **Security operations correlation:** fuse IDS, EDR, and identity streams into per-entity partitions, evaluate multi-signal rules in CEL, and route high-confidence incidents to downstream SOC tooling with quorum-backed durability.
- **Telco network QoS assurance:** maintain rolling quantiles and baselines for cell and backhaul performance, trigger mitigations on jitter, packet loss, or latency regressions, and persist metrics for disaster recovery without sacrificing real-time latency.
- **E-commerce supply chain visibility:** track warehouse scans, carrier updates, and inventory telemetry, compare short-term deviations against historical baselines, and emit remediation workflows when SLA breaches or stock shocks emerge.
- **Connected gaming telemetry with cheat detection:** consolidate session metrics, matchmaking stats, and anti-cheat signals, compute per-cohort latency and win-rate deltas, and escalate suspicious patterns for live operations or automated countermeasures.

---

## 2  Partitioning and Routing

* Deterministic partitioning by 64-bit hash on a logical key (e.g., device identifier, customer account, geography).  
* Three replicas (`R0 R1 R2`) per partition.  
* **Raft-based replication:** each partition is a 3-voter Raft group (PRG).  
  - Client appends → Raft leader → replication to followers → quorum commit.  
  - Leader index (`log_index`) serves as `seqno`.  
  - Follower catch-up and snapshot handled by Raft.  
* **Durability acknowledgment:** leader ACKs client only after quorum commit per durability mode.  
* **Event duplication** across partitions is allowed and expected.  
* **Leader affinity:** AP colocated with leader by default; warm standby AP on followers.

### Lane Budgets and Grouping
* **Lane limits:** `LANE_MAX_PER_PARTITION = 64`; warning when active lanes `>= LANE_WARN_THRESHOLD = 48`.  
* **Group-aware definitions:** `by(...)` is permitted only on labels whose domains are proven to have bounded cardinality within the configured lane budget. Bundles must declare limits (e.g., `facility_id <= 32`, `pod <= 8`) when the compiler cannot infer them. Fresh workloads without historical telemetry MUST include explicit `lane_domains.max_per_partition` declarations for every label whose bound cannot be proven statically; the control plane rejects the bundle otherwise. Labels whose true domain would exceed the cap must be folded into the partition key or otherwise sharded so that the per-partition bound remains ≤64.  
* **Validation:** the control plane rejects bundles whose projected lane count may exceed the bound; operators receive diagnostics pointing at offending expressions. Grouping by the partition key is allowed but redundant and produces a single lane per partition.  
  Workloads requiring higher cardinality must either encode that label into the partition key (reducing the residual per-partition bound to 1) or shard the workload across multiple partitions before compilation.
* **Runtime overflow:** if live data would allocate a lane beyond the limit, lane creation freezes for that metric; existing lanes continue to update. Events whose grouping would require a new lane are not applied to that metric: the AP flags the event with `LANE_OVERFLOW`, emits the `definition_lane_overflow` audit/metric, and continues processing other metrics. No fallback “overflow lane” or probabilistic bucketing is permitted, keeping behaviour deterministic and surfacing the misconfiguration explicitly for operators.

**Projection**
- The control plane projects lane counts using declared `lane_domains.max_per_partition`, static analysis of label matchers, and optional 24 h P95 historical cardinality telemetry.  
- Multi-label groupings multiply per-label maxima unless constrained by matchers.  
- If projected lanes fall within `[LANE_WARN_THRESHOLD, LANE_MAX]`, the bundle is admitted with WARN; projections exceeding `LANE_MAX` are rejected with diagnostics.

### Client Append Interface
*Transport & Security*
- Clients connect to the Raft leader over mTLS (TLS 1.3) with mutual authentication; unauthorized calls are rejected before routing.
- Client append RPCs run on gRPC over HTTP/2 with pipelined unary or streaming channels per partition leader. Every message carries the standardized envelope `{event_id, part_id, ts_ns, event_key_hash, schema_key, payload_ref}`.

*Request semantics*
- Exactly one logical event per message; batching is disabled. Multiple in-flight messages are permitted up to the advertised credit window on each streamed or pipelined channel.
- Retries must reuse the same `event_id`; the leader treats duplicates as idempotent and returns the prior outcome.
- Append responses include `{status, commit_index, credit_hint}` where `credit_hint` advertises the next window size derived from §17 flow control.

*Error model*
- **Transient failures** (`TRANSIENT_RETRY`, `TRANSIENT_REDIRECT`): leader change, quorum timeout, backpressure. Clients backoff with jitter and retry the same event after honoring the new credit window.
- **Permanent failures** (`PERMANENT_SCHEMA`, `PERMANENT_EPOCH`, `PERMANENT_PAYLOAD`): schema blocked, stale epoch, validation failure. Clients must not retry; operators inspect the audit log.
- **Durability fence** (`PERMANENT_DURABILITY`): deferred quorum fsync failed; partition is fenced in Strict mode until operators remediate storage and acknowledge the downgrade. Clients treat this as non-retriable and must pause append loops for the partition until the fence clears.

*Backpressure*
- When the leader’s credit budget is exhausted it responds with `TRANSIENT_BACKPRESSURE` and a reduced `credit_hint`; clients halve their in-flight window and retry after the indicated delay.
- If `/readyz` reports the target partition as `NotReady`, or the overall healthy-partition ratio drops below the ≥99% (configurable) threshold, the endpoint rejects appends with `TRANSIENT_WARMUP`, signalling the caller to pause until readiness is restored.

*Observability*
- Every append request is logged with `{event_id, status, retry_count}`; aggregated metrics feed `client_window_size` and `reject_overload_total` in §17.7.

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

* `commit_epoch_granularity_ns = 1_000_000` (1 ms). The Raft leader samples `CLOCK_MONOTONIC_RAW` when acknowledging a batch, computes `tick = floor(now_ns / commit_epoch_granularity_ns)`, clamps it with `commit_epoch_ticks = max(tick, last_commit_epoch_tick)` (no forced `+1`), and persists that value into every WAL record in the batch. Multiple records acknowledged within the same millisecond therefore share identical ticks, preserving the coarse wall-clock semantics required by dedup retention. `last_commit_epoch_tick` (initially `0`) is updated after each append so the sequence remains monotone even if the clock stalls. With this construction, `commit_epoch_now - commit_epoch_ticks` measures elapsed milliseconds rather than “events since last sample.”
* The Active Processor tracks `commit_epoch_now = last_applied_commit_epoch_tick` and persists the pair `{commit_epoch_now, last_applied_index}` inside checkpoints. When Raft leadership changes, the new leader MUST seed `last_commit_epoch_tick` before acknowledging any writes by loading the greatest `commit_epoch_ticks` observed in durable state (checkpoint metadata if available, otherwise a bounded WAL scan of the open segment). This prevents the tick stream from regressing after failover and keeps every replica's eviction predicates that depend on `commit_epoch_now - commit_epoch_ticks` well-formed.
* Each partition maintains a deterministic throughput estimator `commit_rate_state = {ewma_per_sec, last_sample_tick, last_sample_index}` with `ewma_per_sec` initialised to `max_ingest_rate_per_partition` from the control plane. When applying new entries, if `delta_tick = commit_epoch_now - last_sample_tick > 0`, compute `delta_time_s = delta_tick × (commit_epoch_granularity_ns / 1e9)` and `sample_rate = (applied_index - last_sample_index) / delta_time_s`. Update `ewma_per_sec` using exponential smoothing with half-life `τ = 10 s`: `α = 1 - exp(-delta_time_s / τ)` and `ewma_per_sec = (1 - α) × ewma_per_sec + α × sample_rate`. Persist the updated state alongside checkpoints. When `delta_tick = 0`, defer the update until the next tick boundary to keep the estimator monotone.  
  - The control plane seeds `max_ingest_rate_per_partition` from operator policy and re-estimates it every 30 s by sampling `commit_rate_state.ewma_per_sec` across the partition’s replicas. The reconciler sets the new target to `ceil(1.5 × p95(ewma_per_sec, 5 min window))`, bounded by operator-configured `{min,max}` caps. Partitions whose local EWMA exceeds the control-plane target for two consecutive samples automatically emit `dedup_rate_divergence_total{part_id}` and request an updated limit; the reconciler either accepts the higher value or issues a throttle so that dedup sizing assumptions remain valid.
* Define `index_distance_for(window_s) = ceil(window_s × ewma_per_sec)` (minimum 1). The estimator relies solely on replicated ticks and indices, so replays deterministically reproduce the same distance.

### Idempotency
Per-event deduplication: A bounded, partition-local dedup table keyed by `event_id` prevents reprocessing of retries.  
Structure: 1024 sharded lock-free cuckoo sets storing `(event_id → {raft_log_index, commit_epoch_ticks})`.  
Retention:
- The control plane exposes `client_retry_window_s` (default 30 min) as the maximum interval that a client is permitted to retry an acknowledged `event_id`. Retries that arrive after this interval are rejected with `PERMANENT_RETRY_EXPIRED`, emit `ap_retry_expired_total{part_id}`, and generate an audit log entry `{event_id, part_id, received_ts_ns}`. Clients MUST treat this status as fatal and MUST NOT retry.  
- Each dedup entry records the Raft index and the replicated `commit_epoch_ticks` captured at the time of acknowledgement.  
- Define `retry_window_ticks = ceil(client_retry_window_s × 1_000)` (because each tick = 1 ms). An entry is eligible for eviction only when both conditions hold:  
  1. `raft_log_index < finalized_horizon_index`; and  
  2. `commit_epoch_now - commit_epoch_ticks ≥ retry_window_ticks`.  
- The deterministic throughput estimator (§3.1) bounds index distance implicitly. To guarantee checkpoint replay safety, the AP also enforces `applied_index - raft_log_index ≥ index_distance_for(2 × checkpoint_full_interval)` before eviction.  
The effective retention window is therefore `dedup_retention_window_s = max(client_retry_window_s, 2 × checkpoint_full_interval)` and is persisted in partition metadata.  
Capacity planning: the control plane enforces `dedup_capacity_per_partition ≥ max(ceil(max_ingest_rate_per_partition × client_retry_window_s), index_distance_for(2 × checkpoint_full_interval))`. Checkpoints record the enforced capacity and the estimator state so replay uses the same bound.  
Whenever the reconciler raises `max_ingest_rate_per_partition`, it seals the current WAL segment with a `DedupCapacityUpdate` marker, applies the new capacity to all replicas atomically, and verifies that the in-memory table is resized before resuming client ACKs. Downward adjustments require operator approval and a quiescent window because they may force eviction; the control plane refuses to shrink capacity while occupancy > 70%.
Eviction policy: entries are retired oldest-first once the two predicates above are satisfied; no entry that may still be referenced by replay or duplicate retries inside `client_retry_window_s` is removed. If the table occupancy reaches `dedup_capacity_per_partition` while eligible entries remain, the AP must drain them deterministically in log-index order. When the occupancy reaches the capacity and no further entries satisfy the eviction predicate, the leader raises `dedup_capacity_exhausted_total{part_id}` and applies `TRANSIENT_BACKPRESSURE` to clients until either the horizon advances or the operator increases the configured capacity. No probabilistic or sampling eviction is permitted.  
Scope: per partition. Duplicate `event_id`s in different partitions are allowed and not deduped.  
Checkpoint persistence: replicas capture a deterministic snapshot of each dedup shard alongside checkpoints (§20). The serialized form orders entries by `(raft_log_index, event_id)` so restore + WAL replay reinstates identical dedup membership and eviction watermarks without scanning historical WAL beyond `dedup_retention_window_s`.  
Last-event-wins per key: `last_seqno_by_key[event_key_hash]` enforces last-event-wins semantics.  
Re-receiving the same `(part_id, event_id)` within `client_retry_window_s` → returns the prior outcome without re-apply.  
An older event for the same `event_key_hash` after a newer `raft_log_index` → skipped.

**Collision handling:** `event_key_hash` is produced via SipHash-2-4 keyed with a cluster secret and stored as a 128-bit `(hash_hi, hash_lo)` pair. Each `last_seqno_by_key` shard persists the canonical key identity alongside the hash: if the normalized partition key is ≤256 bytes the identity is stored inline; otherwise the shard stores the full byte string in an arena slot plus a SHA-256 digest. Updates only apply when the persisted identity matches byte-for-byte; any mismatch routes the lookup through the bounded spillover map that also stores the full key bytes. The validator emits an audit record (`ap_key_hash_collision_total`) if a collision is detected, but last-write-wins semantics rely exclusively on the canonical identity rather than a truncated fingerprint, eliminating false matches.

### Finalized Horizon
Each partition maintains a deterministic `finalized_horizon_index` and a per-metric `finalized_horizon_ts_ns` describing which panes are immutable for replay and eviction:

1. On every watermark update (§13.3), compute `finalizable_ts(metric) = WM_e(metric) - C(metric)` where `C(metric)` is the correction horizon chosen for that metric (`0` for DROP-only aggregators, default `1h` otherwise).  
2. Mark all panes whose right edge ≤ `finalizable_ts(metric)` as finalized for that metric. The newest event’s Raft index among those panes becomes the candidate finalized index.  
3. `finalized_horizon_index` is the minimum of those candidate indices across all metrics in the partition; it advances monotonically.  
4. Persist both `finalized_horizon_index` and the per-metric `finalized_horizon_ts_ns` map in checkpoints (§20) so replay reproduces identical bounds.

Operators surface `finalized_horizon_index` via `/readyz` metrics (`ap_finalized_panes_total`) and can monitor lag between the applied index and the finalized index to validate late-data policy configuration.

**Horizon stall remediation:** If `ap_finalized_panes_total` or `finalized_horizon_index` remains flat for more than `2 × lateness_allowance_L` while `applied_index` advances, the Control Plane raises `finalized_horizon_stall{part_id}` and throttles new appends once dedup occupancy exceeds 80% of `dedup_capacity_per_partition`. Operators must investigate late-data policy or correction horizons: either tighten `C(metric)` for the offending metrics or move them to DROP-only mode. After configuration change, the CP records an override audit entry and releases the throttle once the finalized horizon catches up. Runbook: (1) inspect `/readyz` for the stuck partition and identify metrics with high lateness via `ap_late_events_total{metric}`; (2) adjust lateness allowance or correction horizon via `/v1/config`; (3) verify `finalized_horizon_index` convergence before restoring prior durability/latency settings. This ensures dedup eviction proceeds without unbounded backlog.

### Integrity
* CRC per block and segment (see § 19).  
* No per-record checksum for efficiency.  
* All payloads validated via schema registry and signature.

---

## 4  Replication and Consistency

* **Consensus:** 3-node Raft per partition; quorum = 2 of 3.  
* **Leader handles sequencing:** index assigned in Raft log.  
* **Followers:** replicate and fsync per durability mode.  
* **Linearizability:** guaranteed within partition.  
* **Deterministic replay:** given identical WAL and config epochs, AP state is bit-identical.  
* **Quorum commit rule:**  
  - `Strict` → ACK after quorum fsync  
  - `Group-fsync` → ACK after page-cache write; group fsync every 5–10 ms  
* **Failure handling:** Raft elections ≤ 300 ms; followers rebuild via snapshot or catch-up.  

---

## 5  Durability and Acknowledgment Policy

Two selectable modes (per partition):

| Mode | ACK Condition | Loss Window |
|------|----------------|-------------|
| **Strict** | Quorum (2/3) `fdatasync` on commit | None |
| **Group-fsync** | Quorum page-cache write; group fsync every 5–10 ms | ≤ flush interval |

*Mode selection is recorded in partition metadata and versioned per epoch.*  
*All modes require quorum acknowledgment; deterministic replay is preserved. Durability windows differ only by flush timing.*

**Mode transitions**  
1. Seal the current WAL segment and append a `ModeChange` marker with the prior mode.  
2. `fdatasync` segment metadata and directory entries.  
3. Start a new segment in the new durability mode.  
Recovery treats the marker as a fence; segments never mix durability semantics.

Group-fsync loss bound: On a crash or power loss, at most the records placed in the OS page cache since the last group fsync may be lost. The Control Plane enforces `fsync_cadence_ms ∈ [5,10]` (default 8 ms) and tunes kernel write-back (`vm.dirty_background_bytes`, `vm.dirty_bytes`, `dirty_expire_centisecs`) to prevent involuntary flushes that would narrow or widen this window unexpectedly. Operators can choose a byte budget `group_fsync_max_bytes` (default 16 MiB/partition). The effective loss bound is the earlier of the cadence timer and the byte budget.

I/O mode:
- Strict: open WAL with `O_DSYNC` (or `fdatasync` per record/batch) and require 2/3 `fdatasync` before commit ACK.  
- Group-fsync: buffered writes are permitted; commit ACK requires page-cache residency confirmation on 2/3 replicas and the partition’s durability mode to match across voters. A background fsync occurs every `fsync_cadence_ms` or `group_fsync_max_bytes`, whichever is reached first.

Page-cache residency confirmation is implemented as follows:
1. The leader batches `AppendEntries` payloads and `pwrite()`s them via `io_uring` to local storage.  
2. Each replica records the returned byte-range in a monotonically increasing `dirty_epoch`, tracks `dirty_bytes_pending`, and timestamps the epoch start. It then queues write-back without blocking for durability via `sync_file_range(SYNC_FILE_RANGE_WRITE)`; on kernels ≥ 6.6 the implementation MUST prefer `io_uring` `IORING_OP_SYNC_FILE_RANGE` with the same `SYNC_FILE_RANGE_WRITE` flag to avoid the deprecated syscall while preserving the queued write-back semantics. If the kernel lacks `IORING_OP_SYNC_FILE_RANGE`, the replica falls back to the blocking syscall with batches capped by `group_fsync_max_bytes` so that the contract still bounds stall time.  
3. The replica includes `{dirty_epoch, durability_mode, max_dirty_log_index}` in its `AppendEntries` response. `dirty_epoch` advances only when the replica has queued write-back for a contiguous prefix ending at `max_dirty_log_index`; the pair `{dirty_epoch, max_dirty_log_index}` is monotone per follower.  
4. The leader acks the client only after receiving matching epochs from a quorum of voters (itself plus at least one follower in the 3-replica configuration) whose `max_dirty_log_index ≥ last_log_index_of_batch`, proving the data is resident in page cache on that quorum under the advertised mode. Responses that regress either field are ignored. The batch is then eligible for the background fsync (timer or byte budget), preserving Raft’s committed-prefix invariant.

This contract relies on Linux guarantees that `pwrite()` completion implies page-cache residency until the next eviction, and that `sync_file_range` schedules write-back without forcing completion; the only loss window is the interval before the scheduled fsync. Replicas do not issue `posix_fadvise(DONTNEED)` or similar hints until after the fsync fence ensures durability. The startup validator rejects nodes that cannot honour these semantics.

**Durability guardrails:** Replicas export `wal_dirty_epoch_age_seconds{part_id,replica}` and `wal_dirty_bytes_pending{part_id,replica}` derived from the tracked epoch timestamp and pending byte count. The leader enforces `dirty_epoch_age_seconds ≤ 2 × fsync_cadence_ms` and `wal_dirty_bytes_pending ≤ group_fsync_max_bytes`. On any violation the leader immediately suppresses new ACKs, emits `TRANSIENT_BACKPRESSURE` to clients, and forces a synchronous fsync across the quorum. If the violation persists for one additional cadence interval, the partition automatically downgrades to `Strict` durability, records `wal_durability_mode_downgrade_total{reason="dirty_epoch_sla"}`, and continues processing only after the downgrade commits. Operators can raise the thresholds via configuration, but the product of cadence and byte budget still bounds the loss window.

**Deferred fsync failure handling:** Every quorum fsync is tracked via a monotonic `fsync_epoch`. A background fsync completion that returns an error (I/O, ENOSPC, EIO, EREMOTEIO) triggers an immediate quorum fence: the leader marks the current `fsync_epoch` as failed, transitions the partition to `Strict` mode, and issues `PERMANENT_DURABILITY` to callers until a full synchronous fsync succeeds on all replicas. The Control Plane records `wal_durability_mode_downgrade_total{reason="fsync_failure"}` and raises `wal_durability_fence_active{part_id}`. Recovery requires operators to remediate the storage fault and acknowledge the downgrade; after a successful strict fsync the partition may re-enable Group-fsync via the standard mode-change barrier. No further client ACKs are emitted while a failed epoch is unresolved, preventing silent data loss after an acknowledged write.

**Kernel/Filesystem scope:** Group-fsync mode is supported only on Linux ≥ 5.15 with ext4 (`data=ordered`, `barrier=1`) or xfs (write barriers enabled). During admission each node executes a writeback probe that verifies the kernel honours the expected ordering—`pwrite` → queued write-back (`sync_file_range(SYNC_FILE_RANGE_WRITE)` or `IORING_OP_SYNC_FILE_RANGE` with `SYNC_FILE_RANGE_WRITE`) → deferred `fsync`—on that filesystem. The probe detects short-circuit behaviours (for example, a no-op `sync_file_range`) but cannot guarantee dirty pages remain in cache until the scheduled fsync; CEPtra therefore treats Group-fsync as a risk-bounded mode with live monitoring. If the probe fails—because the syscall is unavailable, short-circuited, or the filesystem violates the ordering contract—the partition automatically downgrades to `Strict` durability and raises `wal_durability_mode_downgrade_total{reason="kernel_capability"}` so operators can remediate.

Surprise flush mitigation:
- Nodes must enable filesystem barriers (ext4: `data=ordered`, `barrier=1`; xfs: ensure `nobarrier` is false) and set CSI cache modes that preserve write ordering.  
- A startup validator inspects effective writeback behaviour and rejects unsafe configurations before admitting the replica to the quorum. Operators must still monitor WAL telemetry for kernel-induced background flushes because the OS may evict dirty pages under memory pressure even when the probe passed.

Supported filesystems: ext4 (`data=ordered`) and xfs with write barriers. Devices must expose write barriers or have their volatile caches disabled. Unsupported filesystems or mount options (`nobarrier`, `writeback`) fail validation at startup.

---

## 6  Storage Layout

* Append-only WAL segments (1–4 GiB), each validated by block-level CRC.  
* Sparse index for O(log n) lookup by `raft_log_index`.  
* Checkpoint records embed compute state (see §20).  
* Crash recovery:
  1. Backward scan for last valid block trailer.  
  2. Truncate file to last valid offset.  
  3. Rebuild sparse index.  
  4. Resume at last committed index.

**Segment sealing:** when full or during durability change.  
**Storage type:** NVMe-backed persistent volume in Kubernetes.  

---

## 7  Active Processor (AP)

* Exactly one **AP leader** per partition, colocated with Raft leader.  
* Reads committed stream directly from local WAL.  
* Maintains in-memory:
  * **Pane ring:** `M` panes × `L` lanes.  
  * **Per-key map:** `last_seqno_by_key` for idempotency.  
  * **Aggregator state:** associative monoids `{identity, ⊕, ⊖}`.  
  * **Rolling windows:** pre-defined fixed horizons.

### Apply Logic
1. Receive committed batch of `(log_index, event)` tuples.  
2. For each event, consult the dedup table:  
   - If `dedup.contains(event_id)` → treat as replay, skip apply, and emit prior status.  
   - Otherwise insert `{event_id, log_index, commit_epoch_ticks}` into the shard corresponding to the event.  
3. If `log_index > last_seqno_by_key[event_key_hash]`:  
   - Apply aggregators for all relevant lanes/windows.  
   - Update `last_seqno_by_key`.  
4. If late relative to watermark → handle via policy (DROP or RETRACT).  
5. Periodically checkpoint to persistent storage (see §20).

### Compute Guarantees
* **Deterministic:** order by Raft index, pure functions only.  
* **Idempotent:** skip if index ≤ recorded last_seqno.  
* **Concurrent:** within partition, sequential; across partitions, parallel.

---

## 8  In-Memory Aggregation Structures

### Pane Ring
* Base granularity `q` (e.g., 250 ms).  
* Ring length `M = ceil(R_max / q)`.  
  * `R_max = max(R_raw, max_window_horizon)` where `R_raw` is the raw-tier retention horizon in §10 and `max_window_horizon` is the longest rolling-window horizon declared for the partition. The ring therefore spans every pane that might be read before compaction or eviction.
* Each pane is an array of lanes × aggregators.

### Lanes
* Fixed predicates evaluated once per event.  
* Stored as bitmask `lane_bitmap`.  
* Compiler enforces lane budgets per §2; active lanes ≥48 emit warnings and lanes >64 are rejected.  
* Examples: device_type, firmware_channel, facility_id (bounded domains only).

### Aggregators
* Pure associative monoids implementing:
  - `⊕(state, event)` add  
  - `⊗(state_a, state_b)` merge (deterministic ordering)  
  - `⊖(state, event)` remove (optional)  
  - `identity()` neutral element  
* Supported v1 families with native PromQL semantics: `sum`, `count`, `avg`, `min`, `max`, `rate`, `increase`, `count_over_time`, `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `quantile_over_time`, `topk`, `distinct`.
* `quantile_over_time` uses a fixed-parameter KLL sketch; deterministic merge order guarantees rank error ≤1% at `p95`.  
  - All compaction levels use a precomputed, deterministic split schedule; no random sampling or coin flips are permitted.  
  - The sketch is seeded with the partition’s `part_id` and metric identifier to select lanes deterministically, and checkpoints persist the exact register buffers so replay and recovery reproduce identical quantiles bit-for-bit.  
* `topk` relies on a bounded heap per lane with deterministic tiebreaks; `distinct` uses HyperLogLog++ (relative error ≤1.6%).  
  - `distinct` hashes values with SipHash-2-4 keyed by the bundle signature digest; register promotion and sparse/dense transitions follow a fixed ordering with no randomness.  
  - Both `topk` and `distinct` persist their serialized register/heap buffers inside checkpoints; replay never recomputes them from higher-level summaries, preserving determinism.
* `quantile_over_time`, `topk`, and `distinct` are **non-retractable** and enforce DROP handling when late data would affect their panes.  
* Aggregators without `⊖` are marked **non-retractable**; RETRACT policies devolve to DROP with audit records.

**Invariant A\*:** All aggregations participating in rolling windows or compaction MUST form associative monoids with identity elements and deterministic merge. Compaction folds panes and mixes tiers (raw vs compacted) exclusively via monoid `⊕` and MUST yield bit-identical results to real-time rolling application.

Examples:
- `avg` implemented as monoid `(sum, count)` with identity `(0,0)` and merge = pairwise add; final value = `sum / count`.  
- `rate`/`increase` defined over per-series monotonic counters with associative sufficient statistics per pane.  
- Non-associative functions are prohibited from compaction and must be marked `NON-COMPACTABLE`; they remain queryable only from raw horizons.

`avg`: implemented as the monoid `(sum: f64, count: u64)`. `⊕` = pairwise add; identity = `(0,0)`; retract applies `(⊖sum, ⊖count)`. The scalar is produced lazily as `sum / count`.

KLL parameters: default `kll_k = 200` with deterministic compaction; target rank error ≤ 1% at `p95`. Configurable per metric set via `policy_version` metadata.  
HyperLogLog++: `hll_p = 14` (16384 registers) with sparse mode enabled up to 3000 distincts; relative error ≤1.6%.  
`topk`: default heap size `k ≤ 100`; ties broken deterministically by `(value, key_hash)`.  
All structures expose hard memory caps per lane; breaching a cap freezes updates and emits `definition_lane_overflow`.

### Fixed Windows
* Pre-maintained rolling sums for horizons (e.g., 5 s, 30 s, 1 m, 1 h).  
* `window[n] = Σ panes[i..i+n]`.

### Memory Bound
`O(A × M × L)` where `A` = aggregators, `M` = panes, `L` = lanes.

*All structures allocated from NUMA-local arenas; no GC.*  
*Checkpointing and compaction free memory via `madvise(DONTNEED)` once sealed.*

---
## 9  Rule Layer and Meta-Aggregation

* Rules combine raw events, side queries, and aggregates to produce **classification events**.
* Rule outputs carry the standard event envelope plus the header flag `DERIVED`. By default they are forwarded only to their configured external sinks (`channel` URI); CEPtra does not loop them back into the ingestion path unless an operator explicitly binds that channel to a `source.*` phase under a different partition key.
* Each rule set belongs to a **versioned DAG**; every emitted record carries `rule_version`.
* Meta-aggregates (counts, ratios, threat scores) reuse the same pane/window logic.

**Invariant R2:** Control Plane validation rejects bundles that consume any `channel` also produced by a rule in the same definition bundle. To build multi-pass pipelines, operators must declare a separate ingestion definition (new partition key or rule version) and fence the epochs explicitly, preventing accidental infinite feedback loops.

### CEL Execution Context (revised)

Each metric binding in CEL refers to a single resolved time series per evaluation with these fields:
- `value: f64` – scalar value of the resolved series at the pane/window boundary.  
- `labels: map<string,string>` – present only when the originating PromQL expression declared a non-empty `by(...)` grouping set.  
- `has_value: bool` – `false` when the metric is suppressed (for example, dropped late data on a non-retractable aggregator); CEL must test this before dereferencing `.value` in optional flows.  
- `flags: set<string>` – declarative markers such as `NON_RETRACTABLE_DROPPED` that describe why a value is absent or adjusted.

Bindings inherit deterministic contextual data:
- Record: `payload`, `headers` (`{part_id, raft_log_index, schema_key, event_id, event_key_hash}`), and `partition_key`.  
- Temporal: `window_start_ns`, `window_end_ns`, `watermark_ns`, `processing_time_ns`.  
- Versioning: `rule_version`, `policy_version`.  
- Helpers: `coalesce(a, b)`, `clamp(v, min, max)`, `safe_div(n, d, default)`.

### Series Resolution

1) **Materialization**
   - If a PromQL expression omits `by(...)`, no labels are materialized; `.labels[...]` access is a compile-time error.  
   - If `by(...)` is present, labels are materialized exactly for the declared set. Accessing labels outside that set is a compile-time error.

2) **Resolution to one series**
   a) *Lane binding:* when the triggering event matched a lane whose grouping set and labels align with the PromQL expression, use that series.  
   b) *Payload join:* otherwise, if all labels in the grouping set are derivable deterministically from the event payload or headers, use the series whose labels match those values.  
   c) *Ambiguity:* if multiple series match or required labels are missing, compilation fails unless the rule employs an explicit `select(metric, {label_map})` helper.

3) **Comparability rule** – CEL may compare `.labels[...]` only between metrics whose PromQL grouping sets are identical. Violations raise compile-time errors.

4) **Value availability** – `.value` is defined only after series resolution succeeds. Unresolved metrics reject the rule bundle at compile time.

Helper: `select(name: string, labels: map<string,string>) -> MetricBinding`  
Notes: `select` may only address a series within the same partition context; `labels` must cover the full grouping set.

**Rule bundle contents**
- Policy definition (PromQL selectors compiled per §14)
- Lane definitions and declared label domains
- Rule expressions
- Version metadata (`def_version`, `policy_version`, allowed schema keys)
- Signatures for integrity

---

## 10  Retention and Compaction

| Tier | Retention | Purpose |
|------|------------|----------|
| **Raw** | `R_raw` (e.g., 24 h) | Fine-grained replay |
| **Compacted** | `R_compact` (e.g., 12 mo) | Long-term aggregates |

`R_raw` supplies the raw-tier horizon used in §8 to compute `R_max`.

### Compaction procedure
1. Fold raw panes over `[T–24h, T)` using associative `⊕`.
2. Write compacted record.
3. Once confirmed durable, mark old panes `<T–24h` reusable.

**Invariant A\*:** Compaction only folds panes via associative monoids with deterministic identity and merge. Mixing raw and compacted tiers uses monoid `⊕` exclusively and MUST reproduce the same result as real-time rolling application.

A metric is eligible for compaction only if its aggregator declares `associative = true` and provides a deterministic `merge` implementation.

**Exact switchover:** queries spanning the boundary use both tiers deterministically. The tier boundary at time `T` is defined as raw data covering `[T−24h, T)` (left-closed, right-open) and compacted data covering `(-∞, T−24h]`. Joins across the boundary evaluate panes as left-closed, right-open ranges so the sample at `T−24h` appears exactly once.

---

## 11  Consistency, Idempotency, and Recovery

* `last_seqno_by_key` enforces last-event-wins semantics.
* During replay, AP skips events where `log_index ≤ last_seqno_by_key[event_key_hash]`.
* Checkpoints (see §20) include both pane state and per-key maps.
* Replaying WAL after a checkpoint is **idempotent** and yields identical state.

### Recovery path
1. Load latest valid checkpoint.
2. Resume Raft apply from `applied_index + 1`.
3. For any missing panes or windows, recompute deterministically.

**Deterministic replay** → identical aggregates across replicas and restarts.

### Derived Event Idempotency
All rule emissions MUST carry `derived_event_id = hash64(part_id, raft_log_index, rule_version, rule_id, channel)`. Sinks SHOULD deduplicate on this key. Replays and re-evaluations never produce a new `derived_event_id` for the same input index and rule.

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
* Default priority `[R0 > R1 > R2]`.
* **Warm standby**: follower AP applies all committed entries through WAL head silently so promotion is instantaneous.
* **Cold standby**: loads latest checkpoint + replay (< 1 s typical).

### Self-healing
* StatefulSet restart → pod re-joins cluster, replays WAL, resumes role.
* All reconciliation done automatically by embedded Control Plane.
---
## 13  Time Semantics and Late Data Policy

### 13.1 Definitions
- **Event time (`ts_ns`)** – UTC nanoseconds within the event payload; drives windowing.  
- **Processing time** – Monotonic node clock (for guards and latency).  
- **Sequence time** – Raft log index; authoritative for replay and rule version selection.

**Invariant T1:** Windowing uses event time; rule versioning uses sequence time.

---

### 13.2 Windowing Model
- Fixed pane size `q` (default 250 ms).
- Window set `{W}` = multiples of `q` (e.g., 5s, 30s, 1m, 1h).
- `pane(ts) = floor(ts / q)`.
- Each lane maintains rolling windows = sum of recent panes.
- Event-time windows are left-closed and right-open; events exactly at the upper bound belong to the following pane.

---

### 13.3 Watermarks
Each AP computes a per-partition watermark `WM_e`:

```
candidate = max_event_ts_seen - L
guarded   = now_ns() - G
WM_e_next = min(candidate, guarded)
WM_e = max(WM_e_prev, WM_e_next)
```

where:
- `L` lateness allowance (default 2s)
- `G` wall-clock guard (default 200ms)

`WM_e` is monotonic, deterministic under replay.

The guarded term uses the partition's monotonic clock, but every evaluation persists the sampled floor `wm_guard_floor_ns` alongside checkpoints (§20). During replay we restore that floor and re-apply the same min/max sequence, so the resulting watermark matches the original even though `now_ns()` advances.

`L` and `G` are hot, per-partition knobs managed by the control plane. Defaults (`L=2s`, `G=200ms`) suit moderate jitter. High-jitter pipelines may increase `L` to 5–10 s while keeping `G` near 200 ms to absorb bursts; strict-latency workloads typically keep `L ≤ 1s` and reduce `G` toward 100 ms to minimize guard delay.

Deterministic guard replay:
- Every time `now_ns() - G` would increase the guard term, the AP captures the new value in `wm_guard_floor_ns` and tags it with the WAL index that triggered the update (`wm_guard_floor_index`). Subsequent evaluations reuse `guarded = wm_guard_floor_ns` until a later index advances the tag.  
- Checkpoints persist both fields. On restore the AP resumes with the recorded pair and suppresses further guard increases until replay has processed beyond `wm_guard_floor_index`, ensuring the guard sequence remains identical even though `now_ns()` during recovery may jump ahead.  
- When a guard update occurs after the checkpoint (new WAL data), the same tag is appended to the deterministic state so later replays repeat it at the identical index.

Overrides:
- `L` (lateness allowance) and `G` (guard) may be overridden per metric via `metric_overrides{L?, G?}`.  
- When a metric override exists, its watermark is `WM_e(metric) = min(max_event_ts_seen(metric) - L(metric), now_ns() - G(metric))` and remains monotone for that metric.  
- Export both `ap_watermark_ts_seconds` (partition) and `ap_watermark_ts_seconds{metric="<name>"}` whenever overrides are active.

After each watermark evaluation, compute `finalizable_ts(metric) = WM_e(metric) - C(metric)` using the metric’s configured correction horizon `C`. Panes whose right edge lies at or before `finalizable_ts(metric)` become finalized for that metric, contributing to the monotone `finalized_horizon_index` defined in §3. Finalization decisions are strictly deterministic functions of the watermark sequence and correction horizons so replay yields the same immutable window.

---

### 13.4 Late-Event Policies

| Policy | Description |
|---------|-------------|
| **DROP** | Ignore events ≤ WM_e; emit audit record to “late” partition. |
| **RETRACT** | Apply inverse `⊖` if aggregator supports it, within correction horizon `C` (default 1h). Non-retractable aggregators devolve to DROP with an audit record. |

Outside `C`, fallback to DROP.

**Invariant T2:** Finalized panes immutable under DROP; RETRACT uses logged inverse ops, deterministic under replay.

Aggregators flagged as non-retractable—`quantile_over_time`, `topk`, and `distinct`—always use DROP when late data would affect them. Quantile panes additionally reject late updates touching finalized ranges to preserve the published error bound.

Late Data for Non-Retractable Aggregators:
- An event “affects” a non-retractable aggregate when its event time maps to a pane at or before the finalized horizon timestamp `finalized_horizon_ts_ns[metric]`.  
- Such events are dropped ONLY for the affected non-retractable aggregators; retractable aggregators continue according to policy.  
- Emit `ap_late_events_total{metric=<name>, reason="NON_RETRACTABLE_FINALIZED"}` and record `{event_id, raft_log_index}` in the audit log.

Rule authors MUST treat bindings that include non-retractable metrics as conditionally sparse because DROP decisions apply per-aggregator. The CEL compiler exposes `binding.has_value` and `binding.flags` so downstream logic can gate comparisons or fall back when a metric was elided by lateness, preventing inadvertent skew between retractable and non-retractable inputs.

---

### 13.5 Admission Checks
* `max_future_skew` = +5s → reject events too far ahead.
* `max_past_skew` = −30min → allowed; handled by late policy.
* Violations → side-channel with reason.

Admission checks run before lateness evaluation: events that pass `max_future_skew` and `max_past_skew` are still subject to the watermark derived from `L` and `G`.

Rejection outcome:
- `max_future_skew` violations are rejected permanently with side-channel audit; clients MUST NOT retry.  
- `max_past_skew` violations pass to lateness policy unchanged.

---

### 13.6 Checkpoints and Replay
Checkpoints persist `WM_e`, `max_event_ts_seen`, open/closed panes, `finalized_horizon_index`, and `finalized_horizon_ts_ns`.  
Replay recomputes identical `WM_e` given same WAL sequence.

---

### 13.7 Metrics
- `ap_watermark_ts_seconds`
- `ap_event_lateness_seconds`
- `ap_late_events_total{reason}`
- `ap_finalized_panes_total`
- `ap_retractions_total`

---

## 14  PromQL Definition Surface

Definition bundles express metric extractions using native PromQL selectors. Each expression is compiled into an associative monoid with deterministic merge semantics for the Active Processor.

### 14.1 Expression Form
CEPtra accepts the native PromQL subset shown below:

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

Selectors use standard PromQL label matchers. Range selectors (`[Δ]`) are mandatory for range functions and optional elsewhere; bracket syntax follows PromQL duration literals (`5m`, `30s`, `1h`). `scalar` follows the PromQL numeric literal grammar. `without(...)` is not supported. Validation enforces that `topk` forms supply exactly one leading scalar argument and that other aggregators omit it; `quantile_over_time` likewise requires a scalar probability followed by the selector.

**Invariant A\*:** All aggregations used for rolling windows or compaction MUST be expressible as associative monoids with identity elements and deterministic merge. Compaction folds panes and mixes tiers solely through monoid `⊕` and MUST match real-time evaluation exactly.

### 14.2 Function Semantics
* `sum`, `count`, `avg`, `min`, `max` – associative/retractable when an inverse exists; typically paired with `by(...)` to control lane cardinality.  
* `rate`, `increase` – incremental counter primitives over range vectors.  
* `count_over_time`, `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time` – sliding-window reductions across panes.  
* `quantile_over_time` – KLL sketch per §8 with ≤1% rank error at `p95` (scalar probability as first argument).  
* `topk` – deterministic heap of the highest `k` samples (scalar `k` as first argument).  
* `distinct` – HyperLogLog++ cardinality estimate.  

`distinct` is a CEPtra-specific extension to PromQL. It accepts the same selector grammar (optional `by(...)`) but the compiler verifies that every grouped label has a declared bound (§2). Generated bundles carry the HLL++ state described in §8 and advertise an approximate error of ≤1.6% at 99% confidence. Control Plane validation emits diagnostics for downstream targets that cannot interpret `distinct`, prompting operators to choose an alternate aggregator.

`quantile_over_time`, `topk`, and `distinct` are non-retractable; late data for these aggregations is handled via DROP (§13.4).

Counter Semantics (normative):
- `rate` and `increase` operate on strictly monotonic per-series counters.  
- Counter resets (monotonicity violations where `delta < 0`) restart the accumulator from the new sample.  
- Negative deltas unrelated to resets (for example, clock skew) are discarded for that pane.  
- Windows require at least two samples; otherwise the result is `NaN` and downstream monoids ignore it.  
- Samples older than twice the range selector are skipped for the current evaluation.

Associativity for Compaction:
- `rate`/`increase` maintain `(Δ, duration, reset_count)` per pane; merges are associative and finalization emits the scalar.  
- `count_over_time`, `sum_over_time`, `min_over_time`, `max_over_time`, `avg_over_time` maintain sufficient statistics (`sum`, `count`, `min`, `max` as required).  
- `quantile_over_time`, `topk`, and `distinct` supply deterministic merges for their sketches/heaps. These functions are `NON-RETRACTABLE` and only compactable when panes combine via their defined merge operators; late data policy §13.4 applies.

### 14.3 Grouping, Lane Admission, and Label Availability
`by(...)` clauses create lanes within a partition and materialize the corresponding labels map for CEL. Only labels with bounded cardinality approved by the Control Plane are accepted under `LANE_MAX_PER_PARTITION = 64`. Definition bundles must declare domain bounds for each grouped label (for example, `gateway_id ≤ 32 per partition`, `az ≤ 6`). If the natural cardinality of a label would exceed the cap, the bundle must partition on that label (yielding a bound of 1) or otherwise bucket it so the residual per-partition bound remains ≤64. Label availability rule: if a query omits `by(...)`, no labels map is present for that metric in CEL. Comparability rule: CEL may only compare `labels[...]` between metrics whose PromQL expressions declare identical grouping sets. Violations are rejected at compile time with diagnostics.

### 14.4 Examples
```
sum_over_time(power_draw_watts{site="plant-7"}[5m])
avg by (zone)(avg_over_time(device_temperature_c{state="ONLINE"}[15m]))
count_over_time(device_auth_failures_total{result="DENIED"}[1h])
quantile_over_time(0.95, network_latency_seconds{link_state="UP"}[30s])
increase(logon_failures_total{realm="corp"}[10m])
```

### 14.5 Translation Note
Bundles authored with the draft `fn over Δ (metric)` form translate directly: `fn over 5m (x{...})` ↔ `fn(x{...}[5m])`. Aggregations without `over` become instant selectors without a range (`sum(metric{...})`).

---

### 14.6 Stage Option Defaults
Definition bundles may supply per-stage hints consistent with the PromQL surface:
- `aggregate.promql` stages accept an optional `window` duration that declares the default range vector expected by the enclosed queries. Every referenced PromQL expression MUST still specify its own explicit range selector; the compiler verifies the selectors align with the declared `window` (for example, equal to the stage window or a documented multiple) and rejects mismatches. The hint is persisted for observability (`definition_window_default_seconds`) but does not alter query semantics.
The Control Plane validates stage hints during bundle admission to ensure they neither expand lane cardinality nor bypass the monoid requirements defined earlier in this section.

---

## 15  Hot Reload and Canary Protocol

### Concepts
* `def_version` – integer version of metrics and rule bundle.
* `barrier_index` – Raft log index at which new version activates.

### Phases
1. **Distribute:** CP publishes bundle `V+1`.  
2. **Prepare:** APs compile, report `PREPARED(V+1)`.  
3. **Warm (shadow apply, non-blocking):** APs evaluate `V+1` in shadow mode while continuing to ingest and serve with `V`. Readiness is evaluated per partition. A partition reports `NotReady` only for that partition until its warmup reaches the WAL head and the shadow state is aligned. The pod remains `Ready` if ≥99% (configurable) of its partitions are ready.  
4. **Commit:** CP (via leader) appends `DEFINE_ACTIVATE{def_version=V+1}` only after every targeted partition reports `SHADOW_READY` (shadow state caught up through the durable head) or the operator-approved readiness quorum (default 100%) is met. The CP refuses to advance while any partition remains behind the WAL head or in shadow compile failure, surfacing `definition_warmup_pending{part_id}` and holding the barrier in a pending state.  
   - Entries `≤ barrier_index` → process with V.  
   - Entries `> barrier_index` → process with V+1.  
   - Atomic swap after processing `≤ barrier_index`.  
5. **Canary clients:** may pin `rule_version` during validation.  
Per-partition readiness exposure: The `/readyz` endpoint includes a `partitions` map with readiness for each partition and diagnostic reasons when any remain `NotReady`.  
6. **Decommission:** after grace window, remove old definitions.

During checkpoint recovery or WAL catch-up, each partition uses the same readiness gate; append requests targeting partitions still warming emit `TRANSIENT_WARMUP` until their replay reaches the durable head (§24.6).

Guarantees atomic cutover and deterministic replay.

---

## 16  Control Plane: Durability, Epochs, and Coordination

### 16.1 Purpose
Embedded CP provides authoritative configuration, orchestration, and durable metadata via its own **CP-Raft** cluster.

### 16.2 CP-Raft Cluster
- First M StatefulSet ordinals (default 3) join as CP-Raft voters.  
- Followers act as observers.  
- Linearizable writes; read-index for safe reads.

### 16.3 Durable Objects
- `ClusterConfig{version,...}`
- `PartitionSpec{part_id, voters, durability_mode, epoch}`
- `DefinitionBundle{def_version,...}`
- `ActivationBarrier{part_id,def_version,index}`
- `SecurityMaterial{ca_bundle,cert_roots}`
- Immutable, versioned objects.

### 16.4 Epochs and Fencing
- Monotone 64-bit epoch per partition.  
- Any topology or durability change → epoch++ and triggers Raft joint consensus; new configurations enter via joint quorum before finalizing.  
- RPCs carry `{part_id, epoch}`; mismatches are rejected with `PERMANENT_EPOCH`.  
- Safety invariant: never advance a minority; joint consensus ensures overlapping majority before commit.

### 16.5 APIs
- `/v1/partitions` create or modify specs (epoch++).  
- `/v1/definitions` upload bundles.  
- `/v1/activate` append activation barriers.  
- `/v1/state` snapshot of nodes, epochs, health.

### 16.6 Reconcilers
Controllers poll every 500–1000 ms:

1. **Membership:** ensure voters running per spec; at most one partition per node undergoes membership change at a time (configurable throttle).  
2. **Leader Placement:** maintain affinity.  
3. **Durability Policy:** push fsync modes.  
4. **AP Placement:** enforce one AP leader + standby.  
5. **Definition Warmup:** preload new bundles.  
6. **Activation Barrier:** append `DEFINE_ACTIVATE`.  
7. **Garbage Collection:** retire old definitions.

### 16.7 Node Heartbeats
Every 250 ms: `{node_id, part_ids, epoch, raft_role, leader_id, commit_index, apply_index, lag_ms}`.

### 16.8 Failure and Recovery
- CP leader fail → Raft elects new leader <150 ms.  
- CP loss of quorum → restore from snapshot; data path continues.  
- Partition epoch mismatch → automatic reconcile.

### 16.9 Observability
- `cp_reconcile_duration_seconds`
- `cp_epoch_mismatch_total`
- `cp_activation_committed{part_id,def_version}`
- `cp_node_status{node_id,status}`

### 16.10 Security
- mTLS between nodes, CP, and clients (see §22).  
- All CP changes signed and audited.

### 16.11 Lane Domain Declarations
Bundles include `lane_domains` describing label names and per-partition cardinality bounds, e.g.:
```
{
  "lane_domains": {
    "player_id": {"max_per_partition": 32},
    "host_id": {"max_per_partition": 8},
    "cell_id": {"max_per_partition": 16},
    "carrier_id": {"max_per_partition": 6},
    "facility_id": {"max_per_partition": 12},
    "gateway_id": {"max_per_partition": 24}
  }
}
```
The Control Plane validates projected lane counts against these bounds and rejects definitions that exceed `LANE_MAX_PER_PARTITION`.

---
## 17  Backpressure and Flow Control

### 17.1 Objectives
Maintain bounded latency, memory, and deterministic throughput under load or replica lag.

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
Leaders recompute the advertised `credit_hint` every 200 ms:

```
base           = min(max_inflight_events,
                     max_inflight_bytes / avg_event_bytes)
headroom       = clamp(1 - queue_utilization(commit_apply_queue), 0, 1)
follower_factor = f(replication_lag_seconds_p95)
credit_hint    = ceil(base * (0.5 + 0.5 * headroom) * follower_factor)
```

`max_inflight_events` and `max_inflight_bytes` come from the partition's flow-control profile stored in CP-Raft (`partition_flow{max_events,max_bytes}`). Defaults are `max_inflight_events = 2048` and `max_inflight_bytes = 16 MiB`. Operators may hot-patch these values via `/v1/config`; the CP propagates new limits to leaders within one reconcile cycle, and the next credit-hint tick applies them. The same profile also constrains client SDKs during bootstrap so their initial window never exceeds the server cap.

`avg_event_bytes` is a moving average over the last second of commits. `credit_hint` is never lower than 1 event and is communicated on every append response and heartbeat.

The follower factor is piecewise:
```
f(lag) = 1.00  if lag ≤ 0.05 s
       = 0.75  if 0.05 s < lag ≤ 0.15 s
       = 0.50  if 0.15 s < lag ≤ 0.40 s
       = 0.25  otherwise
```

Client SDK behaviour:
- Initial window = `min(1000 events, 10 MiB)`.
- On overload, timeout, or `TRANSIENT_BACKPRESSURE`: halve the current window and apply jittered backoff (20–1000 ms).
- On receiving a `credit_hint`, raise the cap to `min(current * 1.25, credit_hint)` after a 500 ms dwell; reductions apply immediately.
- On `PERMANENT_DURABILITY`, halt retries for the affected partition, surface the alert (`wal_durability_fence_active{part_id}`), and resume only after readiness reports the fence cleared or the Control Plane marks the partition healthy.
- The effective limit is enforced on both event count and bytes; whichever saturates first pauses new appends.

This contract keeps commit→apply headroom while adapting to follower lag.

Outlier protection:
- Enforce `max_event_bytes` to prevent a single payload from consuming the entire credit window; violations return `PERMANENT_PAYLOAD`.  
  - `max_event_bytes` defaults to 1 MiB per partition. Operators may override it via `/v1/config` (`partition_flow{max_event_bytes}`), bounded to `[64 KiB, 8 MiB]` by the control plane.  
  - The reconciler stages updates by broadcasting the new limit in `credit_hint` responses for one dwell (500 ms) before enforcement flips, giving clients time to shrink batches deterministically.  
- When `avg_event_bytes` volatility exceeds 3× over a 1 s window, clamp `credit_hint` growth to 10% per dwell.

---

### 17.4 Raft Internal Flow Control
* Per-follower inflight byte credit (default 16 MB).  
* Leader limits AppendEntries accordingly.  
* Lag > 100 ms sustained → mark degraded; throttle new appends.

**WRITE_THROTTLE:** when 2+ followers exceed 250 ms lag → reduce append rate by 50%.  
**Severe (>1 s):** temporarily reject new appends.

---

### 17.5 Commit→Apply Queue
* Lock-free ring buffer of 50 K events (≈ 20–40 MB).  
* >75% → warn; >100% → pause ACKs.  
* AP apply loop consumes in batches (256 events / 1 ms poll).  
* AP sets `apply_lag_seconds = tail.commit_time - head.apply_time`.

ACK pause:
- When Commit→Apply is full, leaders return `TRANSIENT_BACKPRESSURE` immediately rather than holding replies open. Clients MUST back off and shrink their windows.

---

### 17.6 AP Emission and Derived Events
* Derived classifications sent through same credit protocol.  
* Emit queue capped (20 K events).  
Emit queue policy:
- Per-channel options = `{drop_oldest | block_with_timeout | dead_letter}`.  
- Default: `block_with_timeout = 200ms`, then `drop_oldest` with `ap_emit_drop_total{channel}` incremented.  
- Channels may override to `dead_letter`.

---

### 17.7 Telemetry
- `queue_depth{type}`
- `replication_lag_seconds`
- `apply_lag_seconds`
- `ap_emit_queue_utilization`
- `client_window_size`
- `reject_overload_total`
- `leader_credit_hint`

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
* Lock-free ring buffers for all queues.  
* Credit-hint timer runs every 200 ms from the Raft leader thread; hints respect the same dwell timers as other throttles.  
* Throttles monotone: dwell ≥500 ms before switching states.  
* Clients use jittered backoff 20–1000 ms on 429/OVERLOADED.

Transport expectations: Client SDKs MUST use gRPC over HTTP/2 with streaming or pipelined unary messages, mutual TLS, and connection pooling per partition leader. One logical event per message; multiple outstanding messages are allowed up to the credit window. Minimum recommended initial concurrency: 256 in-flight messages or 10 MiB, whichever is reached first.

---

## 18  Schema Evolution and Replay Compatibility

### 18.1 Goals
Deterministic replay; safe forward evolution of payloads, aggregators, and rules.

### 18.2 Payload Schemas
* Format: Protobuf or FlatBuffers.
* `schema_id = <format>:<major>.<minor>:<content_hash>`.
* Registered via CP; mapped to `schema_key` (u64).

| Change | Safe | Requires Barrier |
|---------|------|------------------|
| Add optional field | Yes | No |
| Remove/rename field | No | Yes |
| Change type | No | Yes |

---

### 18.3 Policies
* Versioned (`policy_version`).
* Declare required fields, state layout, inverse availability.
* Adding new aggregations within a policy is safe if they are not referenced by active rules.

---

### 18.4 Rule Bundles
* Identified by `def_version`.
* Contain `policy_version`, allowed schema keys, compiled functions.
* Activation via barrier (`DEFINE_ACTIVATE`).

---

### 18.5 Event Headers
```
part_id, raft_log_index, ts_ns, event_id, event_key_hash,
schema_key, def_version_hint, lane_bitmap, flags
```

`flags` includes `DERIVED` for rule emissions, `LATE` for late-policy handling, and `RETRACT` for inverse replays. Downstream consumers can key on `DERIVED` to enforce routing policies.

---

### 18.6 Processing Rules
* Rule version chosen by Raft index vs activation barrier.
* Schema determined by header `schema_key`.
* Policy version chosen by rule bundle.

Same event never processed by multiple rule versions.

---

### 18.7 Validation
Pre-apply validators per schema:
- Required field checks.
- Range normalization.
- Drop or rewrite invalids deterministically.

Metrics: `ap_validate_fail_total{schema_id,reason}`.

---

### 18.8 Checkpoint Upgrade
* Include `policy_version` and `ap_state_version`.  
* On load, upgrade via aggregator `upgrade(from,to)` or replay from snapshot.

Upgrade Decision:
- If `policy_version(to) - policy_version(from) ≤ 1` and every aggregator exposes `upgrade(from, to)`, perform in-place upgrade during load.  
- Otherwise replay from the latest full checkpoint bounded by `checkpoint_retained_fulls`; if the replay chain is incomplete, refuse readiness and emit `ERR_STATE_UPGRADE_FAILED`.

---

### 18.9 Registry
* CP stores schema descriptors content-addressed.  
* AP caches ≤256 schemas; lazy fetch on miss.  
* Clients register new schemas before append.

---

### 18.10 Errors
| Error | Meaning |
|--------|---------|
| `ERR_UNKNOWN_SCHEMA` | Key not registered |
| `ERR_SCHEMA_BLOCKED` | Deprecated |
| `ERR_STATE_UPGRADE_FAILED` | Aggregator upgrade failed → replay fallback |
| `PERMANENT_DURABILITY` | Quorum fsync failure fenced the partition; retries must wait for operator resolution |

---

### 18.11 Invariants
- Schema selection purely from header.  
- Rule version purely from Raft index.  
- Aggregator state deterministic; replay identical outputs.

---

## 19  Storage Format Details

### 19.1 File Layout
`/var/lib/ceptra/partitions/<part_id>/wal/`

`seg-<startIdx>-<endIdx>.wal` + `.idx` + `.meta`

Segment size: 2 GiB (1–4 allowed).

---

### 19.2 Segment Structure
```
SegmentHeader (128 B)
Block[0..N]
SegmentFooter (256 B)
```

**Blocks**
- 64–256 KiB compressed groups.
- Header includes: uncompressed/compressed lengths, record count, first/last log index, codec, CRC.
- Payload: RecordTable + concatenated records.
- Trailer: block CRC.

**Compression:** LZ4 default, Zstd optional.

---

### 19.3 Record Structure
```
EventHeader (fixed)
payload (variable)
```

No per-record checksum; block CRC sufficient.

---

### 19.4 Sparse Index
Fenceposts every 256 records or 1 MiB:
`{log_index, block_offset, record_ordinal}`.

Optional bloom filter by `event_key_hash`.

---

### 19.5 Write Path
1. Accumulate batch (~64 KB / 0.5 ms).  
2. Compress, write with io_uring.  
3. Update index.  
4. Fsync cadence per durability mode.  
5. Seal segment when near size limit or mode change.

io_uring submits registered buffers pinned to the NVMe queue set. Operators align kernel write-back tunables (`vm.dirty_background_bytes`, `dirty_expire_centisecs`) with the configured fsync cadence to avoid surprise flush storms that would violate durability-mode assumptions.

Compression does not need to be byte-identical across replicas; determinism concerns record order and observable state. Block CRCs validate integrity during transfer.

---

### 19.6 Recovery
1. Inspect last segment; if footer invalid → scan back for valid block.  
2. Truncate at boundary.  
3. Rebuild index.  
4. Resume at last committed index.

Idempotent and bounded by open-segment size. Block-level CRCs guarantee detection of any corruption within a 64–256 KiB block; recovery discards at most the damaged block and replays from the previous valid boundary.

Post-repair reconciliation:
- After truncating to the last valid block, the replica issues a Raft `ReadIndex` to learn the committed index and catches up via `AppendEntries` or snapshot transfer.  
- Truncation MUST NOT remove committed entries; if local loss affects only uncommitted tail records they are re-fetched from the leader, preserving DR-2 (no rewind of committed indices).

---

### 19.7 Integrity & Encryption
* CRC per block and segment.  
* Optional AES-256-GCM encryption of payload region.  
* Keys managed by node KMS (see §22).

---

## 20  Checkpointing

### 20.1 Purpose
Bounded recovery (<1 s typical) and deterministic restart.

### 20.2 Contents
Header, open/closed panes, windows, dedup table shards, key-map shards, aggregator state, `WM_e`, `finalized_horizon_index`, `finalized_horizon_ts_ns{metric}`, config/version info, checksum, signature.

---

### 20.3 Cadence
- `checkpoint_full_interval = 30s` or 50 M entries, whichever occurs first.  
- `checkpoint_incremental_interval = 10s`; the scheduler caps chains at ≤3 incrementals between fulls.  
- `checkpoint_bandwidth_cap = 40 MB/s` per AP, enforced via token bucket on serialization threads.  
- `checkpoint_retained_fulls = 2`; older fulls and their incrementals are evicted once the retention window is satisfied.
- Definition bundles may request per-stage overrides via `sink.checkpoint` options `{full_interval, incremental_interval}`. Overrides must remain within `[5s, 5m]` for incremental and `[15s, 5m]` for full checkpoints (configurable cluster-wide). The Control Plane enforces these bounds, records approved values in the partition’s config metadata, and rejects bundles that would violate the durability and bandwidth guardrails above.

---

### 20.4 Per-Key Map and Dedup Table
* `last_seqno_by_key` sharded (default 1024).  
* Each shard: sorted `(key_hash,last_seqno)` pairs.  
* Dedup shards mirrored 1:1 with the key shards; each entry encodes `(event_id, raft_log_index, commit_epoch_ticks)` in deterministic order.  
* Incremental checkpoints emit only changed keys and dedup entries; sealed shards are copied verbatim.  
* Eviction for both structures is deterministic on `finalized_horizon_index` and `dedup_retention_window_s`.

---

### 20.5 Concurrency
* Copy-on-write snapshot; AP continues on new buffers.  
* Serialization in background thread.  
* Publish manifest after successful fsync + rename.

---

### 20.6 Restore Path
1. Locate latest full + incrementals.  
2. Load header; validate checksum & signature.  
3. Apply incrementals.  
4. Set AP watermarks.  
5. Resume from `applied_index + 1`.

**Invariant C3:** restored state + replay = pre-checkpoint state exactly.

---

### 20.7 Failure Handling
Partial write → discarded; last valid used.  
Checksum fail → fallback to previous chain; corrupt incrementals are skipped and replay resumes from the last verified full + incremental sequence.

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
- Deterministic low latency (<10 ms p99).  
- Partition isolation.  
- No global locks or blocking GC (Rust runtime).  
- NUMA-local memory access and predictable scheduling.

---

### 21.2 Roles (single binary)
Each pod runs:
- **CP module** – Control Plane Raft and API.  
- **PRG module** – Per-partition Raft replica and WAL I/O.  
- **AP module** – Apply and aggregation logic.  
- **CA module** – Heartbeat, reconciliation, metrics.  

---

### 21.3 Thread Layout per Partition
| Thread | Responsibility | Binding |
|---------|----------------|----------|
| I/O Reactor | Raft RPCs, AppendEntries | pinned to NUMA node with NVMe queue |
| WAL Writer | Batch compress/write, fsync | same node as NVMe |
| Apply Worker | Decode & aggregate (Tokio current-thread runtime) | same node |
| Checkpoint Worker | Serialize frozen snapshot | shares core when idle |
| Telemetry Worker | Metrics/GC | shared pool |

Typical load: one dedicated apply thread with shared I/O and checkpoint workers supports 10–50 active partitions per 32-core node while holding apply `p99 ≤ 5 ms`. At peak, a saturated partition may consume roughly three core-equivalents.

---

### 21.4 Queues
All queues = lock-free ring buffers.

| Queue | Size | Backpressure |
|--------|------|--------------|
| Ingress | 64 K events | WRITE_THROTTLE |
| Replication | 16 MB/follower | credit protocol |
| Commit→Apply | 50 K | pause ACKs |
| Emit | 20 K | drop oldest |
| Checkpoint hand-off | 1 | swap snapshot |

---

### 21.5 Scheduling
- PRG threads: epoll / io_uring.  
- AP: Tokio single-thread executor co-located with the Apply Worker; each partition owns one current-thread runtime pinned to its apply core, ensuring the “single mutator thread” invariant while still allowing async I/O within that thread.
- CP: async runtime, cooperative tasks.  
- CA: lightweight async loop.

---

### 21.6 NUMA
- Partition memory pinned to local node.  
- NVMe IRQs steered to same CPU.  
- NIC RSS hashed by `part_id` → local core.

---

### 21.7 Synchronisation
- WAL: single writer, no locks.  
- Apply: single thread.  
- Checkpoint: read-only snapshot via COW.  
- Key-map shards: atomic or per-shard spinlock.  
- Metrics: atomic counters.

---

### 21.8 Failure & Watchdog
- Queue overflow → backpressure → client throttle.  
- Heartbeat counter per thread; >1 s stall triggers restart.  
- Restart isolates to partition.

---

### 21.9 Memory Model
- Partition-scoped arenas; freed only on teardown.  
- Temporary buffers from per-thread slab.  
- `madvise(DONTNEED)` after compaction.

---

### 21.10 Invariants
- One mutator thread per partition.  
- Bounded queues.  
- No cross-partition locks.  
- Checkpoint swaps atomic pointer exchange.  
- Thread failure isolated.

---

## 22  Security Model

### 22.1 Goals
- mTLS for all communication.  
- AES-GCM encryption at rest.  
- Signed definitions & checkpoints.  
- Full audit logging.

---

### 22.2 Trust & Certificates
- CP acts as CA; bootstrap tokens allow new nodes to enroll and obtain their node certificate plus envelope key.  
- Root → Cluster → Node hierarchy.  
- Node certs (30 d), clients (≤24 h).  
- Mutual TLS 1.3 everywhere (`TLS_AES_128_GCM_SHA256` or `TLS_AES_256_GCM_SHA384`).

Enrollment exchanges:
- Operator provisions a single-use bootstrap token bound to node identity and expiry.
- Node presents token to CP, receives signed node certificate and envelope encryption key material.
- CP records issuance in `SecurityMaterial` for audit.

---

### 22.3 Authorization
Roles: `ingest`, `observer`, `operator`, `admin`.  
ACLs stored in CP-Raft `SecurityMaterial`.  
RPCs rejected if role lacks permission.

---

### 22.4 Encryption at Rest
| Component | Algorithm | Scope |
|------------|------------|--------|
| WAL payload | AES-256-GCM | per segment |
| Checkpoint | AES-256-GCM | per file |
| Bundle | AES-256-GCM | full |
| Keys | Envelope KMS | per node |

Rotation schedule: WAL and checkpoint data-encryption keys rotate weekly; CP snapshot keys rotate quarterly. Dual-read mode keeps prior keys active until all artifacts referencing them age out, guaranteeing continuity during rotation.

Key retention:
- Maintain the current key and the immediately previous key per artifact class (WAL, checkpoint) to support dual-read.  
- Enforce a maximum of two active keys per class.

---

### 22.5 Integrity
- All bundles & checkpoints signed (SHA-256 + Ed25519).  
- Signatures verified before activation or load.

---

### 22.6 Audit
Immutable JSON-lines log, signed HMAC per record.  
Streamed to CP-Raft; 12-month retention.

---

### 22.7 Hardening
- Signed binaries, `setuid ceptra`.  
- `seccomp` filter minimal syscalls.  
- No core dumps, `CAP_NET_BIND_SERVICE` only.  
- `/etc/ceptra/secure` mode 0700 for secrets.

---

### 22.8 Metrics
`security_cert_expiry_seconds`, `security_tls_handshake_failures_total`,  
`security_audit_events_total`, `security_kms_latency_seconds`,  
`security_kms_unavailable_total`, `wal_kms_block_seconds`.

---

### 22.9 Invariants
- All RPCs authenticated.  
- All durable artifacts checksummed or signed.  
- Rotation never breaks readability.  
- Every change audited.

### 22.10 KMS Outage Policy
- Cached keys allow read access to existing WAL segments and checkpoints without contacting the external KMS.  
- New WAL segment sealing and checkpoint creation proceed for up to 60 s using cached data-encryption keys; after the grace window, creation blocks and emits alerts (`security_kms_unavailable_total`, `wal_kms_block_seconds`).  
- Once KMS connectivity returns, blocked writers resume and emit a recovery audit record.

Outage during rotation:
- If rotation begins and the KMS outage exceeds the 60 s grace window, sealing new segments blocks while reads continue with cached keys.  
- Writing to the currently open segment continues until sealing is required.

---

## 23  Configuration Parameters and Operational Controls

### 23.1 Principles
Declarative, dynamic, versioned, auditable.

### 23.2 Hierarchy
Cluster → Node → Partition → Definition → Test override.

### 23.3 Representation
YAML / JSON served at `/v1/config`; validated against protobuf schema.

### 23.4 Propagation
1. Operator PATCH → CP-Raft commit.  
2. CA detects `version++`.  
3. Local modules reload or bump epoch.

Safe after all partitions ack `APPLIED(V)`.

### 23.5 Rollback
`POST /v1/config/rollback?to=<V-1>` restores previous blob.

---

### 23.6 Hot vs Static
| Type | Reload |
|------|---------|
| `lateness_allowance_L`, `pane_size_q` | Hot |
| `durability_mode`, `fsync_cadence` | Reconfigure |
| `wal_path`, `numa_affinity` | Restart |

---

### 23.7 Environment
`CEPTRA_HOME`, `CEPTRA_CONFIG`, `CEPTRA_NODE_ID`,  
`CEPTRA_LOG_LEVEL`, `CEPTRA_TEST_MODE`.

---

### 23.8 Observability
`config_version_current`, `config_reload_duration_seconds`,  
`config_epoch_bumps_total`, `config_validation_failures_total`.

---

### 23.9 Safety
- Mixed versions within a partition forbidden.  
- Epoch++ for topology changes.  
- Checkpoints record `config_version`.  
- Test overrides ignored in prod.

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
| `durability_mode` | GROUP_FSYNC |
| `fsync_cadence_ms` | 5–10 |
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
Single binary exports:
- `/metrics` Prometheus endpoint  
- `/healthz` liveness  
- `/readyz` readiness with per-partition detail  
- `/trace` OTLP optional

All modules share one registry.

---

### 24.2 Metric Style
Prometheus exposition, 250 ms update interval, `ceptra_` prefix. All latency/time metrics use seconds (suffix `_seconds`). Millisecond-prefixed metrics are deprecated; when encountered, rename to seconds with the appropriate scale factor. Compatibility: legacy `_ms` metrics continue to emit for one release as deprecated aliases.

Key examples:
- `raft_replication_lag_seconds`
- `wal_fsync_latency_seconds`
- `ap_apply_latency_seconds`
- `cp_reconcile_duration_seconds`
- `security_cert_expiry_seconds`
- `queue_depth{type}`
- `definition_lane_overflow_total`
- `ap_key_hash_collision_total`
- `dedup_capacity_exhausted_total`
- `finalized_horizon_stall{part_id}`
- `wal_dirty_epoch_age_seconds`
- `wal_dirty_bytes_pending`
- `wal_durability_fence_active{part_id}`
- `definition_warmup_pending{part_id}`

---

### 24.3 Tracing
OpenTelemetry spans:
`AppendEvent`, `Raft/replicate`, `WAL/fsync`, `AP/apply`, `AP/checkpoint`.  
Sampling = 1 % default; controlled by CP.

---

### 24.4 Logging
JSON lines with `ts`, `level`, `module`, `part_id`, `log_index`.  
Rotation = 1 GiB × 10 files × 7 days.  
`/v1/loglevel` dynamic adjustment.

---

### 24.5 Alerts
| Condition | Metric | Threshold |
|------------|---------|-----------|
| Follower lag | `replication_lag_seconds` > 0.25 s p99 | Warn |
| Apply backlog | `apply_lag_seconds` > 0.20 s | Warn |
| Fsync slow | `wal_fsync_latency_seconds` > 0.010 s p99 | Alert |
| Late rate | `ap_late_events_total` > 1 % | Alert |
| Cert expiry | `< 3 days` | Alert |
| Durability fence | `wal_durability_fence_active{part_id}` > 0 for 30 s | Page storage on-call |
| Finalized horizon stall | `finalized_horizon_stall{part_id}` > 0 for 5 min | Page streaming on-call |

---

### 24.6 Probes and Per-Partition Readiness
`/healthz`: `200` when the process is alive.  
`/readyz`: structured JSON with overall readiness and per-partition readiness:
```
{
  "ready": true|false,
  "threshold": {"healthy_partition_ratio": 0.99},
  "reasons": ["cp_quorum", "insufficient_healthy_partitions", "..."],
  "partitions": {
    "<part_id>": {
      "ready": true|false,
      "reasons": ["warmup", "apply_lag", "raft_quorum", "cert_expiry", "checkpoint_age"],
      "metrics": {
        "apply_lag_seconds": 0.123,
        "replication_lag_seconds": 0.045
      }
      }
    }
}
```
A partition is ready if all predicates hold (with per-partition thresholds):
`raft_quorum_ok == true`.  
`apply_lag_seconds <= 0.20` (configurable).  
Warmup complete for the active `def_version` (shadow caught up).  
`cert_expiry_seconds > 3d`.  
`checkpoint_age <= 2 × checkpoint_full_interval`.

Overall pod readiness: `true` if ≥99% (configurable) of partitions are ready; otherwise `false`. Expose `ceptra_partition_ready{part_id}` and `ceptra_ready_partitions_ratio`.

Defaults:

| Parameter | Default |
|-----------|---------|
| `healthy_partition_ratio` | 0.99 |
| `apply_lag_ms_threshold` | 200 |
| `replication_lag_ms_threshold` | 250 |
| `checkpoint_age_threshold` | `2 × checkpoint_full_interval` |
| `cert_expiry_seconds_threshold` | 3 days |

---

### 24.7 Dashboards
Cluster overview, storage I/O, AP latency, lateness, checkpoint times, CP health, security expiry.

---

### 24.8 Testing
`TEST/ExportMetricsSnapshot` → JSON dump for deterministic comparison.

---

### 24.9 Invariants
- Metrics monotone or resettable.  
- Non-blocking emission.  
- Timestamps from injected clock (for replay).  
- `ceptra_` namespace.

---
## 25  Disaster Recovery and Cluster Recovery

### 25.1 Objectives
- **RPO:** 0 (Strict) / ≤ group-fsync cadence (Group-fsync).  
- **RTO:** single pod <1 min; multi-pod <10 min; regional DR <1 h.  
- Deterministic, auditable recovery from any failure.

---

### 25.2 Failure Classes
| Class | Example | Effect | Recovery |
|--------|----------|---------|----------|
| F1 | Node/pod loss | PRGs elect new leader; AP failover | Auto |
| F2 | PVC loss | Follower rebuilds | Raft snapshot |
| F3 | Two replicas down | Writes paused | Operator restore quorum |
| F4 | CP quorum loss | Mgmt API down; data ok | CP restore |
| F5 | WAL corruption | Partial write | Local repair |
| F6 | Definition/security state loss | Missing bundles | Rehydrate |
| F7 | Regional failure | Site outage | Promote DR cluster |

---

### 25.3 Cold Start
1. Binary starts; discovers peers via StatefulSet DNS.  
2. Ordinals <M form CP-Raft quorum.  
3. CP reconciles partition assignments.  
4. PRGs replay WAL, repair open segment, load checkpoints, replay tail.  
5. `/readyz` reports `ready=true` when ≥99% (configurable) of partitions meet quorum, apply lag, and auxiliary thresholds.

---

### 25.4 Node/Pod Loss
Automatic Raft election <300 ms, AP warm standby takes over.  
Traffic throttled during transition.

---

### 25.5 PVC Loss
Follower receives snapshot; rebuilds WAL.  
No data loss; quorum intact.

---

### 25.6 Two Replicas Down
Writes halt immediately and leaders reject appends with `PERMANENT_EPOCH`. Recovery sequence:
1. Operator replaces at least one failed replica; CP issues an epoch++ and drives the Raft joint-consensus change.
2. Once both joint configurations commit, quorum resumes and clients may retry.

Safety-first: never advance a minority. CP throttles to one membership change per node and surfaces progress via `cp_epoch_mismatch_total`.

---

### 25.7 CP Quorum Loss
Data path unaffected.  
Restore CP pods or snapshot; reconcile drift via partition state.

---

### 25.8 WAL Corruption
Auto repair on boot: scan back to valid block, truncate, rebuild index.  
Resume at last committed index.

---

### 25.9 Definition / Security State Loss
Reload bundles, schema descriptors, keys from blob store and verify signatures.  
Reissue certs via CP.

---

### 25.10 Regional DR
Primary and warm DR clusters run with asynchronous WAL and checkpoint shipping. Cutover uses an idempotent sequence:
1. **Fence primary:** CP issues `FreezePartitionGroup` (epoch fence), causing leaders to reject appends with `PERMANENT_EPOCH`. By default this step is operator-triggered; automation may invoke the same API.  
2. **Verify replication:** ensure WAL/checkpoint shipping has delivered entries through the latest committed index (RPO bounded by the durability mode).  
3. **Promote DR:** CP runs Raft joint consensus to convert DR observers into voters and increments the epoch.  
4. **Cut ingress:** update DNS, load balancers, and client routing to point at the DR cluster; resume ingestion once `/readyz` reports the ≥99% healthy-partition threshold and the target partitions are `ready=true`.  
5. **Reconcile primary:** when the primary site returns, reconcile epochs, replay missing WAL, and rejoin partitions as followers under the new epoch fence.

RPO ≤ shipping interval; operational RTO typically 30–60 min.

---

### 25.11 Backups
| Type | Frequency | Retention |
|-------|------------|------------|
| CP-Raft snapshot | 60 s | 24 h |
| Checkpoints | 30 s full | 24 h |
| WAL archival | after compaction | `R_compact` |

Nightly export to object store; quarterly restore drills.

---

### 25.12 Replacement and Rebalance
New pod joins; CP assigns partitions; snapshot catch-up.  
Rebalance by capacity weight; one partition at a time.

---

### 25.13 Catastrophic Recovery
Recreate cluster → restore CP snapshot → replay checkpoints/WALs → resume traffic.

---

### 25.14 Health Gates
Healthy partition: quorum, lag < thresholds, checkpoint age <2×interval, cert valid.  
Cluster ready if ≥99% healthy.

---

### 25.15 Test Hooks
- `InjectThreadDelay`, `DisableBackpressure`, `CorruptLastBlock`, `TimeShiftCertExpiry`.  
- Deterministic tests using manual clock.

---

### 25.16 Invariants
- DR-1: No appends without quorum.  
- DR-2: Never rewind committed indices.  
- DR-3: All restores verified by checksum/signature.  
- DR-4: Epochs unique and consistent.  
- DR-5: Operator runbooks deterministic.

---

## 26  Deployment and Bootstrap (Kubernetes)

### 26.1 Single Binary Model
All modules linked; each pod self-identifies role(s).

---

### 26.2 Auto-Arrangement
1. Pods discover peers via headless Service (`ceptra-0`, `ceptra-1`, ...).  
2. First M ordinals form CP-Raft voters.  
3. Others join as observers.  
4. CP assigns partitions using consistent hashing and capacity weights.  
5. PRGs and APs start automatically; CA module reports health.

---

### 26.3 Configuration
Environment:
```
CLUSTER_NAME=prod
STATEFULSET_SIZE=9
CP_VOTERS=3
SERVICE_DNS=ceptra
```
Optional flags: `--role=auto|force-cp|no-cp`, `--pki=/etc/ceptra/pki`.

---

### 26.4 Kubernetes Resources
**StatefulSet**
- Headless Service for DNS.  
- PersistentVolumeClaim for `/var/lib/ceptra`.  
- Anti-affinity across nodes.  
- PodDisruptionBudget (minAvailable ≥ quorum).  
- Liveness `/healthz`, readiness `/readyz` (per-partition readiness, pod ready when ≥99% of partitions pass checks).  
- `preStop`: drain appends, fsync, checkpoint.

**Rolling Update**
- `maxUnavailable=1`.  
- Leader transfer before termination.

---

### 26.5 Resources
- NVMe PVC (ext4/xfs `noatime`).  
- CPU pinning via requests/limits.  
- Memory: per partition 1–2 GB.

SLO Prerequisites (Kubernetes):
- Guaranteed QoS: set CPU requests equal to limits and use topology manager policy `restricted` or `single-numa-node`.  
- Enable the Kubernetes CPU Manager static policy and pin critical threads via the runtime.  
- Align NVMe IRQs to the same NUMA node; document node labels/affinity so scheduling preserves placement.

---

### 26.6 Security Integration
- Certs via Kubernetes Secret or CSI.  
- mTLS internal; no sidecars required.  
- Optional network policy for port 8080 (metrics).

---

### 26.7 Observability
Prometheus ServiceMonitor on `/metrics`.  
Grafana dashboards bundled.  
Logs collected via FluentBit or OTEL sidecar.

---

### 26.8 Bootstrap Sequence
1. Pod starts → load config.  
2. Discover peers.  
3. Join CP-Raft or observer mode.  
4. Wait for CP quorum.  
5. Fetch assignment → start PRGs/APs.  
6. Report ready.

---

### 26.9 Scaling
Add pod → auto-discovers, joins cluster → CP rebalances partitions → epoch++ for affected.

---

### 26.10 Testing / Dev Mode
`--role=standalone` → single-node all-in-one (no quorum).  
`CEPTRA_TEST_MODE=true` enables all test RPCs and manual clock.

### 26.11 Kubernetes Readiness Integration
Readiness probe targets `/readyz`. Use a custom `successThreshold` and `failureThreshold` consistent with the per-partition policy. Optionally add a `PodReadinessGate` backed by a `Condition` updated from `ceptra_ready_partitions_ratio`. For large partition counts, set `terminationGracePeriodSeconds` to allow shadow warmup on graceful rollouts and enable leader transfer `preStop` hook. Expose per-partition readiness metrics for HPA or custom controllers.

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

# End of Specification

---

Example bundles below assume the workload partitions on any naturally unbounded labels (for example `facility_id` or `device_id`) so that the residual per-partition domain stays within `LANE_MAX_PER_PARTITION = 64`. Declared `lane_domains` reflect that sharding.

bundle:
  name: gaming_liveops_cheat_detection
  lane_domains:
    player_id: { max_per_partition: 32 }
  def_version: 1
workflow:
  name: gaming_liveops_cheat_detection
  phases:
    - name: ingest_game_events
      type: source.kafka
      options:
        connector: game_telemetry_kafka
        decode: schema_registry
        enrich_headers: true

    - name: aggregate_fast_signals
      type: aggregate.promql
      options:
        window: 15s
        queries:
          anti_cheat_flags:
            expression: sum by (player_id)(
              increase(anti_cheat_flags_total{queue="ranked"}[15s])
            )
          headshot_ratio_short:
            expression: avg by (player_id)(
              avg_over_time(player_headshot_ratio{queue="ranked"}[15s])
            )
          movement_anomaly_score:
            expression: avg by (player_id)(
              avg_over_time(player_movement_deviation_score{mode="live"}[15s])
            )
          matchmaking_latency_p95:
            expression: quantile_over_time(
              0.95,
              matchmaking_latency_seconds{queue="ranked"}[15s]
            )

    - name: aggregate_behavior_baseline
      type: aggregate.promql
      options:
        window: 30m
        queries:
          anti_cheat_flag_baseline:
            expression: avg by (player_id)(
              avg_over_time(anti_cheat_flags_total{queue="ranked"}[30m])
            )
          headshot_ratio_baseline:
            expression: avg by (player_id)(
              avg_over_time(player_headshot_ratio{queue="ranked"}[30m])
            )
          movement_anomaly_baseline:
            expression: avg by (player_id)(
              avg_over_time(player_movement_deviation_score{mode="live"}[30m])
            )

    - name: evaluate_player_risk
      type: classify.cel
      options:
        bindings:
          fast: phase.aggregate_fast_signals.metrics
          baseline: phase.aggregate_behavior_baseline.metrics
        rules:
          - name: potential_aimbot_user
            when: |
              fast["headshot_ratio_short"].labels["player_id"] == baseline["headshot_ratio_baseline"].labels["player_id"] &&
              fast["headshot_ratio_short"].value >= baseline["headshot_ratio_baseline"].value + 0.35 &&
              fast["anti_cheat_flags"].labels["player_id"] == fast["headshot_ratio_short"].labels["player_id"] &&
              fast["anti_cheat_flags"].value >= 2
            emit:
              channel: kafkas://liveops.cheat_alerts
              schema_key: suspected_aimbot_v1
              payload:
                player_id: fast["headshot_ratio_short"].labels["player_id"]
                headshot_ratio: fast["headshot_ratio_short"].value
                baseline_headshot_ratio: baseline["headshot_ratio_baseline"].value
                anti_cheat_flags: fast["anti_cheat_flags"].value
                session_id: payload.session_id

          - name: speed_hack_candidate
            when: |
              fast["movement_anomaly_score"].labels["player_id"] == baseline["movement_anomaly_baseline"].labels["player_id"] &&
              fast["anti_cheat_flags"].labels["player_id"] == fast["movement_anomaly_score"].labels["player_id"] &&
              fast["movement_anomaly_score"].value > 1.5 * max(1, baseline["movement_anomaly_baseline"].value) &&
              fast["anti_cheat_flags"].value >= 1 &&
              fast["matchmaking_latency_p95"].value < 0.04
            emit:
              channel: grpcs://anti-cheat/QueueInvestigation
              metadata:
                severity: "CRITICAL"
              payload:
                player_id: fast["movement_anomaly_score"].labels["player_id"]
                anomaly_score: fast["movement_anomaly_score"].value
                baseline_score: baseline["movement_anomaly_baseline"].value
                anti_cheat_flags: fast["anti_cheat_flags"].value
                latency_p95_seconds: fast["matchmaking_latency_p95"].value

    - name: retain_state
      type: sink.checkpoint
      options:
        full_interval: 60s
        incremental_interval: 20s
        include:
          - aggregate_fast_signals
          - aggregate_behavior_baseline

---

bundle:
  name: soc_multi_signal
  lane_domains:
    entity_id: { max_per_partition: 48 }
    host_id:   { max_per_partition: 32 }
  def_version: 1
workflow:
  name: soc_multi_signal
  phases:
    - name: ingest_security_events
      type: source.kafka
      options:
        connector: security_events_kafka
        decode: schema_registry
        lane_bitmap_from: payload.alert_lanes

    - name: aggregate_short_window
      type: aggregate.promql
      options:
        window: 60s
        queries:
          correlated_auth_failures:
            expression: sum by (entity_id)(
              increase(auth_failure_events_total{outcome="DENIED"}[60s])
            )
          malware_alert_burst:
            expression: sum by (host_id)(
              increase(malware_detected_total{severity=~"HIGH|CRITICAL"}[60s])
            )
          lateral_movement_signals:
            expression: count by (host_id)(
              count_over_time(lateral_movement_flags{stage="suspected"}[60s])
            )

    - name: aggregate_baseline_window
      type: aggregate.promql
      options:
        window: 1h
        queries:
          auth_failure_baseline:
            expression: avg by (entity_id)(
              avg_over_time(auth_failure_events_total{outcome="DENIED"}[1h])
            )
          malware_alert_baseline:
            expression: avg by (host_id)(
              avg_over_time(malware_detected_total{severity=~"HIGH|CRITICAL"}[1h])
            )

    - name: evaluate_incidents
      type: classify.cel
      options:
        bindings:
          short: phase.aggregate_short_window.metrics
          baseline: phase.aggregate_baseline_window.metrics
        rules:
          - name: credential_stuffing_cluster
            when: |
              short["correlated_auth_failures"].labels["entity_id"] == baseline["auth_failure_baseline"].labels["entity_id"] &&
              short["correlated_auth_failures"].value >= 100 &&
              short["correlated_auth_failures"].value > 3 * max(1, baseline["auth_failure_baseline"].value)
            emit:
              channel: grpcs://soc.escalations/CreateIncident
              metadata:
                severity: "HIGH"
                playbook: "PB-CREDENTIAL-STUFFING"
              payload:
                entity_id: short["correlated_auth_failures"].labels["entity_id"]
                burst_failures: short["correlated_auth_failures"].value
                baseline_failures: baseline["auth_failure_baseline"].value
                window_started_ns: window_start_ns
                window_ended_ns: window_end_ns

          - name: host_compromise_candidate
            when: |
              short["malware_alert_burst"].labels["host_id"] == baseline["malware_alert_baseline"].labels["host_id"] &&
              short["lateral_movement_signals"].labels["host_id"] == short["malware_alert_burst"].labels["host_id"] &&
              short["malware_alert_burst"].value >= 10 &&
              short["lateral_movement_signals"].value >= 3
            emit:
              channel: kafkas://soc.alerts
              schema_key: host_compromise_v1
              payload:
                host_id: short["malware_alert_burst"].labels["host_id"]
                high_severity_alerts: short["malware_alert_burst"].value
                baseline_alert_rate: baseline["malware_alert_baseline"].value
                lateral_indicators: short["lateral_movement_signals"].value
                correlation_id: payload.envelope_id

    - name: publish_state
      type: sink.checkpoint
      options:
        full_interval: 2m
        incremental_interval: 40s
        include:
          - aggregate_short_window
          - aggregate_baseline_window

---

bundle:
  name: ecommerce_supply_chain
  lane_domains:
    facility_id: { max_per_partition: 48 }
    carrier_id:  { max_per_partition: 24 }
  def_version: 1
workflow:
  name: ecommerce_supply_chain
  phases:
    - name: ingest_logistics_events
      type: source.kafka
      options:
        connector: logistics_events_kafka
        decode: schema_registry
        enrich_headers: true

    - name: aggregate_short_term_flow
      type: aggregate.promql
      options:
        window: 10m
        queries:
          outbound_orders_rate:
            expression: rate(
              warehouse_orders_processed_total[10m]
            )
          outbound_orders_rate_by_facility:
            expression: sum by (facility_id)(
              rate(warehouse_orders_processed_total[10m])
            )
          delayed_departures:
            expression: sum by (facility_id)(
              increase(shipment_delay_events_total{reason="MISSED_DEPARTURE"}[10m])
            )
          carrier_exception_spike:
            expression: sum by (carrier_id)(
              increase(carrier_exception_events_total{severity=~"MAJOR|CRITICAL"}[10m])
            )

    - name: aggregate_operational_baseline
      type: aggregate.promql
      options:
        window: 6h
        queries:
          outbound_orders_baseline:
            expression: avg by (facility_id)(
              avg_over_time(warehouse_orders_processed_total[6h])
            )
          carrier_exception_baseline:
            expression: avg by (carrier_id)(
              avg_over_time(carrier_exception_events_total{severity=~"MAJOR|CRITICAL"}[6h])
            )

    - name: evaluate_supply_risks
      type: classify.cel
      options:
        bindings:
          short: phase.aggregate_short_term_flow.metrics
          baseline: phase.aggregate_operational_baseline.metrics
        rules:
          - name: fulfillment_sla_slip
            when: |
              short["outbound_orders_rate_by_facility"].labels["facility_id"] == baseline["outbound_orders_baseline"].labels["facility_id"] &&
              short["outbound_orders_rate_by_facility"].value < 0.8 * max(1, baseline["outbound_orders_baseline"].value) &&
              payload.facility_tags["priority"] == "HIGH"
            emit:
              channel: kafkas://ops.sla_alerts
              schema_key: fulfillment_sla_slip_v1
              payload:
                facility_id: short["outbound_orders_rate_by_facility"].labels["facility_id"]
                current_rate: short["outbound_orders_rate_by_facility"].value
                baseline_rate: baseline["outbound_orders_baseline"].value
                window_started_ns: window_start_ns

          - name: carrier_disruption
            when: |
              short["carrier_exception_spike"].labels["carrier_id"] == baseline["carrier_exception_baseline"].labels["carrier_id"] &&
              short["carrier_exception_spike"].value >= 5 &&
              short["carrier_exception_spike"].value > 2 * max(1, baseline["carrier_exception_baseline"].value)
            emit:
              channel: grpcs://supply-chain/TriggerRemediation
              metadata:
                severity: "MAJOR"
              payload:
                carrier_id: short["carrier_exception_spike"].labels["carrier_id"]
                exception_count: short["carrier_exception_spike"].value
                baseline_count: baseline["carrier_exception_baseline"].value
                impacted_orders: payload.metrics["orders_in_transit"]

    - name: archive_state
      type: sink.checkpoint
      options:
        full_interval: 15m
        incremental_interval: 5m
        include:
          - aggregate_short_term_flow
          - aggregate_operational_baseline

---

bundle:
  name: telco_qos_guardrails
  lane_domains:
    cell_id: { max_per_partition: 48 }
  def_version: 1
workflow:
  name: telco_qos_guardrails
  phases:
    - name: ingest_radio_events
      type: source.kafka
      options:
        connector: ran_metrics_kafka
        decode: schema_registry
        enrich_headers: true

    - name: aggregate_cell_health_fast
      type: aggregate.promql
      options:
        window: 30s
        queries:
          cell_latency_p95:
            expression: quantile_over_time(
              0.95,
              cell_round_trip_seconds{link_state="UP"}[30s]
            )
          cell_jitter_avg:
            expression: avg by (cell_id)(
              avg_over_time(cell_jitter_seconds[30s])
            )
          packet_loss_short:
            expression: avg by (cell_id)(
              avg_over_time(packet_loss_ratio{direction="downlink"}[30s])
            )

    - name: aggregate_cell_health_baseline
      type: aggregate.promql
      options:
        window: 2h
        queries:
          cell_latency_baseline:
            expression: avg by (cell_id)(
              avg_over_time(cell_round_trip_seconds{link_state="UP"}[2h])
            )
          packet_loss_baseline:
            expression: avg by (cell_id)(
              avg_over_time(packet_loss_ratio{direction="downlink"}[2h])
            )

    - name: evaluate_qos_degradations
      type: classify.cel
      options:
        bindings:
          fast: phase.aggregate_cell_health_fast.metrics
          baseline: phase.aggregate_cell_health_baseline.metrics
        rules:
          - name: latency_regression
            when: |
              fast["cell_latency_p95"].labels["cell_id"] == baseline["cell_latency_baseline"].labels["cell_id"] &&
              fast["cell_latency_p95"].value > baseline["cell_latency_baseline"].value + 0.02 &&
              payload.cell_metadata["tier"] == "urban"
            emit:
              channel: kafkas://noc.latency_incidents
              schema_key: cell_latency_regression_v1
              payload:
                cell_id: fast["cell_latency_p95"].labels["cell_id"]
                latency_p95_seconds: fast["cell_latency_p95"].value
                baseline_seconds: baseline["cell_latency_baseline"].value
                sector: payload.cell_metadata["sector"]
                observed_at_ns: watermark_ns

          - name: packet_loss_spike
            when: |
              fast["packet_loss_short"].labels["cell_id"] == baseline["packet_loss_baseline"].labels["cell_id"] &&
              fast["packet_loss_short"].value >= 2 * max(0.01, baseline["packet_loss_baseline"].value) &&
              fast["cell_jitter_avg"].labels["cell_id"] == fast["packet_loss_short"].labels["cell_id"] &&
              fast["cell_jitter_avg"].value > 0.005
            emit:
              channel: grpcs://noc/TriggerMitigation
              metadata:
                severity: "CRITICAL"
              payload:
                cell_id: fast["packet_loss_short"].labels["cell_id"]
                packet_loss_ratio: fast["packet_loss_short"].value
                jitter_seconds: fast["cell_jitter_avg"].value
                baseline_loss: baseline["packet_loss_baseline"].value

    - name: persist_state
      type: sink.checkpoint
      options:
        full_interval: 5m
        incremental_interval: 1m
        include:
          - aggregate_cell_health_fast
          - aggregate_cell_health_baseline

---

bundle:
  name: iot_fleet_monitoring
  lane_domains:
    device_id: { max_per_partition: 64 }
    gateway_id:{ max_per_partition: 32 }
    zone:      { max_per_partition: 16 }
  def_version: 1
workflow:
  name: iot_fleet_monitoring
  phases:
    - name: ingest_iot_events
      type: source.kafka
      options:
        connector: sensors_kafka
        decode: schema_registry
        enrich_headers: true

    - name: aggregate_fast_metrics
      type: aggregate.promql
      options:
        window: 30s
        queries:
          device_temp_peak:
            expression: max by (device_id)(
              max_over_time(device_temperature_c{state="ONLINE"}[30s])
            )
          gateway_latency_p95:
            expression: quantile_over_time(
              0.95,
              network_latency_seconds{link_state="UP"}[30s]
            )
          auth_fail_short:
            expression: sum by (gateway_id)(
              increase(device_auth_failures_total{result="DENIED"}[30s])
            )

    - name: aggregate_baseline_metrics
      type: aggregate.promql
      options:
        window: 15m
        queries:
          device_temp_baseline:
            expression: avg by (zone)(
              avg_over_time(device_temperature_c{state="ONLINE"}[15m])
            )
          auth_fail_baseline:
            expression: avg by (gateway_id)(
              avg_over_time(device_auth_failures_total{result="DENIED"}[15m])
            )

    - name: evaluate_policies
      type: classify.cel
      options:
        bindings:
          fast: phase.aggregate_fast_metrics.metrics
          baseline: phase.aggregate_baseline_metrics.metrics
        rules:
          - name: overheating_device
            when: |
              baseline["device_temp_baseline"].labels["zone"] == payload.location.zone &&
              fast["device_temp_peak"].value > baseline["device_temp_baseline"].value + 5 &&
              payload.device_tags["safetyCritical"] == true
            emit:
              channel: kafkas://iot.alerts
              schema_key: device_overheat_v1
              payload:
                device_id: payload.device_id
                zone: payload.location.zone
                temp_peak_c: fast["device_temp_peak"].value
                temp_baseline_c: baseline["device_temp_baseline"].value
                observed_at_ns: watermark_ns

          - name: authentication_threat
            when: |
              fast["auth_fail_short"].labels["gateway_id"] == baseline["auth_fail_baseline"].labels["gateway_id"] &&
              fast["auth_fail_short"].value >= 20 &&
              fast["auth_fail_short"].value > 1.5 * baseline["auth_fail_baseline"].value
            emit:
              channel: grpcs://security-ops/NotifyThreat
              metadata:
                severity: "CRITICAL"
              payload:
                gateway_id: fast["auth_fail_short"].labels["gateway_id"]
                short_window_failures: fast["auth_fail_short"].value
                baseline_failures: baseline["auth_fail_baseline"].value
                threat_type: "UNUSUAL_AUTH_FAILURE_RATE"

    - name: publish_state
      type: sink.checkpoint
      options:
        full_interval: 30s
        incremental_interval: 10s
        include:
          - aggregate_fast_metrics
          - aggregate_baseline_metrics

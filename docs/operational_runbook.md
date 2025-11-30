# CEPtra on Clustor: operational runbook

## Prerequisites
- Keep the sibling `../clustor` checkout present; the build guard hashes `manifests/ceptra_manifest.json` against `../clustor/docs/specification.md` and fails if they drift. Run `make spec-sync-write` after bumping either spec.
- Build with the default `clustor-net` feature. The runtime always requires a control-plane URL and mTLS materials; the stub path has been removed.
- Provision TLS materials via `CEPTRA_TLS_IDENTITY_CERT`, `CEPTRA_TLS_IDENTITY_KEY`, `CEPTRA_TLS_TRUST_BUNDLE`, and the `CEPTRA_NODE_SPIFFE` identity. The runtime rejects missing or unreadable files.

## Non-stub boot checklist
1. Point `CEPTRA_CONTROL_PLANE_URL` at the control-plane admin surface and keep `CEPTRA_CP_ROUTING_PATH`/`CEPTRA_CP_FEATURE_PATH` aligned with the upstream defaults (`/routing`, `/features`).
1. Keep `CEPTRA_CP_WARMUP_PATH`/`CEPTRA_CP_ACTIVATION_PATH` aligned with the control-plane endpoints that publish warmup readiness and activation barriers (defaults `/warmup`, `/activation`).
2. Bind admin/Raft listeners via `CEPTRA_ADMIN_BIND`/`CEPTRA_RAFT_BIND`, set `CEPTRA_ADVERTISE_HOST` to the routable hostname in certificates, and ensure the ports are reachable by peers.
3. Provide mTLS materials (`CEPTRA_TLS_IDENTITY_CERT`, `CEPTRA_TLS_IDENTITY_KEY`, `CEPTRA_TLS_TRUST_BUNDLE`) that cover both admin and Raft surfaces; mismatched SPIFFE IDs or SANs will fail fast at boot.
4. Optional: tune outbound Raft RPCs with `CEPTRA_RAFT_CLIENT_TIMEOUT_MS`, `CEPTRA_RAFT_CLIENT_RETRY_ATTEMPTS`, and `CEPTRA_RAFT_CLIENT_BACKOFF_MS` if networks are lossy.
5. Wait for placement/feature feeds to refresh; `/readyz` will fail if the placement feed is stale or no Raft leader is known.

## Ports and addresses
- Admin HTTP: `CEPTRA_ADMIN_BIND` (default `127.0.0.1:26020`)
- Raft transport: `CEPTRA_RAFT_BIND` (default `127.0.0.1:26021`)
- Advertised host: `CEPTRA_ADVERTISE_HOST` (defaults to the bind host)
- Control plane: `CEPTRA_CONTROL_PLANE_URL` with routing path `CEPTRA_CP_ROUTING_PATH`, feature path `CEPTRA_CP_FEATURE_PATH`, warmup path `CEPTRA_CP_WARMUP_PATH`, and activation path `CEPTRA_CP_ACTIVATION_PATH` (defaults `/routing`, `/features`, `/warmup`, `/activation`)

## Storage layout
- Root: `CEPTRA_DATA_DIR` (default `target/ceptra-clustor`)
- WAL: `CEPTRA_WAL_DIR` (defaults to `${CEPTRA_DATA_DIR}/wal`)
- Snapshots: `CEPTRA_SNAPSHOT_DIR` (defaults to `${CEPTRA_DATA_DIR}/snapshots`)
- Startup creates the directories and writes a `.write_probe`; failures are fatal so operators catch permission issues before traffic arrives.

## Health and telemetry
- `/healthz` returns liveness, `/readyz` wraps the Clustor readiness snapshot with CEP overlays, `/metrics` exports Prometheus gauges with a guard that rejects `_seconds` outputs (must publish `_ms` or read-only aliases).
- Feed freshness metrics (`clustor_feed_staleness_ms`, `clustor_feed_latency_ms`) warn in logs when staleness exceeds 30s. System log surfaces are mirrored to `clustor_system_logs_total` and stderr for audit trails.
- Readiness now gates on Raft state: `ceptra_partition_committed_index`, `ceptra_partition_match_index`, and leader presence feed `/readyz`; a stale placement feed (`clustor_feed_staleness_ms{feed="placement"}`) or unknown leader will flip readiness to false.
- Use the readiness ratios and `clustor_feed_*` metrics to gate deployments; refer to `docs/disaster_recovery.md` for DR sequencing and to the Clustor spec for quorum and WAL repair flows.

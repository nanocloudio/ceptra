# CEPtra Alert Policies

The CEPtra control plane shares Grafana dashboards and Alertmanager rules with
the Clustor fleet. The generated artifacts live in this repository so the
dashboards and alert bindings evolve in lock step with the code they observe.

- Grafana dashboard definition: [`grafana_dashboard.json`](grafana_dashboard.json)  
- Alertmanager rules: [`alertmanager_rules.yaml`](alertmanager_rules.yaml)

| Alert | Metric | Threshold | Severity | Playbook |
|-------|--------|-----------|----------|----------|
| Apply lag high | `ceptra_apply_lag_ms` | `> 200 ms` for 2 minutes | Warning | Inspect `/readyz` for the lagging partitions, confirm Raft health, and throttle ingest if the PID controller is saturated. |
| Replication lag high | `ceptra_replication_lag_ms` | `> 250 ms` for 2 minutes | Warning | Check follower health, look for disk or network saturation, and consider moving hot partitions. |
| Late events surge | `ceptra_ap_event_lateness_ms` | `> 5 s` p95 for 5 minutes | Critical | Verify upstream data sources, enforce stricter lateness policies, or isolate workloads creating skew. |
| WAL fsync slow | `ceptra_wal_fsync_latency_ms` | `> 10 ms` p99 for 5 minutes | Critical | Escalate to storage, confirm fsync mode, and drain pods if durable disks regress. |
| Certificate expiry | `ceptra_security_cert_expiry_ms` | `< 259_200_000 ms` (3 days) | Warning | Rotate node certificates, confirm mTLS health, and audit key epochs. |
| Durability fence active | `wal_durability_fence_active{part_id}` | `> 0` for 30 seconds | Critical | Follow the durability fence runbook: inspect WAL reasons and reconcile storage pressure. |
| Finalized horizon stall | `ceptra_finalized_horizon_stall{part_id}` | `> 0` for 5 minutes | Critical | Check `/readyz` reasons, reduce lateness allowances, or disable problematic bundles. |
| Readiness ratio low | `ceptra_ready_partitions_ratio`, `ceptra_partition_ready{part_id}` | `< 0.99` for 5 minutes | Warning | Drain pods that fail to warm, verify checkpoint freshness, and confirm that warmup probes have completed. |

The dashboards surface the same metrics with curated panels so operators can
spot regressions before alerts fire. Each metric definition inside
`telemetry/catalog.json` references the relevant dashboard panel and alert
playbook so docs and automation stay synchronized.

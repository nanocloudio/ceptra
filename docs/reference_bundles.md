# CEPtra Reference Bundles

Operators frequently start from the sample bundles in `examples/`. This note lifts the
structure of those files out of the YAML so the relevant PromQL windows, lane domains,
and CEL idioms (for example `has_value` guards) are easy to skim before wiring real
data sources.

## Gaming live ops cheat detection

- **Bundle:** `examples/gaming_cheat_detection.yaml`  
- **Lane domains:** `player_id` is capped at 32 per partition so multi-tenant shards can
  satisfy the `LANE_MAX_PER_PARTITION` limit. The bundle explicitly partitions on the
  same identifier so the residual domain never exceeds 32.
- **PromQL queries:** two windows capture live behaviour—`aggregate_fast_signals`
  computes P95 matchmaking latency and anti-cheat counters over `15s`; a companion
  `aggregate_behavior_baseline` window recalculates averages over `30m`. Each query
  runs `sum by (player_id)` to preserve the lane dimension.
- **CEL guards:** the `speed_hack_candidate` rules demonstrate the preferred guard
  pattern. Latency spans only emit when the input carries a value:

```cel
fast["movement_anomaly_score"].value > 1.5 * max(1, baseline["movement_anomaly_baseline"].value) &&
fast["matchmaking_latency_p95"].has_value &&
fast["matchmaking_latency_p95"].value < 0.04
```

The guard ensures partially warm nodes (that have not yet produced
`matchmaking_latency_p95`) do not block activation and mirrors the requirements in §13.4.

## Supply-chain SLA monitoring

- **Bundle:** `examples/supply_chain_workflow.yaml`
- **Lane domains:** bounded on `facility_id ≤ 48` and `carrier_id ≤ 24`. The operators
  partition on either label before sending traffic to CEP so each partition inherits the
  same bounds.
- **PromQL queries:** `aggregate_short_term_flow` runs a `10m` window to track outbound
  throughput and carrier exceptions. `aggregate_operational_baseline` produces a `6h`
  reference. Both windows explicitly join on the same labels so lane projections stay
  predictable.
- **CEL guards & `has_value`:** the `fulfillment_sla_slip` rule compares short-term and
  baseline rates and also restricts the payload to high-priority facilities:

```cel
short["outbound_orders_rate_by_facility"].labels["facility_id"] ==
baseline["outbound_orders_baseline"].labels["facility_id"] &&
short["outbound_orders_rate_by_facility"].value <
0.8 * max(1, baseline["outbound_orders_baseline"].value) &&
payload.facility_tags["priority"] == "HIGH"
```

Downstream rules can add `has_value` guards whenever baselines may be sparse (for example,
seasonal carriers) without duplicating the entire guard.

## Bundle deployer workflow

`tools/bundle_deployer` wraps the lane-domain validation pipeline, a Clustor activation
barrier evaluation, and a `rule_version` canary demo into a single CLI so operators can
exercise a bundle before shipping. Example usage matching the gaming sample:

```
cargo run --manifest-path tools/bundle_deployer/Cargo.toml -- \
  --bundle examples/gaming_cheat_detection.yaml \
  --activation examples/gaming_activation_ready.json \
  --stage-def-version 2 \
  --stage-at-log 1200 \
  --canary-log-index 1100
```

The command emits:

1. Lane-validation totals (projection counts, overflow warnings, histogram) so the
   submitted bundle proves it respects the shared lane cap.
2. The activation-barrier verdict from `examples/gaming_activation_ready.json`. Missing
   partitions and the readiness digest are surfaced if the Clustor barrier remains
   pending.
3. A canary snapshot that shows the computed `rule_version` for the provided
   `--canary-log-index`. When `--stage-def-version`/`--stage-at-log` are supplied the
   output identifies the pending barrier so canary clients can pin the `rule_version`
   until the activation entry applies.

The activation JSON mirrors the Clustor readiness telemetry schema, so the same file can
be generated from `/readyz` and replayed locally to case activation issues.

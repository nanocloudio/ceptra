use ceptra::{
    AggregationConfig, AggregationError, AggregationPipeline, AggregatorKind, CommittedEvent,
    EventMeasurement, LateDataPolicy, MetricSpec, PaneAggregationPipeline, PaneRingBudget,
    WalEntry, NON_RETRACTABLE_FLAG,
};
use std::collections::BTreeMap;

fn event_with_value(log_index: u64, tick_ms: u64, value: f64, lane_bitmap: u64) -> CommittedEvent {
    let wal = WalEntry::new(log_index, tick_ms, format!("payload-{log_index}"));
    let measurement = EventMeasurement::new("metric.sum")
        .with_value(value)
        .with_lane_bitmap(lane_bitmap)
        .with_labels(default_labels());
    CommittedEvent::new(
        "partition-a".to_string(),
        format!("evt-{log_index}"),
        "key".to_string(),
        wal,
    )
    .with_measurements(vec![measurement])
}

fn event_with_item(log_index: u64, tick_ms: u64, metric: &str, item: &str) -> CommittedEvent {
    let wal = WalEntry::new(log_index, tick_ms, format!("payload-{log_index}"));
    let measurement = EventMeasurement::new(metric)
        .with_item(item.to_string())
        .with_lane_bitmap(0)
        .with_labels(default_labels());
    CommittedEvent::new(
        "partition-a".to_string(),
        format!("evt-{log_index}"),
        "key".to_string(),
        wal,
    )
    .with_measurements(vec![measurement])
}

fn default_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("lane".to_string(), "default".to_string())])
}

fn sum_config() -> AggregationConfig {
    AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 500,
        max_window_horizon_ms: 400,
        metrics: vec![MetricSpec {
            name: "metric.sum".to_string(),
            kind: AggregatorKind::Sum,
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    }
}

#[test]
fn pane_ring_respects_window_and_lane_masks() {
    let mut pipeline =
        PaneAggregationPipeline::new(sum_config()).expect("sum config should be valid");
    let record1 = pipeline.ingest(&event_with_value(1, 100, 5.0, 0));
    assert_eq!(record1.lane_bitmap, 1);
    assert_eq!(record1.bindings.len(), 1);
    assert!(record1.bindings[0].has_value);
    assert_eq!(record1.bindings[0].value, 5.0);

    // Same window accumulates the sum.
    let record2 = pipeline.ingest(&event_with_value(2, 150, 3.0, 0));
    assert_eq!(record2.lane_bitmap, 1);
    assert_eq!(record2.bindings.len(), 1);
    assert_eq!(record2.bindings[0].value, 8.0);

    // Jump far enough ahead so previous panes age out of the 200 ms window.
    let record3 = pipeline.ingest(&event_with_value(3, 450, 7.0, 0));
    assert_eq!(record3.bindings[0].value, 7.0);
}

#[test]
fn snapshot_restores_distinct_state() {
    let config = AggregationConfig {
        pane_width_ms: 50,
        raw_retention_ms: 400,
        max_window_horizon_ms: 400,
        metrics: vec![MetricSpec {
            name: "metric.distinct".to_string(),
            kind: AggregatorKind::Distinct,
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    let mut pipeline =
        PaneAggregationPipeline::new(config.clone()).expect("distinct config should be valid");
    pipeline.ingest(&event_with_item(1, 50, "metric.distinct", "user-a"));
    pipeline.ingest(&event_with_item(2, 75, "metric.distinct", "user-b"));
    let snapshot = pipeline.snapshot();

    let mut restored =
        PaneAggregationPipeline::new(config).expect("restored config should be valid");
    restored.restore(&snapshot);
    let record = restored.ingest(&event_with_item(3, 125, "metric.distinct", "user-c"));
    assert_eq!(record.bindings.len(), 1);
    assert_eq!(record.bindings[0].value, 3.0);
}

#[test]
fn rejects_non_retractable_retract_policy() {
    let config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 500,
        max_window_horizon_ms: 500,
        metrics: vec![MetricSpec {
            name: "metric.topk".to_string(),
            kind: AggregatorKind::TopK { k: 5 },
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Retract,
        }],
    };
    match PaneAggregationPipeline::new(config) {
        Ok(_) => panic!("expected non-retractable policy error"),
        Err(AggregationError::NonRetractablePolicy { metric }) => {
            assert_eq!(metric, "metric.topk");
        }
        Err(other) => panic!("unexpected error: {other}"),
    }
}

#[test]
fn quantile_absent_sets_flag() {
    let config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 500,
        max_window_horizon_ms: 500,
        metrics: vec![MetricSpec {
            name: "metric.quantile".to_string(),
            kind: AggregatorKind::QuantileOverTime { quantile: 0.5 },
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    let mut pipeline =
        PaneAggregationPipeline::new(config).expect("quantile config should be valid");
    let wal = WalEntry::new(10, 100, "payload");
    let measurement = EventMeasurement::new("metric.quantile")
        .with_lane_bitmap(0)
        .with_labels(default_labels());
    let event = CommittedEvent::new(
        "partition-a".to_string(),
        "evt-q".to_string(),
        "key".to_string(),
        wal,
    )
    .with_measurements(vec![measurement]);
    let record = pipeline.ingest(&event);
    assert_eq!(record.bindings.len(), 1);
    let binding = &record.bindings[0];
    assert!(!binding.has_value);
    assert!(binding.flags.contains(NON_RETRACTABLE_FLAG));
}

#[test]
fn cross_tier_boundary_is_left_closed_right_open() {
    let config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 200,
        max_window_horizon_ms: 200,
        metrics: vec![MetricSpec {
            name: "metric.sum".to_string(),
            kind: AggregatorKind::Sum,
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    let mut pipeline =
        PaneAggregationPipeline::new(config).expect("compaction config should be valid");
    let series = [
        (1_u64, 100_u64, 1.0_f64),
        (2_u64, 200_u64, 2.0_f64),
        (3_u64, 300_u64, 3.0_f64),
        (4_u64, 400_u64, 4.0_f64),
    ];
    for (log_index, tick, value) in series {
        pipeline.ingest(&event_with_value(log_index, tick, value, 0));
    }
    assert_eq!(pipeline.compaction_frontier_tick(), 300);
    let total = pipeline
        .cross_tier_value("metric.sum", 0)
        .expect("sum should be defined");
    assert_eq!(total, 10.0);
}

#[test]
fn policy_upgrade_requires_replay_for_large_gaps() {
    let mut pipeline =
        PaneAggregationPipeline::new(sum_config()).expect("sum config should be valid");
    pipeline
        .upgrade_policy(1, 2)
        .expect("adjacent policy upgrade should succeed");
    match pipeline.upgrade_policy(2, 4) {
        Err(AggregationError::PolicyUpgradeRequiresReplay { from, to }) => {
            assert_eq!(from, 2);
            assert_eq!(to, 4);
        }
        other => panic!("unexpected upgrade result: {other:?}"),
    }
}

#[test]
fn cross_tier_queries_match_realtime_evaluation() {
    let compact_config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 300,
        max_window_horizon_ms: 300,
        metrics: vec![MetricSpec {
            name: "metric.sum".to_string(),
            kind: AggregatorKind::Sum,
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    let realtime_config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 1_000,
        max_window_horizon_ms: 1_000,
        metrics: vec![MetricSpec {
            name: "metric.sum".to_string(),
            kind: AggregatorKind::Sum,
            window_horizon_ms: 200,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    let mut compacting =
        PaneAggregationPipeline::new(compact_config).expect("compaction config should be valid");
    let mut realtime =
        PaneAggregationPipeline::new(realtime_config).expect("realtime config should be valid");
    for idx in 0..6 {
        let tick = (idx + 1) as u64 * 100;
        let value = (idx + 1) as f64;
        let event = event_with_value(idx as u64 + 1, tick, value, 0);
        compacting.ingest(&event);
        realtime.ingest(&event);
    }
    assert!(compacting.compaction_frontier_tick() > 0);
    let compacted_total = compacting
        .cross_tier_value("metric.sum", 0)
        .expect("compacted sum should be defined");
    let realtime_total = realtime
        .cross_tier_value("metric.sum", 0)
        .expect("realtime sum should be defined");
    assert_eq!(compacted_total, realtime_total);
}

#[test]
fn ring_budget_validation_reports_estimates() {
    let config = sum_config();
    let budget = PaneRingBudget {
        projected_lanes: 32,
        bytes_per_state: 64,
        max_memory_bytes: 1_000_000,
    };
    let report = config
        .validate_ring_budget(&budget)
        .expect("budget should pass");
    assert_eq!(report.r_max_ms, 500);
    assert_eq!(report.ring_panes, 5);
    assert_eq!(report.projected_state_slots, 160);
    assert_eq!(report.estimated_memory_bytes, 10_240);
}

#[test]
fn ring_budget_validation_errors_when_exceeded() {
    let config = sum_config();
    let budget = PaneRingBudget {
        projected_lanes: 64,
        bytes_per_state: 1_024,
        max_memory_bytes: 512,
    };
    match config.validate_ring_budget(&budget) {
        Err(AggregationError::RingBudgetExceeded {
            estimated_bytes, ..
        }) => {
            assert!(estimated_bytes > budget.max_memory_bytes);
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn alerts_when_metric_window_exceeds_ring() {
    let config = AggregationConfig {
        pane_width_ms: 100,
        raw_retention_ms: 200,
        max_window_horizon_ms: 200,
        metrics: vec![MetricSpec {
            name: "metric.sum".to_string(),
            kind: AggregatorKind::Sum,
            window_horizon_ms: 400,
            correction_horizon_ms: 200,
            labels: vec![],
            late_data_policy: LateDataPolicy::Drop,
        }],
    };
    match PaneAggregationPipeline::new(config) {
        Err(AggregationError::WindowExceedsRing {
            metric,
            window_ms,
            r_max_ms,
        }) => {
            assert_eq!(metric, "metric.sum");
            assert_eq!(window_ms, 400);
            assert_eq!(r_max_ms, 200);
        }
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected window alert"),
    }
}

use ceptra::{
    AggregatorKind, CommittedEvent, EventAdmission, EventMeasurement, LateDataPolicy,
    LateEventReason, MetricBinding, MetricSpec, MonotonicClock, TimeSemantics, WalEntry,
    WatermarkConfig,
};
use std::collections::{BTreeMap, BTreeSet};

struct MockClock {
    readings: Vec<u128>,
    idx: usize,
}

impl MockClock {
    fn new(readings: Vec<u128>) -> Self {
        Self { readings, idx: 0 }
    }
}

impl MonotonicClock for MockClock {
    fn now_ns(&mut self) -> u128 {
        let reading = self
            .readings
            .get(self.idx)
            .copied()
            .unwrap_or_else(|| *self.readings.last().unwrap());
        self.idx += 1;
        reading
    }
}

fn committed_event(log_index: u64, tick_ms: u64, metric: &str, value: f64) -> CommittedEvent {
    let wal = WalEntry::new(log_index, tick_ms, format!("payload-{log_index}"));
    let measurement = EventMeasurement::new(metric)
        .with_value(value)
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

#[test]
fn drops_non_retractable_events_and_records_audit() {
    let spec = MetricSpec {
        name: "metric.late".to_string(),
        kind: AggregatorKind::QuantileOverTime { quantile: 0.5 },
        window_horizon_ms: 100,
        correction_horizon_ms: 0,
        labels: vec![],
        late_data_policy: LateDataPolicy::Drop,
    };
    let mut semantics = TimeSemantics::with_components(
        &[spec],
        WatermarkConfig::new(0, 0),
        Box::new(MockClock::new(vec![2_000_000_000; 6])),
    );
    let first = committed_event(1, 1_000, "metric.late", 5.0);
    let result1 = semantics.evaluate_event(&first);
    assert_eq!(result1.filtered_measurements.len(), 1);
    assert!(result1.dropped_metrics.is_empty());

    let late = committed_event(2, 900, "metric.late", 7.0);
    let result2 = semantics.evaluate_event(&late);
    assert!(result2.filtered_measurements.is_empty());
    assert!(result2.dropped_metrics.contains("metric.late"));
    assert_eq!(semantics.audit_log().len(), 1);
    let audit = &semantics.audit_log()[0];
    assert_eq!(audit.metric, "metric.late");
    assert_eq!(audit.event_id, "evt-2");
    let counters = semantics.late_event_counters();
    assert_eq!(counters.get("NON_RETRACTABLE_FINALIZED"), Some(&1));
}

#[test]
fn rejects_future_skew_events() {
    let spec = MetricSpec {
        name: "metric.future".to_string(),
        kind: AggregatorKind::Sum,
        window_horizon_ms: 100,
        correction_horizon_ms: 100,
        labels: vec![],
        late_data_policy: LateDataPolicy::Drop,
    };
    let mut semantics = TimeSemantics::with_components(
        &[spec],
        WatermarkConfig::new(0, 0),
        Box::new(MockClock::new(vec![0, 0])),
    );
    let future = committed_event(1, 10_000, "metric.future", 1.0);
    let result = semantics.evaluate_event(&future);
    assert_eq!(result.admission, EventAdmission::RejectedFutureSkew);
    assert_eq!(result.filtered_measurements.len(), 0);
    assert_eq!(semantics.audit_log().len(), 1);
    assert_eq!(semantics.audit_log()[0].reason, LateEventReason::FutureSkew);
    let counters = semantics.late_event_counters();
    assert_eq!(counters.get("FUTURE_SKEW"), Some(&1));
}

#[test]
fn records_past_skew_counters() {
    let spec = MetricSpec {
        name: "metric.past".to_string(),
        kind: AggregatorKind::Sum,
        window_horizon_ms: 100,
        correction_horizon_ms: 100,
        labels: vec![],
        late_data_policy: LateDataPolicy::Drop,
    };
    let mut semantics = TimeSemantics::with_components(
        &[spec],
        WatermarkConfig::new(0, 0),
        Box::new(MockClock::new(vec![2_000_000_000_000, 2_000_000_000_000])),
    );
    let old_event = committed_event(1, 0, "metric.past", 2.0);
    let _ = semantics.evaluate_event(&old_event);
    let counters = semantics.late_event_counters();
    assert_eq!(counters.get("PAST_SKEW"), Some(&1));
}

#[test]
fn decorate_bindings_marks_flags_for_drops() {
    let semantics = TimeSemantics::disabled();
    let mut bindings = vec![MetricBinding::with_value("metric.late", 10.0)];
    let mut dropped = BTreeSet::new();
    dropped.insert("metric.late".to_string());
    semantics.decorate_bindings(&mut bindings, &dropped);
    assert!(bindings[0].flags.contains("NON_RETRACTABLE_DROPPED"));
}

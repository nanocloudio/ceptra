use ceptra::{
    FinalizedHorizon, FinalizedHorizonSnapshot, FinalizedHorizonStall, MetricFinalization,
};

#[test]
fn computes_finalized_indices_per_metric() {
    let mut horizon = FinalizedHorizon::new();
    horizon.record_metric("metric_a", 120, 10);
    horizon.record_metric("metric_b", 150, 30);
    let snapshot = horizon.snapshot();
    assert_eq!(snapshot.finalized_horizon_index, 120 - 10);
    assert_eq!(snapshot.metrics.len(), 2);
}

#[test]
fn snapshot_roundtrip_restores_state() {
    let snapshot = FinalizedHorizonSnapshot {
        finalized_horizon_index: 42,
        metrics: vec![MetricFinalization {
            metric: "metric_x".into(),
            watermark_index: 100,
            correction_horizon: 5,
            finalized_index: 95,
        }],
    };
    let restored = FinalizedHorizon::from_snapshot(&snapshot);
    assert_eq!(restored.finalized_horizon_index(), Some(42));
    let new_snapshot = restored.snapshot();
    assert_eq!(new_snapshot.finalized_horizon_index, 42);
}

#[test]
fn detects_stall_when_lag_exceeds_threshold() {
    let mut horizon = FinalizedHorizon::new();
    horizon.record_metric("metric", 200, 10);
    let stall = horizon
        .stall_state(300, 30)
        .expect("lag should exceed threshold");
    assert_eq!(
        stall,
        FinalizedHorizonStall {
            lag: 300 - 190,
            throttle_threshold: 60
        }
    );
    assert!(horizon.stall_state(200, 30).is_none());
}

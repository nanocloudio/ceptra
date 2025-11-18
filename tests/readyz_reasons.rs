use ceptra::{
    PartitionReadinessInputs, PartitionReadinessPolicy, ReadyGate, ReadyzReasons,
    RolloutAutomation, APPLY_LAG_REASON, BACKUP_LAG_REASON, CERTIFICATE_EXPIRY_REASON,
    CHECKPOINT_AGE_REASON, PARTITION_THRESHOLD_REASON, WARMUP_PENDING_REASON,
    WATERMARK_STALL_REASON,
};

#[test]
fn records_checkpoint_age_reason() {
    let mut reasons = ReadyzReasons::new();
    reasons.record_checkpoint_age(5000, 1000);
    assert!(reasons.reasons().contains(CHECKPOINT_AGE_REASON));
}

#[test]
fn records_watermark_stall_reason() {
    let mut reasons = ReadyzReasons::new();
    reasons.record_watermark_stall(5_000_000, 1);
    assert!(reasons.reasons().contains(WATERMARK_STALL_REASON));
}

#[test]
fn records_backup_lag_reason() {
    let mut reasons = ReadyzReasons::new();
    reasons.record_backup_lag(100, 10);
    assert!(reasons.reasons().contains(BACKUP_LAG_REASON));
}

#[test]
fn readiness_gate_enforces_threshold() {
    let mut gate = ReadyGate::new(0.99);
    for idx in 0..100 {
        let mut reasons = ReadyzReasons::new();
        let ready = idx < 98;
        if !ready {
            reasons.add_reason("not_ready");
        }
        gate.record_partition(format!("p{idx}"), ready, &reasons);
    }
    let report = gate.evaluate();
    assert!(!report.ready);
    assert!(report
        .reasons
        .contains(&PARTITION_THRESHOLD_REASON.to_string()));
    let failing = report.partitions.iter().filter(|part| !part.ready).count();
    assert_eq!(failing, 2);
}

#[test]
fn readiness_gate_respects_criticality_overrides() {
    let mut gate = ReadyGate::new(0.99);
    for idx in 0..100 {
        let mut reasons = ReadyzReasons::new();
        let ready = idx < 99;
        if !ready {
            reasons.add_reason("shadow_pending");
        }
        gate.record_partition(format!("p{idx}"), ready, &reasons);
    }
    gate.override_criticality("p99", false);
    let report = gate.evaluate();
    assert!(report.ready);
    let last = report
        .partitions
        .iter()
        .find(|part| part.partition_id == "p99")
        .unwrap();
    assert!(!last.critical);
    assert!(!last.ready);
}

#[test]
fn readiness_policy_enforces_thresholds() {
    let policy = PartitionReadinessPolicy::default();
    let mut reasons = ReadyzReasons::new();
    let ready = policy.evaluate(
        &PartitionReadinessInputs {
            apply_lag_ms: 500,
            replication_lag_ms: 100,
            checkpoint_age_ms: 5_000,
            checkpoint_full_interval_ms: 1_000,
            cert_expiry_ms: 1_000,
            warmup_ready: false,
        },
        &mut reasons,
    );
    assert!(!ready);
    assert!(reasons.reasons().contains(APPLY_LAG_REASON));
    assert!(reasons.reasons().contains(CHECKPOINT_AGE_REASON));
    assert!(reasons.reasons().contains(CERTIFICATE_EXPIRY_REASON));
    assert!(reasons.reasons().contains(WARMUP_PENDING_REASON));
}

#[test]
fn readiness_metrics_track_partition_ratio() {
    let mut gate = ReadyGate::new(0.5);
    let policy = PartitionReadinessPolicy::default();
    gate.record_partition_with_policy(
        "p0",
        PartitionReadinessInputs {
            apply_lag_ms: 100,
            replication_lag_ms: 100,
            checkpoint_age_ms: 500,
            checkpoint_full_interval_ms: 1_000,
            cert_expiry_ms: 1_000_000_000,
            warmup_ready: true,
        },
        &policy,
    );
    gate.record_partition_with_policy(
        "p1",
        PartitionReadinessInputs {
            apply_lag_ms: 500,
            replication_lag_ms: 500,
            checkpoint_age_ms: 5_000,
            checkpoint_full_interval_ms: 1_000,
            cert_expiry_ms: 1_000,
            warmup_ready: false,
        },
        &policy,
    );
    let metrics = gate.metrics();
    assert_eq!(metrics.partitions.len(), 2);
    assert!(metrics
        .partitions
        .iter()
        .any(|metric| metric.partition_id == "p0" && metric.ready));
    assert!(metrics
        .partitions
        .iter()
        .any(|metric| metric.partition_id == "p1" && !metric.ready));
    assert!((metrics.ready_ratio() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn rollout_automation_blocks_when_threshold_not_met() {
    let mut gate = ReadyGate::new(0.99);
    let policy = PartitionReadinessPolicy::default();
    gate.record_partition_with_policy(
        "p0",
        PartitionReadinessInputs {
            apply_lag_ms: 100,
            replication_lag_ms: 100,
            checkpoint_age_ms: 500,
            checkpoint_full_interval_ms: 1_000,
            cert_expiry_ms: 1_000_000,
            warmup_ready: true,
        },
        &policy,
    );
    gate.record_partition_with_policy(
        "p1",
        PartitionReadinessInputs {
            apply_lag_ms: 1_000,
            replication_lag_ms: 1_000,
            checkpoint_age_ms: 10_000,
            checkpoint_full_interval_ms: 1_000,
            cert_expiry_ms: 500,
            warmup_ready: false,
        },
        &policy,
    );
    let automation = RolloutAutomation::new(0.99);
    let decision = automation.evaluate(&gate);
    assert!(decision.should_block());
}

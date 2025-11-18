use ceptra::{MetricsSnapshot, ObservabilityError, ObservabilityService, ReadyGate, ReadyzReasons};
use serde_json::Value;

#[test]
fn validates_metrics_exposition() {
    let service = ObservabilityService::new("shared");
    let good = "ceptra_ready_partitions_ratio 1\n";
    service.validate_metrics(good).expect("ms metrics accepted");

    let err = service
        .validate_metrics("ceptra_ready_partitions_seconds 1\n")
        .unwrap_err();
    assert!(matches!(err, ObservabilityError::Telemetry(_)));
}

#[test]
fn wraps_readyz_report_with_registry() {
    let mut reasons = ReadyzReasons::new();
    reasons.record_checkpoint_age(5_000, 2_000);
    let mut gate = ReadyGate::new(0.99);
    gate.record_partition("p0", true, &reasons);
    let cep_report = gate.evaluate();
    let service = ObservabilityService::new("shared");
    let envelope = service.readyz_envelope(cep_report.clone());
    assert_eq!(envelope.registry, "shared");
    assert_eq!(envelope.ceptra.ready, cep_report.ready);
}

#[test]
fn metrics_snapshot_enforces_ms_suffix() {
    let mut snapshot = MetricsSnapshot::new("shared", 1234);
    snapshot
        .record_metric("ceptra_ready_partitions_ratio", 1)
        .expect("ms metric succeeds");
    let err = snapshot
        .record_metric("ceptra_ready_seconds", 1)
        .unwrap_err();
    assert!(matches!(err, ObservabilityError::InvalidMetric { .. }));
}

#[test]
fn test_export_metrics_snapshot_produces_json() {
    let mut snapshot = MetricsSnapshot::new("shared", 77);
    snapshot
        .record_metric("ceptra_ready_partitions_ratio", 999)
        .unwrap();
    let service = ObservabilityService::new("shared");
    let json = service
        .test_export_metrics_snapshot(&snapshot)
        .expect("registry matches");
    let parsed: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["registry"], "shared");
    assert_eq!(
        parsed["metrics"].as_array().unwrap().len(),
        1,
        "one metric snapshot recorded"
    );

    let mismatch = ObservabilityService::new("other");
    let err = mismatch
        .test_export_metrics_snapshot(&snapshot)
        .unwrap_err();
    assert!(matches!(err, ObservabilityError::RegistryMismatch { .. }));
}

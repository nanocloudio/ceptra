use ceptra::{ConfigError, ConfigKnobClass, ConfigService};
use serde_json::{json, Value};

#[test]
fn applies_patch_and_classifies_restart() {
    let mut service = ConfigService::new(json!({
        "lateness_allowance_L": 60,
        "wal_path": "/var/lib/ceptra"
    }))
    .unwrap();
    let result = service
        .patch(json!({
            "wal_path": "/opt/ceptra",
            "pane_size_q": 32
        }))
        .unwrap();
    assert_eq!(result.version, 2);
    assert_eq!(result.impact, ConfigKnobClass::Restart);
    assert!(result.changed_keys.contains(&"wal_path".to_string()));
    assert!(result.changed_keys.contains(&"pane_size_q".to_string()));
    assert_eq!(service.telemetry().version, 2);
}

#[test]
fn rejects_non_object_patch_and_counts_failure() {
    let mut service = ConfigService::new(json!({ "durability_mode": "Strict" })).unwrap();
    let err = service
        .patch(Value::String("invalid".to_string()))
        .unwrap_err();
    match err {
        ConfigError::InvalidPatch(_) => {}
        other => panic!("unexpected error: {:?}", other),
    }
    assert_eq!(service.telemetry().validation_failures_total, 1);
}

#[test]
fn rollbacks_restore_previous_snapshot() {
    let mut service = ConfigService::new(json!({
        "lateness_allowance_L": 60,
        "durability_mode": "Strict"
    }))
    .unwrap();
    service
        .patch(json!({ "lateness_allowance_L": 120 }))
        .unwrap();
    service
        .patch(json!({ "durability_mode": "Group" }))
        .unwrap();
    let rollback = service.rollback(1).unwrap();
    assert_eq!(rollback.version, 4);
    assert_eq!(rollback.impact, ConfigKnobClass::Reconfigure);
    assert_eq!(
        service.current_config(),
        json!({
            "lateness_allowance_L": 60,
            "durability_mode": "Strict"
        })
    );
}

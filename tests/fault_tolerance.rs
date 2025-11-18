use ceptra::{
    ApAssignment, DisasterRecoveryPlan, FailoverTelemetry, FaultToleranceError, PartitionGroupPlan,
    WalShipment, WarmStandbyDirective, WarmStandbyManager, PERMANENT_EPOCH_REASON,
};

fn assignment(
    partition_id: &str,
    leader: &str,
    followers: &[&str],
    lease_epoch: u64,
) -> ApAssignment {
    ApAssignment {
        partition_id: partition_id.to_string(),
        leader: leader.to_string(),
        warm_followers: followers.iter().map(|id| id.to_string()).collect(),
        lease_epoch,
    }
}

fn dr_plan() -> DisasterRecoveryPlan {
    DisasterRecoveryPlan {
        primary_cluster: "cluster-a".into(),
        dr_cluster: "cluster-b".into(),
        partition_groups: vec![
            PartitionGroupPlan::new("pg-0", true),
            PartitionGroupPlan::new("pg-1", true),
        ],
        wal_shipments: vec![
            WalShipment::new("pg-0", 1024, true),
            WalShipment::new("pg-1", 2048, true),
        ],
        ingress_routes: vec!["gw-a".into(), "gw-b".into()],
        source_epoch: 40,
        target_epoch: 41,
    }
}

#[test]
fn warm_followers_stay_synced_and_promote_new_leader() {
    let mut manager = WarmStandbyManager::new();
    let initial = assignment("p0", "pod-0", &["pod-1", "pod-2"], 10);
    let outcome = manager.apply_assignment(initial);
    assert_eq!(outcome.partition_id, "p0");
    assert!(outcome
        .directives()
        .iter()
        .all(|directive| matches!(directive, WarmStandbyDirective::ActivateWarmApply { .. })),);

    let promotion = assignment("p0", "pod-1", &["pod-0", "pod-2"], 11);
    let promoted = manager.apply_assignment(promotion);
    assert!(promoted
        .directives()
        .iter()
        .any(|directive| matches!(directive, WarmStandbyDirective::PromoteLeader { replica, .. } if replica == "pod-1")));
    assert!(promoted
        .directives()
        .iter()
        .any(|directive| matches!(directive, WarmStandbyDirective::ActivateWarmApply { replica } if replica == "pod-0")));
    assert!(promoted
        .directives()
        .iter()
        .any(|directive| matches!(directive, WarmStandbyDirective::DeactivateWarmApply { replica } if replica == "pod-1")));
}

#[test]
fn simulate_leader_loss_and_double_failure() {
    let mut manager = WarmStandbyManager::new();
    manager.apply_assignment(assignment("p9", "node-a", &["node-b", "node-c"], 7));

    let leader_loss = manager
        .simulate_loss("p9", ["node-a"])
        .expect("leader should be known");
    assert_eq!(leader_loss.promoted.as_deref(), Some("node-b"));
    assert!(!leader_loss.is_permanent_epoch());

    let double_loss = manager
        .simulate_loss("p9", ["node-a", "node-b"])
        .expect("replicas should exist");
    assert_eq!(double_loss.promoted, None);
    assert_eq!(double_loss.reason, Some(PERMANENT_EPOCH_REASON));

    let mut telemetry = FailoverTelemetry::default();
    telemetry.record(&leader_loss);
    telemetry.record(&double_loss);
    assert_eq!(
        telemetry.metrics(),
        vec![
            ("ceptra_failover_events_total", 2),
            ("ceptra_failover_promotions_total", 1),
            ("ceptra_failover_permanent_epoch_total", 1),
        ]
    );
}

#[test]
fn dr_plan_requires_frozen_groups_and_verified_wal() {
    let plan = dr_plan();
    let report = plan.cutover().expect("plan should be valid");
    assert_eq!(
        report.fenced_groups,
        vec!["pg-0".to_string(), "pg-1".to_string()]
    );
    assert_eq!(
        report.verified_shipments,
        vec!["pg-0".to_string(), "pg-1".to_string()]
    );
    assert_eq!(
        report.ingress_rerouted,
        vec!["gw-a".to_string(), "gw-b".to_string()]
    );
    assert_eq!(report.reconciled_epoch, 41);
}

#[test]
fn dr_plan_surfaces_validation_errors() {
    let mut plan = dr_plan();
    plan.partition_groups[0].freeze_issued = false;
    plan.wal_shipments[1].verified = false;
    let err = plan.cutover().unwrap_err();
    assert!(matches!(
        err,
        FaultToleranceError::PartitionGroupNotFrozen { ref group_id } if group_id == "pg-0"
    ));

    let mut ingress_missing = dr_plan();
    ingress_missing.ingress_routes.clear();
    let err = ingress_missing.cutover().unwrap_err();
    assert!(matches!(err, FaultToleranceError::MissingIngressRoutes));

    let mut epoch_regresses = dr_plan();
    epoch_regresses.target_epoch = 10;
    epoch_regresses.source_epoch = 11;
    let err = epoch_regresses.cutover().unwrap_err();
    assert!(matches!(
        err,
        FaultToleranceError::EpochMismatch {
            target_epoch: 10,
            source_epoch: 11,
        }
    ));
}

use ceptra::{
    CepShadowGateState, HotReloadGate, PartitionWarmupProbe, SHADOW_READY, SHADOW_TIMEOUT,
    SHADOW_WAITING_FOR_WAL,
};
use clustor::{ActivationBarrier, ActivationDigestError, ShadowApplyState, WarmupReadinessRecord};

fn barrier<'a>(partitions: impl IntoIterator<Item = &'a str>, now_ms: u64) -> ActivationBarrier {
    ActivationBarrier {
        barrier_id: "b1".to_string(),
        bundle_id: "bundle_v2".to_string(),
        partitions: partitions.into_iter().map(|p| p.to_string()).collect(),
        readiness_threshold: 1.0,
        warmup_deadline_ms: now_ms + 600_000,
        readiness_window_ms: 300_000,
    }
}

fn readiness_record(
    partition_id: &str,
    state: ShadowApplyState,
    checkpoint: u64,
    ratio: f64,
    updated_at_ms: u64,
) -> WarmupReadinessRecord {
    WarmupReadinessRecord {
        partition_id: partition_id.to_string(),
        bundle_id: "bundle_v2".to_string(),
        shadow_apply_state: state,
        shadow_apply_checkpoint_index: checkpoint,
        warmup_ready_ratio: ratio,
        updated_at_ms,
    }
}

#[test]
fn gate_ready_when_shadow_matches_wal_head() -> Result<(), ActivationDigestError> {
    let now = 1_000_000;
    let barrier = barrier(["p1", "p2"], now);
    let readiness = vec![
        readiness_record("p1", ShadowApplyState::Ready, 120, 1.0, now),
        readiness_record("p2", ShadowApplyState::Ready, 80, 1.0, now),
    ];
    let probes = vec![
        PartitionWarmupProbe {
            partition_id: "p1".to_string(),
            wal_head_index: 120,
            reasons: vec![],
        },
        PartitionWarmupProbe {
            partition_id: "p2".to_string(),
            wal_head_index: 80,
            reasons: vec![],
        },
    ];
    let gate = HotReloadGate::new(600_000);
    let decision = gate.evaluate(&barrier, &readiness, &probes, now)?;
    assert!(decision.ready());
    assert!(matches!(decision.cep_state, CepShadowGateState::Ready));
    assert!(decision
        .partitions
        .iter()
        .all(|partition| partition.shadow_state == SHADOW_READY));
    Ok(())
}

#[test]
fn gate_blocks_when_shadow_lags_wal_head() -> Result<(), ActivationDigestError> {
    let now = 2_000_000;
    let barrier = barrier(["p1"], now);
    let readiness = vec![readiness_record(
        "p1",
        ShadowApplyState::Ready,
        90,
        1.0,
        now - 10_000,
    )];
    let probes = vec![PartitionWarmupProbe {
        partition_id: "p1".to_string(),
        wal_head_index: 120,
        reasons: vec![],
    }];
    let gate = HotReloadGate::new(600_000);
    let decision = gate.evaluate(&barrier, &readiness, &probes, now)?;
    assert!(!decision.ready());
    assert!(matches!(
        decision.cep_state,
        CepShadowGateState::Pending { .. }
    ));
    assert_eq!(decision.partitions[0].shadow_state, SHADOW_WAITING_FOR_WAL);
    Ok(())
}

#[test]
fn gate_requires_override_after_grace_window() -> Result<(), ActivationDigestError> {
    let now = 4_000_000;
    let mut barrier = barrier(["p1"], now);
    barrier.readiness_window_ms = 1_000_000; // keep Clustor barrier happy.
    let readiness = vec![readiness_record(
        "p1",
        ShadowApplyState::Ready,
        25,
        1.0,
        now - 1_200_000,
    )];
    let probes = vec![PartitionWarmupProbe {
        partition_id: "p1".to_string(),
        wal_head_index: 100,
        reasons: vec!["lane_validation_pending".to_string()],
    }];
    let mut gate = HotReloadGate::new(600_000);
    let decision = gate.evaluate(&barrier, &readiness, &probes, now)?;
    assert!(matches!(
        decision.cep_state,
        CepShadowGateState::RequiresOverride { .. }
    ));
    assert_eq!(decision.partitions[0].shadow_state, SHADOW_TIMEOUT);

    gate.add_override("p1");
    let decision = gate.evaluate(&barrier, &readiness, &probes, now)?;
    assert!(matches!(decision.cep_state, CepShadowGateState::Ready));
    assert!(decision.partitions[0].override_applied);
    Ok(())
}

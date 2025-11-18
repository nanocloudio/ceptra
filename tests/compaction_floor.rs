use ceptra::{
    raise_learner_slack_floor, CheckpointChain, CommitRateSnapshot, DedupRetentionInputs,
    MinRetainPlanner,
};
use clustor::storage::{
    authorization_chain_hash, CompactionAuthAck, CompactionBlockReason, CompactionDecision,
    CompactionGate, CompactionPlanRequest, CompactionState, ManifestGate, SegmentHealth,
    SnapshotAuthorizationRecord,
};

fn sample_manifest_gate(base_index: u64) -> ManifestGate {
    let manifest_id = "manifest-1".to_string();
    let manifest_hash = "deadbeef".to_string();
    let record = SnapshotAuthorizationRecord {
        manifest_id: manifest_id.clone(),
        base_index,
        auth_seq: 1,
        manifest_hash: manifest_hash.clone(),
        recorded_at_ms: 1_000,
        chain_hash: authorization_chain_hash(None, &manifest_id, 1, &manifest_hash),
    };
    let ack = CompactionAuthAck::from_record(&record, None, record.recorded_at_ms + 10);
    ManifestGate {
        relisted: true,
        signature_valid: true,
        authorization: Some(record),
        acknowledgement: Some(ack),
    }
}

fn sample_compaction_state(base_index: u64) -> CompactionState {
    CompactionState {
        learner_slack_floor: None,
        quorum_applied_index: base_index + 250,
        snapshot_base_index: base_index,
        quorum_sm_durable_index: base_index + 400,
        guard_bytes_satisfied: true,
        learner_retirement_pending: false,
        manifest_gate: sample_manifest_gate(base_index),
    }
}

fn sample_segments() -> Vec<SegmentHealth> {
    vec![
        SegmentHealth {
            segment_seq: 1,
            max_index_in_segment: 1_500,
            has_pending_nonce_reservation: false,
            abandon_record_present: true,
            rewrite_inflight: false,
        },
        SegmentHealth {
            segment_seq: 2,
            max_index_in_segment: 1_650,
            has_pending_nonce_reservation: false,
            abandon_record_present: true,
            rewrite_inflight: false,
        },
    ]
}

fn planner() -> MinRetainPlanner {
    let checkpoint = CheckpointChain {
        full_checkpoint_index: 1_200,
        incremental_indices: vec![1_500, 1_700],
    };
    let dedup_inputs = DedupRetentionInputs {
        finalized_horizon_index: 1_600,
        dedup_retention_window_s: 120,
        commit_rate: CommitRateSnapshot {
            ewma_rate_per_sec: 50.0,
            ..Default::default()
        },
    };
    MinRetainPlanner::new(checkpoint, dedup_inputs)
}

#[test]
fn retention_plan_sets_learner_floor_for_compaction() {
    let planner = planner();
    let plan = planner.plan();
    assert_eq!(plan.checkpoint_floor, 1_700);
    assert_eq!(plan.min_retain_index, 1_700);
    let mut state = sample_compaction_state(1_500);
    raise_learner_slack_floor(&mut state, &plan);
    assert_eq!(state.learner_slack_floor, Some(1_700));
    let request = CompactionPlanRequest {
        state,
        segments: sample_segments(),
    };
    match CompactionGate::plan(request) {
        CompactionDecision::Ready {
            floor_effective,
            deletable_segments,
            ..
        } => {
            assert_eq!(floor_effective, 1_700);
            assert_eq!(deletable_segments, vec![1, 2]);
        }
        other => panic!("unexpected compaction decision: {other:?}"),
    }
}

#[test]
fn manifest_ack_ordering_enforced_with_retention_floor() {
    let planner = planner();
    let plan = planner.plan();
    let mut state = sample_compaction_state(1_500);
    state.manifest_gate.acknowledgement = None;
    raise_learner_slack_floor(&mut state, &plan);
    let request = CompactionPlanRequest {
        state,
        segments: sample_segments(),
    };
    match CompactionGate::plan(request) {
        CompactionDecision::Blocked(reasons) => {
            assert!(reasons
                .iter()
                .any(|reason| matches!(reason, CompactionBlockReason::ManifestAckMissing { .. })));
        }
        other => panic!("expected manifest ordering block, saw {other:?}"),
    }
}

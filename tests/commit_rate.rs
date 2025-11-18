use ceptra::{
    CommitEpochCheckpoint, CommitRateState, ControlPlaneReconciler, DedupCapacityUpdate, WalEntry,
};

fn wal_entry(log_index: u64, tick_ms: u64) -> WalEntry {
    WalEntry::new(log_index, tick_ms, format!("entry-{log_index}"))
}

#[test]
fn ewma_state_round_trips_with_checkpoint() {
    let mut state = CommitRateState::new("p0");
    let wal = vec![
        wal_entry(1, 1_000),
        wal_entry(2, 2_000),
        wal_entry(3, 3_200),
    ];
    state.record_entries(&wal);
    let snapshot = state.snapshot();
    let rebuilt = CommitRateState::from_snapshot("p0", &snapshot);
    assert!((rebuilt.ewma_rate_per_sec() - state.ewma_rate_per_sec()).abs() < 1e-6);
    assert_eq!(rebuilt.last_log_index(), state.last_log_index());
    assert_eq!(rebuilt.sample_count(), state.sample_count());
}

#[test]
fn wal_replay_after_checkpoint_is_deterministic() {
    let mut state = CommitRateState::new("p42");
    let initial = vec![
        wal_entry(1, 1_000),
        wal_entry(2, 1_500),
        wal_entry(3, 2_100),
    ];
    state.record_entries(&initial);
    let checkpoint = CommitEpochCheckpoint::default().with_commit_rate(state.snapshot());
    let mut expected =
        CommitRateState::from_snapshot("p42", checkpoint.commit_rate_snapshot().unwrap());
    let late_entries = vec![wal_entry(4, 4_500), wal_entry(5, 6_000)];
    expected.record_entries(&late_entries);

    let rebuilt =
        CommitRateState::rebuild_from_checkpoint(checkpoint.commit_rate_snapshot(), &late_entries);
    let rebuilt_snapshot = rebuilt.snapshot();
    let expected_snapshot = expected.snapshot();
    assert!(
        (rebuilt_snapshot.ewma_rate_per_sec - expected_snapshot.ewma_rate_per_sec).abs() < 1e-6
    );
    assert_eq!(
        rebuilt_snapshot.last_log_index,
        expected_snapshot.last_log_index
    );
}

#[test]
fn reconciler_emits_capacity_updates_every_interval() {
    let mut state = CommitRateState::new("p-cap");
    let wal = vec![
        wal_entry(1, 1_000),
        wal_entry(2, 2_100),
        wal_entry(3, 3_200),
    ];
    state.record_entries(&wal);
    let mut reconciler = ControlPlaneReconciler::with_sample_interval("p-cap", 1);
    let first = reconciler
        .maybe_emit_update(&state)
        .expect("first sample should emit update");
    assert_eq!(
        first,
        DedupCapacityUpdate {
            partition_id: "p-cap".into(),
            max_ingest_rate_per_partition: state.predicted_capacity(),
        }
    );
    // Without new ticks we stay within the sampling interval.
    assert!(reconciler.maybe_emit_update(&state).is_none());

    // Advance ticks with faster throughput to trigger another update.
    let faster = vec![
        wal_entry(4, 3_203),
        wal_entry(5, 3_206),
        wal_entry(6, 3_209),
    ];
    state.record_entries(&faster);
    let second = reconciler
        .maybe_emit_update(&state)
        .expect("faster rate should raise capacity");
    assert!(second.max_ingest_rate_per_partition > first.max_ingest_rate_per_partition);
}

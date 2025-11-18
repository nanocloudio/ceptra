use ceptra::{BackupLagTracker, BackupSchedule, ReadyzReasons, BACKUP_LAG_REASON};

#[test]
fn backup_schedule_plans_future_events() {
    let schedule = BackupSchedule::standard();
    let status = schedule.plan(1_000, 5);
    assert_eq!(status.next_cp_snapshot_ms, 61_000);
    assert_eq!(status.next_checkpoint_ms, 31_000);
    assert_eq!(status.next_export_ms, 86_401_000);
    assert_eq!(status.wal_compaction_batches, 5);
}

#[test]
fn tracker_flags_backup_lag_for_readyz() {
    let schedule = BackupSchedule::standard();
    let mut tracker = BackupLagTracker::new();
    let mut reasons = ReadyzReasons::new();
    tracker.apply_readyz(10, &schedule, &mut reasons);
    assert!(reasons.reasons().contains(BACKUP_LAG_REASON));

    tracker.record_export(1_000);
    let mut reset = ReadyzReasons::new();
    tracker.apply_readyz(
        1_000 + schedule.nightly_export_interval_ms() + 1,
        &schedule,
        &mut reset,
    );
    assert!(reset.reasons().contains(BACKUP_LAG_REASON));
}

#[test]
fn restore_drill_due_when_quarter_lapses() {
    let mut tracker = BackupLagTracker::new();
    assert!(tracker.restore_drill_due(1));
    tracker.record_restore_drill(1);
    let three_months_ms = 90 * 86_400_000;
    assert!(!tracker.restore_drill_due(1 + three_months_ms / 2));
    assert!(tracker.restore_drill_due(1 + three_months_ms + 1));
}

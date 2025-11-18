use std::cell::RefCell;
use std::rc::Rc;

use ceptra::{
    CheckpointKind, CheckpointLoader, CheckpointRestoreError, CheckpointResult, CheckpointSink,
    CheckpointSkipReason, CheckpointWriter, PersistedCheckpoint, ReadyzReasons,
};

#[derive(Clone, Default)]
struct RecordingSink {
    records: Rc<RefCell<Vec<PersistedCheckpoint>>>,
}

impl RecordingSink {
    fn handle(&self) -> Rc<RefCell<Vec<PersistedCheckpoint>>> {
        self.records.clone()
    }
}

impl CheckpointSink for RecordingSink {
    fn persist(&mut self, record: PersistedCheckpoint) -> Result<(), ceptra::CheckpointError> {
        self.records.borrow_mut().push(record);
        Ok(())
    }
}

#[test]
fn writes_full_and_incrementals_with_limits() {
    let sink = RecordingSink::default();
    let handle = sink.handle();
    let mut writer = CheckpointWriter::new(sink, 0);
    let full = writer
        .write_payload(vec![1, 2, 3], 10, 0)
        .expect("first checkpoint succeeds");
    assert!(matches!(full, CheckpointResult::Full(_)));

    for idx in 0..3 {
        let result = writer
            .write_payload(vec![idx], 11 + idx as u64, 1_000 + idx as u64)
            .expect("incremental checkpoint succeeds");
        assert!(matches!(result, CheckpointResult::Incremental(_)));
    }

    let forced = writer
        .write_payload(vec![9], 50, 2_000)
        .expect("full checkpoint after incrementals");
    assert!(matches!(forced, CheckpointResult::Full(_)));

    let records = handle.borrow();
    assert_eq!(records.len(), 5);
    assert!(records.iter().any(|rec| rec.kind == CheckpointKind::Full));
}

#[test]
fn enforces_bandwidth_cap_and_resets_over_time() {
    let sink = RecordingSink::default();
    let mut writer = CheckpointWriter::new(sink, 4);
    assert!(matches!(
        writer
            .write_payload(vec![1, 2], 1, 0)
            .expect("first write permitted"),
        CheckpointResult::Full(_)
    ));
    assert!(matches!(
        writer
            .write_payload(vec![3, 4, 5], 2, 100)
            .expect("bandwidth cap enforced"),
        CheckpointResult::Skipped(CheckpointSkipReason::BandwidthExceeded)
    ));
    assert!(matches!(
        writer
            .write_payload(vec![9], 3, 1_500)
            .expect("window resets after one second"),
        CheckpointResult::Incremental(_)
    ));
}

#[test]
fn write_snapshot_serializes_payloads() {
    #[derive(serde::Serialize)]
    struct Snapshot {
        idx: u64,
        name: &'static str,
    }
    let sink = RecordingSink::default();
    let handle = sink.handle();
    let mut writer = CheckpointWriter::new(sink, 0);
    writer
        .write_snapshot(
            &Snapshot {
                idx: 7,
                name: "alpha",
            },
            7,
            0,
        )
        .expect("full checkpoint");
    let payload = handle.borrow();
    assert_eq!(payload.len(), 1);
    let json = std::str::from_utf8(&payload[0].payload).expect("valid json");
    assert!(json.contains("\"idx\":7"));
    assert!(!payload[0].checksum.is_empty());
    assert!(!payload[0].signature.is_empty());
}

#[test]
fn loader_restores_latest_chain() {
    let sink = RecordingSink::default();
    let handle = sink.handle();
    let mut writer = CheckpointWriter::new(sink, 0);
    writer
        .write_payload(vec![1, 2, 3], 5, 0)
        .expect("initial full checkpoint");
    writer
        .write_payload(vec![4], 6, 1_000)
        .expect("first incremental");
    writer
        .write_payload(vec![5], 7, 2_000)
        .expect("second incremental");
    let loader = CheckpointLoader::default();
    let plan = loader
        .load(&handle.borrow())
        .expect("loader should build plan");
    assert_eq!(plan.incrementals.len(), 2);
    assert_eq!(plan.resume_index, 8);
    assert_eq!(plan.full.applied_index, 5);
}

#[test]
fn loader_detects_corruption_and_sets_readyz_reason() {
    let sink = RecordingSink::default();
    let handle = sink.handle();
    let mut writer = CheckpointWriter::new(sink, 0);
    writer
        .write_payload(vec![1, 2, 3], 5, 0)
        .expect("full checkpoint");
    writer
        .write_payload(vec![4, 5], 6, 1_000)
        .expect("incremental checkpoint");
    let mut records = handle.borrow().clone();
    records[1].payload[0] ^= 0xff;
    let loader = CheckpointLoader::default();
    let err = loader.load(&records).expect_err("corruption detected");
    match err {
        CheckpointRestoreError::ChainDegraded { .. } => {}
        other => panic!("expected degradation, got {other:?}"),
    }
    let mut reasons = ReadyzReasons::new();
    reasons.mark_checkpoint_chain_degraded();
    assert!(reasons
        .reasons()
        .contains(&"checkpoint_chain_degraded".to_string()));
}

use ceptra::{
    DedupCapacityUpdate, DedupDecision, DedupTable, DedupTableConfig, PERMANENT_RETRY_EXPIRED,
};

fn table_with_window(seconds: u64) -> DedupTable {
    DedupTable::new(DedupTableConfig {
        client_retry_window_s: seconds,
    })
}

#[test]
fn rejects_retries_beyond_window() {
    let mut table = table_with_window(2);
    let fresh = table.check("event-1", b"payload", 1_000);
    assert_eq!(fresh.decision, DedupDecision::Fresh);
    let dup = table.check("event-1", b"payload", 2_000);
    assert_eq!(dup.decision, DedupDecision::Duplicate);
    let expired = table.check("event-1", b"payload", 5_100);
    assert_eq!(expired.decision, DedupDecision::RetryExpired);
    assert_eq!(expired.status_reason, Some(PERMANENT_RETRY_EXPIRED));
}

#[test]
fn tracks_divergence_metrics() {
    let mut table = table_with_window(5);
    table.check("event-2", b"a", 100);
    let divergent = table.check("event-2", b"b", 200);
    assert_eq!(divergent.decision, DedupDecision::Divergent);
    assert_eq!(table.divergence_total(), 1);
}

#[test]
fn capacity_updates_trim_oldest_entries() {
    let mut table = table_with_window(10);
    for idx in 0..512u64 {
        table.check(&format!("event-{idx}"), b"x", idx);
    }
    assert_eq!(table.occupancy(), 512);
    let update = DedupCapacityUpdate {
        partition_id: "p0".into(),
        max_ingest_rate_per_partition: 5,
    };
    table.apply_capacity_update(&update);
    assert!(table.occupancy() <= 50);
}

use ceptra::{
    KeyApplyDecision, KeySequencer, PartitionKeyHasher, CEPTRA_AP_KEY_HASH_COLLISION_TOTAL,
};

struct ConstantHasher;

impl PartitionKeyHasher for ConstantHasher {
    fn hash(&self, _partition_id: &str, _key: &[u8]) -> u64 {
        1
    }
}

#[test]
fn enforces_last_write_wins_for_keys() {
    let mut sequencer = KeySequencer::new();
    let first = sequencer.apply("p1", b"key", 10);
    assert_eq!(first.decision, KeyApplyDecision::Applied);
    let stale = sequencer.apply("p1", b"key", 8);
    assert_eq!(stale.decision, KeyApplyDecision::Ignored { last_seqno: 10 });
    assert_eq!(sequencer.last_seqno("p1", b"key"), Some(10));
}

#[test]
fn records_hash_collisions_and_metrics() {
    let mut sequencer = KeySequencer::with_hasher(Box::new(ConstantHasher));
    let first = sequencer.apply("p1", b"alpha", 1);
    assert!(!first.collision_detected);
    let second = sequencer.apply("p2", b"beta", 1);
    assert!(second.collision_detected);
    assert_eq!(sequencer.collision_total(), 1);
    assert_eq!(
        CEPTRA_AP_KEY_HASH_COLLISION_TOTAL,
        "ceptra_ap_key_hash_collision_total"
    );
}

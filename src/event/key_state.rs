use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Metric name incremented whenever a SipHash collision is observed.
pub const CEPTRA_AP_KEY_HASH_COLLISION_TOTAL: &str = "ceptra_ap_key_hash_collision_total";

/// Decision returned after applying a key update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyApplyDecision {
    Applied,
    Ignored { last_seqno: u64 },
}

/// Outcome emitted by the sequencer, including collision information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyApplyOutcome {
    pub decision: KeyApplyDecision,
    pub collision_detected: bool,
}

/// Canonical key identity stored in the sequencer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyIdentity {
    partition_id: String,
    key_bytes: Vec<u8>,
}

impl KeyIdentity {
    pub fn new(partition_id: impl Into<String>, key_bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            partition_id: partition_id.into(),
            key_bytes: key_bytes.into(),
        }
    }

    fn matches(&self, partition_id: &str, key: &[u8]) -> bool {
        self.partition_id == partition_id && self.key_bytes == key
    }
}

#[derive(Debug, Clone)]
struct KeyRecord {
    identity: KeyIdentity,
    last_seqno: u64,
}

/// Hashing abstraction so tests can inject deterministic collisions.
pub trait PartitionKeyHasher: Send + Sync {
    fn hash(&self, partition_id: &str, key: &[u8]) -> u64;
}

/// SipHash-based hasher used in production.
pub struct SipHashPartitionHasher;

impl PartitionKeyHasher for SipHashPartitionHasher {
    fn hash(&self, partition_id: &str, key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        partition_id.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }
}

/// Tracks last-seen seqnos per key while detecting SipHash collisions.
pub struct KeySequencer {
    buckets: HashMap<u64, Vec<KeyRecord>>,
    collision_total: u64,
    hasher: Box<dyn PartitionKeyHasher>,
}

impl Default for KeySequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl KeySequencer {
    /// Creates the sequencer with the default SipHash hasher.
    pub fn new() -> Self {
        Self::with_hasher(Box::new(SipHashPartitionHasher))
    }

    /// Creates the sequencer with a custom hasher (used in tests).
    pub fn with_hasher(hasher: Box<dyn PartitionKeyHasher>) -> Self {
        Self {
            buckets: HashMap::new(),
            collision_total: 0,
            hasher,
        }
    }

    /// Applies an update and enforces last-write-wins semantics.
    pub fn apply(&mut self, partition_id: &str, key: &[u8], seqno: u64) -> KeyApplyOutcome {
        let hash = self.hasher.hash(partition_id, key);
        let bucket = self.buckets.entry(hash).or_default();
        for record in bucket.iter_mut() {
            if record.identity.matches(partition_id, key) {
                if seqno > record.last_seqno {
                    record.last_seqno = seqno;
                    return KeyApplyOutcome {
                        decision: KeyApplyDecision::Applied,
                        collision_detected: false,
                    };
                }
                return KeyApplyOutcome {
                    decision: KeyApplyDecision::Ignored {
                        last_seqno: record.last_seqno,
                    },
                    collision_detected: false,
                };
            }
        }
        let identity = KeyIdentity::new(partition_id.to_string(), key.to_vec());
        bucket.push(KeyRecord {
            identity,
            last_seqno: seqno,
        });
        let collision_detected = bucket.len() > 1;
        if collision_detected {
            self.collision_total += 1;
        }
        KeyApplyOutcome {
            decision: KeyApplyDecision::Applied,
            collision_detected,
        }
    }

    /// Returns the number of detected hash collisions.
    pub fn collision_total(&self) -> u64 {
        self.collision_total
    }

    /// Returns the last recorded seqno for a key, if present.
    pub fn last_seqno(&self, partition_id: &str, key: &[u8]) -> Option<u64> {
        let hash = self.hasher.hash(partition_id, key);
        self.buckets.get(&hash).and_then(|bucket| {
            bucket
                .iter()
                .find(|record| record.identity.matches(partition_id, key))
                .map(|record| record.last_seqno)
        })
    }
}

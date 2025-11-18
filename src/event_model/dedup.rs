use super::commit_rate::DedupCapacityUpdate;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{Hash, Hasher};

const DEDUP_SHARDS: usize = 1024;

/// CEP-local status reason emitted when dedup retries expire.
pub const PERMANENT_RETRY_EXPIRED: &str = "PERMANENT_RETRY_EXPIRED";

/// Configures the dedup table behaviour.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DedupTableConfig {
    pub client_retry_window_s: u64,
}

impl DedupTableConfig {
    pub fn retry_window_ms(&self) -> u64 {
        self.client_retry_window_s.saturating_mul(1_000)
    }
}

/// Result of processing an incoming append against the dedup table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupDecision {
    Fresh,
    Duplicate,
    Divergent,
    RetryExpired,
}

/// Outcome surfaced to callers, including status reasons suitable for clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupOutcome {
    pub decision: DedupDecision,
    pub status_reason: Option<&'static str>,
}

impl DedupOutcome {
    fn new(decision: DedupDecision) -> Self {
        let status_reason = match decision {
            DedupDecision::RetryExpired => Some(PERMANENT_RETRY_EXPIRED),
            _ => None,
        };
        Self {
            decision,
            status_reason,
        }
    }
}

/// Deduplication table sharded with SipHash for deterministic performance.
pub struct DedupTable {
    shards: Vec<DedupShard>,
    retry_window_ms: u64,
    retry_window_s: u64,
    max_entries: usize,
    occupancy: usize,
    divergence_total: u64,
}

impl DedupTable {
    /// Builds the dedup table with 1024 shards.
    pub fn new(config: DedupTableConfig) -> Self {
        Self {
            shards: (0..DEDUP_SHARDS).map(|_| DedupShard::default()).collect(),
            retry_window_ms: config.retry_window_ms(),
            retry_window_s: config.client_retry_window_s.max(1),
            max_entries: DEDUP_SHARDS,
            occupancy: 0,
            divergence_total: 0,
        }
    }

    /// Applies a dedup capacity update emitted by the control-plane reconciler.
    pub fn apply_capacity_update(&mut self, update: &DedupCapacityUpdate) {
        let window = self.retry_window_s.max(1);
        let capacity = update
            .max_ingest_rate_per_partition
            .saturating_mul(window)
            .max(1);
        self.max_entries = capacity as usize;
        self.enforce_capacity();
    }

    /// Processes an append and updates dedup occupancy/metrics.
    pub fn check(&mut self, event_id: &str, payload: &[u8], commit_tick_ms: u64) -> DedupOutcome {
        let shard_idx = self.shard_for(event_id);
        let shard = &mut self.shards[shard_idx];
        if let Some(entry) = shard.entries.get(event_id) {
            let age_ms = commit_tick_ms.saturating_sub(entry.last_seen_tick_ms);
            if age_ms > self.retry_window_ms {
                shard.remove_entry(event_id);
                self.occupancy = self.occupancy.saturating_sub(1);
                return DedupOutcome::new(DedupDecision::RetryExpired);
            }
            let payload_hash = siphash_bytes(payload);
            if payload_hash == entry.payload_hash {
                shard.update_timestamp(event_id, commit_tick_ms);
                return DedupOutcome::new(DedupDecision::Duplicate);
            }
            self.divergence_total += 1;
            return DedupOutcome::new(DedupDecision::Divergent);
        }
        shard.insert_new(event_id, payload, commit_tick_ms);
        self.occupancy += 1;
        self.occupancy = self
            .occupancy
            .saturating_sub(shard.evict_expired(commit_tick_ms, self.retry_window_ms));
        self.enforce_capacity();
        DedupOutcome::new(DedupDecision::Fresh)
    }

    /// Current number of entries stored across all shards.
    pub fn occupancy(&self) -> usize {
        self.occupancy
    }

    /// Total divergent duplicates observed since boot.
    pub fn divergence_total(&self) -> u64 {
        self.divergence_total
    }

    fn shard_for(&self, event_id: &str) -> usize {
        let hash = siphash_str(event_id);
        (hash as usize) & (DEDUP_SHARDS - 1)
    }

    fn enforce_capacity(&mut self) {
        if self.occupancy <= self.max_entries {
            return;
        }
        while self.occupancy > self.max_entries {
            let mut removed = false;
            for shard in &mut self.shards {
                if shard.remove_oldest() {
                    self.occupancy = self.occupancy.saturating_sub(1);
                    removed = true;
                    if self.occupancy <= self.max_entries {
                        break;
                    }
                }
            }
            if !removed {
                break;
            }
        }
    }
}

#[derive(Default)]
struct DedupShard {
    entries: HashMap<String, DedupEntry>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
}

impl DedupShard {
    fn insert_new(&mut self, event_id: &str, payload: &[u8], commit_tick_ms: u64) {
        let payload_hash = siphash_bytes(payload);
        self.entries.insert(
            event_id.to_string(),
            DedupEntry {
                payload_hash,
                last_seen_tick_ms: commit_tick_ms,
            },
        );
        self.heap.push(Reverse(HeapEntry::new(
            commit_tick_ms,
            event_id.to_string(),
        )));
    }

    fn update_timestamp(&mut self, event_id: &str, commit_tick_ms: u64) {
        if let Some(entry) = self.entries.get_mut(event_id) {
            entry.last_seen_tick_ms = commit_tick_ms;
            self.heap.push(Reverse(HeapEntry::new(
                commit_tick_ms,
                event_id.to_string(),
            )));
        }
    }

    fn remove_entry(&mut self, event_id: &str) {
        self.entries.remove(event_id);
    }

    fn evict_expired(&mut self, commit_tick_ms: u64, window_ms: u64) -> usize {
        let mut removed = 0;
        while let Some(oldest) = self.peek_oldest_entry() {
            if commit_tick_ms.saturating_sub(oldest.last_seen_tick_ms) <= window_ms {
                break;
            }
            if self.entries.remove(&oldest.event_id).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn remove_oldest(&mut self) -> bool {
        if let Some(oldest) = self.peek_oldest_entry() {
            return self.entries.remove(&oldest.event_id).is_some();
        }
        false
    }

    fn peek_oldest_entry(&mut self) -> Option<HeapEntry> {
        loop {
            let Reverse(candidate) = self.heap.peek()?.clone();
            match self.entries.get(&candidate.event_id) {
                Some(entry) if entry.last_seen_tick_ms == candidate.last_seen_tick_ms => {
                    return Some(candidate);
                }
                _ => {
                    self.heap.pop();
                }
            }
        }
    }
}

#[derive(Clone)]
struct DedupEntry {
    payload_hash: u64,
    last_seen_tick_ms: u64,
}

#[derive(Clone, Eq, PartialEq)]
struct HeapEntry {
    last_seen_tick_ms: u64,
    event_id: String,
}

impl HeapEntry {
    fn new(last_seen_tick_ms: u64, event_id: String) -> Self {
        Self {
            last_seen_tick_ms,
            event_id,
        }
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.last_seen_tick_ms
            .cmp(&other.last_seen_tick_ms)
            .then_with(|| other.event_id.cmp(&self.event_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn siphash_str(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

fn siphash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

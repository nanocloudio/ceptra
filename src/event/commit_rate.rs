use crate::event::commit_epoch::WalEntry;
use serde::{Deserialize, Serialize};

const DEFAULT_ALPHA: f64 = 0.2;
const SAMPLE_INTERVAL_MS: u64 = 30_000;
const MS_PER_SECOND: f64 = 1_000.0;

/// Snapshot persisted alongside checkpoints so commit rate state can resume deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CommitRateSnapshot {
    pub ewma_rate_per_sec: f64,
    pub last_tick_ms: Option<u64>,
    pub last_log_index: u64,
    pub sample_count: u64,
}

impl Default for CommitRateSnapshot {
    fn default() -> Self {
        Self {
            ewma_rate_per_sec: 0.0,
            last_tick_ms: None,
            last_log_index: 0,
            sample_count: 0,
        }
    }
}

/// Deterministic EWMA-based throughput estimator (`commit_rate_state`).
#[derive(Debug, Clone)]
pub struct CommitRateState {
    alpha: f64,
    ewma_rate_per_sec: f64,
    last_tick_ms: Option<u64>,
    last_log_index: u64,
    sample_count: u64,
}

impl CommitRateState {
    /// Creates a new state for the provided partition.
    pub fn new(_partition_id: impl Into<String>) -> Self {
        Self::with_alpha(DEFAULT_ALPHA)
    }

    /// Creates a state with a custom EWMA smoothing factor (helpful for tests).
    pub fn with_alpha(alpha: f64) -> Self {
        Self {
            alpha,
            ewma_rate_per_sec: 0.0,
            last_tick_ms: None,
            last_log_index: 0,
            sample_count: 0,
        }
    }

    /// Records a WAL entry containing a single ingested event.
    pub fn record_entry(&mut self, entry: &WalEntry) -> f64 {
        self.record_entry_with_events(entry, 1)
    }

    /// Records a WAL entry that batches `events` ingested events.
    pub fn record_entry_with_events(&mut self, entry: &WalEntry, events: usize) -> f64 {
        if let Some(last_tick) = self.last_tick_ms {
            let delta_ms = entry.commit_epoch_tick_ms.saturating_sub(last_tick).max(1);
            let events_per_sec = (events as f64) * MS_PER_SECOND / delta_ms as f64;
            self.ewma_rate_per_sec = if self.sample_count == 0 {
                events_per_sec
            } else {
                self.alpha * events_per_sec + (1.0 - self.alpha) * self.ewma_rate_per_sec
            };
            self.sample_count += 1;
        }
        self.last_tick_ms = Some(entry.commit_epoch_tick_ms);
        self.last_log_index = entry.log_index;
        self.ewma_rate_per_sec
    }

    /// Records an entire WAL segment.
    pub fn record_entries(&mut self, entries: &[WalEntry]) {
        for entry in entries {
            self.record_entry(entry);
        }
    }

    /// Returns the current EWMA (events per second).
    pub fn ewma_rate_per_sec(&self) -> f64 {
        self.ewma_rate_per_sec
    }

    /// Returns the last tick covered by this estimator.
    pub fn last_tick_ms(&self) -> Option<u64> {
        self.last_tick_ms
    }

    /// Number of EWMA samples applied so far.
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Returns the last processed log index.
    pub fn last_log_index(&self) -> u64 {
        self.last_log_index
    }

    /// Encodes the estimator into a checkpoint snapshot.
    pub fn snapshot(&self) -> CommitRateSnapshot {
        CommitRateSnapshot {
            ewma_rate_per_sec: self.ewma_rate_per_sec,
            last_tick_ms: self.last_tick_ms,
            last_log_index: self.last_log_index,
            sample_count: self.sample_count,
        }
    }

    /// Restores state from a checkpoint snapshot.
    pub fn from_snapshot(_partition_id: impl Into<String>, snapshot: &CommitRateSnapshot) -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            ewma_rate_per_sec: snapshot.ewma_rate_per_sec,
            last_tick_ms: snapshot.last_tick_ms,
            last_log_index: snapshot.last_log_index,
            sample_count: snapshot.sample_count,
        }
    }

    /// Replays WAL entries recorded after `snapshot` to rebuild deterministic state.
    pub fn rebuild_from_checkpoint(
        snapshot: Option<&CommitRateSnapshot>,
        wal_entries: &[WalEntry],
    ) -> Self {
        let mut state = if let Some(snapshot) = snapshot {
            CommitRateState::from_snapshot("replay", snapshot)
        } else {
            CommitRateState::new("replay")
        };
        for entry in wal_entries {
            if entry.log_index > state.last_log_index {
                state.record_entry(entry);
            }
        }
        state
    }

    /// Predicts the dedup capacity limit (ceil of EWMA).
    pub fn predicted_capacity(&self) -> u64 {
        self.ewma_rate_per_sec.ceil() as u64
    }
}

/// Update propagated to the dedup tables when the control-plane raises ingest limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupCapacityUpdate {
    pub partition_id: String,
    pub max_ingest_rate_per_partition: u64,
}

/// Control-plane reconciler that samples EWMA every 30 s and emits capacity markers.
#[derive(Debug, Clone)]
pub struct ControlPlaneReconciler {
    partition_id: String,
    sample_interval_ms: u64,
    last_sample_tick: Option<u64>,
    current_capacity: u64,
}

impl ControlPlaneReconciler {
    /// Creates a reconciler that samples at the spec-defined 30 s interval.
    pub fn new(partition_id: impl Into<String>) -> Self {
        Self::with_sample_interval(partition_id, SAMPLE_INTERVAL_MS)
    }

    /// Allows tests to override the sampling interval.
    pub fn with_sample_interval(partition_id: impl Into<String>, interval_ms: u64) -> Self {
        Self {
            partition_id: partition_id.into(),
            sample_interval_ms: interval_ms,
            last_sample_tick: None,
            current_capacity: 0,
        }
    }

    /// Samples the EWMA and returns an update when raising the ingest capacity.
    pub fn maybe_emit_update(
        &mut self,
        rate_state: &CommitRateState,
    ) -> Option<DedupCapacityUpdate> {
        let tick = rate_state.last_tick_ms()?;
        if rate_state.sample_count() == 0 {
            return None;
        }
        if let Some(last) = self.last_sample_tick {
            if tick.saturating_sub(last) < self.sample_interval_ms {
                return None;
            }
        }
        self.last_sample_tick = Some(tick);
        let proposed = rate_state.predicted_capacity();
        if proposed > self.current_capacity {
            self.current_capacity = proposed;
            Some(DedupCapacityUpdate {
                partition_id: self.partition_id.clone(),
                max_ingest_rate_per_partition: proposed,
            })
        } else {
            None
        }
    }
}

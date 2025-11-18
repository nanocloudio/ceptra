use super::commit_rate::CommitRateSnapshot;
use super::finalized::FinalizedHorizonSnapshot;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Instant;

/// Monotonic time source representing the Clustor HAL (`CLOCK_MONOTONIC_RAW`).
pub trait MonotonicClock {
    /// Returns the current monotonic timestamp in nanoseconds.
    fn now_ns(&mut self) -> u128;
}

/// System clock implementation backed by `Instant`.
#[derive(Clone)]
pub struct SystemMonotonicClock {
    start: Instant,
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl SystemMonotonicClock {
    /// Creates the system HAL clock wrapper.
    pub fn new() -> Self {
        Self::default()
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now_ns(&mut self) -> u128 {
        self.start.elapsed().as_nanos()
    }
}

/// CEP WAL entry annotated with the commit epoch tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalEntry {
    /// Log index associated with the entry.
    pub log_index: u64,
    /// Tick derived from `CLOCK_MONOTONIC_RAW` at 1 ms granularity.
    pub commit_epoch_tick_ms: u64,
    /// Payload mirrored from the upstream WAL.
    pub payload: Vec<u8>,
}

impl WalEntry {
    /// Creates a new WAL entry with the provided payload.
    pub fn new(log_index: u64, commit_epoch_tick_ms: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            log_index,
            commit_epoch_tick_ms,
            payload: payload.into(),
        }
    }
}

/// Persisted checkpoint fields required to restart commit epoch tracking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CommitEpochCheckpoint {
    /// Most recent `commit_epoch_now` tick observed before the checkpoint.
    pub commit_epoch_now: u64,
    /// `last_applied_index` stored alongside the checkpoint.
    pub last_applied_index: u64,
    /// Definition bundle version active at checkpoint time.
    #[serde(default)]
    pub def_version: u64,
    /// Policy version paired with the checkpoint (if declared).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_version: Option<u64>,
    /// Optional commit rate snapshot persisted with the checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_rate_snapshot: Option<CommitRateSnapshot>,
    /// Finalized horizon map persisted at checkpoint boundaries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_horizon_snapshot: Option<FinalizedHorizonSnapshot>,
}

impl CommitEpochCheckpoint {
    /// Serializes the checkpoint to JSON for storage.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Restores a checkpoint from JSON.
    pub fn from_json(payload: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(payload)
    }

    /// Attaches a commit rate snapshot to the checkpoint.
    pub fn with_commit_rate(mut self, snapshot: CommitRateSnapshot) -> Self {
        self.commit_rate_snapshot = Some(snapshot);
        self
    }

    /// Attaches definition + policy version metadata to the checkpoint.
    pub fn with_versions(mut self, def_version: u64, policy_version: Option<u64>) -> Self {
        self.def_version = def_version;
        self.policy_version = policy_version;
        self
    }

    /// Returns the commit rate snapshot, if present.
    pub fn commit_rate_snapshot(&self) -> Option<&CommitRateSnapshot> {
        self.commit_rate_snapshot.as_ref()
    }

    /// Attaches finalized horizon state to the checkpoint snapshot.
    pub fn with_finalized_horizon(mut self, snapshot: FinalizedHorizonSnapshot) -> Self {
        self.finalized_horizon_snapshot = Some(snapshot);
        self
    }

    /// Returns the finalized horizon snapshot, if present.
    pub fn finalized_horizon_snapshot(&self) -> Option<&FinalizedHorizonSnapshot> {
        self.finalized_horizon_snapshot.as_ref()
    }

    /// Returns the definition version stored in the checkpoint.
    pub fn def_version(&self) -> u64 {
        self.def_version
    }

    /// Returns the policy version stored in the checkpoint, when present.
    pub fn policy_version(&self) -> Option<u64> {
        self.policy_version
    }
}

/// Runtime state maintained while stamping WAL entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CommitEpochState {
    commit_epoch_now: u64,
    last_applied_index: u64,
    def_version: u64,
    policy_version: Option<u64>,
}

impl CommitEpochState {
    /// Returns the latest recorded commit epoch tick.
    pub fn commit_epoch_now(&self) -> u64 {
        self.commit_epoch_now
    }

    /// Returns the last applied log index observed by the ticker.
    pub fn last_applied_index(&self) -> u64 {
        self.last_applied_index
    }

    /// Updates the state using a checkpoint snapshot.
    pub fn apply_checkpoint(&mut self, checkpoint: &CommitEpochCheckpoint) {
        self.commit_epoch_now = checkpoint.commit_epoch_now;
        self.last_applied_index = checkpoint.last_applied_index;
        self.def_version = checkpoint.def_version;
        self.policy_version = checkpoint.policy_version;
    }

    /// Creates a checkpoint record mirroring the current state.
    pub fn to_checkpoint(&self) -> CommitEpochCheckpoint {
        CommitEpochCheckpoint {
            commit_epoch_now: self.commit_epoch_now,
            last_applied_index: self.last_applied_index,
            def_version: self.def_version,
            policy_version: self.policy_version,
            commit_rate_snapshot: None,
            finalized_horizon_snapshot: None,
        }
    }

    /// Records the active definition and policy versions.
    pub fn set_versions(&mut self, def_version: u64, policy_version: Option<u64>) {
        self.def_version = def_version;
        self.policy_version = policy_version;
    }

    /// Returns the definition version tracked by the state machine.
    pub fn def_version(&self) -> u64 {
        self.def_version
    }

    /// Returns the policy version tracked by the state machine.
    pub fn policy_version(&self) -> Option<u64> {
        self.policy_version
    }

    /// Applies a WAL entry that has already been stamped.
    pub fn apply_entry(&mut self, entry: &WalEntry) {
        if entry.log_index >= self.last_applied_index {
            self.commit_epoch_now = entry.commit_epoch_tick_ms;
            self.last_applied_index = entry.log_index;
        }
    }

    /// Rebuilds state after a leadership change by replaying WAL entries produced after a checkpoint.
    pub fn rebuild_from_checkpoint(
        checkpoint: &CommitEpochCheckpoint,
        wal_entries: &[WalEntry],
    ) -> Self {
        let mut state = CommitEpochState::default();
        state.apply_checkpoint(checkpoint);
        for entry in wal_entries {
            if entry.log_index > checkpoint.last_applied_index {
                state.apply_entry(entry);
            }
        }
        state
    }
}

/// Leader-side helper that stamps commit epoch ticks on WAL entries.
pub struct CommitEpochTicker<C: MonotonicClock> {
    clock: C,
    state: CommitEpochState,
    last_tick_ms: Option<u64>,
}

impl<C: MonotonicClock> CommitEpochTicker<C> {
    /// Creates a ticker with a fresh state.
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            state: CommitEpochState::default(),
            last_tick_ms: None,
        }
    }

    /// Seeds the ticker using data recovered from a checkpoint/WAL replay.
    pub fn with_state(clock: C, state: CommitEpochState) -> Self {
        let last_tick_ms = Some(state.commit_epoch_now());
        Self {
            clock,
            state,
            last_tick_ms,
        }
    }

    /// Returns the current commit epoch state.
    pub fn state(&self) -> &CommitEpochState {
        &self.state
    }

    /// Stamps a WAL entry with the next commit epoch tick (1 ms granularity).
    pub fn stamp_entry(&mut self, log_index: u64, payload: impl Into<Vec<u8>>) -> WalEntry {
        let tick = self.next_tick_ms();
        let entry = WalEntry::new(log_index, tick, payload);
        self.state.commit_epoch_now = tick;
        self.state.last_applied_index = log_index;
        entry
    }

    fn next_tick_ms(&mut self) -> u64 {
        let mut tick_ms = (self.clock.now_ns() / 1_000_000) as u64;
        if let Some(last) = self.last_tick_ms {
            if tick_ms <= last {
                tick_ms = last + 1;
            }
        }
        self.last_tick_ms = Some(tick_ms);
        tick_ms
    }
}

impl fmt::Debug for CommitEpochTicker<SystemMonotonicClock> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitEpochTicker")
            .field("commit_epoch_now", &self.state.commit_epoch_now)
            .field("last_applied_index", &self.state.last_applied_index)
            .finish()
    }
}

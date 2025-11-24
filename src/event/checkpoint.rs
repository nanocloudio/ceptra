use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Default interval for full checkpoints (30 seconds).
pub const FULL_CHECKPOINT_INTERVAL_MS: u64 = 30_000;
/// Maximum number of log entries permitted between full checkpoints.
pub const FULL_CHECKPOINT_ENTRY_INTERVAL: u64 = 50_000_000;
/// Maximum number of incrementals permitted before forcing a full snapshot.
pub const MAX_INCREMENTALS_PER_FULL: u8 = 3;

/// Kind of checkpoint emitted by the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointKind {
    Full,
    Incremental,
}

/// Outcome of attempting to write a checkpoint.
#[derive(Debug, Clone)]
pub enum CheckpointResult {
    Full(PersistedCheckpoint),
    Incremental(PersistedCheckpoint),
    Skipped(CheckpointSkipReason),
}

/// Reason why a checkpoint attempt was skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointSkipReason {
    NotDue,
    BandwidthExceeded,
}

/// Error surfaced when checkpoint persistence fails.
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("checkpoint persistence failed: {0}")]
    Persist(String),
}

/// Contract implemented by storage sinks (e.g. object stores, disks).
pub trait CheckpointSink {
    fn persist(&mut self, record: PersistedCheckpoint) -> Result<(), CheckpointError>;
}

/// Persisted checkpoint artifact, including checksum/signature metadata.
#[derive(Debug, Clone)]
pub struct PersistedCheckpoint {
    pub sequence: u64,
    pub applied_index: u64,
    pub kind: CheckpointKind,
    pub checksum: String,
    pub signature: String,
    pub bytes: usize,
    pub payload: Vec<u8>,
}

/// Writer that enforces cadence/capacity rules and emits full + incremental checkpoints.
pub struct CheckpointWriter<S: CheckpointSink> {
    sink: S,
    bandwidth_cap_bytes: u64,
    window_start_ms: u64,
    window_bytes: u64,
    window_initialized: bool,
    last_full_ms: u64,
    last_full_index: u64,
    incrementals_since_full: u8,
    sequence: u64,
    has_written_full: bool,
}

impl<S: CheckpointSink> CheckpointWriter<S> {
    /// Creates the writer with the provided sink and bandwidth cap (bytes/sec, 0 = unlimited).
    pub fn new(sink: S, bandwidth_cap_bytes: u64) -> Self {
        Self {
            sink,
            bandwidth_cap_bytes,
            window_start_ms: 0,
            window_bytes: 0,
            window_initialized: false,
            last_full_ms: 0,
            last_full_index: 0,
            incrementals_since_full: 0,
            sequence: 0,
            has_written_full: false,
        }
    }

    /// Serializes the provided snapshot via copy-on-write and decides whether to persist it.
    pub fn write_snapshot<T: Serialize>(
        &mut self,
        snapshot: &T,
        last_applied_index: u64,
        now_ms: u64,
    ) -> Result<CheckpointResult, CheckpointError> {
        let payload = serde_json::to_vec(snapshot).expect("snapshot serialization must succeed");
        self.write_payload(payload, last_applied_index, now_ms)
    }

    /// Persists an already-materialized payload (useful when snapshots are produced asynchronously).
    pub fn write_payload(
        &mut self,
        payload: Vec<u8>,
        last_applied_index: u64,
        now_ms: u64,
    ) -> Result<CheckpointResult, CheckpointError> {
        if !self.bandwidth_available(now_ms, payload.len() as u64) {
            return Ok(CheckpointResult::Skipped(
                CheckpointSkipReason::BandwidthExceeded,
            ));
        }

        let should_force_full = self.should_force_full(last_applied_index, now_ms);
        let should_write_incremental =
            !should_force_full && self.incrementals_since_full < MAX_INCREMENTALS_PER_FULL;
        if !should_force_full && !should_write_incremental {
            return Ok(CheckpointResult::Skipped(CheckpointSkipReason::NotDue));
        }

        let kind = if should_force_full {
            CheckpointKind::Full
        } else {
            CheckpointKind::Incremental
        };

        let record = self.build_record(payload, last_applied_index, kind);
        self.sink.persist(record.clone())?;
        self.sequence = self.sequence.saturating_add(1);
        self.window_bytes = self.window_bytes.saturating_add(record.bytes as u64);

        match kind {
            CheckpointKind::Full => {
                self.last_full_ms = now_ms;
                self.last_full_index = last_applied_index;
                self.incrementals_since_full = 0;
                self.has_written_full = true;
                Ok(CheckpointResult::Full(record))
            }
            CheckpointKind::Incremental => {
                self.incrementals_since_full = self.incrementals_since_full.saturating_add(1);
                Ok(CheckpointResult::Incremental(record))
            }
        }
    }

    fn bandwidth_window_reset(&mut self, now_ms: u64) {
        self.window_start_ms = now_ms;
        self.window_bytes = 0;
        self.window_initialized = true;
    }

    fn bandwidth_available(&mut self, now_ms: u64, bytes: u64) -> bool {
        if self.bandwidth_cap_bytes == 0 {
            return true;
        }
        if !self.window_initialized || now_ms.saturating_sub(self.window_start_ms) >= 1_000 {
            self.bandwidth_window_reset(now_ms);
        }
        self.window_bytes + bytes <= self.bandwidth_cap_bytes
    }

    fn should_force_full(&mut self, last_applied_index: u64, now_ms: u64) -> bool {
        if !self.has_written_full {
            return true;
        }
        let due_by_time = now_ms.saturating_sub(self.last_full_ms) >= FULL_CHECKPOINT_INTERVAL_MS;
        let due_by_entries = last_applied_index.saturating_sub(self.last_full_index)
            >= FULL_CHECKPOINT_ENTRY_INTERVAL;
        due_by_time || due_by_entries || self.incrementals_since_full >= MAX_INCREMENTALS_PER_FULL
    }

    fn build_record(
        &self,
        payload: Vec<u8>,
        last_applied_index: u64,
        kind: CheckpointKind,
    ) -> PersistedCheckpoint {
        let checksum = compute_checksum(&payload);
        let signature = compute_signature(&checksum, self.sequence);
        PersistedCheckpoint {
            sequence: self.sequence,
            applied_index: last_applied_index,
            kind,
            checksum,
            signature,
            bytes: payload.len(),
            payload,
        }
    }
}

fn compute_checksum(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    to_hex(&digest)
}

fn compute_signature(checksum: &str, sequence: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(checksum.as_bytes());
    hasher.update(sequence.to_be_bytes());
    let digest = hasher.finalize();
    to_hex(&digest)
}

fn to_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{:02x}", byte));
    }
    encoded
}

/// Loader that validates checkpoint chains and surfaces restore plans.
pub struct CheckpointLoader {
    max_incrementals: usize,
}

impl Default for CheckpointLoader {
    fn default() -> Self {
        Self {
            max_incrementals: MAX_INCREMENTALS_PER_FULL as usize,
        }
    }
}

impl CheckpointLoader {
    /// Creates a loader with the provided incremental cap.
    pub fn new(max_incrementals: usize) -> Self {
        Self { max_incrementals }
    }

    /// Restores the latest full + incremental checkpoint chain.
    pub fn load(
        &self,
        records: &[PersistedCheckpoint],
    ) -> Result<CheckpointRestorePlan, CheckpointRestoreError> {
        if records.is_empty() {
            return Err(CheckpointRestoreError::NoFullCheckpoint);
        }
        let mut ordered = records.to_vec();
        ordered.sort_by_key(|record| record.sequence);
        let mut full_pos = None;
        for (idx, record) in ordered.iter().enumerate().rev() {
            if record.kind == CheckpointKind::Full {
                self.validate(record)?;
                full_pos = Some(idx);
                break;
            }
        }
        let full_index = full_pos.ok_or(CheckpointRestoreError::NoFullCheckpoint)?;
        let full = ordered[full_index].clone();
        let mut incrementals = Vec::new();
        for record in ordered.iter().skip(full_index + 1) {
            match record.kind {
                CheckpointKind::Incremental => {
                    self.validate(record)?;
                    incrementals.push(record.clone());
                }
                CheckpointKind::Full => break,
            }
        }
        if incrementals.len() > self.max_incrementals {
            return Err(CheckpointRestoreError::ChainDegraded {
                reason: format!(
                    "incremental count {} exceeds max {}",
                    incrementals.len(),
                    self.max_incrementals
                ),
            });
        }
        let resume_index = incrementals
            .last()
            .map(|record| record.applied_index.saturating_add(1))
            .unwrap_or_else(|| full.applied_index.saturating_add(1));
        Ok(CheckpointRestorePlan {
            full,
            incrementals,
            resume_index,
        })
    }

    fn validate(&self, record: &PersistedCheckpoint) -> Result<(), CheckpointRestoreError> {
        let checksum = compute_checksum(&record.payload);
        if checksum != record.checksum {
            return Err(CheckpointRestoreError::ChainDegraded {
                reason: format!("checksum mismatch (sequence {})", record.sequence),
            });
        }
        let signature = compute_signature(&checksum, record.sequence);
        if signature != record.signature {
            return Err(CheckpointRestoreError::ChainDegraded {
                reason: format!("signature mismatch (sequence {})", record.sequence),
            });
        }
        Ok(())
    }
}

/// Restore plan produced by the loader.
#[derive(Debug, Clone)]
pub struct CheckpointRestorePlan {
    pub full: PersistedCheckpoint,
    pub incrementals: Vec<PersistedCheckpoint>,
    pub resume_index: u64,
}

/// Loader failure modes surfaced to the caller.
#[derive(Debug, Error)]
pub enum CheckpointRestoreError {
    #[error("checkpoint chain degraded: {reason}")]
    ChainDegraded { reason: String },
    #[error("no full checkpoint available")]
    NoFullCheckpoint,
}

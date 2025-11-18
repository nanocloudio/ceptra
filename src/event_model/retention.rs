use super::commit_rate::CommitRateSnapshot;

/// Describes the most recent full + incremental checkpoint chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointChain {
    /// Log index of the latest verified full checkpoint.
    pub full_checkpoint_index: u64,
    /// Log indices covered by subsequent incremental checkpoints.
    pub incremental_indices: Vec<u64>,
}

impl CheckpointChain {
    /// Returns the index guaranteed by the checkpoint chain.
    pub fn latest_index(&self) -> u64 {
        self.incremental_indices
            .iter()
            .copied()
            .max()
            .unwrap_or(self.full_checkpoint_index)
            .max(self.full_checkpoint_index)
    }
}

/// Inputs required to derive the dedup retention guard.
#[derive(Debug, Clone, PartialEq)]
pub struct DedupRetentionInputs {
    /// Latest finalized Raft index emitted by the AP.
    pub finalized_horizon_index: u64,
    /// Effective dedup retention window in seconds (per spec §3).
    pub dedup_retention_window_s: u64,
    /// Commit-rate snapshot persisted alongside checkpoints.
    pub commit_rate: CommitRateSnapshot,
}

/// Result of combining checkpoint and dedup constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    pub checkpoint_floor: u64,
    pub dedup_floor: u64,
    pub min_retain_index: u64,
}

/// Planner that derives the `min_retain_index` described in spec §10/§19.
pub struct MinRetainPlanner {
    checkpoint: CheckpointChain,
    dedup: DedupRetentionInputs,
}

impl MinRetainPlanner {
    pub fn new(checkpoint: CheckpointChain, dedup: DedupRetentionInputs) -> Self {
        Self { checkpoint, dedup }
    }

    /// Computes the checkpoint/dedup floors and the resulting `min_retain_index`.
    pub fn plan(&self) -> RetentionPlan {
        let checkpoint_floor = self.checkpoint.latest_index();
        let dedup_distance = self.index_distance_for(self.dedup.dedup_retention_window_s);
        let dedup_floor = self
            .dedup
            .finalized_horizon_index
            .saturating_sub(dedup_distance);
        let min_retain_index = checkpoint_floor.max(dedup_floor);
        RetentionPlan {
            checkpoint_floor,
            dedup_floor,
            min_retain_index,
        }
    }

    fn index_distance_for(&self, window_s: u64) -> u64 {
        let rate = self.dedup.commit_rate.ewma_rate_per_sec.max(1.0);
        let seconds = window_s.max(1) as f64;
        let distance = (seconds * rate).ceil() as u64;
        distance.max(1)
    }
}

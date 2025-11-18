//! Event model primitives spanning commit epochs, deduplication, and finalized horizons.
//!
//! This module hosts the building blocks for Epic 3 stories (commit ticks, dedup tables,
//! last-seqno tracking, etc.). Individual submodules are kept small so each story can
//! evolve independently.

pub mod checkpoint;
pub mod commit_epoch;
pub mod commit_rate;
pub mod dedup;
pub mod finalized;
pub mod key_state;
pub mod retention;
pub mod watermark;

pub use checkpoint::{
    CheckpointError, CheckpointKind, CheckpointLoader, CheckpointRestoreError,
    CheckpointRestorePlan, CheckpointResult, CheckpointSink, CheckpointSkipReason,
    CheckpointWriter, PersistedCheckpoint, FULL_CHECKPOINT_ENTRY_INTERVAL,
    FULL_CHECKPOINT_INTERVAL_MS, MAX_INCREMENTALS_PER_FULL,
};
pub use commit_epoch::{
    CommitEpochCheckpoint, CommitEpochState, CommitEpochTicker, MonotonicClock,
    SystemMonotonicClock, WalEntry,
};
pub use commit_rate::{
    CommitRateSnapshot, CommitRateState, ControlPlaneReconciler, DedupCapacityUpdate,
};
pub use dedup::{
    DedupDecision, DedupOutcome, DedupTable, DedupTableConfig, PERMANENT_RETRY_EXPIRED,
};
pub use finalized::{
    FinalizedHorizon, FinalizedHorizonSnapshot, FinalizedHorizonStall, MetricFinalization,
};
pub use key_state::{
    KeyApplyDecision, KeyApplyOutcome, KeyIdentity, KeySequencer, PartitionKeyHasher,
    CEPTRA_AP_KEY_HASH_COLLISION_TOTAL,
};
pub use retention::{CheckpointChain, DedupRetentionInputs, MinRetainPlanner, RetentionPlan};
pub use watermark::{
    GuardFloorSnapshot, MetricWatermarkOverride, MetricWatermarkSnapshot, MetricWatermarkTelemetry,
    WatermarkConfig, WatermarkDecision, WatermarkSnapshot, WatermarkTelemetry, WatermarkTracker,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-metric finalization entry persisted in checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricFinalization {
    pub metric: String,
    pub watermark_index: u64,
    pub correction_horizon: u64,
    pub finalized_index: u64,
}

impl MetricFinalization {
    fn new(metric: impl Into<String>, watermark_index: u64, correction_horizon: u64) -> Self {
        let metric = metric.into();
        let finalized_index = watermark_index.saturating_sub(correction_horizon);
        Self {
            metric,
            watermark_index,
            correction_horizon,
            finalized_index,
        }
    }
}

/// Snapshot serialized alongside checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FinalizedHorizonSnapshot {
    pub finalized_horizon_index: u64,
    pub metrics: Vec<MetricFinalization>,
}

/// Throttle information raised when the finalized horizon lags applied index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedHorizonStall {
    pub lag: u64,
    pub throttle_threshold: u64,
}

/// Tracks finalized horizon and per-metric finalization maps.
#[derive(Debug, Default)]
pub struct FinalizedHorizon {
    metrics: HashMap<String, MetricFinalization>,
    finalized_index: Option<u64>,
}

impl FinalizedHorizon {
    /// Creates an empty finalized horizon state.
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            finalized_index: None,
        }
    }

    /// Records a metric watermark + correction horizon pair.
    pub fn record_metric(
        &mut self,
        metric: impl Into<String>,
        watermark_index: u64,
        correction_horizon: u64,
    ) {
        let entry = MetricFinalization::new(metric, watermark_index, correction_horizon);
        self.metrics.insert(entry.metric.clone(), entry);
        self.recompute_overall();
    }

    fn recompute_overall(&mut self) {
        self.finalized_index = self
            .metrics
            .values()
            .map(|entry| entry.finalized_index)
            .min();
    }

    /// Returns the finalized horizon index, if defined.
    pub fn finalized_horizon_index(&self) -> Option<u64> {
        self.finalized_index
    }

    /// Builds a snapshot suitable for persisting in checkpoints.
    pub fn snapshot(&self) -> FinalizedHorizonSnapshot {
        let metrics = self.metrics.values().cloned().collect();
        FinalizedHorizonSnapshot {
            finalized_horizon_index: self.finalized_index.unwrap_or(0),
            metrics,
        }
    }

    /// Restores the horizon from a checkpoint snapshot.
    pub fn from_snapshot(snapshot: &FinalizedHorizonSnapshot) -> Self {
        let metrics = snapshot
            .metrics
            .iter()
            .cloned()
            .map(|entry| (entry.metric.clone(), entry))
            .collect();
        Self {
            metrics,
            finalized_index: Some(snapshot.finalized_horizon_index),
        }
    }

    /// Determines whether throttling is required when lag exceeds `2×lateness_allowance`.
    pub fn stall_state(
        &self,
        applied_index: u64,
        lateness_allowance: u64,
    ) -> Option<FinalizedHorizonStall> {
        let finalized = self.finalized_index?;
        let threshold = lateness_allowance.saturating_mul(2);
        let lag = applied_index.saturating_sub(finalized);
        if lag > threshold {
            Some(FinalizedHorizonStall {
                lag,
                throttle_threshold: threshold,
            })
        } else {
            None
        }
    }
}

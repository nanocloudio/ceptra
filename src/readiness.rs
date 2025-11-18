use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub const CHECKPOINT_AGE_REASON: &str = "checkpoint_age_exceeded";
pub const WATERMARK_STALL_REASON: &str = "watermark_stall";
pub const PARTITION_THRESHOLD_REASON: &str = "partitions_below_threshold";
pub const CHECKPOINT_CHAIN_DEGRADED: &str = "checkpoint_chain_degraded";
pub const APPLY_LAG_REASON: &str = "apply_lag_exceeded";
pub const REPLICATION_LAG_REASON: &str = "replication_lag_exceeded";
pub const CERTIFICATE_EXPIRY_REASON: &str = "certificate_expiring";
pub const WARMUP_PENDING_REASON: &str = "warmup_pending";
pub const BACKUP_LAG_REASON: &str = "backup_lag_exceeded";

/// Tracks CEP-specific `/readyz` reasons derived from time semantics.
#[derive(Debug, Default, Clone)]
pub struct ReadyzReasons {
    reasons: BTreeSet<String>,
}

impl ReadyzReasons {
    /// Creates an empty reason tracker.
    pub fn new() -> Self {
        Self {
            reasons: BTreeSet::new(),
        }
    }

    /// Records when checkpoint age exceeds `2×checkpoint_full_interval`.
    pub fn record_checkpoint_age(&mut self, checkpoint_age_ms: u64, full_interval_ms: u64) {
        if full_interval_ms == 0 {
            return;
        }
        let threshold = full_interval_ms.saturating_mul(2);
        if checkpoint_age_ms > threshold {
            self.reasons.insert(CHECKPOINT_AGE_REASON.to_string());
        }
    }

    /// Records watermark stalls when lag exceeds the provided allowance (ms).
    pub fn record_watermark_stall(&mut self, watermark_lag_ns: u64, stall_allowance_ms: u64) {
        let allowance_ns = stall_allowance_ms.saturating_mul(1_000_000);
        if watermark_lag_ns > allowance_ns {
            self.reasons.insert(WATERMARK_STALL_REASON.to_string());
        }
    }

    /// Records when backups fall behind the nightly export cadence.
    pub fn record_backup_lag(&mut self, backup_lag_ms: u64, threshold_ms: u64) {
        if threshold_ms == 0 {
            return;
        }
        if backup_lag_ms > threshold_ms {
            self.reasons.insert(BACKUP_LAG_REASON.to_string());
        }
    }

    /// Returns the accumulated reasons in deterministic order.
    pub fn reasons(&self) -> &BTreeSet<String> {
        &self.reasons
    }

    /// Adds a custom reason string.
    pub fn add_reason(&mut self, reason: impl Into<String>) {
        let text = reason.into();
        if !text.trim().is_empty() {
            self.reasons.insert(text);
        }
    }

    /// Marks the checkpoint chain as degraded so `/readyz` surfaces the reason.
    pub fn mark_checkpoint_chain_degraded(&mut self) {
        self.reasons.insert(CHECKPOINT_CHAIN_DEGRADED.to_string());
    }
}

#[derive(Debug, Default)]
struct PartitionGateState {
    ready: bool,
    critical: bool,
    reasons: BTreeSet<String>,
}

/// Aggregates per-partition readiness into a CEP-specific `/readyz` response.
#[derive(Debug, Default)]
pub struct ReadyGate {
    threshold: f64,
    partitions: BTreeMap<String, PartitionGateState>,
    reasons: BTreeSet<String>,
}

impl ReadyGate {
    /// Creates a new gate with the provided healthy-partition threshold.
    pub fn new(threshold: f64) -> Self {
        Self {
            threshold: threshold.clamp(0.0, 1.0),
            partitions: BTreeMap::new(),
            reasons: BTreeSet::new(),
        }
    }

    /// Returns the configured threshold.
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Records a partition state with default criticality.
    pub fn record_partition(
        &mut self,
        partition_id: impl Into<String>,
        ready: bool,
        reasons: &ReadyzReasons,
    ) {
        self.record_partition_with_criticality(partition_id, ready, reasons, true);
    }

    /// Records a partition state after evaluating the policy thresholds.
    pub fn record_partition_with_policy(
        &mut self,
        partition_id: impl Into<String>,
        inputs: PartitionReadinessInputs,
        policy: &PartitionReadinessPolicy,
    ) {
        let mut reasons = ReadyzReasons::new();
        let ready = policy.evaluate(&inputs, &mut reasons);
        self.record_partition(partition_id, ready, &reasons);
    }

    /// Records a partition state while overriding its criticality.
    pub fn record_partition_with_criticality(
        &mut self,
        partition_id: impl Into<String>,
        ready: bool,
        reasons: &ReadyzReasons,
        critical: bool,
    ) {
        self.partitions.insert(
            partition_id.into(),
            PartitionGateState {
                ready,
                critical,
                reasons: reasons.reasons().iter().cloned().collect(),
            },
        );
    }

    /// Overrides the criticality classification for the given partition.
    pub fn override_criticality(&mut self, partition_id: &str, critical: bool) {
        self.partitions
            .entry(partition_id.to_string())
            .and_modify(|state| state.critical = critical)
            .or_insert(PartitionGateState {
                ready: false,
                critical,
                reasons: BTreeSet::new(),
            });
    }

    /// Adds a global readiness reason.
    pub fn add_reason(&mut self, reason: impl Into<String>) {
        let text = reason.into();
        if !text.trim().is_empty() {
            self.reasons.insert(text);
        }
    }

    /// Evaluates the gate and returns the CEP `/readyz` section.
    pub fn evaluate(&self) -> CepReadyzReport {
        let (partitions, _, critical_ready, critical_total) = self.partition_snapshot();
        let ratio = ReadyGate::compute_ratio(critical_ready, critical_total);
        let meets_threshold = ratio + f64::EPSILON >= self.threshold;
        let mut reasons: Vec<String> = self.reasons.iter().cloned().collect();
        if !meets_threshold {
            reasons.push(PARTITION_THRESHOLD_REASON.to_string());
        }
        CepReadyzReport {
            ready: meets_threshold,
            threshold: self.threshold,
            reasons,
            partitions,
        }
    }

    /// Returns the metric series backing `ceptra_partition_ready` and `..._ratio`.
    pub fn metrics(&self) -> ReadyMetrics {
        let (_, partitions, critical_ready, critical_total) = self.partition_snapshot();
        ReadyMetrics {
            partitions,
            ready_ratio: ReadyGate::compute_ratio(critical_ready, critical_total),
        }
    }

    fn partition_snapshot(&self) -> (Vec<CepReadyzPartition>, Vec<PartitionReadyMetric>, u64, u64) {
        let mut partitions = Vec::with_capacity(self.partitions.len());
        let mut partition_metrics = Vec::with_capacity(self.partitions.len());
        let mut critical_ready = 0u64;
        let mut critical_total = 0u64;
        for (id, state) in &self.partitions {
            if state.critical {
                critical_total = critical_total.saturating_add(1);
                if state.ready {
                    critical_ready = critical_ready.saturating_add(1);
                }
            }
            partitions.push(CepReadyzPartition {
                partition_id: id.clone(),
                ready: state.ready,
                critical: state.critical,
                reasons: state.reasons.iter().cloned().collect(),
            });
            partition_metrics.push(PartitionReadyMetric {
                partition_id: id.clone(),
                ready: state.ready,
            });
        }
        partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
        partition_metrics.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
        (
            partitions,
            partition_metrics,
            critical_ready,
            critical_total,
        )
    }

    fn compute_ratio(critical_ready: u64, critical_total: u64) -> f64 {
        if critical_total == 0 {
            1.0
        } else {
            critical_ready as f64 / critical_total as f64
        }
    }
}

/// Inputs used to evaluate per-partition readiness thresholds.
#[derive(Debug, Clone, Default)]
pub struct PartitionReadinessInputs {
    pub apply_lag_ms: u64,
    pub replication_lag_ms: u64,
    pub checkpoint_age_ms: u64,
    pub checkpoint_full_interval_ms: u64,
    pub cert_expiry_ms: u64,
    pub warmup_ready: bool,
}

/// CEP-specific readiness policy built atop the Clustor barrier.
#[derive(Debug, Clone)]
pub struct PartitionReadinessPolicy {
    pub apply_lag_ms_threshold: u64,
    pub replication_lag_ms_threshold: u64,
    pub checkpoint_age_multiplier: u64,
    pub cert_expiry_ms_threshold: u64,
}

impl Default for PartitionReadinessPolicy {
    fn default() -> Self {
        Self {
            apply_lag_ms_threshold: 200,
            replication_lag_ms_threshold: 250,
            checkpoint_age_multiplier: 2,
            cert_expiry_ms_threshold: 3 * 24 * 60 * 60 * 1000,
        }
    }
}

impl PartitionReadinessPolicy {
    /// Applies the default thresholds to determine readiness.
    pub fn evaluate(&self, inputs: &PartitionReadinessInputs, reasons: &mut ReadyzReasons) -> bool {
        let mut ready = true;
        if inputs.apply_lag_ms > self.apply_lag_ms_threshold {
            reasons.add_reason(APPLY_LAG_REASON);
            ready = false;
        }
        if inputs.replication_lag_ms > self.replication_lag_ms_threshold {
            reasons.add_reason(REPLICATION_LAG_REASON);
            ready = false;
        }
        reasons.record_checkpoint_age(inputs.checkpoint_age_ms, inputs.checkpoint_full_interval_ms);
        if reasons.reasons().contains(CHECKPOINT_AGE_REASON) {
            ready = false;
        }
        if inputs.cert_expiry_ms <= self.cert_expiry_ms_threshold {
            reasons.add_reason(CERTIFICATE_EXPIRY_REASON);
            ready = false;
        }
        if !inputs.warmup_ready {
            reasons.add_reason(WARMUP_PENDING_REASON);
            ready = false;
        }
        ready
    }
}

/// Partition entry embedded within `/readyz`.
#[derive(Debug, Clone, Serialize)]
pub struct CepReadyzPartition {
    pub partition_id: String,
    pub ready: bool,
    pub critical: bool,
    pub reasons: Vec<String>,
}

/// CEP-specific readiness section.
#[derive(Debug, Clone, Serialize)]
pub struct CepReadyzReport {
    pub ready: bool,
    pub threshold: f64,
    pub reasons: Vec<String>,
    pub partitions: Vec<CepReadyzPartition>,
}

/// Metric entry for `ceptra_partition_ready{partition_id}`.
#[derive(Debug, Clone)]
pub struct PartitionReadyMetric {
    pub partition_id: String,
    pub ready: bool,
}

/// Aggregated metrics backing `/metrics` exports.
#[derive(Debug, Clone)]
pub struct ReadyMetrics {
    pub partitions: Vec<PartitionReadyMetric>,
    pub ready_ratio: f64,
}

impl ReadyMetrics {
    /// Deterministic partition-ready series.
    pub fn partitions(&self) -> &[PartitionReadyMetric] {
        &self.partitions
    }

    /// Healthy partition ratio.
    pub fn ready_ratio(&self) -> f64 {
        self.ready_ratio
    }
}

/// Automation that blocks rollouts when the healthy ratio falls below a threshold.
#[derive(Debug, Clone)]
pub struct RolloutAutomation {
    threshold: f64,
}

impl RolloutAutomation {
    pub fn new(threshold: f64) -> Self {
        Self {
            threshold: threshold.clamp(0.0, 1.0),
        }
    }

    pub fn evaluate(&self, gate: &ReadyGate) -> RolloutDecision {
        let report = gate.evaluate();
        let metrics = gate.metrics();
        if metrics.ready_ratio() + f64::EPSILON >= self.threshold {
            RolloutDecision::Proceed
        } else {
            let summary = if report.reasons.is_empty() {
                "no readiness reasons reported".to_string()
            } else {
                report.reasons.join(", ")
            };
            RolloutDecision::Block {
                reason: format!(
                    "ready ratio {:.2}% below target {:.2}% ({summary})",
                    metrics.ready_ratio() * 100.0,
                    self.threshold * 100.0
                ),
            }
        }
    }
}

/// Result produced by the rollout automation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutDecision {
    Proceed,
    Block { reason: String },
}

impl RolloutDecision {
    pub fn should_block(&self) -> bool {
        matches!(self, RolloutDecision::Block { .. })
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            RolloutDecision::Block { reason } => Some(reason.as_str()),
            RolloutDecision::Proceed => None,
        }
    }
}

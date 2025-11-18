use clustor::{
    ActivationBarrier, ActivationBarrierDecision, ActivationBarrierEvaluator,
    ActivationBarrierState, ActivationDigestError, ShadowApplyState, WarmupReadinessRecord,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub const SHADOW_PENDING: &str = "SHADOW_PENDING";
pub const SHADOW_REPLAYING: &str = "SHADOW_REPLAYING";
pub const SHADOW_WAITING_FOR_WAL: &str = "SHADOW_WAITING_FOR_WAL";
pub const SHADOW_READY: &str = "SHADOW_READY";
pub const SHADOW_FAILED: &str = "SHADOW_FAILED";
pub const SHADOW_TIMEOUT: &str = "SHADOW_TIMEOUT";

/// Tracks CEP-specific CE shadow state and activation gating.
#[derive(Debug, Clone)]
pub struct HotReloadGate {
    grace_window_ms: u64,
    overrides: BTreeSet<String>,
}

impl HotReloadGate {
    /// Creates a new gate with the provided grace window in milliseconds.
    pub fn new(grace_window_ms: u64) -> Self {
        Self {
            grace_window_ms,
            overrides: BTreeSet::new(),
        }
    }

    /// Returns the grace window used for readiness timeouts.
    pub fn grace_window_ms(&self) -> u64 {
        self.grace_window_ms
    }

    /// Adds an operator override for the given partition.
    pub fn add_override(&mut self, partition_id: impl Into<String>) -> bool {
        self.overrides.insert(partition_id.into())
    }

    /// Removes an operator override, returning true when it existed.
    pub fn remove_override(&mut self, partition_id: &str) -> bool {
        self.overrides.remove(partition_id)
    }

    /// Returns the current override set.
    pub fn overrides(&self) -> &BTreeSet<String> {
        &self.overrides
    }

    /// Evaluates the activation gate for the given barrier, readiness snapshot, and probes.
    pub fn evaluate(
        &self,
        barrier: &ActivationBarrier,
        readiness: &[WarmupReadinessRecord],
        probes: &[PartitionWarmupProbe],
        now_ms: u64,
    ) -> Result<HotReloadDecision, ActivationDigestError> {
        let barrier_decision = ActivationBarrierEvaluator::evaluate(barrier, readiness, now_ms)?;
        let readiness_map = readiness_map_for(barrier, readiness);
        let probe_map = probe_map_from(probes);
        let mut partitions = Vec::with_capacity(barrier.partitions.len());
        for partition in &barrier.partitions {
            let record = readiness_map.get(partition);
            let probe = probe_map.get(partition);
            let override_applied = self.overrides.contains(partition);
            let state = build_partition_state(
                partition,
                record,
                probe,
                barrier.readiness_threshold,
                self.grace_window_ms,
                now_ms,
                override_applied,
            );
            partitions.push(state);
        }
        partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
        let cep_state = classify_gate_state(&partitions);
        Ok(HotReloadDecision {
            barrier: barrier_decision,
            cep_state,
            partitions,
        })
    }
}

fn readiness_map_for(
    barrier: &ActivationBarrier,
    readiness: &[WarmupReadinessRecord],
) -> BTreeMap<String, WarmupReadinessRecord> {
    readiness
        .iter()
        .filter(|record| record.bundle_id == barrier.bundle_id)
        .map(|record| (record.partition_id.clone(), record.clone()))
        .collect()
}

fn probe_map_from(probes: &[PartitionWarmupProbe]) -> BTreeMap<String, PartitionWarmupProbe> {
    probes
        .iter()
        .map(|probe| (probe.partition_id.clone(), probe.clone()))
        .collect()
}

fn build_partition_state(
    partition_id: &str,
    record: Option<&WarmupReadinessRecord>,
    probe: Option<&PartitionWarmupProbe>,
    readiness_threshold: f64,
    grace_window_ms: u64,
    now_ms: u64,
    override_applied: bool,
) -> ShadowPartitionState {
    let mut shadow_state: String;
    let mut reasons = Vec::new();
    let mut warmup_ready_ratio = 0.0;
    let mut checkpoint_index = 0u64;
    let mut updated_at_ms = now_ms;
    let mut wal_head_index = 0u64;

    if let Some(probe) = probe {
        wal_head_index = probe.wal_head_index;
        for reason in &probe.reasons {
            if !reason.trim().is_empty() {
                reasons.push(reason.clone());
            }
        }
    }

    if let Some(record) = record {
        warmup_ready_ratio = record.warmup_ready_ratio;
        checkpoint_index = record.shadow_apply_checkpoint_index;
        updated_at_ms = record.updated_at_ms;
        if wal_head_index == 0 {
            wal_head_index = checkpoint_index;
        }
        match record.shadow_apply_state {
            ShadowApplyState::Pending => {
                shadow_state = SHADOW_PENDING.to_string();
            }
            ShadowApplyState::Replaying => {
                shadow_state = SHADOW_REPLAYING.to_string();
            }
            ShadowApplyState::Ready => {
                if warmup_ready_ratio + f64::EPSILON < readiness_threshold {
                    shadow_state = SHADOW_PENDING.to_string();
                    reasons.push(format!(
                        "warmup_ratio_below_threshold({:.2})",
                        warmup_ready_ratio
                    ));
                } else if checkpoint_index < wal_head_index {
                    shadow_state = SHADOW_WAITING_FOR_WAL.to_string();
                    reasons.push("wal_head_not_reached".to_string());
                } else {
                    shadow_state = SHADOW_READY.to_string();
                }
            }
            ShadowApplyState::Expired => {
                shadow_state = SHADOW_FAILED.to_string();
                reasons.push("shadow_apply_state=Expired".to_string());
            }
        }
    } else {
        shadow_state = SHADOW_PENDING.to_string();
        reasons.push("no_warmup_record".to_string());
    }

    if shadow_state != SHADOW_READY
        && grace_window_ms > 0
        && now_ms.saturating_sub(updated_at_ms) > grace_window_ms
    {
        shadow_state = SHADOW_TIMEOUT.to_string();
        reasons.push("grace_window_exceeded".to_string());
    }

    ShadowPartitionState {
        partition_id: partition_id.to_string(),
        shadow_state,
        shadow_apply_checkpoint_index: checkpoint_index,
        wal_head_index,
        warmup_ready_ratio,
        updated_at_ms,
        override_applied,
        reasons,
    }
}

fn classify_gate_state(partitions: &[ShadowPartitionState]) -> CepShadowGateState {
    let mut expired = Vec::new();
    let mut pending = Vec::new();
    for partition in partitions {
        if partition.shadow_state == SHADOW_READY
            || partition.override_applied
            || partition.shadow_state.is_empty()
        {
            continue;
        }
        if partition.shadow_state == SHADOW_FAILED || partition.shadow_state == SHADOW_TIMEOUT {
            expired.push(partition.partition_id.clone());
        } else {
            pending.push(partition.partition_id.clone());
        }
    }
    if !expired.is_empty() {
        CepShadowGateState::RequiresOverride { expired }
    } else if !pending.is_empty() {
        CepShadowGateState::Pending { blocked: pending }
    } else {
        CepShadowGateState::Ready
    }
}

/// Result of evaluating the CEP-specific activation predicates.
#[derive(Debug, Clone)]
pub struct HotReloadDecision {
    pub barrier: ActivationBarrierDecision,
    pub cep_state: CepShadowGateState,
    pub partitions: Vec<ShadowPartitionState>,
}

impl HotReloadDecision {
    /// True when both the Clustor barrier and CEP shadow state are ready.
    pub fn ready(&self) -> bool {
        matches!(self.barrier.state, ActivationBarrierState::Ready)
            && matches!(self.cep_state, CepShadowGateState::Ready)
    }
}

/// CEP-state for the activation barrier predicate.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum CepShadowGateState {
    Ready,
    Pending { blocked: Vec<String> },
    RequiresOverride { expired: Vec<String> },
}

/// Per-partition snapshot reported to `/readyz`.
#[derive(Debug, Clone, Serialize)]
pub struct ShadowPartitionState {
    pub partition_id: String,
    pub shadow_state: String,
    pub shadow_apply_checkpoint_index: u64,
    pub wal_head_index: u64,
    pub warmup_ready_ratio: f64,
    pub updated_at_ms: u64,
    pub override_applied: bool,
    pub reasons: Vec<String>,
}

/// CEP probe describing WAL progress and additional reasons.
#[derive(Debug, Clone, Default)]
pub struct PartitionWarmupProbe {
    pub partition_id: String,
    pub wal_head_index: u64,
    pub reasons: Vec<String>,
}

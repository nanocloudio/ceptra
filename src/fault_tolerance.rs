use crate::partition::ApAssignment;
use crate::readiness::ReadyzReasons;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

/// Status reason emitted when a partition can no longer advance the Raft epoch.
pub const PERMANENT_EPOCH_REASON: &str = "PERMANENT_EPOCH";

/// Error raised when failover orchestration encounters invalid input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FaultToleranceError {
    /// Partition is unknown to the orchestrator.
    #[error("unknown partition {partition_id}")]
    UnknownPartition { partition_id: String },
    /// Replica was not part of the recorded assignment.
    #[error("replica {replica} is not part of partition {partition_id}")]
    UnknownReplica {
        partition_id: String,
        replica: String,
    },
    /// Partition group did not issue `FreezePartitionGroup` before cutover.
    #[error("partition group {group_id} must be frozen via FreezePartitionGroup")]
    PartitionGroupNotFrozen { group_id: String },
    /// WAL shipping has not been verified for a partition group.
    #[error("missing verified WAL shipment for {group_id}")]
    WalVerificationMissing { group_id: String },
    /// Ingress routes are required so clients can be rerouted.
    #[error("no ingress routes provided for disaster recovery plan")]
    MissingIngressRoutes,
    /// Target epoch must not regress when promoting the DR cluster.
    #[error("target epoch {target_epoch} cannot be behind source {source_epoch}")]
    EpochMismatch {
        target_epoch: u64,
        source_epoch: u64,
    },
}

#[derive(Debug, Clone)]
struct WarmAssignmentState {
    leader: String,
    warm_followers: BTreeSet<String>,
    lease_epoch: u64,
}

impl WarmAssignmentState {
    fn new(assignment: &ApAssignment) -> Self {
        Self {
            leader: assignment.leader.clone(),
            warm_followers: assignment.warm_followers.iter().cloned().collect(),
            lease_epoch: assignment.lease_epoch,
        }
    }

    fn members(&self) -> BTreeSet<String> {
        let mut members = BTreeSet::new();
        members.insert(self.leader.clone());
        members.extend(self.warm_followers.iter().cloned());
        members
    }
}

/// Directive emitted to keep followers in warm-standby apply mode or promote them to leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarmStandbyDirective {
    ActivateWarmApply { replica: String },
    DeactivateWarmApply { replica: String },
    PromoteLeader { replica: String, lease_epoch: u64 },
}

/// Result of applying an AP assignment to the warm-standby manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmStandbyOutcome {
    pub partition_id: String,
    pub lease_epoch: u64,
    directives: Vec<WarmStandbyDirective>,
}

impl WarmStandbyOutcome {
    /// Directives required to keep the warm followers synchronized.
    pub fn directives(&self) -> &[WarmStandbyDirective] {
        &self.directives
    }

    /// Consumes the outcome and returns the directive list.
    pub fn into_directives(self) -> Vec<WarmStandbyDirective> {
        self.directives
    }
}

/// Tracks per-partition warm-standby configuration and orchestrates failovers.
#[derive(Debug, Default)]
pub struct WarmStandbyManager {
    assignments: HashMap<String, WarmAssignmentState>,
}

impl WarmStandbyManager {
    /// Creates an empty manager.
    pub fn new() -> Self {
        Self {
            assignments: HashMap::new(),
        }
    }

    /// Applies the latest AP assignment and returns directives to keep followers warm.
    pub fn apply_assignment(&mut self, assignment: ApAssignment) -> WarmStandbyOutcome {
        let mut directives = Vec::new();
        let new_state = WarmAssignmentState::new(&assignment);
        let partition_id = assignment.partition_id.clone();
        if let Some(previous) = self.assignments.get(&partition_id) {
            if previous.leader != new_state.leader {
                directives.push(WarmStandbyDirective::PromoteLeader {
                    replica: new_state.leader.clone(),
                    lease_epoch: new_state.lease_epoch,
                });
            }
            for replica in new_state
                .warm_followers
                .difference(&previous.warm_followers)
            {
                directives.push(WarmStandbyDirective::ActivateWarmApply {
                    replica: replica.clone(),
                });
            }
            for replica in previous
                .warm_followers
                .difference(&new_state.warm_followers)
            {
                directives.push(WarmStandbyDirective::DeactivateWarmApply {
                    replica: replica.clone(),
                });
            }
        } else {
            for replica in &new_state.warm_followers {
                directives.push(WarmStandbyDirective::ActivateWarmApply {
                    replica: replica.clone(),
                });
            }
        }
        self.assignments.insert(partition_id.clone(), new_state);
        WarmStandbyOutcome {
            partition_id,
            lease_epoch: assignment.lease_epoch,
            directives,
        }
    }

    fn assignment(&self, partition_id: &str) -> Result<&WarmAssignmentState, FaultToleranceError> {
        self.assignments
            .get(partition_id)
            .ok_or_else(|| FaultToleranceError::UnknownPartition {
                partition_id: partition_id.to_string(),
            })
    }

    /// Simulates replica loss to validate warm-standby promotions and PERMANENT_EPOCH fences.
    pub fn simulate_loss<I, S>(
        &self,
        partition_id: &str,
        lost_replicas: I,
    ) -> Result<FailoverEvent, FaultToleranceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let state = self.assignment(partition_id)?;
        let members = state.members();
        let mut lost_set = BTreeSet::new();
        for replica in lost_replicas {
            let replica = replica.as_ref().to_string();
            if !members.contains(&replica) {
                return Err(FaultToleranceError::UnknownReplica {
                    partition_id: partition_id.to_string(),
                    replica,
                });
            }
            lost_set.insert(replica);
        }
        let surviving: BTreeSet<String> = members.difference(&lost_set).cloned().collect();
        let mut promoted = None;
        if lost_set.contains(&state.leader) {
            if let Some(candidate) = surviving.iter().next().cloned() {
                promoted = Some(candidate);
            }
        }
        let total = members.len();
        let majority = total / 2 + 1;
        let mut reason = None;
        if surviving.len() < majority {
            reason = Some(PERMANENT_EPOCH_REASON);
            promoted = None;
        }
        Ok(FailoverEvent {
            partition_id: partition_id.to_string(),
            lease_epoch: state.lease_epoch,
            lost_replicas: lost_set.iter().cloned().collect(),
            surviving_replicas: surviving.iter().cloned().collect(),
            promoted,
            reason,
        })
    }
}

/// Result of simulating replica loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverEvent {
    pub partition_id: String,
    pub lease_epoch: u64,
    pub lost_replicas: Vec<String>,
    pub surviving_replicas: Vec<String>,
    pub promoted: Option<String>,
    pub reason: Option<&'static str>,
}

impl FailoverEvent {
    /// Returns true when the failure caused a permanent epoch fence.
    pub fn is_permanent_epoch(&self) -> bool {
        self.reason == Some(PERMANENT_EPOCH_REASON)
    }
}

/// Counter set exported to `/metrics` so operators can audit failover health.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FailoverTelemetry {
    failover_events: u64,
    warm_promotions: u64,
    permanent_epoch_events: u64,
}

impl FailoverTelemetry {
    /// Records a failover event.
    pub fn record(&mut self, event: &FailoverEvent) {
        self.failover_events = self.failover_events.saturating_add(1);
        if event.promoted.is_some() {
            self.warm_promotions = self.warm_promotions.saturating_add(1);
        }
        if event.is_permanent_epoch() {
            self.permanent_epoch_events = self.permanent_epoch_events.saturating_add(1);
        }
    }

    /// Metric samples emitted to `/metrics`.
    pub fn metrics(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("ceptra_failover_events_total", self.failover_events),
            ("ceptra_failover_promotions_total", self.warm_promotions),
            (
                "ceptra_failover_permanent_epoch_total",
                self.permanent_epoch_events,
            ),
        ]
    }
}

/// Partition-group metadata required for DR cutover.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionGroupPlan {
    pub group_id: String,
    pub freeze_issued: bool,
}

impl PartitionGroupPlan {
    pub fn new(group_id: impl Into<String>, freeze_issued: bool) -> Self {
        Self {
            group_id: group_id.into(),
            freeze_issued,
        }
    }
}

/// WAL shipment verification results per partition group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalShipment {
    pub group_id: String,
    pub verified: bool,
    pub last_applied_offset: u64,
}

impl WalShipment {
    pub fn new(group_id: impl Into<String>, last_applied_offset: u64, verified: bool) -> Self {
        Self {
            group_id: group_id.into(),
            verified,
            last_applied_offset,
        }
    }
}

/// End-to-end DR orchestration plan shared with the cutover tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DisasterRecoveryPlan {
    pub primary_cluster: String,
    pub dr_cluster: String,
    pub partition_groups: Vec<PartitionGroupPlan>,
    pub wal_shipments: Vec<WalShipment>,
    pub ingress_routes: Vec<String>,
    pub source_epoch: u64,
    pub target_epoch: u64,
}

impl DisasterRecoveryPlan {
    /// Ensures the plan is valid and produces the resulting cutover report.
    pub fn cutover(&self) -> Result<DrCutoverReport, FaultToleranceError> {
        self.ensure_frozen()?;
        self.ensure_wal_verified()?;
        self.ensure_ingress()?;
        self.ensure_epochs()?;
        Ok(DrCutoverReport {
            primary_cluster: self.primary_cluster.clone(),
            dr_cluster: self.dr_cluster.clone(),
            fenced_groups: self
                .partition_groups
                .iter()
                .map(|plan| plan.group_id.clone())
                .collect(),
            verified_shipments: self
                .wal_shipments
                .iter()
                .filter(|shipment| shipment.verified)
                .map(|shipment| shipment.group_id.clone())
                .collect(),
            ingress_rerouted: self.ingress_routes.clone(),
            reconciled_epoch: self.target_epoch,
        })
    }

    fn ensure_frozen(&self) -> Result<(), FaultToleranceError> {
        for group in &self.partition_groups {
            if !group.freeze_issued {
                return Err(FaultToleranceError::PartitionGroupNotFrozen {
                    group_id: group.group_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn ensure_wal_verified(&self) -> Result<(), FaultToleranceError> {
        for group in &self.partition_groups {
            let verified = self
                .wal_shipments
                .iter()
                .any(|shipment| shipment.group_id == group.group_id && shipment.verified);
            if !verified {
                return Err(FaultToleranceError::WalVerificationMissing {
                    group_id: group.group_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn ensure_ingress(&self) -> Result<(), FaultToleranceError> {
        if self.ingress_routes.is_empty() {
            return Err(FaultToleranceError::MissingIngressRoutes);
        }
        Ok(())
    }

    fn ensure_epochs(&self) -> Result<(), FaultToleranceError> {
        if self.target_epoch < self.source_epoch {
            return Err(FaultToleranceError::EpochMismatch {
                target_epoch: self.target_epoch,
                source_epoch: self.source_epoch,
            });
        }
        Ok(())
    }

    /// Primary cluster identifier.
    pub fn primary_cluster(&self) -> &str {
        &self.primary_cluster
    }

    /// DR cluster identifier.
    pub fn dr_cluster(&self) -> &str {
        &self.dr_cluster
    }
}

/// Result emitted after validating and executing a DR plan.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DrCutoverReport {
    pub primary_cluster: String,
    pub dr_cluster: String,
    pub fenced_groups: Vec<String>,
    pub verified_shipments: Vec<String>,
    pub ingress_rerouted: Vec<String>,
    pub reconciled_epoch: u64,
}

/// Default cadence for CP snapshots, checkpoints, and exports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSchedule {
    cp_snapshot_interval_ms: u64,
    checkpoint_full_interval_ms: u64,
    nightly_export_interval_ms: u64,
}

impl BackupSchedule {
    /// Creates the spec-defined schedule (CP snapshots every 60s, checkpoints every 30s, nightly exports).
    pub fn standard() -> Self {
        Self {
            cp_snapshot_interval_ms: 60_000,
            checkpoint_full_interval_ms: 30_000,
            nightly_export_interval_ms: 86_400_000,
        }
    }

    pub fn cp_snapshot_interval_ms(&self) -> u64 {
        self.cp_snapshot_interval_ms
    }

    pub fn checkpoint_full_interval_ms(&self) -> u64 {
        self.checkpoint_full_interval_ms
    }

    pub fn nightly_export_interval_ms(&self) -> u64 {
        self.nightly_export_interval_ms
    }

    /// Plans the next set of operations relative to `now_ms` and the configured `R_compact` value.
    pub fn plan(&self, now_ms: u64, r_compact: u64) -> BackupStatus {
        BackupStatus {
            next_cp_snapshot_ms: now_ms.saturating_add(self.cp_snapshot_interval_ms),
            next_checkpoint_ms: now_ms.saturating_add(self.checkpoint_full_interval_ms),
            wal_compaction_batches: r_compact.max(1),
            next_export_ms: now_ms.saturating_add(self.nightly_export_interval_ms),
        }
    }
}

/// Snapshot of upcoming backup work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupStatus {
    pub next_cp_snapshot_ms: u64,
    pub next_checkpoint_ms: u64,
    pub wal_compaction_batches: u64,
    pub next_export_ms: u64,
}

/// Tracks recent backup activity so readiness can surface lag or overdue drills.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackupLagTracker {
    last_cp_snapshot_ms: Option<u64>,
    last_checkpoint_ms: Option<u64>,
    last_export_ms: Option<u64>,
    last_restore_drill_ms: Option<u64>,
}

impl BackupLagTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_cp_snapshot(&mut self, timestamp_ms: u64) {
        self.last_cp_snapshot_ms = Some(timestamp_ms);
    }

    pub fn record_checkpoint(&mut self, timestamp_ms: u64) {
        self.last_checkpoint_ms = Some(timestamp_ms);
    }

    pub fn record_export(&mut self, timestamp_ms: u64) {
        self.last_export_ms = Some(timestamp_ms);
    }

    pub fn record_restore_drill(&mut self, timestamp_ms: u64) {
        self.last_restore_drill_ms = Some(timestamp_ms);
    }

    /// Returns the lag since the last object-store export if recorded.
    pub fn backup_lag_ms(&self, now_ms: u64) -> Option<u64> {
        self.last_export_ms.map(|ts| now_ms.saturating_sub(ts))
    }

    /// Returns the time since the last checkpoint was written.
    pub fn checkpoint_age_ms(&self, now_ms: u64) -> Option<u64> {
        self.last_checkpoint_ms.map(|ts| now_ms.saturating_sub(ts))
    }

    /// Returns true when a quarterly restore drill has not been executed in time.
    pub fn restore_drill_due(&self, now_ms: u64) -> bool {
        const RESTORE_DRILL_INTERVAL_MS: u64 = 90 * 86_400_000;
        match self.last_restore_drill_ms {
            Some(ts) => now_ms.saturating_sub(ts) > RESTORE_DRILL_INTERVAL_MS,
            None => true,
        }
    }

    /// Appends `/readyz` reasons whenever backup lag exceeds the scheduled threshold.
    pub fn apply_readyz(
        &self,
        now_ms: u64,
        schedule: &BackupSchedule,
        reasons: &mut ReadyzReasons,
    ) {
        let lag_ms = self
            .backup_lag_ms(now_ms)
            .unwrap_or_else(|| schedule.nightly_export_interval_ms().saturating_add(1));
        reasons.record_backup_lag(lag_ms, schedule.nightly_export_interval_ms());
    }
}

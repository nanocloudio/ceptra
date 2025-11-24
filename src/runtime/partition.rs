use clustor::{PlacementRecord, PlacementSnapshot};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use thiserror::Error;

/// Number of replicas per partition group (RPG).
pub const RPG_REPLICA_COUNT: usize = 3;

/// Deterministic hash used to select a partition for a given logical key.
pub fn hash_partition_key(key: impl AsRef<[u8]>) -> u64 {
    // 64-bit FNV-1a keeps the hash stable across toolchains without extra dependencies.
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    key.as_ref().iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

/// Error raised while constructing or consuming placement metadata.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartitionError {
    #[error("partition {partition_id} declared multiple times")]
    DuplicatePartition { partition_id: String },
    #[error("partition table requires at least one RPG")]
    EmptyPlacements,
    #[error("expected {expected} replicas for partition {partition_id} but observed {found}")]
    InvalidReplicaCount {
        partition_id: String,
        expected: usize,
        found: usize,
    },
    #[error("lease leader {leader} for {partition_id} is not part of the placement")]
    LeaderMismatch {
        partition_id: String,
        leader: String,
    },
    #[error("lease update references unknown partition {partition_id}")]
    UnknownPartition { partition_id: String },
    #[error("lease epoch {lease_epoch} for {partition_id} is stale (current {current_epoch})")]
    StaleLease {
        partition_id: String,
        lease_epoch: u64,
        current_epoch: u64,
    },
}

/// Placement details for a single partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacement {
    partition_id: String,
    routing_epoch: u64,
    lease_epoch: u64,
    members: Vec<String>,
}

impl PartitionPlacement {
    fn from_record(record: PlacementRecord) -> Result<Self, PartitionError> {
        if record.members.len() != RPG_REPLICA_COUNT {
            return Err(PartitionError::InvalidReplicaCount {
                partition_id: record.partition_id,
                expected: RPG_REPLICA_COUNT,
                found: record.members.len(),
            });
        }
        Ok(Self {
            partition_id: record.partition_id,
            routing_epoch: record.routing_epoch,
            lease_epoch: record.lease_epoch,
            members: record.members,
        })
    }

    /// Identifier assigned by the control plane.
    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    /// Routing epoch enforced by the control plane.
    pub fn routing_epoch(&self) -> u64 {
        self.routing_epoch
    }

    /// Last known lease epoch for this placement.
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Replica identifiers that participate in the RPG.
    pub fn replicas(&self) -> &[String] {
        &self.members
    }
}

impl From<PlacementSnapshot> for PartitionPlacement {
    fn from(snapshot: PlacementSnapshot) -> Self {
        Self {
            partition_id: snapshot.record.partition_id,
            routing_epoch: snapshot.record.routing_epoch,
            lease_epoch: snapshot.record.lease_epoch,
            members: snapshot.record.members,
        }
    }
}

/// Topology built from placement metadata.
#[derive(Debug, Clone)]
pub struct PartitionTopology {
    placements: Vec<PartitionPlacement>,
    index_by_id: HashMap<String, usize>,
}

impl PartitionTopology {
    /// Builds the topology from control-plane placement records.
    pub fn from_records(
        records: impl IntoIterator<Item = PlacementRecord>,
    ) -> Result<Self, PartitionError> {
        let mut placements = Vec::new();
        let mut index_by_id = HashMap::new();
        for record in records {
            let partition_id = record.partition_id.clone();
            let placement = PartitionPlacement::from_record(record)?;
            if index_by_id
                .insert(partition_id.clone(), placements.len())
                .is_some()
            {
                return Err(PartitionError::DuplicatePartition { partition_id });
            }
            placements.push(placement);
        }
        if placements.is_empty() {
            return Err(PartitionError::EmptyPlacements);
        }
        Ok(Self {
            placements,
            index_by_id,
        })
    }

    /// Builds the topology from cached placement snapshots.
    pub fn from_snapshots(
        snapshots: impl IntoIterator<Item = PlacementSnapshot>,
    ) -> Result<Self, PartitionError> {
        Self::from_records(snapshots.into_iter().map(|snapshot| snapshot.record))
    }

    /// Routes a logical key to a deterministic partition.
    pub fn route(&self, key: impl AsRef<[u8]>) -> PartitionRoute<'_> {
        let hash = hash_partition_key(key);
        let idx = (hash % self.placements.len() as u64) as usize;
        PartitionRoute {
            key_hash: hash,
            placement: &self.placements[idx],
        }
    }

    /// Returns the placement metadata for a partition, if present.
    pub fn placement(&self, partition_id: &str) -> Option<&PartitionPlacement> {
        self.index_by_id
            .get(partition_id)
            .and_then(|idx| self.placements.get(*idx))
    }

    /// All known placements in deterministic order.
    pub fn placements(&self) -> &[PartitionPlacement] {
        &self.placements
    }
}

/// Routing decision derived from a logical key.
#[derive(Debug, Clone, Copy)]
pub struct PartitionRoute<'a> {
    key_hash: u64,
    placement: &'a PartitionPlacement,
}

impl<'a> PartitionRoute<'a> {
    /// Returns the hash derived from the logical key.
    pub fn key_hash(&self) -> u64 {
        self.key_hash
    }

    /// Returns the selected placement.
    pub fn placement(&self) -> &'a PartitionPlacement {
        self.placement
    }

    /// Partition identifier.
    pub fn partition_id(&self) -> &'a str {
        self.placement.partition_id()
    }

    /// Replica identifiers participating in the RPG.
    pub fn replicas(&self) -> &'a [String] {
        self.placement.replicas()
    }
}

/// Clustor lease update describing the current Raft leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseUpdate {
    pub partition_id: String,
    pub lease_epoch: u64,
    pub leader: String,
}

/// Tracks active AP owner for each partition based on lease events.
#[derive(Debug, Default)]
pub struct ApLeaderAffinity {
    assignments: HashMap<String, ApAssignment>,
}

impl ApLeaderAffinity {
    /// Applies a lease update; returns a reassignment when the leader changes.
    pub fn apply(
        &mut self,
        topology: &PartitionTopology,
        update: LeaseUpdate,
    ) -> Result<Option<ApReassignment>, PartitionError> {
        let placement = topology.placement(&update.partition_id).ok_or_else(|| {
            PartitionError::UnknownPartition {
                partition_id: update.partition_id.clone(),
            }
        })?;
        if !placement
            .replicas()
            .iter()
            .any(|member| member == &update.leader)
        {
            return Err(PartitionError::LeaderMismatch {
                partition_id: placement.partition_id().to_string(),
                leader: update.leader,
            });
        }

        match self.assignments.entry(update.partition_id.clone()) {
            Entry::Vacant(vacant) => {
                let assignment = ApAssignment::from_update(placement, update);
                vacant.insert(assignment.clone());
                Ok(Some(ApReassignment {
                    partition_id: assignment.partition_id.clone(),
                    previous_leader: None,
                    new_leader: assignment.leader.clone(),
                    lease_epoch: assignment.lease_epoch,
                }))
            }
            Entry::Occupied(mut occupied) => {
                let current = occupied.get();
                if update.lease_epoch < current.lease_epoch {
                    return Err(PartitionError::StaleLease {
                        partition_id: current.partition_id.clone(),
                        lease_epoch: update.lease_epoch,
                        current_epoch: current.lease_epoch,
                    });
                }
                if update.lease_epoch == current.lease_epoch && current.leader == update.leader {
                    return Ok(None);
                }
                let new_assignment = ApAssignment::from_update(placement, update);
                let previous = occupied.get().leader.clone();
                occupied.insert(new_assignment.clone());
                Ok(Some(ApReassignment {
                    partition_id: new_assignment.partition_id.clone(),
                    previous_leader: Some(previous),
                    new_leader: new_assignment.leader.clone(),
                    lease_epoch: new_assignment.lease_epoch,
                }))
            }
        }
    }

    /// Returns the current AP assignment for a partition.
    pub fn assignment(&self, partition_id: &str) -> Option<&ApAssignment> {
        self.assignments.get(partition_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApAssignment {
    pub partition_id: String,
    pub leader: String,
    pub warm_followers: Vec<String>,
    pub lease_epoch: u64,
}

impl ApAssignment {
    fn from_update(placement: &PartitionPlacement, update: LeaseUpdate) -> Self {
        let warm_followers = placement
            .replicas()
            .iter()
            .filter(|replica| *replica != &update.leader)
            .cloned()
            .collect();
        Self {
            partition_id: placement.partition_id().to_string(),
            leader: update.leader,
            warm_followers,
            lease_epoch: update.lease_epoch,
        }
    }
}

/// Event generated when the AP assignment changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApReassignment {
    pub partition_id: String,
    pub previous_leader: Option<String>,
    pub new_leader: String,
    pub lease_epoch: u64,
}

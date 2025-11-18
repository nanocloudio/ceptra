use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Pod participating in the CEPtra StatefulSet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PodIdentity {
    pub name: String,
    pub ordinal: usize,
    pub ready: bool,
}

impl PodIdentity {
    pub fn new(name: impl Into<String>, ordinal: usize, ready: bool) -> Self {
        Self {
            name: name.into(),
            ordinal,
            ready,
        }
    }
}

/// Plan describing the elected voters/learners and partition layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BootstrapPlan {
    pub voters: Vec<String>,
    pub learners: Vec<String>,
    pub assignments: BTreeMap<String, Vec<String>>,
}

/// Action issued during a pod preStop hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreStopAction {
    pub transfer_to: Option<String>,
    pub fence_epoch: u64,
}

/// Controller responsible for bootstrapping and scaling CEPtra StatefulSets.
pub struct BootstrapController;

impl BootstrapController {
    pub fn plan(
        pods: Vec<PodIdentity>,
        cp_quorum: usize,
        partitions: Vec<String>,
    ) -> BootstrapPlan {
        let peers = Self::discover(pods);
        let voters = Self::elect_voters(&peers, cp_quorum);
        let voters_set: BTreeSet<_> = voters.iter().cloned().collect();
        let learners = peers
            .iter()
            .filter(|pod| !voters_set.contains(&pod.name))
            .map(|pod| pod.name.clone())
            .collect();
        let assignments = Self::assign_partitions(&peers, &partitions);
        BootstrapPlan {
            voters,
            learners,
            assignments,
        }
    }

    pub fn discover(mut pods: Vec<PodIdentity>) -> Vec<PodIdentity> {
        pods.sort_by_key(|pod| pod.ordinal);
        pods
    }

    pub fn elect_voters(peers: &[PodIdentity], cp_quorum: usize) -> Vec<String> {
        peers
            .iter()
            .filter(|pod| pod.ready)
            .take(cp_quorum)
            .map(|pod| pod.name.clone())
            .collect()
    }

    pub fn assign_partitions(
        peers: &[PodIdentity],
        partitions: &[String],
    ) -> BTreeMap<String, Vec<String>> {
        let mut assignments: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let active: Vec<&PodIdentity> = peers.iter().filter(|pod| pod.ready).collect();
        if active.is_empty() {
            return assignments;
        }
        for (idx, partition_id) in partitions.iter().enumerate() {
            let peer_idx = idx % active.len();
            assignments
                .entry(active[peer_idx].name.clone())
                .or_default()
                .push(partition_id.clone());
        }
        assignments
    }

    pub fn plan_pre_stop(
        peers: &[PodIdentity],
        departing_pod: &str,
        current_leader: &str,
        current_epoch: u64,
    ) -> PreStopAction {
        let transfer_to = if departing_pod == current_leader {
            Self::next_ready(peers, departing_pod)
        } else {
            None
        };
        PreStopAction {
            transfer_to,
            fence_epoch: current_epoch.saturating_add(1),
        }
    }

    fn next_ready(peers: &[PodIdentity], departing: &str) -> Option<String> {
        let mut candidates: Vec<&PodIdentity> = peers
            .iter()
            .filter(|pod| pod.ready && pod.name != departing)
            .collect();
        candidates.sort_by_key(|pod| pod.ordinal);
        candidates.first().map(|pod| pod.name.clone())
    }
}

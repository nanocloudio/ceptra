use crate::runtime::threading::{
    DeviceQueuePin, PartitionRole, PartitionThreadPins, ThreadPinTarget,
};
use thiserror::Error;

/// NVMe or NIC queue specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSpec {
    pub name: String,
    pub queues: usize,
}

impl DeviceSpec {
    pub fn new(name: impl Into<String>, queues: usize) -> Self {
        Self {
            name: name.into(),
            queues,
        }
    }
}

/// NUMA node topology used for affinity planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumaNodeSpec {
    pub id: u32,
    pub cpus: Vec<usize>,
    pub memory_gb: u64,
    pub nvme: Vec<DeviceSpec>,
    pub nic: Vec<DeviceSpec>,
}

impl NumaNodeSpec {
    pub fn new(
        id: u32,
        cpus: Vec<usize>,
        memory_gb: u64,
        nvme: Vec<DeviceSpec>,
        nic: Vec<DeviceSpec>,
    ) -> Self {
        Self {
            id,
            cpus,
            memory_gb,
            nvme,
            nic,
        }
    }
}

/// Resulting pinning for a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPinning {
    pub partition_id: String,
    pub numa_node: u32,
    pub memory_gb: u64,
    pub pins: PartitionThreadPins,
}

impl PartitionPinning {
    pub fn thread_pins(&self) -> &PartitionThreadPins {
        &self.pins
    }
}

/// Error emitted while building a pinning plan.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum NumaPlanError {
    #[error("topology must include at least one node")]
    EmptyTopology,
    #[error("node {node_id} does not expose any CPUs")]
    NodeMissingCpus { node_id: u32 },
    #[error("node {node_id} was assigned no partitions")]
    NodeUntouched { node_id: u32 },
}

/// Planner responsible for assigning partitions to NUMA nodes.
#[derive(Debug)]
pub struct NumaPlanner {
    nodes: Vec<NumaNodeState>,
}

impl NumaPlanner {
    pub fn new(nodes: Vec<NumaNodeSpec>) -> Result<Self, NumaPlanError> {
        if nodes.is_empty() {
            return Err(NumaPlanError::EmptyTopology);
        }
        let mut states = Vec::new();
        for spec in nodes {
            if spec.cpus.is_empty() {
                return Err(NumaPlanError::NodeMissingCpus { node_id: spec.id });
            }
            states.push(NumaNodeState::new(spec));
        }
        Ok(Self { nodes: states })
    }

    /// Plans pinning for the provided partition identifiers.
    pub fn plan_partitions(
        &mut self,
        partition_ids: impl IntoIterator<Item = String>,
    ) -> Result<Vec<PartitionPinning>, NumaPlanError> {
        let ids: Vec<String> = partition_ids.into_iter().collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let node_count = self.nodes.len();
        let mut distribution = vec![0usize; node_count];
        for (idx, _) in ids.iter().enumerate() {
            distribution[idx % node_count] += 1;
        }
        for (idx, count) in distribution.iter().enumerate() {
            self.nodes[idx].desire_assignments(*count);
        }

        let mut plans = Vec::with_capacity(ids.len());
        for (idx, partition_id) in ids.into_iter().enumerate() {
            let node_idx = idx % node_count;
            let pinning = self.nodes[node_idx].plan_partition(&partition_id)?;
            plans.push(pinning);
        }
        Ok(plans)
    }
}

#[derive(Debug)]
struct NumaNodeState {
    spec: NumaNodeSpec,
    cpu_cursor: usize,
    nvme_cursor: usize,
    nic_cursor: usize,
    planned: usize,
    issued: usize,
    memory_issued: u64,
    nvme_pins: Vec<DeviceQueuePin>,
    nic_pins: Vec<DeviceQueuePin>,
}

impl NumaNodeState {
    fn new(spec: NumaNodeSpec) -> Self {
        let nvme_pins = expand_devices(&spec.nvme);
        let nic_pins = expand_devices(&spec.nic);
        Self {
            spec,
            cpu_cursor: 0,
            nvme_cursor: 0,
            nic_cursor: 0,
            planned: 0,
            issued: 0,
            memory_issued: 0,
            nvme_pins,
            nic_pins,
        }
    }

    fn desire_assignments(&mut self, count: usize) {
        self.planned = count;
        self.issued = 0;
        self.memory_issued = 0;
    }

    fn plan_partition(&mut self, partition_id: &str) -> Result<PartitionPinning, NumaPlanError> {
        if self.planned == 0 {
            return Err(NumaPlanError::NodeUntouched {
                node_id: self.spec.id,
            });
        }
        let mut pins = PartitionThreadPins::new();
        for role in PartitionRole::all() {
            let cpu = self.next_cpu();
            let mut device_queues = Vec::new();
            if let Some(pin) = self.next_nvme() {
                device_queues.push(pin);
            }
            if let Some(pin) = self.next_nic() {
                device_queues.push(pin);
            }
            pins.insert(
                role,
                ThreadPinTarget {
                    numa_node: self.spec.id,
                    cpus: vec![cpu],
                    device_queues,
                },
            );
        }
        let memory_gb = self.next_memory_slice();
        Ok(PartitionPinning {
            partition_id: partition_id.to_string(),
            numa_node: self.spec.id,
            memory_gb,
            pins,
        })
    }

    fn next_cpu(&mut self) -> usize {
        let cpu = self.spec.cpus[self.cpu_cursor % self.spec.cpus.len()];
        self.cpu_cursor += 1;
        cpu
    }

    fn next_nvme(&mut self) -> Option<DeviceQueuePin> {
        if self.nvme_pins.is_empty() {
            return None;
        }
        let pin = self.nvme_pins[self.nvme_cursor % self.nvme_pins.len()].clone();
        self.nvme_cursor += 1;
        Some(pin)
    }

    fn next_nic(&mut self) -> Option<DeviceQueuePin> {
        if self.nic_pins.is_empty() {
            return None;
        }
        let pin = self.nic_pins[self.nic_cursor % self.nic_pins.len()].clone();
        self.nic_cursor += 1;
        Some(pin)
    }

    fn next_memory_slice(&mut self) -> u64 {
        self.issued += 1;
        let remaining = self
            .spec
            .memory_gb
            .saturating_sub(self.memory_issued)
            .max(1);
        let remaining_slots = (self.planned.saturating_sub(self.issued) + 1) as u64;
        let slice = remaining / remaining_slots;
        self.memory_issued += slice;
        slice
    }
}

fn expand_devices(specs: &[DeviceSpec]) -> Vec<DeviceQueuePin> {
    let mut pins = Vec::new();
    for spec in specs {
        for queue in 0..spec.queues {
            pins.push(DeviceQueuePin {
                device: spec.name.clone(),
                queue,
            });
        }
    }
    pins
}

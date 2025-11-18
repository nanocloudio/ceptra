use ceptra::{
    DeviceSpec, NumaNodeSpec, NumaPlanError, NumaPlanner, PartitionPinning, PartitionRole,
};

fn sample_topology() -> Vec<NumaNodeSpec> {
    vec![
        NumaNodeSpec::new(
            0,
            vec![0, 1, 2, 3],
            64,
            vec![DeviceSpec::new("nvme0", 2)],
            vec![DeviceSpec::new("nic0", 2)],
        ),
        NumaNodeSpec::new(
            1,
            vec![4, 5, 6, 7],
            64,
            vec![DeviceSpec::new("nvme1", 2)],
            vec![DeviceSpec::new("nic1", 2)],
        ),
    ]
}

fn assert_partition(plan: &PartitionPinning) {
    for role in PartitionRole::all() {
        let pin = plan
            .thread_pins()
            .pin_for(role)
            .unwrap_or_else(|| panic!("missing pin for {:?}", role));
        assert_eq!(pin.numa_node, plan.numa_node);
        assert!(!pin.cpus.is_empty());
    }
}

#[test]
fn planner_assigns_partitions_round_robin() {
    let mut planner = NumaPlanner::new(sample_topology()).unwrap();
    let plans = planner
        .plan_partitions(vec!["p0".to_string(), "p1".to_string()])
        .unwrap();
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].numa_node, 0);
    assert_eq!(plans[1].numa_node, 1);
    assert_partition(&plans[0]);
    assert_partition(&plans[1]);
}

#[test]
fn planner_errors_without_topology() {
    let err = NumaPlanner::new(vec![]).unwrap_err();
    assert_eq!(err, NumaPlanError::EmptyTopology);
}

#[test]
fn planner_requires_cpu_inventory() {
    let err = NumaPlanner::new(vec![NumaNodeSpec::new(0, vec![], 32, vec![], vec![])]).unwrap_err();
    assert_eq!(err, NumaPlanError::NodeMissingCpus { node_id: 0 });
}

use ceptra::{ApLeaderAffinity, LeaseUpdate, PartitionError, PartitionTopology, RPG_REPLICA_COUNT};
use clustor::PlacementRecord;

fn placement_record(
    partition_id: &str,
    routing_epoch: u64,
    lease_epoch: u64,
    members: [&str; RPG_REPLICA_COUNT],
) -> PlacementRecord {
    PlacementRecord {
        partition_id: partition_id.into(),
        routing_epoch,
        lease_epoch,
        members: members.iter().map(|m| (*m).to_string()).collect(),
    }
}

#[test]
fn routes_keys_deterministically() {
    let topology = PartitionTopology::from_records(vec![
        placement_record("p0", 1, 1, ["a0", "a1", "a2"]),
        placement_record("p1", 1, 1, ["b0", "b1", "b2"]),
        placement_record("p2", 1, 1, ["c0", "c1", "c2"]),
    ])
    .expect("topology builds");
    let first = topology.route("tenant-42").partition_id().to_string();
    for _ in 0..10 {
        assert_eq!(topology.route("tenant-42").partition_id(), first);
    }
    let different = topology.route("tenant-43").partition_id().to_string();
    assert_ne!(first, different);
}

#[test]
fn rejects_invalid_replica_counts() {
    let record = PlacementRecord {
        partition_id: "p0".into(),
        routing_epoch: 1,
        lease_epoch: 1,
        members: vec!["a0".into(), "a1".into()],
    };
    let err = PartitionTopology::from_records(vec![record]).expect_err("invalid replica count");
    assert!(matches!(
        err,
        PartitionError::InvalidReplicaCount {
            expected: RPG_REPLICA_COUNT,
            ..
        }
    ));
}

#[test]
fn leader_affinity_tracks_changes() {
    let topology =
        PartitionTopology::from_records(vec![placement_record("p42", 7, 3, ["r0", "r1", "r2"])])
            .unwrap();
    let mut affinity = ApLeaderAffinity::default();
    let first = affinity
        .apply(
            &topology,
            LeaseUpdate {
                partition_id: "p42".into(),
                lease_epoch: 10,
                leader: "r1".into(),
            },
        )
        .expect("valid lease")
        .expect("assignment should be recorded");
    assert_eq!(first.previous_leader, None);
    assert_eq!(first.new_leader, "r1");

    let second = affinity
        .apply(
            &topology,
            LeaseUpdate {
                partition_id: "p42".into(),
                lease_epoch: 11,
                leader: "r2".into(),
            },
        )
        .expect("valid lease")
        .expect("leader change should reassign");
    assert_eq!(second.previous_leader.as_deref(), Some("r1"));
    assert_eq!(second.new_leader, "r2");

    let assignment = affinity.assignment("p42").expect("assignment recorded");
    assert_eq!(assignment.leader, "r2");
    assert_eq!(assignment.warm_followers.len(), 2);
}

#[test]
fn leader_affinity_rejects_stale_updates() {
    let topology =
        PartitionTopology::from_records(vec![placement_record("p9", 4, 1, ["x0", "x1", "x2"])])
            .unwrap();
    let mut affinity = ApLeaderAffinity::default();
    affinity
        .apply(
            &topology,
            LeaseUpdate {
                partition_id: "p9".into(),
                lease_epoch: 5,
                leader: "x0".into(),
            },
        )
        .expect("initial lease should apply")
        .expect("assignment created");
    let err = affinity
        .apply(
            &topology,
            LeaseUpdate {
                partition_id: "p9".into(),
                lease_epoch: 4,
                leader: "x1".into(),
            },
        )
        .expect_err("stale epoch should fail");
    assert!(matches!(
        err,
        PartitionError::StaleLease {
            lease_epoch: 4,
            current_epoch: 5,
            ..
        }
    ));
}

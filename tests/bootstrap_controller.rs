use ceptra::{BootstrapController, PodIdentity, PreStopAction};

fn sample_pods() -> Vec<PodIdentity> {
    vec![
        PodIdentity::new("pod-0", 0, true),
        PodIdentity::new("pod-1", 1, true),
        PodIdentity::new("pod-2", 2, false),
    ]
}

#[test]
fn bootstrap_plan_elects_first_ready_voters() {
    let plan = BootstrapController::plan(
        sample_pods(),
        2,
        vec!["p0".into(), "p1".into(), "p2".into()],
    );
    assert_eq!(plan.voters, vec!["pod-0", "pod-1"]);
    assert!(plan.learners.contains(&"pod-2".to_string()));
    let pod0 = plan.assignments.get("pod-0").unwrap();
    assert_eq!(pod0.len(), 2);
}

#[test]
fn pre_stop_transfers_leadership_and_fences_epochs() {
    let peers = sample_pods();
    let action = BootstrapController::plan_pre_stop(&peers, "pod-0", "pod-0", 41);
    assert_eq!(
        action,
        PreStopAction {
            transfer_to: Some("pod-1".into()),
            fence_epoch: 42
        }
    );

    let no_transfer = BootstrapController::plan_pre_stop(&peers, "pod-1", "pod-0", 10);
    assert_eq!(
        no_transfer,
        PreStopAction {
            transfer_to: None,
            fence_epoch: 11
        }
    );
}

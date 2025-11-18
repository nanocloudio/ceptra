use ceptra::{RuleVersionSelector, RuleVersionSnapshot};

#[test]
fn selector_switches_versions_at_activation_barrier() {
    let mut selector = RuleVersionSelector::new(1, Some(1));
    assert_eq!(
        selector.active(),
        &RuleVersionSnapshot {
            def_version: 1,
            policy_version: Some(1)
        }
    );
    selector.stage(50, 2, Some(2));
    let before = selector.select(40);
    assert_eq!(before.def_version, 1);
    assert_eq!(before.policy_version, Some(1));
    let after = selector.select(50);
    assert_eq!(after.def_version, 2);
    assert_eq!(after.policy_version, Some(2));
    selector.commit(60);
    assert_eq!(selector.active().def_version, 2);
    assert_eq!(selector.active().policy_version, Some(2));
    assert!(selector.pending().is_none());
}

#[test]
fn selector_allows_clearing_pending_versions() {
    let mut selector = RuleVersionSelector::new(3, None);
    selector.stage(100, 4, Some(1));
    selector.clear_pending();
    let snapshot = selector.select(150);
    assert_eq!(snapshot.def_version, 3);
    assert_eq!(snapshot.policy_version, None);
}

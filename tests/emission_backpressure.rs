use ceptra::{ChannelPolicies, ChannelPolicy, DerivedEmission, EmissionBatch, EmissionDropReason};

fn derived(channel: &str, id: &str) -> DerivedEmission {
    DerivedEmission::new(
        channel,
        "schema.alerts",
        format!("payload-{id}"),
        format!("derived-{id}"),
        "rule",
        1,
    )
}

#[test]
fn drop_oldest_records_log_entries() {
    let mut policies = ChannelPolicies::default();
    policies.set("alerts", ChannelPolicy::drop_oldest(1));
    let mut batch = EmissionBatch::default();
    batch.record(derived("alerts", "evt-1"), &policies);
    batch.record(derived("alerts", "evt-2"), &policies);
    assert_eq!(batch.drop_metrics().get("alerts"), Some(&1));
    let logs = batch.drop_logs();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].channel, "alerts");
    assert_eq!(logs[0].reason, EmissionDropReason::QueueFullDropOldest);
}

#[test]
fn block_with_timeout_logs_overflow() {
    let mut policies = ChannelPolicies::default();
    policies.set("blocked", ChannelPolicy::block_with_timeout(1, 50));
    let mut batch = EmissionBatch::default();
    batch.record(derived("blocked", "evt-1"), &policies);
    batch.record(derived("blocked", "evt-2"), &policies);
    assert_eq!(batch.drop_metrics().get("blocked"), Some(&1));
    let logs = batch.drop_logs();
    assert_eq!(
        logs.last().unwrap().reason,
        EmissionDropReason::QueueFullBlockTimeout
    );
}

#[test]
fn dead_letter_logs_overflow_and_loop() {
    let mut policies = ChannelPolicies::default();
    policies.set("alerts", ChannelPolicy::dead_letter(1, "dead-letter"));
    let mut batch = EmissionBatch::default();
    batch.record(derived("alerts", "evt-1"), &policies);
    batch.record(derived("alerts", "evt-2"), &policies);
    batch.record(derived("alerts", "evt-3"), &policies);
    assert!(batch
        .drop_logs()
        .iter()
        .any(|log| log.reason == EmissionDropReason::DeadLetterOverflow));

    let mut loop_policies = ChannelPolicies::default();
    loop_policies.set("loop", ChannelPolicy::dead_letter(1, "loop"));
    let mut loop_batch = EmissionBatch::default();
    loop_batch.record(derived("loop", "evt-1"), &loop_policies);
    loop_batch.record(derived("loop", "evt-2"), &loop_policies);
    assert!(loop_batch
        .drop_logs()
        .iter()
        .any(|log| log.reason == EmissionDropReason::DeadLetterLoop));
}

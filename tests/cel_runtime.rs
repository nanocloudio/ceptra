use ceptra::{
    CelComparisonOp, CelError, CelEvaluationRequest, CelExpr, CelRule, CelRuntime, CelSandbox,
    CommittedEvent, MetricBinding, WalEntry,
};
use std::collections::{BTreeMap, BTreeSet};

fn allowed_labels(metric: &str, labels: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
    let mut map = BTreeMap::new();
    let mut set = BTreeSet::new();
    for label in labels {
        set.insert((*label).to_string());
    }
    map.insert(metric.to_string(), set);
    map
}

fn dummy_event() -> CommittedEvent {
    let wal = WalEntry::new(1, 1, "payload");
    CommittedEvent::new(
        "partition-a".to_string(),
        "evt-1".to_string(),
        "key".to_string(),
        wal,
    )
}

#[test]
fn cel_rule_rejects_unknown_labels() {
    let expr = CelExpr::LabelEquals {
        metric: "metric.sum".to_string(),
        label: "region".to_string(),
        value: "us-east-1".to_string(),
    };
    let allowed = allowed_labels("metric.sum", &["lane"]);
    let err = CelRule::new(
        "alerts",
        "schema.default",
        expr,
        allowed,
        "payload",
        "rule-a",
        1,
    );
    assert!(matches!(
        err,
        Err(CelError::InvalidLabel {
            metric,
            label
        }) if metric == "metric.sum" && label == "region"
    ));
}

#[test]
fn cel_sandbox_emits_when_conditions_match() {
    let expr = CelExpr::And(vec![
        CelExpr::Compare {
            metric: "metric.sum".to_string(),
            op: CelComparisonOp::Gt,
            value: 5.0,
        },
        CelExpr::LabelEquals {
            metric: "metric.sum".to_string(),
            label: "lane".to_string(),
            value: "default".to_string(),
        },
    ]);
    let mut labels = BTreeMap::new();
    labels.insert("lane".to_string(), "default".to_string());
    let binding = MetricBinding::with_value("metric.sum", 10.0).with_labels(labels);
    let request = CelEvaluationRequest {
        event: &dummy_event(),
        bindings: std::slice::from_ref(&binding),
        lane_bitmap: 1,
    };
    let rule = CelRule::new(
        "alerts",
        "schema.default",
        expr,
        allowed_labels("metric.sum", &["lane"]),
        "payload",
        "rule-a",
        7,
    )
    .expect("rule should compile");
    let mut sandbox = CelSandbox::new(vec![rule]);
    let result = sandbox.evaluate(request);
    assert_eq!(result.derived.len(), 1);
    assert_eq!(result.derived[0].channel, "alerts");
    assert_eq!(result.derived[0].payload, b"payload");
}

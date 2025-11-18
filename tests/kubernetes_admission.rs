use std::collections::BTreeMap;

use ceptra::{validate_node_labels, AdmissionError, REQUIRED_NODE_LABELS};

fn baseline_labels() -> BTreeMap<String, String> {
    REQUIRED_NODE_LABELS
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn admission_passes_with_required_labels() {
    let labels = baseline_labels();
    assert!(validate_node_labels(&labels).is_ok());
}

#[test]
fn admission_detects_missing_label() {
    let mut labels = baseline_labels();
    labels.remove("ceptra.dev/anti-affinity-domain");
    let err = validate_node_labels(&labels).unwrap_err();
    assert_eq!(
        err,
        AdmissionError::MissingLabel {
            label: "ceptra.dev/anti-affinity-domain".to_string()
        }
    );
}

#[test]
fn admission_detects_wrong_policy() {
    let mut labels = baseline_labels();
    labels.insert(
        "node.kubernetes.io/cpu-manager-policy".to_string(),
        "none".to_string(),
    );
    let err = validate_node_labels(&labels).unwrap_err();
    assert_eq!(
        err,
        AdmissionError::ValueMismatch {
            label: "node.kubernetes.io/cpu-manager-policy".to_string(),
            expected: "static".to_string(),
            found: "none".to_string(),
        }
    );
}

use std::collections::BTreeMap;
use thiserror::Error;

/// Labels required on CEPtra worker nodes to ensure NUMA-safe scheduling.
pub const REQUIRED_NODE_LABELS: &[(&str, &str)] = &[
    ("node.kubernetes.io/cpu-manager-policy", "static"),
    ("node.kubernetes.io/topology-manager-policy", "restricted"),
    ("ceptra.dev/anti-affinity-domain", "enabled"),
];

/// Error raised when a node does not meet admission requirements.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmissionError {
    #[error("node missing label {label}")]
    MissingLabel { label: String },
    #[error("node label {label} expected {expected} but observed {found}")]
    ValueMismatch {
        label: String,
        expected: String,
        found: String,
    },
}

/// Validates that the provided node labels satisfy CEPtra expectations.
pub fn validate_node_labels(labels: &BTreeMap<String, String>) -> Result<(), AdmissionError> {
    for (label, expected) in REQUIRED_NODE_LABELS {
        match labels.get(*label) {
            Some(value) if value == expected => continue,
            Some(value) => {
                return Err(AdmissionError::ValueMismatch {
                    label: (*label).to_string(),
                    expected: (*expected).to_string(),
                    found: value.clone(),
                })
            }
            None => {
                return Err(AdmissionError::MissingLabel {
                    label: (*label).to_string(),
                })
            }
        }
    }
    Ok(())
}

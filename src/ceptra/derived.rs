use std::collections::BTreeSet;

pub const DERIVED_FLAG: &str = "DERIVED";

/// Derived event emitted by the CEL layer toward downstream channels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedEmission {
    pub channel: String,
    pub schema_key: String,
    pub payload: Vec<u8>,
    pub derived_event_id: String,
    pub rule_id: String,
    pub rule_version: u64,
    pub flags: BTreeSet<String>,
}

impl DerivedEmission {
    pub fn new(
        channel: impl Into<String>,
        schema_key: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        derived_event_id: impl Into<String>,
        rule_id: impl Into<String>,
        rule_version: u64,
    ) -> Self {
        let mut flags = BTreeSet::new();
        flags.insert(DERIVED_FLAG.to_string());
        Self {
            channel: channel.into(),
            schema_key: schema_key.into(),
            payload: payload.into(),
            derived_event_id: derived_event_id.into(),
            rule_id: rule_id.into(),
            rule_version,
            flags,
        }
    }
}

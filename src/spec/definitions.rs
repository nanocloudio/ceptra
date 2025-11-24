use crate::runtime::lanes::{
    promql_group_labels, validate_lane_domains, LaneDomainMap, LaneValidationError,
    LaneValidationReport, MetricGrouping,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Entry point for definition bundle validation.
pub fn validate_definition_document(
    doc: &DefinitionDocument,
) -> Result<DefinitionAdmission, DefinitionError> {
    if doc.bundle.lane_domains.is_empty() {
        return Err(DefinitionError::MissingLaneDomains);
    }
    let mut compiled_queries = Vec::new();
    let mut metrics = Vec::new();
    let mut warnings = BTreeSet::new();
    let mut default_windows_ms = BTreeMap::new();

    for phase in &doc.workflow.phases {
        if phase.phase_type == "aggregate.promql" {
            let options: AggregatePromqlOptions =
                phase.deserialize_options().map_err(|reason| {
                    DefinitionError::InvalidPhaseOptions {
                        phase: phase.name.clone(),
                        reason,
                    }
                })?;
            if let Some(window) = options.window.as_deref() {
                match parse_duration_ms(window) {
                    Some(ms) => {
                        default_windows_ms.insert(phase.name.clone(), ms);
                    }
                    None => {
                        warnings.insert(format!(
                            "phase '{}' declares invalid window '{}'",
                            phase.name, window
                        ));
                    }
                }
            }
            for (metric, query) in options.queries.iter() {
                let compiled = compile_promql_query(&phase.name, metric, &query.expression)?;
                if compiled.range_ms.is_none() {
                    warnings.insert(format!(
                        "query '{}' has no range selector (required by §14)",
                        metric
                    ));
                }
                metrics.push(MetricGrouping {
                    name: metric.clone(),
                    labels: compiled.labels.clone(),
                });
                compiled_queries.push(compiled);
            }
        }
    }

    if compiled_queries.is_empty() {
        return Err(DefinitionError::MissingPromqlQueries);
    }

    let channel_loop = detect_channel_loop(doc);
    if let Some(channel) = channel_loop {
        return Err(DefinitionError::ChannelLoop {
            bundle: doc.bundle.name.clone(),
            channel,
        });
    }

    let lane_report =
        validate_lane_domains(&doc.bundle.lane_domains, &metrics).map_err(DefinitionError::Lane)?;
    let mut warnings_vec: Vec<String> = warnings.into_iter().collect();
    warnings_vec.sort();

    let diagnostics = DefinitionDiagnostics {
        bundle_name: doc.bundle.name.clone(),
        def_version: doc.bundle.def_version,
        policy_version: doc.bundle.policy_version,
        lane_report: lane_report.clone(),
        policy_metadata: PolicyMetadata {
            workflow_name: doc.workflow.name.clone(),
            query_count: compiled_queries.len(),
            default_windows_ms,
            lane_projection_count: lane_report.projections.len(),
            warnings: warnings_vec,
            projected_total_lanes: lane_report.total_projected_lanes,
        },
    };

    Ok(DefinitionAdmission {
        bundle_name: doc.bundle.name.clone(),
        def_version: doc.bundle.def_version,
        compiled_queries,
        diagnostics,
    })
}

fn detect_channel_loop(doc: &DefinitionDocument) -> Option<String> {
    let produced = doc.workflow.produced_channels();
    if produced.is_empty() {
        return None;
    }
    let consumed = doc.workflow.consumed_channels();
    produced
        .into_iter()
        .find(|channel| consumed.contains(channel))
}

fn compile_promql_query(
    phase: &str,
    metric: &str,
    expression: &str,
) -> Result<CompiledPromqlQuery, DefinitionError> {
    let trimmed = expression.trim();
    if trimmed.is_empty() {
        return Err(DefinitionError::InvalidPromql {
            query: metric.to_string(),
            reason: "expression is empty".to_string(),
        });
    }
    validate_delimiters(trimmed, metric)?;
    let aggregator = parse_aggregator(trimmed).ok_or_else(|| DefinitionError::InvalidPromql {
        query: metric.to_string(),
        reason: "unable to determine aggregator".to_string(),
    })?;
    let labels = promql_group_labels(trimmed);
    let range_ms = extract_range_ms(trimmed);
    Ok(CompiledPromqlQuery {
        phase: phase.to_string(),
        metric: metric.to_string(),
        aggregator,
        labels,
        expression: trimmed.to_string(),
        range_ms,
    })
}

fn validate_delimiters(expression: &str, metric: &str) -> Result<(), DefinitionError> {
    let mut paren = 0i32;
    let mut bracket = 0i32;
    for ch in expression.chars() {
        match ch {
            '(' => paren += 1,
            ')' => paren -= 1,
            '[' => bracket += 1,
            ']' => bracket -= 1,
            _ => {}
        }
        if paren < 0 || bracket < 0 {
            return Err(DefinitionError::InvalidPromql {
                query: metric.to_string(),
                reason: "mismatched delimiters".to_string(),
            });
        }
    }
    if paren != 0 || bracket != 0 {
        return Err(DefinitionError::InvalidPromql {
            query: metric.to_string(),
            reason: "mismatched delimiters".to_string(),
        });
    }
    Ok(())
}

fn parse_aggregator(expression: &str) -> Option<String> {
    let trimmed = expression.trim_start();
    let chars = trimmed.chars();
    let mut aggregator = String::new();
    for ch in chars {
        if aggregator.is_empty() {
            if ch.is_ascii_alphabetic() || ch == '_' {
                aggregator.push(ch);
            } else if ch.is_ascii_whitespace() {
                continue;
            } else {
                return None;
            }
        } else if ch.is_ascii_alphanumeric() || ch == '_' {
            aggregator.push(ch);
        } else {
            break;
        }
    }
    if aggregator.is_empty() {
        None
    } else {
        Some(aggregator)
    }
}

fn extract_range_ms(expression: &str) -> Option<u64> {
    let mut last = None;
    let bytes = expression.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] == b'[' {
            let start = idx + 1;
            idx += 1;
            while idx < bytes.len() && bytes[idx] != b']' {
                idx += 1;
            }
            if idx <= bytes.len() {
                let literal = expression[start..idx].trim();
                if let Some(ms) = parse_duration_ms(literal) {
                    last = Some(ms);
                }
            }
        }
        idx += 1;
    }
    last
}

fn parse_duration_ms(literal: &str) -> Option<u64> {
    if literal.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut idx = 0usize;
    let bytes = literal.as_bytes();
    while idx < bytes.len() {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() {
            break;
        }
        let start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if start == idx {
            return None;
        }
        let value: u64 = literal[start..idx].parse().ok()?;
        let unit_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_alphabetic() {
            idx += 1;
        }
        if unit_start == idx {
            return None;
        }
        let unit = literal[unit_start..idx].to_ascii_lowercase();
        let factor = match unit.as_str() {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        total = total.saturating_add(value.saturating_mul(factor));
    }
    Some(total)
}

/// Definition YAML/JSON document matching `examples/*.yaml`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DefinitionDocument {
    #[serde(default)]
    pub bundle: BundleSection,
    #[serde(default)]
    pub workflow: WorkflowSection,
}

/// Bundle-level metadata.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BundleSection {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub def_version: u64,
    #[serde(default)]
    pub policy_version: Option<u64>,
    #[serde(default)]
    pub lane_domains: LaneDomainMap,
}

/// Workflow specification containing ordered phases.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowSection {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub phases: Vec<WorkflowPhase>,
}

impl WorkflowSection {
    fn produced_channels(&self) -> BTreeSet<String> {
        let mut channels = BTreeSet::new();
        for phase in &self.phases {
            if phase.phase_type == "classify.cel" {
                if let Ok(opts) = phase.deserialize_options::<ClassifyCelOptions>() {
                    for rule in opts.rules {
                        if let Some(channel) = rule.emit.channel {
                            if !channel.trim().is_empty() {
                                channels.insert(channel);
                            }
                        }
                    }
                }
            }
        }
        channels
    }

    fn consumed_channels(&self) -> BTreeSet<String> {
        let mut channels = BTreeSet::new();
        for phase in &self.phases {
            if phase.phase_type.starts_with("source.") {
                if let Value::Object(map) = &phase.options {
                    if let Some(Value::String(channel)) = map.get("channel") {
                        if !channel.trim().is_empty() {
                            channels.insert(channel.clone());
                        }
                    }
                    if let Some(Value::Array(entries)) = map.get("channels") {
                        for entry in entries {
                            if let Value::String(channel) = entry {
                                if !channel.trim().is_empty() {
                                    channels.insert(channel.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        channels
    }
}

/// Individual workflow phase.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowPhase {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "type")]
    pub phase_type: String,
    #[serde(default)]
    pub options: Value,
}

impl WorkflowPhase {
    fn deserialize_options<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_value(self.options.clone()).map_err(|err| err.to_string())
    }
}

/// Options supported by `aggregate.promql` phases.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AggregatePromqlOptions {
    #[serde(default)]
    pub window: Option<String>,
    #[serde(default)]
    pub queries: BTreeMap<String, PromqlQuerySpec>,
}

/// Individual PromQL query.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PromqlQuerySpec {
    #[serde(default)]
    pub expression: String,
}

/// Options for `classify.cel` phases.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClassifyCelOptions {
    #[serde(default)]
    pub rules: Vec<CelRuleSpec>,
}

/// Classification rule definition.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CelRuleSpec {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub emit: EmitSpec,
}

/// Emit target for a CEL rule.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EmitSpec {
    #[serde(default)]
    pub channel: Option<String>,
}

/// Result of validating a definition bundle.
#[derive(Debug, Clone, Serialize)]
pub struct DefinitionAdmission {
    pub bundle_name: String,
    pub def_version: u64,
    pub compiled_queries: Vec<CompiledPromqlQuery>,
    pub diagnostics: DefinitionDiagnostics,
}

/// Compiled representation of a PromQL query.
#[derive(Debug, Clone, Serialize)]
pub struct CompiledPromqlQuery {
    pub phase: String,
    pub metric: String,
    pub aggregator: String,
    pub labels: Vec<String>,
    pub expression: String,
    pub range_ms: Option<u64>,
}

/// Diagnostics surfaced by `/v1/definitions/validate`.
#[derive(Debug, Clone, Serialize)]
pub struct DefinitionDiagnostics {
    pub bundle_name: String,
    pub def_version: u64,
    pub policy_version: Option<u64>,
    pub lane_report: LaneValidationReport,
    pub policy_metadata: PolicyMetadata,
}

/// Policy metadata persisted alongside the bundle.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PolicyMetadata {
    pub workflow_name: String,
    pub query_count: usize,
    pub default_windows_ms: BTreeMap<String, u64>,
    pub lane_projection_count: usize,
    pub projected_total_lanes: u32,
    pub warnings: Vec<String>,
}

/// Validation errors surfaced by the admission path.
#[derive(Debug, Error)]
pub enum DefinitionError {
    #[error("bundle must declare at least one lane domain")]
    MissingLaneDomains,
    #[error("bundle declares no PromQL queries to validate")]
    MissingPromqlQueries,
    #[error("phase '{phase}' options invalid: {reason}")]
    InvalidPhaseOptions { phase: String, reason: String },
    #[error("invalid query '{query}': {reason}")]
    InvalidPromql { query: String, reason: String },
    #[error("lane validation failed: {0}")]
    Lane(#[from] LaneValidationError),
    #[error("bundle '{bundle}' consumes channel '{channel}' that it emits (Invariant R2)")]
    ChannelLoop { bundle: String, channel: String },
}

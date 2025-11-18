use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Hard ceiling on the number of lanes a partition may allocate.
pub const LANE_MAX_PER_PARTITION: u32 = 64;
/// Threshold at which the compiler emits warnings for projected lane usage.
pub const LANE_WARN_THRESHOLD: u32 = 48;

/// Lane-domain bound declared by bundle authors or inferred by the compiler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LaneDomainSpec {
    pub max_per_partition: u32,
}

/// Mapping from label name to maximum per-partition cardinality.
pub type LaneDomainMap = BTreeMap<String, LaneDomainSpec>;

/// Metric grouping definition describing which labels participate in lane admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricGrouping {
    pub name: String,
    pub labels: Vec<String>,
}

impl MetricGrouping {
    /// Returns the unique label set sorted for deterministic diagnostics.
    fn normalized_labels(&self) -> Vec<String> {
        let mut normalized = BTreeSet::new();
        for label in &self.labels {
            normalized.insert(label.trim().to_string());
        }
        normalized.into_iter().collect()
    }
}

/// Error returned when the validator cannot derive a projection.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LaneValidationError {
    #[error("metric '{metric}' references unknown lane domain '{label}'")]
    UnknownLaneDomain { metric: String, label: String },
    #[error("lane domain '{label}' must declare max_per_partition > 0")]
    ZeroLaneBound { label: String },
}

/// Health of a projected metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LaneProjectionStatus {
    Healthy,
    Warning,
    Overflow,
}

/// Per-metric projection metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LaneProjection {
    pub metric: String,
    pub labels: Vec<String>,
    pub projected_lanes: u32,
    pub status: LaneProjectionStatus,
}

/// Histogram of projected lane usage.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct LaneHistogram {
    buckets: BTreeMap<u32, u32>,
    overflow: u32,
}

impl LaneHistogram {
    fn record(&mut self, projected: u32, overflow: bool) {
        if overflow {
            self.overflow += 1;
        } else {
            *self.buckets.entry(projected).or_insert(0) += 1;
        }
    }

    /// Renders the histogram as a Markdown table suitable for the spec appendix.
    pub fn to_markdown(&self) -> String {
        let mut output = String::from("| projected_lanes | metrics |\n| --- | --- |\n");
        for (lanes, count) in &self.buckets {
            output.push_str(&format!("| {lanes} | {count} |\n"));
        }
        if self.overflow > 0 {
            output.push_str(&format!(
                "| >{LANE_MAX_PER_PARTITION} | {} |\n",
                self.overflow
            ));
        }
        output
    }
}

/// Complete validation report for a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LaneValidationReport {
    pub projections: Vec<LaneProjection>,
    pub total_projected_lanes: u32,
    pub histogram: LaneHistogram,
}

impl LaneValidationReport {
    /// True when any metric or the aggregate exceeds the hard cap.
    pub fn has_overflow(&self) -> bool {
        self.projections
            .iter()
            .any(|projection| projection.status == LaneProjectionStatus::Overflow)
            || self.total_projected_lanes > LANE_MAX_PER_PARTITION
    }

    /// True when the bundle sits in the warning band.
    pub fn has_warnings(&self) -> bool {
        self.total_projected_lanes >= LANE_WARN_THRESHOLD
            || self.projections.iter().any(|projection| {
                projection.status == LaneProjectionStatus::Warning
                    || projection.status == LaneProjectionStatus::Overflow
            })
    }

    /// Serializes the report to Markdown for inclusion in the spec appendix.
    pub fn to_markdown(&self) -> String {
        let mut output = String::from(
            "| metric | projected_lanes | labels | status |\n| --- | --- | --- | --- |\n",
        );
        for projection in &self.projections {
            let labels = if projection.labels.is_empty() {
                "-".to_string()
            } else {
                projection.labels.join(", ")
            };
            output.push_str(&format!(
                "| {} | {} | {} | {:?} |\n",
                projection.metric, projection.projected_lanes, labels, projection.status
            ));
        }
        output.push('\n');
        output.push_str(&self.histogram.to_markdown());
        output
    }
}

/// Validates lane-domain declarations against the hard ceiling.
pub fn validate_lane_domains(
    domains: &LaneDomainMap,
    metrics: &[MetricGrouping],
) -> Result<LaneValidationReport, LaneValidationError> {
    let mut projections = Vec::with_capacity(metrics.len());
    let mut histogram = LaneHistogram::default();
    let mut total = 0u64;

    for metric in metrics {
        let normalized_labels = metric.normalized_labels();
        let mut projected: u64 = 1;
        for label in &normalized_labels {
            let spec =
                domains
                    .get(label)
                    .ok_or_else(|| LaneValidationError::UnknownLaneDomain {
                        metric: metric.name.clone(),
                        label: label.clone(),
                    })?;
            if spec.max_per_partition == 0 {
                return Err(LaneValidationError::ZeroLaneBound {
                    label: label.clone(),
                });
            }
            projected = projected.saturating_mul(spec.max_per_partition as u64);
            if projected > u64::from(u32::MAX) {
                break;
            }
        }
        let overflow = projected > u64::from(LANE_MAX_PER_PARTITION);
        histogram.record(projected.min(u64::from(u32::MAX)) as u32, overflow);
        total = total.saturating_add(projected);
        projections.push(LaneProjection {
            metric: metric.name.clone(),
            labels: normalized_labels,
            projected_lanes: projected.min(u64::from(u32::MAX)) as u32,
            status: if overflow {
                LaneProjectionStatus::Overflow
            } else if projected >= u64::from(LANE_WARN_THRESHOLD) {
                LaneProjectionStatus::Warning
            } else {
                LaneProjectionStatus::Healthy
            },
        });
    }

    Ok(LaneValidationReport {
        projections,
        total_projected_lanes: total.min(u64::from(u32::MAX)) as u32,
        histogram,
    })
}

/// Extracts labels referenced in PromQL `by(...)` clauses.
pub fn promql_group_labels(expression: &str) -> Vec<String> {
    let mut labels = Vec::new();
    let bytes = expression.as_bytes();
    let mut idx = 0usize;
    while idx + 2 <= bytes.len() {
        if bytes[idx].eq(&b'b') && bytes[idx + 1].eq(&b'y') {
            idx += 2;
            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            if idx < bytes.len() && bytes[idx] == b'(' {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b')' {
                    idx += 1;
                }
                let segment = &expression[start..idx];
                for label in segment.split(',') {
                    let trimmed = label.trim();
                    if !trimmed.is_empty() {
                        labels.push(trimmed.to_string());
                    }
                }
                if idx < bytes.len() {
                    idx += 1;
                }
            }
        } else {
            idx += 1;
        }
    }
    labels
}

/// Marker used when a new lane would exceed the configured ceiling.
pub const LANE_OVERFLOW_MARKER: &str = "LANE_OVERFLOW";

/// Configures the runtime lane admission gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneAdmissionConfig {
    pub lane_cap: u32,
}

impl Default for LaneAdmissionConfig {
    fn default() -> Self {
        Self {
            lane_cap: LANE_MAX_PER_PARTITION,
        }
    }
}

#[derive(Debug, Default)]
struct LaneUsage {
    observed: BTreeSet<String>,
}

/// Tracks live lane usage inside the AP and enforces the hard ceiling.
#[derive(Debug, Default)]
pub struct LaneAdmission {
    config: LaneAdmissionConfig,
    metrics: HashMap<String, LaneUsage>,
    total_lanes: u32,
    overflow_total: u64,
    audit_log: Vec<LaneOverflowAudit>,
}

impl LaneAdmission {
    /// Constructs an admission gate using the provided configuration.
    pub fn new(config: LaneAdmissionConfig) -> Self {
        Self {
            config,
            metrics: HashMap::new(),
            total_lanes: 0,
            overflow_total: 0,
            audit_log: Vec::new(),
        }
    }

    fn usage_for_metric(&mut self, metric: &str) -> &mut LaneUsage {
        self.metrics.entry(metric.to_string()).or_default()
    }

    /// Attempts to allocate a new lane for the provided metric and key.
    pub fn admit(&mut self, metric: &str, lane_key: impl Into<String>) -> LaneAdmissionResult {
        let lane_key = lane_key.into();
        if self.usage_for_metric(metric).observed.contains(&lane_key) {
            return LaneAdmissionResult::Existing;
        }
        if self.total_lanes >= self.config.lane_cap {
            self.overflow_total = self.overflow_total.saturating_add(1);
            let audit = LaneOverflowAudit {
                metric: metric.to_string(),
                lane_key,
                reason: LANE_OVERFLOW_MARKER,
                observed_lanes: self.total_lanes,
            };
            self.audit_log.push(audit.clone());
            return LaneAdmissionResult::Overflow(LaneOverflowRecord {
                metric: audit.metric,
                lane_key: audit.lane_key,
                marker: LANE_OVERFLOW_MARKER,
                observed_lanes: audit.observed_lanes,
            });
        }
        self.usage_for_metric(metric).observed.insert(lane_key);
        self.total_lanes = self.total_lanes.saturating_add(1);
        LaneAdmissionResult::Admitted {
            total_lanes: self.total_lanes,
        }
    }

    /// Total number of lanes allocated across all metrics.
    pub fn total_lanes(&self) -> u32 {
        self.total_lanes
    }

    /// Total number of rejected lane creations.
    pub fn overflow_total(&self) -> u64 {
        self.overflow_total
    }

    /// Returns the audit log for `LANE_OVERFLOW` events.
    pub fn audit_log(&self) -> &[LaneOverflowAudit] {
        &self.audit_log
    }
}

/// Result of attempting to admit a lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaneAdmissionResult {
    Existing,
    Admitted { total_lanes: u32 },
    Overflow(LaneOverflowRecord),
}

/// Snapshot of an overflow event surfaced to runtime callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneOverflowRecord {
    pub metric: String,
    pub lane_key: String,
    pub marker: &'static str,
    pub observed_lanes: u32,
}

/// Audit log entry recorded for every overflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneOverflowAudit {
    pub metric: String,
    pub lane_key: String,
    pub reason: &'static str,
    pub observed_lanes: u32,
}

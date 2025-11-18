use super::aggregation::MetricBinding;
use super::apply::CommittedEvent;
use super::derived::DerivedEmission;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use thiserror::Error;

/// Request dispatched to the CEL runtime per ingested event.
#[derive(Debug)]
pub struct CelEvaluationRequest<'a> {
    pub event: &'a CommittedEvent,
    pub bindings: &'a [MetricBinding],
    pub lane_bitmap: u64,
}

/// Result containing derived emission requests grouped by rule/channel.
#[derive(Debug, Default, Clone)]
pub struct CelEvaluationResult {
    pub derived: Vec<DerivedEmission>,
}

/// Deterministic CEL runtime contract.
pub trait CelRuntime: Send {
    fn evaluate(&mut self, request: CelEvaluationRequest<'_>) -> CelEvaluationResult;
}

/// Error returned while compiling CEL rules.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CelError {
    #[error("metric '{metric}' does not allow label '{label}'")]
    InvalidLabel { metric: String, label: String },
}

/// Comparison operators supported by the sandbox.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CelComparisonOp {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
}

/// Deterministic CEL expression tree (subset tailored for CEPtra).
#[derive(Debug, Clone, PartialEq)]
pub enum CelExpr {
    True,
    False,
    HasValue {
        metric: String,
    },
    Compare {
        metric: String,
        op: CelComparisonOp,
        value: f64,
    },
    LabelEquals {
        metric: String,
        label: String,
        value: String,
    },
    FlagContains {
        metric: String,
        flag: String,
    },
    Not(Box<CelExpr>),
    And(Vec<CelExpr>),
    Or(Vec<CelExpr>),
}

impl CelExpr {
    fn collect_label_refs(&self, refs: &mut BTreeSet<(String, String)>) {
        match self {
            CelExpr::LabelEquals { metric, label, .. } => {
                refs.insert((metric.clone(), label.clone()));
            }
            CelExpr::Not(inner) => inner.collect_label_refs(refs),
            CelExpr::And(children) | CelExpr::Or(children) => {
                for child in children {
                    child.collect_label_refs(refs);
                }
            }
            _ => {}
        }
    }

    fn evaluate(&self, ctx: &CelContext<'_>) -> bool {
        match self {
            CelExpr::True => true,
            CelExpr::False => false,
            CelExpr::HasValue { metric } => {
                ctx.binding(metric).map(|b| b.has_value).unwrap_or(false)
            }
            CelExpr::Compare { metric, op, value } => ctx
                .binding(metric)
                .filter(|binding| binding.has_value)
                .map(|binding| compare(binding.value, *value, *op))
                .unwrap_or(false),
            CelExpr::LabelEquals {
                metric,
                label,
                value,
            } => ctx
                .binding(metric)
                .and_then(|binding| binding.labels.get(label))
                .map(|observed| observed == value)
                .unwrap_or(false),
            CelExpr::FlagContains { metric, flag } => ctx
                .binding(metric)
                .map(|binding| binding.flags.contains(flag))
                .unwrap_or(false),
            CelExpr::Not(inner) => !inner.evaluate(ctx),
            CelExpr::And(children) => children.iter().all(|child| child.evaluate(ctx)),
            CelExpr::Or(children) => children.iter().any(|child| child.evaluate(ctx)),
        }
    }
}

fn compare(lhs: f64, rhs: f64, op: CelComparisonOp) -> bool {
    match op {
        CelComparisonOp::Gt => lhs > rhs,
        CelComparisonOp::Ge => lhs >= rhs,
        CelComparisonOp::Lt => lhs < rhs,
        CelComparisonOp::Le => lhs <= rhs,
        CelComparisonOp::Eq => (lhs - rhs).abs() < f64::EPSILON,
    }
}

struct CelContext<'a> {
    bindings: HashMap<&'a str, &'a MetricBinding>,
}

impl<'a> CelContext<'a> {
    fn from_request(request: &'a CelEvaluationRequest<'a>) -> Self {
        let mut bindings = HashMap::new();
        for binding in request.bindings {
            bindings.insert(binding.name.as_str(), binding);
        }
        Self { bindings }
    }

    fn binding(&self, metric: &str) -> Option<&MetricBinding> {
        self.bindings.get(metric).copied()
    }
}

/// Pre-compiled CEL rule definition.
#[derive(Debug, Clone)]
pub struct CelRule {
    channel: String,
    schema_key: String,
    expr: CelExpr,
    _allowed_labels: BTreeMap<String, BTreeSet<String>>,
    payload: Vec<u8>,
    rule_id: String,
    rule_version: u64,
}

impl CelRule {
    pub fn new(
        channel: impl Into<String>,
        schema_key: impl Into<String>,
        expr: CelExpr,
        allowed_labels: BTreeMap<String, BTreeSet<String>>,
        payload: impl Into<Vec<u8>>,
        rule_id: impl Into<String>,
        rule_version: u64,
    ) -> Result<Self, CelError> {
        let mut refs = BTreeSet::new();
        expr.collect_label_refs(&mut refs);
        for (metric, label) in refs {
            match allowed_labels.get(&metric) {
                Some(labels) if labels.contains(&label) => {}
                _ => {
                    return Err(CelError::InvalidLabel { metric, label });
                }
            }
        }
        Ok(Self {
            channel: channel.into(),
            schema_key: schema_key.into(),
            expr,
            _allowed_labels: allowed_labels,
            payload: payload.into(),
            rule_id: rule_id.into(),
            rule_version,
        })
    }

    fn evaluate(&self, ctx: &CelContext<'_>) -> bool {
        self.expr.evaluate(ctx)
    }
}

/// Deterministic CEL runtime built with a restricted helper set.
pub struct CelSandbox {
    rules: Vec<CelRule>,
}

impl CelSandbox {
    /// Creates the sandbox with a pre-validated rule list.
    ///
    /// The sandbox intentionally omits helpers such as wall clocks, randomness, or I/O hooks
    /// so evaluation remains a pure function of the replicated bindings.
    pub fn new(rules: Vec<CelRule>) -> Self {
        Self { rules }
    }
}

impl CelRuntime for CelSandbox {
    fn evaluate(&mut self, request: CelEvaluationRequest<'_>) -> CelEvaluationResult {
        let ctx = CelContext::from_request(&request);
        let mut derived = Vec::new();
        for rule in &self.rules {
            if rule.evaluate(&ctx) {
                let derived_event_id = compute_derived_event_id(request.event, rule);
                derived.push(DerivedEmission::new(
                    rule.channel.clone(),
                    rule.schema_key.clone(),
                    rule.payload.clone(),
                    derived_event_id,
                    rule.rule_id.clone(),
                    rule.rule_version,
                ));
            }
        }
        CelEvaluationResult { derived }
    }
}

fn compute_derived_event_id(event: &CommittedEvent, rule: &CelRule) -> String {
    let mut hasher = DefaultHasher::new();
    event.partition_id().hash(&mut hasher);
    event.log_index().hash(&mut hasher);
    rule.rule_version.hash(&mut hasher);
    rule.rule_id.hash(&mut hasher);
    rule.channel.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

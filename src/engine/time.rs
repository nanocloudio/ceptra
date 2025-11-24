use crate::engine::aggregation::{MetricBinding, MetricSpec, NON_RETRACTABLE_FLAG};
use crate::engine::apply::{CommittedEvent, EventMeasurement};
use crate::event::commit_epoch::{MonotonicClock, SystemMonotonicClock};
use crate::event::finalized::FinalizedHorizon;
use crate::event::watermark::{WatermarkConfig, WatermarkTracker};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const DEFAULT_MAX_FUTURE_SKEW_NS: u64 = 5_000_000_000;
const DEFAULT_MAX_PAST_SKEW_NS: u64 = 30 * 60 * 1_000_000_000;
const PARTITION_AUDIT_METRIC: &str = "__partition__";

/// Audit entry produced when a late event is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateEventAudit {
    pub metric: String,
    pub event_id: String,
    pub log_index: u64,
    pub reason: LateEventReason,
}

/// Reason recorded for a late-event audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LateEventReason {
    NonRetractableFinalized,
    FutureSkew,
    PastSkew,
}

impl LateEventReason {
    fn as_str(&self) -> &'static str {
        match self {
            LateEventReason::NonRetractableFinalized => "NON_RETRACTABLE_FINALIZED",
            LateEventReason::FutureSkew => "FUTURE_SKEW",
            LateEventReason::PastSkew => "PAST_SKEW",
        }
    }
}

/// Result of evaluating a committed event against lateness policies.
#[derive(Debug, Clone)]
pub struct LatenessResult {
    pub filtered_measurements: Vec<EventMeasurement>,
    pub dropped_metrics: BTreeSet<String>,
    pub admission: EventAdmission,
}

impl Default for LatenessResult {
    fn default() -> Self {
        Self {
            filtered_measurements: Vec::new(),
            dropped_metrics: BTreeSet::new(),
            admission: EventAdmission::Accepted,
        }
    }
}

/// Admission decision returned by the time semantics engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventAdmission {
    Accepted,
    RejectedFutureSkew,
}

type DynClock = Box<dyn MonotonicClock + Send>;

/// Coordinates watermark tracking and late-data enforcement.
pub struct TimeSemantics {
    state: TimeSemanticsState,
}

enum TimeSemanticsState {
    Disabled,
    Enabled(Box<TimeSemanticsInner>),
}

struct TimeSemanticsInner {
    watermark: WatermarkTracker,
    finalized: FinalizedHorizon,
    policies: HashMap<String, MetricTimePolicy>,
    clock: DynClock,
    audits: Vec<LateEventAudit>,
    late_counters: HashMap<LateEventReason, u64>,
    max_future_skew_ns: u64,
    max_past_skew_ns: u64,
}

#[derive(Debug, Clone)]
struct MetricTimePolicy {
    correction_horizon_ns: u64,
    retractable: bool,
}

impl TimeSemantics {
    /// Creates an instance that enforces policies for the provided specs.
    pub fn for_pipeline(specs: Option<&[MetricSpec]>) -> Self {
        match specs {
            Some(specs) if !specs.is_empty() => Self::enabled(
                specs,
                WatermarkConfig::default(),
                Box::new(SystemMonotonicClock::new()),
            ),
            _ => Self::disabled(),
        }
    }

    /// Creates an enabled instance with explicit config/clock (tests, tooling).
    pub fn with_components(
        specs: &[MetricSpec],
        watermark_config: WatermarkConfig,
        clock: DynClock,
    ) -> Self {
        if specs.is_empty() {
            return Self::disabled();
        }
        Self::enabled(specs, watermark_config, clock)
    }

    /// Creates a disabled instance (no lateness enforcement).
    pub fn disabled() -> Self {
        Self {
            state: TimeSemanticsState::Disabled,
        }
    }

    fn enabled(specs: &[MetricSpec], config: WatermarkConfig, clock: DynClock) -> Self {
        let mut policies = HashMap::new();
        for spec in specs {
            policies.insert(
                spec.name.clone(),
                MetricTimePolicy {
                    correction_horizon_ns: spec.correction_horizon_ms.saturating_mul(1_000_000),
                    retractable: spec.kind.is_retractable(),
                },
            );
        }
        Self {
            state: TimeSemanticsState::Enabled(Box::new(TimeSemanticsInner {
                watermark: WatermarkTracker::new(config),
                finalized: FinalizedHorizon::new(),
                policies,
                clock,
                audits: Vec::new(),
                late_counters: HashMap::new(),
                max_future_skew_ns: DEFAULT_MAX_FUTURE_SKEW_NS,
                max_past_skew_ns: DEFAULT_MAX_PAST_SKEW_NS,
            })),
        }
    }

    /// Evaluates the event and returns the filtered measurements + dropped metrics.
    pub fn evaluate_event(&mut self, event: &CommittedEvent) -> LatenessResult {
        match &mut self.state {
            TimeSemanticsState::Disabled => LatenessResult {
                filtered_measurements: event.measurements().to_vec(),
                dropped_metrics: BTreeSet::new(),
                admission: EventAdmission::Accepted,
            },
            TimeSemanticsState::Enabled(inner) => inner.evaluate_event(event),
        }
    }

    /// Adds CEL binding flags for metrics dropped due to lateness.
    pub fn decorate_bindings(
        &self,
        bindings: &mut [MetricBinding],
        dropped_metrics: &BTreeSet<String>,
    ) {
        if dropped_metrics.is_empty() {
            return;
        }
        for binding in bindings {
            if dropped_metrics.contains(&binding.name) {
                binding.add_flag(NON_RETRACTABLE_FLAG);
            }
        }
    }

    /// Returns the audit log accumulated so far.
    pub fn audit_log(&self) -> &[LateEventAudit] {
        match &self.state {
            TimeSemanticsState::Disabled => &[],
            TimeSemanticsState::Enabled(inner) => &inner.audits,
        }
    }

    /// Returns cumulative counters for late-event reasons.
    pub fn late_event_counters(&self) -> BTreeMap<String, u64> {
        match &self.state {
            TimeSemanticsState::Disabled => BTreeMap::new(),
            TimeSemanticsState::Enabled(inner) => inner
                .late_counters
                .iter()
                .map(|(reason, total)| (reason.as_str().to_string(), *total))
                .collect(),
        }
    }

    /// Returns the watermark lag (ns) if time semantics is enabled.
    pub fn watermark_lag_ns(&self) -> Option<u64> {
        match &self.state {
            TimeSemanticsState::Disabled => None,
            TimeSemanticsState::Enabled(inner) => Some(inner.watermark.watermark_lag_ns()),
        }
    }
}

impl TimeSemanticsInner {
    fn evaluate_event(&mut self, event: &CommittedEvent) -> LatenessResult {
        let mut result = LatenessResult::default();
        let event_ts_ns = event.commit_epoch_tick_ms().saturating_mul(1_000_000);
        let now_ns = self.clock.now_ns();
        let event_ts_128 = u128::from(event_ts_ns);
        let future_limit = now_ns.saturating_add(self.max_future_skew_ns as u128);
        if event_ts_128 > future_limit {
            self.record_counter(LateEventReason::FutureSkew);
            self.push_audit(PARTITION_AUDIT_METRIC, event, LateEventReason::FutureSkew);
            result.admission = EventAdmission::RejectedFutureSkew;
            return result;
        }
        let past_limit = now_ns.saturating_sub(self.max_past_skew_ns as u128);
        if event_ts_128 < past_limit {
            self.record_counter(LateEventReason::PastSkew);
        }
        if event.measurements().is_empty() {
            let _ = self.watermark.observe_event(
                "__partition__",
                event_ts_ns,
                event.log_index(),
                self.clock.as_mut(),
            );
            return result;
        }
        for measurement in event.measurements() {
            let decision = self.watermark.observe_event(
                &measurement.metric,
                event_ts_ns,
                event.log_index(),
                self.clock.as_mut(),
            );
            let Some(policy) = self.policies.get(&measurement.metric) else {
                result.filtered_measurements.push(measurement.clone());
                continue;
            };
            let metric_watermark_ns = decision
                .metric_watermark_ns
                .unwrap_or(decision.partition_watermark_ns);
            self.finalized.record_metric(
                &measurement.metric,
                metric_watermark_ns,
                policy.correction_horizon_ns,
            );
            let finalized_ts = metric_watermark_ns.saturating_sub(policy.correction_horizon_ns);
            if event_ts_ns < finalized_ts {
                if !policy.retractable {
                    self.record_counter(LateEventReason::NonRetractableFinalized);
                    self.push_audit(
                        measurement.metric.clone(),
                        event,
                        LateEventReason::NonRetractableFinalized,
                    );
                    result.dropped_metrics.insert(measurement.metric.clone());
                }
                continue;
            }
            result.filtered_measurements.push(measurement.clone());
        }
        result
    }

    fn record_counter(&mut self, reason: LateEventReason) {
        *self.late_counters.entry(reason).or_insert(0) += 1;
    }

    fn push_audit(
        &mut self,
        metric: impl Into<String>,
        event: &CommittedEvent,
        reason: LateEventReason,
    ) {
        self.audits.push(LateEventAudit {
            metric: metric.into(),
            event_id: event.event_id.clone(),
            log_index: event.log_index(),
            reason,
        });
    }
}

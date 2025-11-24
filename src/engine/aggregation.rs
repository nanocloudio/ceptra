use crate::engine::apply::{CommittedEvent, EventMeasurement};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use thiserror::Error;

const DEFAULT_LANE: u8 = 0;
pub const NON_RETRACTABLE_FLAG: &str = "NON_RETRACTABLE_DROPPED";

/// Binding exposed to the CEL runtime for a single metric.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricBinding {
    pub name: String,
    pub value: f64,
    pub has_value: bool,
    pub labels: BTreeMap<String, String>,
    pub flags: BTreeSet<String>,
}

impl MetricBinding {
    /// Creates a binding with the provided value and default metadata.
    pub fn with_value(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
            has_value: true,
            labels: BTreeMap::new(),
            flags: BTreeSet::new(),
        }
    }

    /// Creates a binding that intentionally lacks a value (for example, dropped lanes).
    pub fn absent(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: 0.0,
            has_value: false,
            labels: BTreeMap::new(),
            flags: BTreeSet::new(),
        }
    }

    /// Attaches an immutable set of labels to the binding.
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Adds a deterministic flag describing the binding state.
    pub fn add_flag(&mut self, flag: impl Into<String>) {
        self.flags.insert(flag.into());
    }
}

/// Aggregation output for a single ingested event.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AggregationRecord {
    pub bindings: Vec<MetricBinding>,
    pub lane_bitmap: u64,
}

/// Trait implemented by apply-loop aggregation pipelines.
pub trait AggregationPipeline: Send {
    /// Applies the event to the in-memory pane structures and returns CEL bindings.
    fn ingest(&mut self, event: &CommittedEvent) -> AggregationRecord;

    /// Returns the metric specifications used by the pipeline, when available.
    fn metric_specs(&self) -> Option<&[MetricSpec]> {
        None
    }

    /// Attempts to upgrade the pipeline in-place for the provided policy versions.
    fn upgrade_policy(&mut self, _from: u64, _to: u64) -> Result<(), AggregationError> {
        Ok(())
    }
}

/// Configures the pane aggregation pipeline.
#[derive(Debug, Clone)]
pub struct AggregationConfig {
    pub pane_width_ms: u64,
    pub raw_retention_ms: u64,
    pub max_window_horizon_ms: u64,
    pub metrics: Vec<MetricSpec>,
}

/// Budget inputs collected during bundle admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneRingBudget {
    /// Projected number of active lanes for the partition.
    pub projected_lanes: u32,
    /// Estimated bytes required per metric/lane state slot.
    pub bytes_per_state: u64,
    /// Maximum memory permitted for the pane ring.
    pub max_memory_bytes: u64,
}

/// Report returned by the ring-sizing validator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingSizingReport {
    /// Effective `R_max` horizon in milliseconds.
    pub r_max_ms: u64,
    /// Number of panes allocated to cover `R_max`.
    pub ring_panes: u64,
    /// Total projected metric/lane state slots.
    pub projected_state_slots: u64,
    /// Estimated memory footprint for the pane ring.
    pub estimated_memory_bytes: u64,
}

/// Policy describing how late data should be handled for a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateDataPolicy {
    Drop,
    Retract,
}

/// Aggregation configuration errors.
#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("metric '{metric}' cannot use RETRACT policy with a non-retractable aggregator")]
    NonRetractablePolicy { metric: String },
    #[error(
        "metric '{metric}' requests window {window_ms} ms but the pane ring spans {r_max_ms} ms; increase raw_retention_ms or max_window_horizon_ms"
    )]
    WindowExceedsRing {
        metric: String,
        window_ms: u64,
        r_max_ms: u64,
    },
    #[error(
        "pane ring budget exceeded: estimated {estimated_bytes} bytes ({projected_state_slots} slots) > budget {budget_bytes} bytes"
    )]
    RingBudgetExceeded {
        estimated_bytes: u64,
        budget_bytes: u64,
        projected_state_slots: u64,
    },
    #[error("policy upgrade from v{from} to v{to} requires checkpoint replay")]
    PolicyUpgradeRequiresReplay { from: u64, to: u64 },
}

/// Per-metric aggregation declaration.
#[derive(Debug, Clone)]
pub struct MetricSpec {
    pub name: String,
    pub kind: AggregatorKind,
    pub window_horizon_ms: u64,
    pub correction_horizon_ms: u64,
    pub labels: Vec<String>,
    pub late_data_policy: LateDataPolicy,
}

/// Supported aggregation families.
#[derive(Debug, Clone)]
pub enum AggregatorKind {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    QuantileOverTime { quantile: f64 },
    TopK { k: usize },
    Distinct,
}

impl AggregatorKind {
    pub fn is_retractable(&self) -> bool {
        !matches!(
            self,
            AggregatorKind::QuantileOverTime { .. }
                | AggregatorKind::TopK { .. }
                | AggregatorKind::Distinct
        )
    }
}

impl AggregationConfig {
    /// Returns `R_max = max(R_raw, max_window_horizon)` in milliseconds.
    pub fn r_max_ms(&self) -> u64 {
        self.raw_retention_ms
            .max(self.max_window_horizon_ms)
            .max(self.pane_width_ms)
    }

    fn ring_pane_count(&self) -> u64 {
        let r_max = self.r_max_ms();
        r_max.div_ceil(self.pane_width_ms).max(1)
    }

    /// Validates the ring sizing against the provided pane budget.
    pub fn validate_ring_budget(
        &self,
        budget: &PaneRingBudget,
    ) -> Result<RingSizingReport, AggregationError> {
        let ring_panes = self.ring_pane_count();
        let metric_count = self.metrics.len().max(1) as u64;
        let projected_lanes = budget.projected_lanes.max(1) as u64;
        let slots = ring_panes
            .saturating_mul(metric_count)
            .saturating_mul(projected_lanes);
        let bytes_per_state = budget.bytes_per_state.max(1);
        let estimated_bytes = slots.saturating_mul(bytes_per_state);
        if estimated_bytes > budget.max_memory_bytes {
            return Err(AggregationError::RingBudgetExceeded {
                estimated_bytes,
                budget_bytes: budget.max_memory_bytes,
                projected_state_slots: slots,
            });
        }
        Ok(RingSizingReport {
            r_max_ms: self.r_max_ms(),
            ring_panes,
            projected_state_slots: slots,
            estimated_memory_bytes: estimated_bytes,
        })
    }
}

/// Snapshot serialized to checkpoints for deterministic recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregationSnapshot {
    pub pane_width_ms: u64,
    pub panes: Vec<PaneSnapshot>,
    #[serde(default)]
    pub compacted: CompactedSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaneSnapshot {
    pub start_tick: u64,
    pub active: bool,
    pub buckets: Vec<MetricLaneSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricLaneSnapshot {
    pub metric: String,
    pub lane: u8,
    pub state: AggregatorStateSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct CompactedSnapshot {
    pub frontier_tick: u64,
    pub buckets: Vec<MetricLaneSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregatorStateSnapshot {
    Sum(f64),
    Count(u64),
    Avg {
        sum: f64,
        count: u64,
    },
    Min(Option<f64>),
    Max(Option<f64>),
    Quantile(Vec<f64>),
    TopK {
        entries: Vec<(String, f64)>,
        k: usize,
    },
    Distinct(Vec<String>),
}

/// Pane-ring aggregation pipeline that feeds CEL bindings.
pub struct PaneAggregationPipeline {
    metrics: HashMap<String, PreparedMetric>,
    ring: PaneRing,
    metric_specs: Vec<MetricSpec>,
    compacted: CompactedTier,
}

impl PaneAggregationPipeline {
    pub fn new(config: AggregationConfig) -> Result<Self, AggregationError> {
        assert!(config.pane_width_ms > 0, "pane width must be > 0");
        let r_max_ms = config.r_max_ms();
        let ring_len = config.ring_pane_count().max(1).min(usize::MAX as u64) as usize;
        let pane_width = config.pane_width_ms;
        let mut metrics = HashMap::new();
        let mut metric_specs = Vec::new();
        for spec in config.metrics {
            if spec.window_horizon_ms > r_max_ms {
                return Err(AggregationError::WindowExceedsRing {
                    metric: spec.name.clone(),
                    window_ms: spec.window_horizon_ms,
                    r_max_ms,
                });
            }
            if !spec.kind.is_retractable() && spec.late_data_policy == LateDataPolicy::Retract {
                return Err(AggregationError::NonRetractablePolicy {
                    metric: spec.name.clone(),
                });
            }
            let window_panes = spec.window_horizon_ms.div_ceil(pane_width).max(1) as usize;
            metric_specs.push(spec.clone());
            metrics.insert(spec.name.clone(), PreparedMetric { spec, window_panes });
        }
        Ok(Self {
            metrics,
            ring: PaneRing::new(pane_width, ring_len.max(1)),
            metric_specs,
            compacted: CompactedTier::new(),
        })
    }

    /// Serializes the pane ring to a snapshot.
    pub fn snapshot(&self) -> AggregationSnapshot {
        let mut snapshot = self.ring.snapshot();
        snapshot.compacted = self.compacted.snapshot();
        snapshot
    }

    /// Restores the pane ring from a checkpoint snapshot.
    pub fn restore(&mut self, snapshot: &AggregationSnapshot) {
        if self.ring.pane_width_ms != snapshot.pane_width_ms {
            panic!(
                "pane width mismatch (expected {}, snapshot {})",
                self.ring.pane_width_ms, snapshot.pane_width_ms
            );
        }
        self.ring.restore(snapshot);
        self.compacted = CompactedTier::from_snapshot(&snapshot.compacted);
    }

    fn compute_binding(
        &self,
        metric: &PreparedMetric,
        lane: u8,
        labels: &BTreeMap<String, String>,
    ) -> MetricBinding {
        let panes = self.ring.recent_panes(metric.window_panes);
        let key = MetricLaneKey::new(metric.spec.name.clone(), lane);
        let mut states = Vec::new();
        for pane in panes {
            if let Some(bucket) = pane.buckets.get(&key) {
                states.push(bucket);
            }
        }
        let maybe_value = fold_states(&metric.spec.kind, &states);
        let mut binding = if let Some(value) = maybe_value {
            MetricBinding::with_value(metric.spec.name.clone(), value)
        } else {
            MetricBinding::absent(metric.spec.name.clone())
        }
        .with_labels(labels.clone());
        if !binding.has_value && !metric.spec.kind.is_retractable() {
            binding.add_flag(NON_RETRACTABLE_FLAG);
        }
        binding
    }

    /// Returns the exclusive tick frontier covered by compacted panes.
    pub fn compaction_frontier_tick(&self) -> u64 {
        self.compacted.frontier_tick()
    }

    /// Queries a metric/lane pair across compacted and raw tiers using monoid folding.
    pub fn cross_tier_value(&self, metric_name: &str, lane: u8) -> Option<f64> {
        let metric = self.metrics.get(metric_name)?;
        let key = MetricLaneKey::new(metric.spec.name.clone(), lane);
        let mut states = Vec::new();
        if let Some(state) = self.compacted.get_state(&key) {
            states.push(state);
        }
        for pane in self.ring.recent_panes(self.ring.capacity()) {
            if let Some(bucket) = pane.buckets.get(&key) {
                states.push(bucket);
            }
        }
        if states.is_empty() {
            None
        } else {
            fold_states(&metric.spec.kind, &states)
        }
    }
}

impl AggregationPipeline for PaneAggregationPipeline {
    fn upgrade_policy(&mut self, from: u64, to: u64) -> Result<(), AggregationError> {
        if to < from || to > from.saturating_add(1) {
            return Err(AggregationError::PolicyUpgradeRequiresReplay { from, to });
        }
        Ok(())
    }

    fn ingest(&mut self, event: &CommittedEvent) -> AggregationRecord {
        if event.measurements().is_empty() {
            return AggregationRecord::default();
        }
        let expired = self.ring.advance(event.commit_epoch_tick_ms());
        if !expired.is_empty() {
            self.compacted
                .absorb_panes(expired, &self.metrics, self.ring.pane_width());
        }

        let mut bindings = Vec::new();
        let mut lane_mask = 0u64;
        for measurement in event.measurements() {
            let Some(metric) = self.metrics.get(&measurement.metric) else {
                continue;
            };
            let lanes = active_lanes(measurement.lane_bitmap);
            for lane in lanes {
                lane_mask |= 1u64 << lane;
                {
                    let pane = self.ring.head_mut();
                    pane.apply_metric(&metric.spec.name, lane, &metric.spec.kind, measurement);
                }
                let binding = self.compute_binding(metric, lane, &measurement.labels);
                bindings.push(binding);
            }
        }
        bindings.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.labels.cmp(&b.labels)));
        AggregationRecord {
            bindings,
            lane_bitmap: if lane_mask == 0 {
                1u64 << DEFAULT_LANE
            } else {
                lane_mask
            },
        }
    }

    fn metric_specs(&self) -> Option<&[MetricSpec]> {
        Some(&self.metric_specs)
    }
}

#[derive(Debug, Clone)]
struct PreparedMetric {
    spec: MetricSpec,
    window_panes: usize,
}

struct PaneRing {
    pane_width_ms: u64,
    panes: Vec<Pane>,
    head_index: usize,
    head_start_tick: u64,
    initialized: bool,
}

impl PaneRing {
    fn new(pane_width_ms: u64, len: usize) -> Self {
        let len = len.max(1);
        Self {
            pane_width_ms,
            panes: (0..len).map(|_| Pane::new()).collect(),
            head_index: len - 1,
            head_start_tick: 0,
            initialized: false,
        }
    }

    fn advance(&mut self, tick_ms: u64) -> Vec<Pane> {
        let mut evicted = Vec::new();
        let pane_width = self.pane_width_ms;
        let target_start = (tick_ms / pane_width) * pane_width;
        if !self.initialized {
            self.initialized = true;
            self.head_start_tick = target_start;
            let len = self.panes.len();
            for offset in 0..len {
                let idx = (self.head_index + len - offset) % len;
                let start = target_start.saturating_sub((offset as u64) * pane_width);
                self.panes[idx].reset(start);
            }
            return evicted;
        }
        if target_start <= self.head_start_tick {
            return evicted;
        }
        let mut start = self.head_start_tick;
        while start < target_start {
            self.head_index = (self.head_index + 1) % self.panes.len();
            start = start.saturating_add(pane_width);
            if self.panes[self.head_index].active {
                let mut pane = Pane::new();
                std::mem::swap(&mut pane, &mut self.panes[self.head_index]);
                evicted.push(pane);
            }
            self.panes[self.head_index].reset(start);
        }
        self.head_start_tick = start;
        evicted
    }

    fn head_mut(&mut self) -> &mut Pane {
        &mut self.panes[self.head_index]
    }

    fn pane_width(&self) -> u64 {
        self.pane_width_ms
    }

    fn capacity(&self) -> usize {
        self.panes.len()
    }

    fn recent_panes(&self, limit: usize) -> Vec<&Pane> {
        if !self.initialized || limit == 0 {
            return Vec::new();
        }
        let mut panes = Vec::new();
        let mut idx = self.head_index;
        let mut visited = 0usize;
        while visited < self.panes.len() && panes.len() < limit {
            let pane = &self.panes[idx];
            if pane.active {
                panes.push(pane);
            }
            if idx == 0 {
                idx = self.panes.len() - 1;
            } else {
                idx -= 1;
            }
            visited += 1;
        }
        panes
    }

    fn snapshot(&self) -> AggregationSnapshot {
        let panes = self.panes.iter().map(|pane| pane.snapshot()).collect();
        AggregationSnapshot {
            pane_width_ms: self.pane_width_ms,
            panes,
            compacted: CompactedSnapshot::default(),
        }
    }

    fn restore(&mut self, snapshot: &AggregationSnapshot) {
        self.panes = snapshot.panes.iter().map(Pane::from_snapshot).collect();
        if self.panes.is_empty() {
            self.panes.push(Pane::new());
        }
        self.initialized = self.panes.iter().any(|pane| pane.active);
        if let Some((idx, pane)) = self
            .panes
            .iter()
            .enumerate()
            .filter(|(_, pane)| pane.active)
            .max_by_key(|(_, pane)| pane.start_tick)
        {
            self.head_index = idx;
            self.head_start_tick = pane.start_tick;
        } else {
            self.head_index = self.panes.len() - 1;
            self.head_start_tick = 0;
        }
    }
}

struct CompactedTier {
    buckets: HashMap<MetricLaneKey, AggregatorData>,
    frontier_tick: u64,
}

impl CompactedTier {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            frontier_tick: 0,
        }
    }

    fn absorb_panes(
        &mut self,
        panes: Vec<Pane>,
        metrics: &HashMap<String, PreparedMetric>,
        pane_width_ms: u64,
    ) {
        for pane in panes {
            if !pane.active {
                continue;
            }
            let pane_end = pane.start_tick.saturating_add(pane_width_ms);
            for (key, state) in pane.buckets {
                if let Some(metric) = metrics.get(&key.metric) {
                    self.merge_bucket(key, state, &metric.spec.kind);
                }
            }
            self.frontier_tick = self.frontier_tick.max(pane_end);
        }
    }

    fn merge_bucket(&mut self, key: MetricLaneKey, state: AggregatorData, kind: &AggregatorKind) {
        match self.buckets.entry(key) {
            Entry::Occupied(mut existing) => {
                existing.get_mut().merge(kind, &state);
            }
            Entry::Vacant(slot) => {
                slot.insert(state);
            }
        }
    }

    fn snapshot(&self) -> CompactedSnapshot {
        let buckets = self
            .buckets
            .iter()
            .map(|(key, state)| MetricLaneSnapshot {
                metric: key.metric.clone(),
                lane: key.lane,
                state: state.to_snapshot(),
            })
            .collect();
        CompactedSnapshot {
            frontier_tick: self.frontier_tick,
            buckets,
        }
    }

    fn from_snapshot(snapshot: &CompactedSnapshot) -> Self {
        let mut buckets = HashMap::new();
        for bucket in &snapshot.buckets {
            buckets.insert(
                MetricLaneKey::new(bucket.metric.clone(), bucket.lane),
                AggregatorData::from_snapshot(&bucket.state),
            );
        }
        Self {
            buckets,
            frontier_tick: snapshot.frontier_tick,
        }
    }

    fn get_state(&self, key: &MetricLaneKey) -> Option<&AggregatorData> {
        self.buckets.get(key)
    }

    fn frontier_tick(&self) -> u64 {
        self.frontier_tick
    }
}

struct Pane {
    start_tick: u64,
    active: bool,
    buckets: HashMap<MetricLaneKey, AggregatorData>,
}

impl Pane {
    fn new() -> Self {
        Self {
            start_tick: 0,
            active: false,
            buckets: HashMap::new(),
        }
    }

    fn reset(&mut self, start_tick: u64) {
        self.start_tick = start_tick;
        self.active = true;
        self.buckets.clear();
    }

    fn apply_metric(
        &mut self,
        metric: &str,
        lane: u8,
        kind: &AggregatorKind,
        measurement: &EventMeasurement,
    ) {
        let key = MetricLaneKey::new(metric.to_string(), lane);
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| AggregatorData::from_kind(kind));
        bucket.apply(kind, measurement);
    }

    fn snapshot(&self) -> PaneSnapshot {
        let buckets = self
            .buckets
            .iter()
            .map(|(key, data)| MetricLaneSnapshot {
                metric: key.metric.clone(),
                lane: key.lane,
                state: data.to_snapshot(),
            })
            .collect();
        PaneSnapshot {
            start_tick: self.start_tick,
            active: self.active,
            buckets,
        }
    }

    fn from_snapshot(snapshot: &PaneSnapshot) -> Self {
        let mut buckets = HashMap::new();
        for bucket in &snapshot.buckets {
            buckets.insert(
                MetricLaneKey::new(bucket.metric.clone(), bucket.lane),
                AggregatorData::from_snapshot(&bucket.state),
            );
        }
        Self {
            start_tick: snapshot.start_tick,
            active: snapshot.active,
            buckets,
        }
    }
}

#[derive(Debug, Clone)]
struct MetricLaneKey {
    metric: String,
    lane: u8,
}

impl MetricLaneKey {
    fn new(metric: String, lane: u8) -> Self {
        Self { metric, lane }
    }
}

impl PartialEq for MetricLaneKey {
    fn eq(&self, other: &Self) -> bool {
        self.metric == other.metric && self.lane == other.lane
    }
}

impl Eq for MetricLaneKey {}

impl Hash for MetricLaneKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric.hash(state);
        self.lane.hash(state);
    }
}

#[derive(Debug, Clone)]
enum AggregatorData {
    Sum(f64),
    Count(u64),
    Avg {
        sum: f64,
        count: u64,
    },
    Min(Option<f64>),
    Max(Option<f64>),
    Quantile(Vec<f64>),
    TopK {
        entries: BTreeMap<String, f64>,
        k: usize,
    },
    Distinct(BTreeSet<String>),
}

impl AggregatorData {
    fn from_kind(kind: &AggregatorKind) -> Self {
        match kind {
            AggregatorKind::Sum => AggregatorData::Sum(0.0),
            AggregatorKind::Count => AggregatorData::Count(0),
            AggregatorKind::Avg => AggregatorData::Avg { sum: 0.0, count: 0 },
            AggregatorKind::Min => AggregatorData::Min(None),
            AggregatorKind::Max => AggregatorData::Max(None),
            AggregatorKind::QuantileOverTime { .. } => AggregatorData::Quantile(Vec::new()),
            AggregatorKind::TopK { k } => AggregatorData::TopK {
                entries: BTreeMap::new(),
                k: *k,
            },
            AggregatorKind::Distinct => AggregatorData::Distinct(BTreeSet::new()),
        }
    }

    fn apply(&mut self, kind: &AggregatorKind, measurement: &EventMeasurement) {
        match (kind, self) {
            (AggregatorKind::Sum, AggregatorData::Sum(total)) => {
                if let Some(value) = measurement.value {
                    *total += value;
                }
            }
            (AggregatorKind::Count, AggregatorData::Count(count)) => {
                *count = count.saturating_add(1);
            }
            (AggregatorKind::Avg, AggregatorData::Avg { sum, count }) => {
                if let Some(value) = measurement.value {
                    *sum += value;
                    *count = count.saturating_add(1);
                }
            }
            (AggregatorKind::Min, AggregatorData::Min(current)) => {
                if let Some(value) = measurement.value {
                    *current = Some(match current {
                        Some(existing) => existing.min(value),
                        None => value,
                    });
                }
            }
            (AggregatorKind::Max, AggregatorData::Max(current)) => {
                if let Some(value) = measurement.value {
                    *current = Some(match current {
                        Some(existing) => existing.max(value),
                        None => value,
                    });
                }
            }
            (AggregatorKind::QuantileOverTime { .. }, AggregatorData::Quantile(values)) => {
                if let Some(value) = measurement.value {
                    values.push(value);
                }
            }
            (
                AggregatorKind::TopK { k },
                AggregatorData::TopK {
                    entries,
                    k: state_k,
                },
            ) => {
                if let (Some(value), Some(item)) = (measurement.value, &measurement.item) {
                    let entry = entries.entry(item.clone()).or_insert(value);
                    if value > *entry {
                        *entry = value;
                    }
                    if entries.len() > *k {
                        trim_topk(entries, *k);
                    }
                    *state_k = *k;
                }
            }
            (AggregatorKind::Distinct, AggregatorData::Distinct(entries)) => {
                if let Some(item) = &measurement.item {
                    entries.insert(item.clone());
                }
            }
            _ => {}
        }
    }

    fn merge(&mut self, kind: &AggregatorKind, other: &AggregatorData) {
        match (kind, self, other) {
            (AggregatorKind::Sum, AggregatorData::Sum(total), AggregatorData::Sum(value)) => {
                *total += value;
            }
            (AggregatorKind::Count, AggregatorData::Count(total), AggregatorData::Count(value)) => {
                let new_total = (*total).saturating_add(*value);
                *total = new_total;
            }
            (
                AggregatorKind::Avg,
                AggregatorData::Avg { sum, count },
                AggregatorData::Avg {
                    sum: other_sum,
                    count: other_count,
                },
            ) => {
                *sum += other_sum;
                let new_count = (*count).saturating_add(*other_count);
                *count = new_count;
            }
            (
                AggregatorKind::Min,
                AggregatorData::Min(current),
                AggregatorData::Min(Some(value)),
            ) => match current {
                Some(existing) => {
                    *existing = existing.min(*value);
                }
                None => {
                    *current = Some(*value);
                }
            },
            (AggregatorKind::Min, AggregatorData::Min(_), AggregatorData::Min(None)) => {}
            (
                AggregatorKind::Max,
                AggregatorData::Max(current),
                AggregatorData::Max(Some(value)),
            ) => match current {
                Some(existing) => {
                    *existing = existing.max(*value);
                }
                None => {
                    *current = Some(*value);
                }
            },
            (AggregatorKind::Max, AggregatorData::Max(_), AggregatorData::Max(None)) => {}
            (
                AggregatorKind::QuantileOverTime { .. },
                AggregatorData::Quantile(values),
                AggregatorData::Quantile(other_values),
            ) => {
                values.extend(other_values.iter().copied());
            }
            (
                AggregatorKind::TopK { k },
                AggregatorData::TopK {
                    entries,
                    k: state_k,
                },
                AggregatorData::TopK {
                    entries: other_entries,
                    ..
                },
            ) => {
                *state_k = *k;
                for (key, value) in other_entries {
                    let entry = entries.entry(key.clone()).or_insert(*value);
                    if *value > *entry {
                        *entry = *value;
                    }
                }
                trim_topk(entries, *k);
            }
            (
                AggregatorKind::Distinct,
                AggregatorData::Distinct(entries),
                AggregatorData::Distinct(other_entries),
            ) => {
                entries.extend(other_entries.iter().cloned());
            }
            _ => {}
        }
    }

    fn to_snapshot(&self) -> AggregatorStateSnapshot {
        match self {
            AggregatorData::Sum(value) => AggregatorStateSnapshot::Sum(*value),
            AggregatorData::Count(count) => AggregatorStateSnapshot::Count(*count),
            AggregatorData::Avg { sum, count } => AggregatorStateSnapshot::Avg {
                sum: *sum,
                count: *count,
            },
            AggregatorData::Min(value) => AggregatorStateSnapshot::Min(*value),
            AggregatorData::Max(value) => AggregatorStateSnapshot::Max(*value),
            AggregatorData::Quantile(values) => AggregatorStateSnapshot::Quantile(values.clone()),
            AggregatorData::TopK { entries, k } => AggregatorStateSnapshot::TopK {
                entries: entries
                    .iter()
                    .map(|(key, value)| (key.clone(), *value))
                    .collect(),
                k: *k,
            },
            AggregatorData::Distinct(entries) => {
                AggregatorStateSnapshot::Distinct(entries.iter().cloned().collect())
            }
        }
    }

    fn from_snapshot(snapshot: &AggregatorStateSnapshot) -> Self {
        match snapshot {
            AggregatorStateSnapshot::Sum(value) => AggregatorData::Sum(*value),
            AggregatorStateSnapshot::Count(count) => AggregatorData::Count(*count),
            AggregatorStateSnapshot::Avg { sum, count } => AggregatorData::Avg {
                sum: *sum,
                count: *count,
            },
            AggregatorStateSnapshot::Min(value) => AggregatorData::Min(*value),
            AggregatorStateSnapshot::Max(value) => AggregatorData::Max(*value),
            AggregatorStateSnapshot::Quantile(values) => AggregatorData::Quantile(values.clone()),
            AggregatorStateSnapshot::TopK { entries, k } => AggregatorData::TopK {
                entries: entries.iter().cloned().collect(),
                k: *k,
            },
            AggregatorStateSnapshot::Distinct(entries) => {
                AggregatorData::Distinct(entries.iter().cloned().collect())
            }
        }
    }
}

fn active_lanes(bitmap: u64) -> Vec<u8> {
    if bitmap == 0 {
        return vec![DEFAULT_LANE];
    }
    let mut lanes = Vec::new();
    let mut bits = bitmap;
    let mut idx = 0u8;
    while bits != 0 {
        if bits & 1 == 1 {
            lanes.push(idx);
        }
        bits >>= 1;
        idx = idx.saturating_add(1);
    }
    lanes
}

fn fold_states(kind: &AggregatorKind, states: &[&AggregatorData]) -> Option<f64> {
    match kind {
        AggregatorKind::Sum => Some(
            states
                .iter()
                .map(|state| match state {
                    AggregatorData::Sum(value) => *value,
                    _ => 0.0,
                })
                .sum(),
        ),
        AggregatorKind::Count => Some(
            states
                .iter()
                .map(|state| match state {
                    AggregatorData::Count(value) => *value as f64,
                    _ => 0.0,
                })
                .sum(),
        ),
        AggregatorKind::Avg => {
            let mut total = 0.0;
            let mut count = 0u64;
            for state in states {
                if let AggregatorData::Avg { sum, count: c } = state {
                    total += *sum;
                    count = count.saturating_add(*c);
                }
            }
            if count > 0 {
                Some(total / count as f64)
            } else {
                None
            }
        }
        AggregatorKind::Min => states
            .iter()
            .filter_map(|state| match state {
                AggregatorData::Min(Some(value)) => Some(*value),
                _ => None,
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)),
        AggregatorKind::Max => states
            .iter()
            .filter_map(|state| match state {
                AggregatorData::Max(Some(value)) => Some(*value),
                _ => None,
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)),
        AggregatorKind::QuantileOverTime { quantile } => {
            let mut values = Vec::new();
            for state in states {
                if let AggregatorData::Quantile(samples) = state {
                    values.extend(samples.iter().copied());
                }
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            if values.is_empty() {
                None
            } else {
                let rank =
                    ((*quantile).clamp(0.0, 1.0) * ((values.len() - 1) as f64)).round() as usize;
                values.get(rank).copied()
            }
        }
        AggregatorKind::TopK { k } => {
            let mut entries = BTreeMap::new();
            for state in states {
                if let AggregatorData::TopK { entries: map, .. } = state {
                    for (key, value) in map {
                        let entry = entries.entry(key.clone()).or_insert(*value);
                        if *value > *entry {
                            *entry = *value;
                        }
                    }
                }
            }
            if entries.is_empty() {
                None
            } else {
                let mut vec: Vec<(String, f64)> =
                    entries.iter().map(|(k, v)| (k.clone(), *v)).collect();
                vec.sort_by(|a, b| {
                    b.1.partial_cmp(&a.1)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| a.0.cmp(&b.0))
                });
                vec.truncate(*k);
                vec.first().map(|(_, value)| *value)
            }
        }
        AggregatorKind::Distinct => {
            let mut set = BTreeSet::new();
            for state in states {
                if let AggregatorData::Distinct(entries) = state {
                    set.extend(entries.iter().cloned());
                }
            }
            Some(set.len() as f64)
        }
    }
}

fn trim_topk(entries: &mut BTreeMap<String, f64>, k: usize) {
    if entries.len() <= k {
        return;
    }
    let mut vec: Vec<(String, f64)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();
    vec.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    vec.truncate(k);
    entries.clear();
    for (key, value) in vec {
        entries.insert(key, value);
    }
}

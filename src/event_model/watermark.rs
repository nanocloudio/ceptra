use super::commit_epoch::MonotonicClock;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::HashMap;

const DEFAULT_LATENESS_NS: u64 = 2_000_000_000; // 2 s
const DEFAULT_GUARD_NS: u64 = 200_000_000; // 200 ms

/// Partition + metric level configuration for watermark computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkConfig {
    lateness_allowance_ns: u64,
    guard_ns: u64,
    metric_overrides: HashMap<String, MetricWatermarkOverride>,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            lateness_allowance_ns: DEFAULT_LATENESS_NS,
            guard_ns: DEFAULT_GUARD_NS,
            metric_overrides: HashMap::new(),
        }
    }
}

impl WatermarkConfig {
    /// Creates a config with custom lateness + guard values.
    pub fn new(lateness_allowance_ns: u64, guard_ns: u64) -> Self {
        Self {
            lateness_allowance_ns,
            guard_ns,
            metric_overrides: HashMap::new(),
        }
    }

    /// Registers a per-metric override.
    pub fn with_metric_override(
        mut self,
        metric: impl Into<String>,
        override_cfg: MetricWatermarkOverride,
    ) -> Self {
        self.metric_overrides.insert(metric.into(), override_cfg);
        self
    }

    /// Returns the global lateness allowance (ns).
    pub fn lateness_allowance_ns(&self) -> u64 {
        self.lateness_allowance_ns
    }

    /// Returns the partition-level guard duration (ns).
    pub fn guard_ns(&self) -> u64 {
        self.guard_ns
    }

    fn override_for(&self, metric: &str) -> Option<&MetricWatermarkOverride> {
        self.metric_overrides.get(metric)
    }
}

/// Per-metric override for lateness/guard knobs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetricWatermarkOverride {
    pub lateness_allowance_ns: Option<u64>,
    pub guard_ns: Option<u64>,
}

impl MetricWatermarkOverride {
    /// Creates an override with specific lateness/guard values.
    pub fn new(lateness_allowance_ns: Option<u64>, guard_ns: Option<u64>) -> Self {
        Self {
            lateness_allowance_ns,
            guard_ns,
        }
    }
}

/// Snapshot-friendly representation of a guard floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GuardFloorSnapshot {
    pub value_ns: u64,
    pub log_index: u64,
}

#[derive(Debug, Clone, Copy)]
struct GuardFloor {
    value_ns: u64,
    log_index: u64,
}

impl GuardFloor {
    fn new() -> Self {
        Self {
            value_ns: 0,
            log_index: 0,
        }
    }

    fn from_snapshot(snapshot: GuardFloorSnapshot) -> Self {
        Self {
            value_ns: snapshot.value_ns,
            log_index: snapshot.log_index,
        }
    }

    fn as_snapshot(&self) -> GuardFloorSnapshot {
        GuardFloorSnapshot {
            value_ns: self.value_ns,
            log_index: self.log_index,
        }
    }

    fn ensure_with_clock<C: MonotonicClock + ?Sized>(
        &mut self,
        log_index: u64,
        guard_ns: u64,
        clock: &mut C,
    ) -> u64 {
        if log_index <= self.log_index {
            return self.value_ns;
        }
        let now_ns = clock.now_ns();
        let guard_value = saturating_u64(now_ns).saturating_sub(guard_ns);
        if guard_value > self.value_ns {
            self.value_ns = guard_value;
            self.log_index = log_index;
        }
        self.value_ns
    }

    fn value_ns(&self) -> u64 {
        self.value_ns
    }
}

/// Serialized snapshot for partition + metric watermark state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WatermarkSnapshot {
    pub partition_watermark_ns: u64,
    pub max_event_ts_ns: u64,
    pub guard_floor: GuardFloorSnapshot,
    pub metrics: Vec<MetricWatermarkSnapshot>,
}

/// Metric-level snapshot persisted in checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricWatermarkSnapshot {
    pub metric: String,
    pub watermark_ns: u64,
    pub max_event_ts_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_floor: Option<GuardFloorSnapshot>,
}

/// Result emitted after recording an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatermarkDecision {
    pub partition_watermark_ns: u64,
    pub metric_watermark_ns: Option<u64>,
    pub event_lateness_ns: u64,
}

/// Prometheus-friendly telemetry snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkTelemetry {
    pub partition_watermark_ms: u64,
    pub guard_floor_ms: u64,
    pub last_event_lateness_ms: u64,
    pub metric_watermarks_ms: Vec<MetricWatermarkTelemetry>,
}

/// Per-metric override telemetry entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricWatermarkTelemetry {
    pub metric: String,
    pub watermark_ms: u64,
}

/// Deterministic watermark tracker that maintains guard floors and telemetry.
#[derive(Debug, Clone)]
pub struct WatermarkTracker {
    config: WatermarkConfig,
    guard_floor: GuardFloor,
    partition_watermark_ns: u64,
    max_event_ts_ns: u64,
    last_event_lateness_ns: u64,
    metric_states: HashMap<String, MetricWatermarkState>,
}

impl WatermarkTracker {
    /// Creates a tracker with the provided configuration.
    pub fn new(config: WatermarkConfig) -> Self {
        Self {
            config,
            guard_floor: GuardFloor::new(),
            partition_watermark_ns: 0,
            max_event_ts_ns: 0,
            last_event_lateness_ns: 0,
            metric_states: HashMap::new(),
        }
    }

    /// Restores the tracker using a snapshot previously captured.
    pub fn from_snapshot(config: WatermarkConfig, snapshot: &WatermarkSnapshot) -> Self {
        let mut tracker = Self {
            guard_floor: GuardFloor::from_snapshot(snapshot.guard_floor),
            partition_watermark_ns: snapshot.partition_watermark_ns,
            max_event_ts_ns: snapshot.max_event_ts_ns,
            last_event_lateness_ns: 0,
            metric_states: HashMap::new(),
            config,
        };
        for metric_snapshot in &snapshot.metrics {
            if let Some(override_cfg) = tracker.config.override_for(&metric_snapshot.metric) {
                tracker.metric_states.insert(
                    metric_snapshot.metric.clone(),
                    MetricWatermarkState::from_snapshot(
                        metric_snapshot,
                        override_cfg
                            .lateness_allowance_ns
                            .unwrap_or(tracker.config.lateness_allowance_ns),
                        override_cfg.guard_ns,
                    ),
                );
            }
        }
        tracker
    }

    /// Serializes the current tracker state into a snapshot suitable for checkpoints.
    pub fn snapshot(&self) -> WatermarkSnapshot {
        let mut metrics: Vec<_> = self
            .metric_states
            .iter()
            .map(|(metric, state)| state.to_snapshot(metric))
            .collect();
        metrics.sort_by(|a, b| a.metric.cmp(&b.metric));
        WatermarkSnapshot {
            partition_watermark_ns: self.partition_watermark_ns,
            max_event_ts_ns: self.max_event_ts_ns,
            guard_floor: self.guard_floor.as_snapshot(),
            metrics,
        }
    }

    /// Returns the current partition watermark (ns).
    pub fn partition_watermark_ns(&self) -> u64 {
        self.partition_watermark_ns
    }

    /// Lag between the latest event timestamp and the current watermark (ns).
    pub fn watermark_lag_ns(&self) -> u64 {
        self.max_event_ts_ns
            .saturating_sub(self.partition_watermark_ns)
    }

    /// Returns telemetry information for publishing via `/metrics`.
    pub fn telemetry(&self) -> WatermarkTelemetry {
        let mut metric_entries: Vec<_> = self
            .metric_states
            .iter()
            .map(|(metric, state)| MetricWatermarkTelemetry {
                metric: metric.clone(),
                watermark_ms: ns_to_ms(state.watermark_ns()),
            })
            .collect();
        metric_entries.sort_by(|a, b| a.metric.cmp(&b.metric));
        WatermarkTelemetry {
            partition_watermark_ms: ns_to_ms(self.partition_watermark_ns),
            guard_floor_ms: ns_to_ms(self.guard_floor.value_ns()),
            last_event_lateness_ms: ns_to_ms(self.last_event_lateness_ns),
            metric_watermarks_ms: metric_entries,
        }
    }

    /// Records an event timestamp + WAL index and returns the resulting watermark decision.
    pub fn observe_event<C: MonotonicClock + ?Sized>(
        &mut self,
        metric: &str,
        event_ts_ns: u64,
        log_index: u64,
        clock: &mut C,
    ) -> WatermarkDecision {
        self.max_event_ts_ns = cmp::max(self.max_event_ts_ns, event_ts_ns);
        let candidate = self
            .max_event_ts_ns
            .saturating_sub(self.config.lateness_allowance_ns);
        let guard_value =
            self.guard_floor
                .ensure_with_clock(log_index, self.config.guard_ns, clock);
        let partition_next = cmp::min(candidate, guard_value);
        self.partition_watermark_ns = cmp::max(self.partition_watermark_ns, partition_next);
        self.last_event_lateness_ns = self.partition_watermark_ns.saturating_sub(event_ts_ns);

        let metric_watermark_ns = self
            .metric_state(metric)
            .map(|state| state.record_event(event_ts_ns, log_index, guard_value, clock));

        WatermarkDecision {
            partition_watermark_ns: self.partition_watermark_ns,
            metric_watermark_ns,
            event_lateness_ns: self.last_event_lateness_ns,
        }
    }

    fn metric_state(&mut self, metric: &str) -> Option<&mut MetricWatermarkState> {
        if let Some(override_cfg) = self.config.override_for(metric) {
            let lateness = override_cfg
                .lateness_allowance_ns
                .unwrap_or(self.config.lateness_allowance_ns);
            let entry = self
                .metric_states
                .entry(metric.to_string())
                .or_insert_with(|| MetricWatermarkState::new(lateness, override_cfg.guard_ns));
            Some(entry)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct MetricWatermarkState {
    lateness_allowance_ns: u64,
    guard_ns: Option<u64>,
    guard_floor: Option<GuardFloor>,
    watermark_ns: u64,
    max_event_ts_ns: u64,
}

impl MetricWatermarkState {
    fn new(lateness_allowance_ns: u64, guard_ns: Option<u64>) -> Self {
        Self {
            lateness_allowance_ns,
            guard_ns,
            guard_floor: guard_ns.map(|_| GuardFloor::new()),
            watermark_ns: 0,
            max_event_ts_ns: 0,
        }
    }

    fn from_snapshot(
        snapshot: &MetricWatermarkSnapshot,
        lateness_allowance_ns: u64,
        guard_ns: Option<u64>,
    ) -> Self {
        let guard_floor = match (guard_ns, snapshot.guard_floor) {
            (Some(_), Some(snapshot)) => Some(GuardFloor::from_snapshot(snapshot)),
            (Some(_), None) => Some(GuardFloor::new()),
            _ => None,
        };
        Self {
            lateness_allowance_ns,
            guard_ns,
            guard_floor,
            watermark_ns: snapshot.watermark_ns,
            max_event_ts_ns: snapshot.max_event_ts_ns,
        }
    }

    fn to_snapshot(&self, metric: &str) -> MetricWatermarkSnapshot {
        MetricWatermarkSnapshot {
            metric: metric.to_string(),
            watermark_ns: self.watermark_ns,
            max_event_ts_ns: self.max_event_ts_ns,
            guard_floor: self.guard_floor.map(|guard| guard.as_snapshot()),
        }
    }

    fn watermark_ns(&self) -> u64 {
        self.watermark_ns
    }

    fn record_event<C: MonotonicClock + ?Sized>(
        &mut self,
        event_ts_ns: u64,
        log_index: u64,
        shared_guard_value: u64,
        clock: &mut C,
    ) -> u64 {
        self.max_event_ts_ns = cmp::max(self.max_event_ts_ns, event_ts_ns);
        let candidate = self
            .max_event_ts_ns
            .saturating_sub(self.lateness_allowance_ns);
        let guard_value = match (self.guard_ns, self.guard_floor.as_mut()) {
            (Some(guard_ns), Some(guard_floor)) => {
                guard_floor.ensure_with_clock(log_index, guard_ns, clock)
            }
            _ => shared_guard_value,
        };
        let next = cmp::min(candidate, guard_value);
        self.watermark_ns = cmp::max(self.watermark_ns, next);
        self.watermark_ns
    }
}

fn ns_to_ms(ns: u64) -> u64 {
    ns / 1_000_000
}

fn saturating_u64(value: u128) -> u64 {
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}

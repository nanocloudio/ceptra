use crate::consensus_core::IngestStatusCode;
use clustor::{FlowDecision, FlowThrottleState};
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default interval for publishing credit window hints (200 ms as per spec).
pub const CREDIT_WINDOW_PUBLISH_INTERVAL: Duration = Duration::from_millis(200);
pub const COMMIT_APPLY_QUEUE_CAPACITY: usize = 50_000;
pub const EMIT_QUEUE_CAPACITY: usize = 20_000;

/// Snapshot of the PID controller state sourced from Clustor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PidSnapshot {
    pub entry_credits: i64,
    pub entry_credit_max: i64,
    pub byte_credits: i64,
    pub byte_credit_max: i64,
    pub throttle_state: FlowThrottleState,
}

impl PidSnapshot {
    fn available_events(&self) -> i64 {
        self.entry_credits.max(0)
    }

    fn clamped_events(&self) -> i64 {
        let cap = self.entry_credit_max.max(0);
        self.available_events().min(cap)
    }

    fn available_bytes(&self) -> i64 {
        (self.byte_credit_max - self.byte_credits).max(0)
    }

    fn clamped_bytes(&self) -> i64 {
        let cap = self.byte_credit_max.max(0);
        self.available_bytes().min(cap)
    }
}

impl From<&FlowDecision> for PidSnapshot {
    fn from(flow: &FlowDecision) -> Self {
        Self {
            entry_credits: flow.entry_credits,
            entry_credit_max: flow.entry_credit_max,
            byte_credits: flow.byte_credits,
            byte_credit_max: flow.byte_credit_max,
            throttle_state: flow.throttle.state.clone(),
        }
    }
}

/// Hint values communicated to CEP client SDKs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditWindowHint {
    pub credit_window_hint_events: u32,
    pub credit_window_hint_bytes: u64,
}

impl CreditWindowHint {
    fn from_snapshot(snapshot: &PidSnapshot) -> Self {
        let events = snapshot.clamped_events().max(0) as u32;
        let bytes = snapshot.clamped_bytes().max(0) as u64;
        Self {
            credit_window_hint_events: events,
            credit_window_hint_bytes: bytes,
        }
    }
}

/// Structured trace emitted on every hint refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditWindowTrace {
    pub at: Instant,
    pub events: u32,
    pub bytes: u64,
    pub throttle: FlowThrottleState,
}

/// Exported metrics for the hint publisher.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CreditWindowMetrics {
    pub credit_window_hint_events: u32,
    pub credit_window_hint_bytes: u64,
    pub credit_window_hint_violation_total: u64,
}

/// Aggregated telemetry for credit window publications.
#[derive(Debug, Default, Clone)]
pub struct CreditWindowTelemetry {
    traces: Vec<CreditWindowTrace>,
    metrics: CreditWindowMetrics,
}

impl CreditWindowTelemetry {
    fn record(&mut self, at: Instant, hint: &CreditWindowHint, snapshot: &PidSnapshot) {
        if (hint.credit_window_hint_events as i64) > snapshot.available_events() {
            self.metrics.credit_window_hint_violation_total += 1;
        }
        if (hint.credit_window_hint_bytes as i64) > snapshot.available_bytes() {
            self.metrics.credit_window_hint_violation_total += 1;
        }
        self.metrics.credit_window_hint_events = hint.credit_window_hint_events;
        self.metrics.credit_window_hint_bytes = hint.credit_window_hint_bytes;
        self.traces.push(CreditWindowTrace {
            at,
            events: hint.credit_window_hint_events,
            bytes: hint.credit_window_hint_bytes,
            throttle: snapshot.throttle_state.clone(),
        });
    }

    pub fn traces(&self) -> &[CreditWindowTrace] {
        &self.traces
    }

    pub fn metrics(&self) -> &CreditWindowMetrics {
        &self.metrics
    }

    pub fn render_metrics(&self) -> String {
        format!(
            "leader_credit_window_hint_events {}\nleader_credit_window_hint_bytes {}\nleader_credit_window_hint_violation_total {}\n",
            self.metrics.credit_window_hint_events,
            self.metrics.credit_window_hint_bytes,
            self.metrics.credit_window_hint_violation_total
        )
    }
}

/// Periodically publishes credit window hints for clients.
#[derive(Debug, Clone)]
pub struct CreditWindowHintTimer {
    interval: Duration,
    last_publish: Option<Instant>,
    telemetry: CreditWindowTelemetry,
}

impl Default for CreditWindowHintTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl CreditWindowHintTimer {
    /// Builds a timer that uses the default 200 ms cadence.
    pub fn new() -> Self {
        Self::with_interval(CREDIT_WINDOW_PUBLISH_INTERVAL)
    }

    /// Allows tests to inject a custom cadence.
    pub fn with_interval(interval: Duration) -> Self {
        Self {
            interval,
            last_publish: None,
            telemetry: CreditWindowTelemetry::default(),
        }
    }

    /// Polls the PID snapshot; returns a hint when the cadence fires while leader.
    pub fn poll(
        &mut self,
        now: Instant,
        snapshot: PidSnapshot,
        is_leader: bool,
    ) -> Option<CreditWindowHint> {
        if !is_leader {
            self.last_publish = None;
            return None;
        }
        if let Some(last) = self.last_publish {
            if now.duration_since(last) < self.interval {
                return None;
            }
        }
        let hint = CreditWindowHint::from_snapshot(&snapshot);
        self.last_publish = Some(now);
        self.telemetry.record(now, &hint, &snapshot);
        Some(hint)
    }

    /// Convenience helper that builds the snapshot from a raw FlowDecision.
    pub fn poll_flow(
        &mut self,
        now: Instant,
        flow: &FlowDecision,
        is_leader: bool,
    ) -> Option<CreditWindowHint> {
        let snapshot = PidSnapshot::from(flow);
        self.poll(now, snapshot, is_leader)
    }

    /// Exposes the collected telemetry.
    pub fn telemetry(&self) -> &CreditWindowTelemetry {
        &self.telemetry
    }

    /// Consumes the timer, returning telemetry for export.
    pub fn into_telemetry(self) -> CreditWindowTelemetry {
        self.telemetry
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureQueueKind {
    CommitApply,
    Emission,
}

impl BackpressureQueueKind {
    fn label(&self) -> &'static str {
        match self {
            BackpressureQueueKind::CommitApply => "commit_apply",
            BackpressureQueueKind::Emission => "emission",
        }
    }
}

/// Lock-free bounded queue used for the commit→apply and emission buffers.
#[derive(Debug, Clone)]
pub struct BackpressureQueue<T> {
    kind: BackpressureQueueKind,
    queue: Arc<ArrayQueue<T>>,
    capacity: usize,
}

impl<T> BackpressureQueue<T> {
    /// Builds a queue for the commit→apply pipeline with the default capacity (50k entries).
    pub fn commit_apply() -> Self {
        Self::new(
            BackpressureQueueKind::CommitApply,
            COMMIT_APPLY_QUEUE_CAPACITY,
        )
    }

    /// Builds a queue for the emission pipeline with the default capacity (20k entries).
    pub fn emission() -> Self {
        Self::new(BackpressureQueueKind::Emission, EMIT_QUEUE_CAPACITY)
    }

    /// Creates a queue with a custom capacity and kind.
    pub fn new(kind: BackpressureQueueKind, capacity: usize) -> Self {
        assert!(capacity > 0, "backpressure queue capacity must be > 0");
        Self {
            kind,
            queue: Arc::new(ArrayQueue::new(capacity)),
            capacity,
        }
    }

    /// Attempts to enqueue an item, returning a backpressure error when saturated.
    pub fn enqueue(&self, item: T) -> Result<(), BackpressureError> {
        self.queue
            .push(item)
            .map_err(|_| BackpressureError { kind: self.kind })
    }

    /// Attempts to dequeue an item.
    pub fn try_dequeue(&self) -> Option<T> {
        self.queue.pop()
    }

    /// Returns the current depth.
    pub fn depth(&self) -> usize {
        self.queue.len()
    }

    /// Returns the configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the queue kind.
    pub fn kind(&self) -> BackpressureQueueKind {
        self.kind
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Error returned when pushing onto a saturated queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackpressureError {
    pub kind: BackpressureQueueKind,
}

impl BackpressureError {
    /// Translates the backpressure condition into a status code for clients.
    pub fn status(&self) -> IngestStatusCode {
        IngestStatusCode::TransientBackpressure
    }

    /// Human readable label describing the saturated queue.
    pub fn message(&self) -> String {
        format!("{}_queue_saturated", self.kind.label())
    }

    /// Builds a `why`-style explanation that can be surfaced via the admin APIs.
    pub fn to_why(&self) -> BackpressureWhy {
        BackpressureWhy {
            status: self.status(),
            code: match self.kind {
                BackpressureQueueKind::CommitApply => "commit_apply_backpressure",
                BackpressureQueueKind::Emission => "emission_queue_backpressure",
            },
            detail: self.message(),
        }
    }
}

/// Minimal envelope that mirrors Clustor's `Why*` records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressureWhy {
    pub status: IngestStatusCode,
    pub code: &'static str,
    pub detail: String,
}

/// Snapshot of queue depths and lag metrics exported via `/metrics`.
#[derive(Debug, Default, Clone)]
pub struct QueueDepthTelemetry {
    commit_depth: usize,
    emission_depth: usize,
    emission_capacity: usize,
    replication_lag_ms: u64,
    apply_lag_ms: u64,
}

impl QueueDepthTelemetry {
    pub fn record_commit_depth(&mut self, depth: usize) {
        self.commit_depth = depth;
    }

    pub fn record_emission_depth(&mut self, depth: usize, capacity: usize) {
        self.emission_depth = depth;
        self.emission_capacity = capacity.max(1);
    }

    pub fn record_replication_lag_ms(&mut self, lag_ms: u64) {
        self.replication_lag_ms = lag_ms;
    }

    pub fn record_apply_lag_ms(&mut self, lag_ms: u64) {
        self.apply_lag_ms = lag_ms;
    }

    pub fn utilization(&self) -> f64 {
        if self.emission_capacity == 0 {
            return 0.0;
        }
        (self.emission_depth as f64 / self.emission_capacity as f64).clamp(0.0, 1.0)
    }

    pub fn render_metrics(&self) -> String {
        format!(
            "queue_depth{{type=\"commit_apply\"}} {}\nqueue_depth{{type=\"emission\"}} {}\nreplication_lag_ms {}\napply_lag_ms {}\nap_emit_queue_utilization {:.6}\n",
            self.commit_depth,
            self.emission_depth,
            self.replication_lag_ms,
            self.apply_lag_ms,
            self.utilization()
        )
    }
}

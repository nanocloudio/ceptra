use crate::ceptra::aggregation::{AggregationPipeline, AggregationRecord};
use crate::ceptra::cel::{CelEvaluationRequest, CelRuntime};
use crate::ceptra::derived::DerivedEmission;
use crate::ceptra::time::{EventAdmission, LateEventAudit, LatenessResult, TimeSemantics};
use crate::event_model::{DedupDecision, DedupTable, WalEntry};
use std::collections::{BTreeMap, HashMap};

/// Committed batch delivered by the Clustor apply threads.
#[derive(Debug, Clone)]
pub struct CommittedBatch {
    partition_id: String,
    events: Vec<CommittedEvent>,
}

impl CommittedBatch {
    pub fn new(partition_id: impl Into<String>, events: Vec<CommittedEvent>) -> Self {
        Self {
            partition_id: partition_id.into(),
            events,
        }
    }

    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn into_events(self) -> Vec<CommittedEvent> {
        self.events
    }
}

/// Event recovered from the WAL alongside commit metadata.
#[derive(Debug, Clone)]
pub struct CommittedEvent {
    partition_id: String,
    pub event_id: String,
    pub partition_key: Vec<u8>,
    pub wal_entry: WalEntry,
    measurements: Vec<EventMeasurement>,
}

impl CommittedEvent {
    pub fn new(
        partition_id: impl Into<String>,
        event_id: impl Into<String>,
        partition_key: impl Into<Vec<u8>>,
        wal_entry: WalEntry,
    ) -> Self {
        Self {
            partition_id: partition_id.into(),
            event_id: event_id.into(),
            partition_key: partition_key.into(),
            wal_entry,
            measurements: Vec::new(),
        }
    }

    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    pub fn log_index(&self) -> u64 {
        self.wal_entry.log_index
    }

    pub fn commit_epoch_tick_ms(&self) -> u64 {
        self.wal_entry.commit_epoch_tick_ms
    }

    pub fn payload(&self) -> &[u8] {
        &self.wal_entry.payload
    }

    pub fn wal_entry(&self) -> &WalEntry {
        &self.wal_entry
    }

    pub fn with_measurements(mut self, measurements: Vec<EventMeasurement>) -> Self {
        self.measurements = measurements;
        self
    }

    pub fn measurements(&self) -> &[EventMeasurement] {
        &self.measurements
    }
}

/// Aggregation-ready measurement emitted by the PromQL compiler.
#[derive(Debug, Clone, PartialEq)]
pub struct EventMeasurement {
    pub metric: String,
    pub value: Option<f64>,
    pub item: Option<String>,
    pub lane_bitmap: u64,
    pub labels: BTreeMap<String, String>,
}

impl EventMeasurement {
    pub fn new(metric: impl Into<String>) -> Self {
        Self {
            metric: metric.into(),
            value: None,
            item: None,
            lane_bitmap: 0,
            labels: BTreeMap::new(),
        }
    }

    pub fn with_value(mut self, value: f64) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_item(mut self, item: impl Into<String>) -> Self {
        self.item = Some(item.into());
        self
    }

    pub fn with_lane_bitmap(mut self, lane_bitmap: u64) -> Self {
        self.lane_bitmap = lane_bitmap;
        self
    }

    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }
}

/// Orchestrates deduplication, aggregation, and CEL evaluation for committed batches.
pub struct ApplyLoop {
    dedup: DedupTable,
    aggregation: Box<dyn AggregationPipeline>,
    cel_runtime: Box<dyn CelRuntime>,
    channel_policies: ChannelPolicies,
    time_semantics: TimeSemantics,
}

impl ApplyLoop {
    pub fn new(
        dedup: DedupTable,
        aggregation: Box<dyn AggregationPipeline>,
        cel_runtime: Box<dyn CelRuntime>,
    ) -> Self {
        let time_semantics = TimeSemantics::for_pipeline(aggregation.metric_specs());
        Self {
            dedup,
            aggregation,
            cel_runtime,
            channel_policies: ChannelPolicies::default(),
            time_semantics,
        }
    }

    pub fn with_channel_policies(
        dedup: DedupTable,
        aggregation: Box<dyn AggregationPipeline>,
        cel_runtime: Box<dyn CelRuntime>,
        channel_policies: ChannelPolicies,
    ) -> Self {
        let time_semantics = TimeSemantics::for_pipeline(aggregation.metric_specs());
        Self {
            dedup,
            aggregation,
            cel_runtime,
            channel_policies,
            time_semantics,
        }
    }

    /// Processes a committed batch, returning stats and derived emissions grouped by channel.
    pub fn process_batch(&mut self, batch: CommittedBatch) -> ApplyBatchResult {
        let mut stats = BatchStats::default();
        let mut emissions = EmissionBatch::default();

        for event in batch.into_events() {
            stats.total_events += 1;
            let dedup_outcome = self.dedup.check(
                &event.event_id,
                event.payload(),
                event.commit_epoch_tick_ms(),
            );
            if dedup_outcome.decision != DedupDecision::Fresh {
                stats.dedup_skipped += 1;
                continue;
            }
            let lateness = self.time_semantics.evaluate_event(&event);
            if lateness.admission != EventAdmission::Accepted {
                continue;
            }
            let LatenessResult {
                filtered_measurements,
                dropped_metrics,
                ..
            } = lateness;
            let event = event.with_measurements(filtered_measurements);
            let AggregationRecord {
                mut bindings,
                lane_bitmap,
            } = self.aggregation.ingest(&event);
            self.time_semantics
                .decorate_bindings(&mut bindings, &dropped_metrics);
            let cel_result = self.cel_runtime.evaluate(CelEvaluationRequest {
                event: &event,
                bindings: &bindings,
                lane_bitmap,
            });
            for derived in cel_result.derived {
                emissions.record(derived, &self.channel_policies);
            }
        }

        ApplyBatchResult { stats, emissions }
    }

    /// Exposes late-event audit entries accumulated by the time semantics engine.
    pub fn late_event_audit(&self) -> &[LateEventAudit] {
        self.time_semantics.audit_log()
    }

    /// Returns per-reason counters for late-event handling.
    pub fn late_event_counters(&self) -> BTreeMap<String, u64> {
        self.time_semantics.late_event_counters()
    }
}

/// Summary statistics for a processed batch.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BatchStats {
    pub total_events: usize,
    pub dedup_skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionDropReason {
    QueueFullDropOldest,
    QueueFullBlockTimeout,
    DeadLetterOverflow,
    DeadLetterLoop,
}

/// Drop log entry emitted whenever a derived event is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmissionDropLog {
    pub channel: String,
    pub reason: EmissionDropReason,
}

/// Derived emission requests grouped by channel.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EmissionBatch {
    by_channel: BTreeMap<String, Vec<DerivedEmission>>,
    drop_counts: BTreeMap<String, u64>,
    drop_logs: Vec<EmissionDropLog>,
}

impl EmissionBatch {
    pub fn record(&mut self, emission: DerivedEmission, policies: &ChannelPolicies) {
        self.record_with_depth(emission, policies, 0);
    }

    pub fn by_channel(&self) -> &BTreeMap<String, Vec<DerivedEmission>> {
        &self.by_channel
    }

    pub fn into_channel_map(self) -> BTreeMap<String, Vec<DerivedEmission>> {
        self.by_channel
    }

    pub fn total(&self) -> usize {
        self.by_channel.values().map(|entries| entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_channel.is_empty()
    }

    pub fn drop_metrics(&self) -> &BTreeMap<String, u64> {
        &self.drop_counts
    }

    pub fn drop_logs(&self) -> &[EmissionDropLog] {
        &self.drop_logs
    }

    fn record_with_depth(
        &mut self,
        emission: DerivedEmission,
        policies: &ChannelPolicies,
        depth: usize,
    ) {
        if depth > 4 {
            self.increment_drop(&emission.channel);
            return;
        }
        let policy = policies.policy_for(&emission.channel);
        match &policy.kind {
            ChannelPolicyKind::DropOldest { capacity } => {
                if self.enforce_capacity(&emission.channel, *capacity, true) {
                    self.log_drop(&emission.channel, EmissionDropReason::QueueFullDropOldest);
                }
                self.by_channel
                    .entry(emission.channel.clone())
                    .or_default()
                    .push(emission);
            }
            ChannelPolicyKind::BlockWithTimeout { capacity, .. } => {
                let queue = self.by_channel.entry(emission.channel.clone()).or_default();
                if queue.len() >= *capacity {
                    self.increment_drop(&emission.channel);
                    self.log_drop(&emission.channel, EmissionDropReason::QueueFullBlockTimeout);
                    return;
                }
                queue.push(emission);
            }
            ChannelPolicyKind::DeadLetter {
                capacity,
                dead_letter_channel,
            } => {
                if *capacity == 0 {
                    let mut rerouted = emission;
                    self.increment_drop(&rerouted.channel);
                    self.log_drop(&rerouted.channel, EmissionDropReason::DeadLetterOverflow);
                    rerouted.channel = dead_letter_channel.clone();
                    self.record_with_depth(rerouted, policies, depth + 1);
                    return;
                }
                let mut overflow = None;
                let channel_name = emission.channel.clone();
                {
                    let queue = self.by_channel.entry(channel_name.clone()).or_default();
                    if queue.len() >= *capacity {
                        overflow = Some(queue.remove(0));
                    }
                    queue.push(emission);
                }
                if overflow.is_some() {
                    self.increment_drop(&channel_name);
                    self.log_drop(&channel_name, EmissionDropReason::DeadLetterOverflow);
                }
                if let Some(mut overflow_event) = overflow {
                    if overflow_event.channel != *dead_letter_channel {
                        overflow_event.channel = dead_letter_channel.clone();
                        self.record_with_depth(overflow_event, policies, depth + 1);
                    } else {
                        self.increment_drop(dead_letter_channel);
                        self.log_drop(dead_letter_channel, EmissionDropReason::DeadLetterLoop);
                    }
                }
            }
        }
    }

    fn enforce_capacity(&mut self, channel: &str, capacity: usize, drop_newest: bool) -> bool {
        if capacity == 0 {
            self.increment_drop(channel);
            return true;
        }
        if let Some(queue) = self.by_channel.get_mut(channel) {
            if queue.len() >= capacity {
                if drop_newest && !queue.is_empty() {
                    queue.remove(0);
                }
                self.increment_drop(channel);
                return true;
            }
        }
        false
    }

    fn increment_drop(&mut self, channel: &str) {
        *self.drop_counts.entry(channel.to_string()).or_insert(0) += 1;
    }

    fn log_drop(&mut self, channel: &str, reason: EmissionDropReason) {
        self.drop_logs.push(EmissionDropLog {
            channel: channel.to_string(),
            reason,
        });
    }
}

/// Result for a processed batch, including stats and derived requests.
#[derive(Debug, Default)]
pub struct ApplyBatchResult {
    pub stats: BatchStats,
    pub emissions: EmissionBatch,
}

#[derive(Debug, Clone)]
pub struct ChannelPolicy {
    pub kind: ChannelPolicyKind,
}

#[derive(Debug, Clone)]
pub enum ChannelPolicyKind {
    DropOldest {
        capacity: usize,
    },
    BlockWithTimeout {
        capacity: usize,
        timeout_ms: u64,
    },
    DeadLetter {
        capacity: usize,
        dead_letter_channel: String,
    },
}

impl ChannelPolicy {
    pub fn drop_oldest(capacity: usize) -> Self {
        Self {
            kind: ChannelPolicyKind::DropOldest { capacity },
        }
    }

    pub fn block_with_timeout(capacity: usize, timeout_ms: u64) -> Self {
        Self {
            kind: ChannelPolicyKind::BlockWithTimeout {
                capacity,
                timeout_ms,
            },
        }
    }

    pub fn dead_letter(capacity: usize, dead_letter_channel: impl Into<String>) -> Self {
        Self {
            kind: ChannelPolicyKind::DeadLetter {
                capacity,
                dead_letter_channel: dead_letter_channel.into(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelPolicies {
    policies: HashMap<String, ChannelPolicy>,
    default: ChannelPolicy,
}

impl Default for ChannelPolicies {
    fn default() -> Self {
        Self {
            policies: HashMap::new(),
            default: ChannelPolicy::drop_oldest(usize::MAX),
        }
    }
}

impl ChannelPolicies {
    pub fn new(default: ChannelPolicy) -> Self {
        Self {
            policies: HashMap::new(),
            default,
        }
    }

    pub fn set(&mut self, channel: impl Into<String>, policy: ChannelPolicy) {
        self.policies.insert(channel.into(), policy);
    }

    fn policy_for(&self, channel: &str) -> &ChannelPolicy {
        self.policies.get(channel).unwrap_or(&self.default)
    }
}

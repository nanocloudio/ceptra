use ceptra::{
    AggregationPipeline, AggregationRecord, ApplyBatchResult, ApplyLoop, CelEvaluationRequest,
    CelEvaluationResult, CelRuntime, ChannelPolicies, ChannelPolicy, CommittedBatch,
    CommittedEvent, DedupTable, DedupTableConfig, DerivedEmission, MetricBinding, WalEntry,
};
use std::sync::{Arc, Mutex};

struct RecordingAggregationPipeline {
    invocations: Arc<Mutex<Vec<String>>>,
}

impl RecordingAggregationPipeline {
    fn new(invocations: Arc<Mutex<Vec<String>>>) -> Self {
        Self { invocations }
    }
}

impl AggregationPipeline for RecordingAggregationPipeline {
    fn ingest(&mut self, event: &CommittedEvent) -> AggregationRecord {
        self.invocations
            .lock()
            .unwrap()
            .push(event.event_id.clone());
        AggregationRecord {
            bindings: vec![MetricBinding::with_value(
                format!("metric-{}", event.event_id),
                event.log_index() as f64,
            )],
            lane_bitmap: 0b01,
        }
    }
}

struct RecordingCelRuntime {
    evaluations: Arc<Mutex<Vec<u64>>>,
}

impl RecordingCelRuntime {
    fn new(evaluations: Arc<Mutex<Vec<u64>>>) -> Self {
        Self { evaluations }
    }
}

struct PolicyTestCelRuntime;

impl CelRuntime for PolicyTestCelRuntime {
    fn evaluate(&mut self, request: CelEvaluationRequest<'_>) -> CelEvaluationResult {
        CelEvaluationResult {
            derived: vec![DerivedEmission::new(
                "alerts",
                "schema.alerts",
                format!("payload-{}", request.event.event_id).as_bytes(),
                format!("derived-{}", request.event.event_id),
                "rule-policy",
                1,
            )],
        }
    }
}

impl CelRuntime for RecordingCelRuntime {
    fn evaluate(&mut self, request: CelEvaluationRequest<'_>) -> CelEvaluationResult {
        self.evaluations
            .lock()
            .unwrap()
            .push(request.event.log_index());
        CelEvaluationResult {
            derived: vec![DerivedEmission::new(
                "alerts",
                "schema.alerts",
                format!("emit-{}", request.event.event_id).as_bytes(),
                format!("derived-{}", request.event.event_id),
                "rule-a",
                1,
            )],
        }
    }
}

fn committed_event(
    partition_id: &str,
    log_index: u64,
    event_id: &str,
    payload: &[u8],
) -> CommittedEvent {
    let wal_entry = WalEntry::new(log_index, log_index, payload);
    CommittedEvent::new(
        partition_id.to_string(),
        event_id.to_string(),
        format!("key-{event_id}"),
        wal_entry,
    )
}

#[test]
fn apply_loop_batches_emissions_and_skips_dedup_hits() {
    let dedup = DedupTable::new(DedupTableConfig {
        client_retry_window_s: 30,
    });
    let aggregation_invocations = Arc::new(Mutex::new(Vec::new()));
    let cel_invocations = Arc::new(Mutex::new(Vec::new()));
    let aggregator = Box::new(RecordingAggregationPipeline::new(
        aggregation_invocations.clone(),
    ));
    let cel_runtime = Box::new(RecordingCelRuntime::new(cel_invocations.clone()));
    let mut apply_loop = ApplyLoop::new(dedup, aggregator, cel_runtime);

    let batch = CommittedBatch::new(
        "partition-a",
        vec![
            committed_event("partition-a", 10, "evt-1", b"alpha"),
            committed_event("partition-a", 11, "evt-2", b"beta"),
            // Replays the first event ID to trigger dedup short-circuiting.
            committed_event("partition-a", 12, "evt-1", b"alpha"),
        ],
    );
    let result = apply_loop.process_batch(batch);

    assert_eq!(result.stats.total_events, 3);
    assert_eq!(result.stats.dedup_skipped, 1);
    assert_eq!(result.emissions.by_channel().len(), 1);
    let alerts = result.emissions.by_channel().get("alerts").unwrap();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].payload, b"emit-evt-1");
    assert_eq!(alerts[1].payload, b"emit-evt-2");

    let aggregation_calls = aggregation_invocations.lock().unwrap();
    assert_eq!(
        *aggregation_calls,
        vec!["evt-1".to_string(), "evt-2".to_string()]
    );
    drop(aggregation_calls);

    let cel_calls = cel_invocations.lock().unwrap();
    assert_eq!(*cel_calls, vec![10, 11]);
}

fn test_policies(policy: ChannelPolicy) -> ApplyBatchResult {
    let dedup = DedupTable::new(DedupTableConfig {
        client_retry_window_s: 30,
    });
    let aggregator = Box::new(RecordingAggregationPipeline::new(Arc::new(Mutex::new(
        Vec::new(),
    ))));
    let mut policies = ChannelPolicies::default();
    policies.set("alerts", policy);
    let cel_runtime = Box::new(PolicyTestCelRuntime);
    let mut apply_loop = ApplyLoop::with_channel_policies(dedup, aggregator, cel_runtime, policies);
    let batch = CommittedBatch::new(
        "partition-a",
        vec![
            committed_event("partition-a", 1, "evt-1", b"a"),
            committed_event("partition-a", 2, "evt-2", b"b"),
        ],
    );
    apply_loop.process_batch(batch)
}

#[test]
fn channel_drop_oldest_discards_prior_entries() {
    let result = test_policies(ChannelPolicy::drop_oldest(1));
    let alerts = result.emissions.by_channel().get("alerts").unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].payload, b"payload-evt-2");
    assert_eq!(result.emissions.drop_metrics().get("alerts"), Some(&1));
}

#[test]
fn channel_block_with_timeout_drops_new_entries() {
    let result = test_policies(ChannelPolicy::block_with_timeout(1, 10));
    let alerts = result.emissions.by_channel().get("alerts").unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].payload, b"payload-evt-1");
    assert_eq!(result.emissions.drop_metrics().get("alerts"), Some(&1));
}

#[test]
fn channel_dead_letter_routes_overflow() {
    let dedup = DedupTable::new(DedupTableConfig {
        client_retry_window_s: 30,
    });
    let aggregator = Box::new(RecordingAggregationPipeline::new(Arc::new(Mutex::new(
        Vec::new(),
    ))));
    let mut policies = ChannelPolicies::default();
    policies.set("alerts", ChannelPolicy::dead_letter(1, "dead-letter"));
    let cel_runtime = Box::new(PolicyTestCelRuntime);
    let mut apply_loop = ApplyLoop::with_channel_policies(dedup, aggregator, cel_runtime, policies);
    let batch = CommittedBatch::new(
        "partition-a",
        vec![
            committed_event("partition-a", 1, "evt-1", b"a"),
            committed_event("partition-a", 2, "evt-2", b"b"),
        ],
    );
    let result = apply_loop.process_batch(batch);
    let alerts = result.emissions.by_channel().get("alerts").unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].payload, b"payload-evt-2");
    let dead_letter = result
        .emissions
        .by_channel()
        .get("dead-letter")
        .expect("dead letter channel missing");
    assert_eq!(dead_letter.len(), 1);
    assert_eq!(dead_letter[0].payload, b"payload-evt-1");
    assert_eq!(result.emissions.drop_metrics().get("alerts"), Some(&1));
}

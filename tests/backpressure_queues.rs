use ceptra::{
    consensus_core::IngestStatusCode, BackpressureQueue, BackpressureQueueKind, QueueDepthTelemetry,
};

#[test]
fn commit_queue_enforces_capacity() {
    let queue = BackpressureQueue::new(BackpressureQueueKind::CommitApply, 2);
    assert!(queue.enqueue(1).is_ok());
    assert!(queue.enqueue(2).is_ok());
    let err = queue.enqueue(3).expect_err("queue should be saturated");
    assert_eq!(err.kind, BackpressureQueueKind::CommitApply);
    assert_eq!(err.status(), IngestStatusCode::TransientBackpressure);
    assert_eq!(err.message(), "commit_apply_queue_saturated");
    assert_eq!(queue.depth(), 2);
    assert_eq!(queue.try_dequeue(), Some(1));
    assert_eq!(queue.try_dequeue(), Some(2));
    assert!(queue.try_dequeue().is_none());
}

#[test]
fn emission_queue_tracks_depth() {
    let queue = BackpressureQueue::emission();
    assert!(queue.is_empty());
    for i in 0..3 {
        queue.enqueue(i).unwrap();
    }
    assert_eq!(queue.depth(), 3);
    assert_eq!(queue.capacity(), ceptra::EMIT_QUEUE_CAPACITY);
}

#[test]
fn backpressure_error_maps_to_why() {
    let queue = BackpressureQueue::new(BackpressureQueueKind::Emission, 1);
    queue.enqueue(42).unwrap();
    let err = queue.enqueue(99).expect_err("should return backpressure");
    let why = err.to_why();
    assert_eq!(why.status, IngestStatusCode::TransientBackpressure);
    assert_eq!(why.code, "emission_queue_backpressure");
    assert!(why.detail.contains("emission"));
}

#[test]
fn queue_depth_metrics_include_utilization() {
    let mut telemetry = QueueDepthTelemetry::default();
    telemetry.record_commit_depth(128);
    telemetry.record_emission_depth(10, 20);
    telemetry.record_replication_lag_ms(15);
    telemetry.record_apply_lag_ms(7);
    let metrics = telemetry.render_metrics();
    assert!(metrics.contains("queue_depth{type=\"commit_apply\"} 128"));
    assert!(metrics.contains("queue_depth{type=\"emission\"} 10"));
    assert!(metrics.contains("replication_lag_ms 15"));
    assert!(metrics.contains("apply_lag_ms 7"));
    assert!(metrics.contains("ap_emit_queue_utilization"));
}

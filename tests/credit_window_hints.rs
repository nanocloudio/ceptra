use ceptra::{CreditWindowHintTimer, PidSnapshot};
use clustor::FlowThrottleState;
use std::time::{Duration, Instant};

fn snapshot(entry: i64, entry_max: i64, byte_debt: i64, byte_max: i64) -> PidSnapshot {
    PidSnapshot {
        entry_credits: entry,
        entry_credit_max: entry_max,
        byte_credits: byte_debt,
        byte_credit_max: byte_max,
        throttle_state: FlowThrottleState::Open,
    }
}

#[test]
fn publishes_every_200ms_for_leader() {
    let mut timer = CreditWindowHintTimer::new();
    let base = Instant::now();
    let snap = snapshot(256, 512, 128, 1024);
    assert!(timer.poll(base, snap.clone(), true).is_some());
    assert!(timer
        .poll(base + Duration::from_millis(50), snap.clone(), true)
        .is_none());
    let hint = timer
        .poll(base + Duration::from_millis(220), snap, true)
        .expect("should publish second hint");
    assert_eq!(hint.credit_window_hint_events, 256);
    assert_eq!(hint.credit_window_hint_bytes, 896);
}

#[test]
fn skips_when_not_leader() {
    let mut timer = CreditWindowHintTimer::new();
    let now = Instant::now();
    let snap = snapshot(128, 512, 0, 1024);
    assert!(timer.poll(now, snap.clone(), false).is_none());
    assert!(timer
        .poll(now + Duration::from_millis(300), snap, false)
        .is_none());
}

#[test]
fn telemetry_tracks_traces_and_clamps() {
    let mut timer = CreditWindowHintTimer::new();
    let now = Instant::now();
    let snap = snapshot(1_024, 256, -128, 1_024);
    let hint = timer
        .poll(now, snap, true)
        .expect("leader should publish immediately");
    assert_eq!(hint.credit_window_hint_events, 256);
    assert_eq!(hint.credit_window_hint_bytes, 1_024);
    let telemetry = timer.telemetry();
    assert_eq!(telemetry.traces().len(), 1);
    let trace = &telemetry.traces()[0];
    assert_eq!(trace.events, 256);
    assert_eq!(trace.bytes, 1_024);
    assert_eq!(trace.throttle, FlowThrottleState::Open);
    assert_eq!(telemetry.metrics().credit_window_hint_violation_total, 0);
    let metrics = telemetry.render_metrics();
    assert!(metrics.contains("leader_credit_window_hint_events 256"));
    assert!(metrics.contains("leader_credit_window_hint_bytes 1024"));
}

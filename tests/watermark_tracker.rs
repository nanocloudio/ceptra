use ceptra::{MetricWatermarkOverride, MonotonicClock, WatermarkConfig, WatermarkTracker};

struct MockClock {
    readings: Vec<u128>,
    idx: usize,
    calls: usize,
}

impl MockClock {
    fn new(readings: Vec<u128>) -> Self {
        Self {
            readings,
            idx: 0,
            calls: 0,
        }
    }

    fn call_count(&self) -> usize {
        self.calls
    }
}

impl MonotonicClock for MockClock {
    fn now_ns(&mut self) -> u128 {
        self.calls += 1;
        let reading = self
            .readings
            .get(self.idx)
            .copied()
            .unwrap_or_else(|| *self.readings.last().unwrap());
        self.idx += 1;
        reading
    }
}

#[test]
fn tracks_watermark_and_guard_floor() {
    let config = WatermarkConfig::default();
    let mut tracker = WatermarkTracker::new(config);
    let mut clock = MockClock::new(vec![300_000_000, 500_000_000, 700_000_000]);

    let decision1 = tracker.observe_event("metric", 3_000_000_000, 10, &mut clock);
    assert!(decision1.partition_watermark_ns > 0);
    assert_eq!(decision1.metric_watermark_ns, None);

    let decision2 = tracker.observe_event("metric", 3_200_000_000, 11, &mut clock);
    assert!(decision2.partition_watermark_ns >= decision1.partition_watermark_ns);
    assert!(tracker.telemetry().guard_floor_ms > 0);
}

#[test]
fn snapshot_replay_skips_clock_until_new_entries() {
    let config = WatermarkConfig::default();
    let mut tracker = WatermarkTracker::new(config.clone());
    let mut clock = MockClock::new(vec![200_000_000, 250_000_000]);
    tracker.observe_event("metric", 800_000_000, 5, &mut clock);
    tracker.observe_event("metric", 900_000_000, 6, &mut clock);
    let guard_index = tracker.snapshot().guard_floor.log_index;

    let snapshot = tracker.snapshot();
    let mut replay_clock = MockClock::new(vec![300_000_000, 400_000_000]);
    let mut restored = WatermarkTracker::from_snapshot(config, &snapshot);

    // Replaying historical entries should not sample the clock again.
    restored.observe_event("metric", 850_000_000, guard_index, &mut replay_clock);
    assert_eq!(replay_clock.call_count(), 0);

    // Advancing past the guard index should resume sampling.
    restored.observe_event("metric", 950_000_000, guard_index + 1, &mut replay_clock);
    assert!(replay_clock.call_count() > 0);
}

#[test]
fn metric_override_tracks_custom_guard() {
    let override_cfg = MetricWatermarkOverride::new(Some(1_000_000_000), Some(50_000_000));
    let config = WatermarkConfig::default().with_metric_override("custom", override_cfg);
    let mut tracker = WatermarkTracker::new(config);
    let mut clock = MockClock::new(vec![100_000_000, 200_000_000]);

    let decision = tracker.observe_event("custom", 500_000_000, 10, &mut clock);
    assert!(decision.metric_watermark_ns.is_some());
    let telemetry = tracker.telemetry();
    assert_eq!(telemetry.metric_watermarks_ms.len(), 1);
    assert_eq!(telemetry.metric_watermarks_ms[0].metric, "custom");
}

#[test]
fn telemetry_reports_last_lateness() {
    let mut tracker = WatermarkTracker::new(WatermarkConfig::default());
    let mut clock = MockClock::new(vec![250_000_000]);
    let decision = tracker.observe_event("metric", 2_500_000_000, 7, &mut clock);
    assert_eq!(
        decision.event_lateness_ns,
        tracker
            .partition_watermark_ns()
            .saturating_sub(2_500_000_000)
    );
    let telemetry = tracker.telemetry();
    assert_eq!(
        telemetry.last_event_lateness_ms,
        decision.event_lateness_ns / 1_000_000
    );
}

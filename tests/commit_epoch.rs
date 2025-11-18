use ceptra::{CommitEpochCheckpoint, CommitEpochState, CommitEpochTicker, MonotonicClock};

struct MockClock {
    readings: Vec<u128>,
    idx: usize,
}

impl MockClock {
    fn new(readings: Vec<u128>) -> Self {
        Self { readings, idx: 0 }
    }
}

impl MonotonicClock for MockClock {
    fn now_ns(&mut self) -> u128 {
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
fn stamps_ticks_with_monotonic_clock() {
    let clock = MockClock::new(vec![1_000_000_000, 2_500_000_000, 4_000_000_000]);
    let mut ticker = CommitEpochTicker::new(clock);
    let entry1 = ticker.stamp_entry(1, b"first");
    let entry2 = ticker.stamp_entry(2, b"second");
    assert!(entry2.commit_epoch_tick_ms > entry1.commit_epoch_tick_ms);
    assert_eq!(entry1.log_index, 1);
}

#[test]
fn advances_tick_when_clock_resolution_is_sub_ms() {
    let clock = MockClock::new(vec![1_000_000_000, 1_000_000_500, 1_000_000_700]);
    let mut ticker = CommitEpochTicker::new(clock);
    let entry1 = ticker.stamp_entry(1, b"a");
    let entry2 = ticker.stamp_entry(2, b"b");
    assert_eq!(entry1.commit_epoch_tick_ms + 1, entry2.commit_epoch_tick_ms);
}

#[test]
fn checkpoint_round_trip_and_wal_replay() {
    let clock = MockClock::new(vec![1_000_000_000, 2_000_000_000, 3_000_000_000]);
    let mut ticker = CommitEpochTicker::new(clock);
    let entry1 = ticker.stamp_entry(41, b"a");
    let entry2 = ticker.stamp_entry(42, b"b");
    let mut state = *ticker.state();
    state.set_versions(7, Some(3));
    let checkpoint = state.to_checkpoint();
    assert_eq!(checkpoint.def_version, 7);
    assert_eq!(checkpoint.policy_version, Some(3));
    let json = checkpoint.to_json().expect("serialize checkpoint");
    let restored = CommitEpochCheckpoint::from_json(&json).expect("restore checkpoint");
    assert_eq!(checkpoint, restored);

    let wal = vec![entry1.clone(), entry2.clone()];
    let rebuilt = CommitEpochState::rebuild_from_checkpoint(&restored, &wal);
    assert_eq!(rebuilt.last_applied_index(), entry2.log_index);
    assert_eq!(rebuilt.commit_epoch_now(), entry2.commit_epoch_tick_ms);
    assert_eq!(rebuilt.def_version(), 7);
    assert_eq!(rebuilt.policy_version(), Some(3));
}

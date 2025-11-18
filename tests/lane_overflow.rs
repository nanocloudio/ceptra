use ceptra::{
    LaneAdmission, LaneAdmissionConfig, LaneAdmissionResult, LANE_OVERFLOW_MARKER,
    LANE_WARN_THRESHOLD,
};

fn admission_with_cap(cap: u32) -> LaneAdmission {
    LaneAdmission::new(LaneAdmissionConfig { lane_cap: cap })
}

#[test]
fn freezes_new_lanes_after_cap() {
    let mut admission = admission_with_cap(4);
    for i in 0..4 {
        let lane = format!("lane-{i}");
        assert!(matches!(
            admission.admit("metric_a", lane),
            LaneAdmissionResult::Admitted { total_lanes } if total_lanes <= 4
        ));
    }
    match admission.admit("metric_a", "lane-overflow") {
        LaneAdmissionResult::Overflow(record) => {
            assert_eq!(record.marker, LANE_OVERFLOW_MARKER);
            assert_eq!(record.observed_lanes, 4);
        }
        other => panic!("expected overflow, got {other:?}"),
    }
    assert_eq!(admission.total_lanes(), 4);
    assert_eq!(admission.audit_log().len(), 1);
    assert_eq!(admission.audit_log()[0].reason, LANE_OVERFLOW_MARKER);

    // Reusing an existing lane should remain a no-op even when frozen.
    assert!(matches!(
        admission.admit("metric_a", "lane-1"),
        LaneAdmissionResult::Existing
    ));
}

#[test]
fn soak_test_drives_overflow_and_records_audit() {
    let mut admission = admission_with_cap(LANE_WARN_THRESHOLD);
    let mut overflow_events = 0;
    for i in 0..(LANE_WARN_THRESHOLD + 16) {
        let metric = if i % 2 == 0 { "metric_a" } else { "metric_b" };
        let lane_key = format!("lane-{i}");
        match admission.admit(metric, lane_key) {
            LaneAdmissionResult::Admitted { .. } => {}
            LaneAdmissionResult::Existing => {}
            LaneAdmissionResult::Overflow(record) => {
                overflow_events += 1;
                assert_eq!(record.marker, LANE_OVERFLOW_MARKER);
            }
        }
    }
    assert_eq!(admission.total_lanes(), LANE_WARN_THRESHOLD);
    assert_eq!(admission.overflow_total(), overflow_events as u64);
    assert_eq!(
        admission.audit_log().len() as u64,
        admission.overflow_total()
    );
    assert!(admission
        .audit_log()
        .iter()
        .all(|entry| entry.reason == LANE_OVERFLOW_MARKER));
}

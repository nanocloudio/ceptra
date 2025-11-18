use std::collections::BTreeMap;

use ceptra::{
    validate_lane_domains, LaneDomainSpec, LaneProjectionStatus, LaneValidationError,
    MetricGrouping, LANE_MAX_PER_PARTITION, LANE_WARN_THRESHOLD,
};

fn lane_domains(values: &[(&str, u32)]) -> BTreeMap<String, LaneDomainSpec> {
    values
        .iter()
        .map(|(label, max)| {
            (
                (*label).to_string(),
                LaneDomainSpec {
                    max_per_partition: *max,
                },
            )
        })
        .collect()
}

#[test]
fn validates_lane_projection_and_histogram() {
    let domains = lane_domains(&[("device_id", 64), ("gateway_id", 32), ("zone", 16)]);
    let metrics = vec![
        MetricGrouping {
            name: "device_temp_peak".into(),
            labels: vec!["device_id".into()],
        },
        MetricGrouping {
            name: "gateway_zone_heatmap".into(),
            labels: vec!["gateway_id".into(), "zone".into()],
        },
        MetricGrouping {
            name: "zone_latency".into(),
            labels: vec!["zone".into()],
        },
    ];
    let report = validate_lane_domains(&domains, &metrics).expect("validation succeeds");
    assert_eq!(report.projections.len(), 3);
    let device_projection = report
        .projections
        .iter()
        .find(|p| p.metric == "device_temp_peak")
        .unwrap();
    assert_eq!(device_projection.projected_lanes, 64);
    assert_eq!(device_projection.status, LaneProjectionStatus::Warning);

    let gateway_projection = report
        .projections
        .iter()
        .find(|p| p.metric == "gateway_zone_heatmap")
        .unwrap();
    assert!(gateway_projection.projected_lanes > LANE_MAX_PER_PARTITION);
    assert_eq!(gateway_projection.status, LaneProjectionStatus::Overflow);
    assert!(report.has_overflow());
    assert!(report.has_warnings());

    let histogram = report.histogram.to_markdown();
    assert!(histogram.contains(&format!(">{LANE_MAX_PER_PARTITION}")));
    assert!(histogram.contains('|'));
}

#[test]
fn errors_when_label_missing() {
    let domains = lane_domains(&[("device_id", 32)]);
    let metrics = vec![MetricGrouping {
        name: "zone_latency".into(),
        labels: vec!["zone".into()],
    }];
    let err = validate_lane_domains(&domains, &metrics).expect_err("missing lane domain");
    assert!(matches!(
        err,
        LaneValidationError::UnknownLaneDomain { label, .. } if label == "zone"
    ));
}

#[test]
fn errors_when_bound_zero() {
    let domains = lane_domains(&[("device_id", 0)]);
    let metrics = vec![MetricGrouping {
        name: "device_temp_peak".into(),
        labels: vec!["device_id".into()],
    }];
    let err = validate_lane_domains(&domains, &metrics).expect_err("zero bound invalid");
    assert!(matches!(
        err,
        LaneValidationError::ZeroLaneBound { label } if label == "device_id"
    ));
}

#[test]
fn warns_when_bundle_total_exceeds_warn_threshold() {
    let domains = lane_domains(&[("region", 12)]);
    let metrics = vec![
        MetricGrouping {
            name: "region_latency".into(),
            labels: vec!["region".into()],
        },
        MetricGrouping {
            name: "region_error_rate".into(),
            labels: vec!["region".into()],
        },
        MetricGrouping {
            name: "region_success_rate".into(),
            labels: vec!["region".into()],
        },
        MetricGrouping {
            name: "region_retries".into(),
            labels: vec!["region".into()],
        },
    ];
    let report = validate_lane_domains(&domains, &metrics).expect("validation succeeds");
    assert!(report.has_warnings());
    assert!(report.total_projected_lanes >= LANE_WARN_THRESHOLD);
}

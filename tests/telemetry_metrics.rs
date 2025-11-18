use ceptra::{ensure_ms_only_metrics, scrape_metric_names};

const GOOD_METRICS: &str = r#"# HELP ceptra metrics
# TYPE ceptra_ap_watermark_ts_ms gauge
ceptra_ap_watermark_ts_ms 42
ceptra_apply_lag_ms{partition_id="1"} 7
ceptra_replication_lag_ms{partition_id="1"} 5
"#;

const BAD_METRICS: &str = r"ap_watermark_ts_seconds 99\n";

#[test]
fn parses_metric_names_without_labels() {
    let names = scrape_metric_names(GOOD_METRICS);
    assert_eq!(
        names,
        vec![
            "ceptra_ap_watermark_ts_ms".to_string(),
            "ceptra_apply_lag_ms".to_string(),
            "ceptra_replication_lag_ms".to_string()
        ]
    );
}

#[test]
fn rejects_seconds_metrics() {
    assert!(ensure_ms_only_metrics(BAD_METRICS).is_err());
}

#[test]
fn accepts_ms_metrics() {
    ensure_ms_only_metrics(GOOD_METRICS).expect("_ms metrics should pass");
}

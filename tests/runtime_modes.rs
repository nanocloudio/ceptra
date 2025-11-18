use ceptra::{RoleMode, RuntimeOptions, TestMode, WarmupEstimate};
use std::env;

#[test]
fn parses_standalone_role_flag() {
    let args = vec![
        "ceptra".to_string(),
        "--role=standalone".to_string(),
        "--foo".to_string(),
    ];
    let opts = RuntimeOptions::from_args(args);
    assert_eq!(opts.role, RoleMode::Standalone);
}

#[test]
fn reads_test_mode_from_env() {
    env::set_var("CEPTRA_TEST_MODE", "fast");
    let opts = RuntimeOptions::from_args(vec!["ceptra".into()]);
    assert_eq!(opts.test_mode, TestMode::Enabled { fast_warmup: true });
    env::remove_var("CEPTRA_TEST_MODE");
}

#[test]
fn warmup_estimates_scale_with_partitions() {
    let WarmupEstimate {
        steady_state,
        fast_path,
    } = RuntimeOptions::warmup_estimate(4, 2);
    assert!(steady_state.as_secs() > fast_path.as_secs());
}

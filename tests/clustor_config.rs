use std::fs;
use std::path::Path;

use ceptra::app::ClustorConfig;

#[cfg(feature = "clustor-net")]
fn base_config(tmp: &Path) -> ClustorConfig {
    ClustorConfig {
        admin_bind: "127.0.0.1:25000".parse().unwrap(),
        raft_bind: "127.0.0.1:25001".parse().unwrap(),
        advertise_host: "127.0.0.1".into(),
        control_plane_base_url: Some("https://cp.invalid".into()),
        identity_cert: None,
        identity_key: None,
        trust_bundle: None,
        cp_routing_path: "/routing".into(),
        cp_feature_path: "/features".into(),
        cp_warmup_path: "/warmup".into(),
        cp_activation_path: "/activation".into(),
        data_dir: tmp.join("data"),
        wal_dir: tmp.join("wal"),
        snapshot_dir: tmp.join("snapshots"),
        raft_client_timeout_ms: 5_000,
        raft_client_retry_attempts: 3,
        raft_client_backoff_ms: 50,
    }
}

#[cfg(feature = "clustor-net")]
#[test]
fn validate_requires_tls_materials() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(tmp.path());
    let err = config.validate().expect_err("missing TLS should fail");
    assert!(
        err.to_string().contains("CEPTRA_TLS_IDENTITY_CERT"),
        "error should mention TLS requirements"
    );
}

#[cfg(feature = "clustor-net")]
#[test]
fn validate_rejects_empty_tls_files() {
    let tmp = tempfile::tempdir().unwrap();
    let cert = tmp.path().join("cert.pem");
    let key = tmp.path().join("key.pem");
    let trust = tmp.path().join("trust.pem");
    fs::write(&cert, "").unwrap();
    fs::write(&key, "").unwrap();
    fs::write(&trust, "").unwrap();
    let mut config = base_config(tmp.path());
    config.identity_cert = Some(cert);
    config.identity_key = Some(key);
    config.trust_bundle = Some(trust);
    let err = config.validate().expect_err("empty TLS files should fail");
    assert!(
        err.to_string().contains("empty"),
        "empty TLS files should be rejected"
    );
}

#[cfg(feature = "clustor-net")]
#[test]
fn validate_accepts_readable_tls_files() {
    let tmp = tempfile::tempdir().unwrap();
    let cert = tmp.path().join("cert.pem");
    let key = tmp.path().join("key.pem");
    let trust = tmp.path().join("trust.pem");
    fs::write(&cert, "-----BEGIN CERTIFICATE-----").unwrap();
    fs::write(&key, "-----BEGIN PRIVATE KEY-----").unwrap();
    fs::write(&trust, "-----BEGIN CERTIFICATE-----").unwrap();
    let mut config = base_config(tmp.path());
    config.identity_cert = Some(cert.clone());
    config.identity_key = Some(key.clone());
    config.trust_bundle = Some(trust.clone());

    config.validate().expect("paths should be accepted");
    assert!(config.data_dir.exists());
    assert!(cert.exists());
    assert!(key.exists());
    assert!(trust.exists());
}

#[cfg(not(feature = "clustor-net"))]
#[test]
fn validate_fails_without_clustor_net() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ClustorConfig {
        admin_bind: "127.0.0.1:25000".parse().unwrap(),
        raft_bind: "127.0.0.1:25001".parse().unwrap(),
        advertise_host: "127.0.0.1".into(),
        control_plane_base_url: Some("https://example.invalid".into()),
        identity_cert: None,
        identity_key: None,
        trust_bundle: None,
        cp_routing_path: "/routing".into(),
        cp_feature_path: "/features".into(),
        cp_warmup_path: "/warmup".into(),
        cp_activation_path: "/activation".into(),
        data_dir: tmp.path().join("data"),
        wal_dir: tmp.path().join("wal"),
        snapshot_dir: tmp.path().join("snapshots"),
        raft_client_timeout_ms: 5_000,
        raft_client_retry_attempts: 3,
        raft_client_backoff_ms: 50,
    };
    let err = config.validate().expect_err("missing clustor-net should fail");
    assert!(err
        .to_string()
        .contains("clustor-net feature is required now that the stub runtime has been removed"));
}

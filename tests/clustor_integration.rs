#![cfg(feature = "clustor-net")]

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ceptra::app::{AppConfig, CeptraRuntime, ClustorConfig};
use ceptra::consensus_core::{
    FeatureGateState, FeatureManifest, FeatureManifestEntry, ShadowApplyState,
    WarmupReadinessRecord,
};
use ceptra::{AppendRequest, CepRole, ClientHints, CombinedReadyz};
use clustor::{future_gates, CreditHint, IngestStatusCode, PlacementRecord};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version;
use rustls::{ServerConfig, ServerConnection, Stream};
use sha2::{Digest, Sha256};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn digest_predicate(predicate: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(predicate.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Clone, serde::Serialize)]
struct ActivationBarrierStub {
    barrier_id: String,
    bundle_id: String,
    partitions: Vec<String>,
    readiness_threshold: f64,
    warmup_deadline_ms: u64,
    readiness_window_ms: u64,
}

struct CpStubServer {
    base_url: String,
    identity_cert: PathBuf,
    identity_key: PathBuf,
    trust_bundle: PathBuf,
    warmup: Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    feature: Arc<Mutex<FeatureManifest>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl CpStubServer {
    fn start(
        tmp: &Path,
        placements: Vec<PlacementRecord>,
        warmup: Vec<WarmupReadinessRecord>,
        feature: FeatureManifest,
        activation: Option<ActivationBarrierStub>,
    ) -> Self {
        let (identity_cert, identity_key, trust_bundle, server_config) =
            generate_tls_materials(tmp, "local");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind control-plane stub");
        let addr = listener.local_addr().expect("discover addr");
        let warmup = Arc::new(Mutex::new(warmup));
        let feature = Arc::new(Mutex::new(feature));
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = shutdown.clone();
        let warmup_state = warmup.clone();
        let feature_state = feature.clone();
        let activation_state = Arc::new(Mutex::new(activation));
        let server_config = Arc::new(server_config);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if server_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(mut tcp) = stream else {
                    continue;
                };
                let mut conn = match ServerConnection::new(server_config.clone()) {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let mut tls = Stream::new(&mut conn, &mut tcp);
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    match tls.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let request = String::from_utf8_lossy(&buf);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                let (status, body) = match path {
                    "/routing" => (
                        200,
                        serde_json::to_vec(&placements).unwrap_or_else(|_| b"[]".to_vec()),
                    ),
                    "/features" => (
                        200,
                        serde_json::to_vec(&*feature_state.lock().unwrap())
                            .unwrap_or_else(|_| b"{}".to_vec()),
                    ),
                    "/warmup" => (
                        200,
                        serde_json::to_vec(&*warmup_state.lock().unwrap())
                            .unwrap_or_else(|_| b"[]".to_vec()),
                    ),
                    "/activation" => {
                        let guard = activation_state.lock().unwrap();
                        if let Some(barrier) = guard.as_ref() {
                            (
                                200,
                                serde_json::to_vec(barrier)
                                    .unwrap_or_else(|_| b"{}".to_vec()),
                            )
                        } else {
                            (200, Vec::new())
                        }
                    }
                    _ => (404, b"{}".to_vec()),
                };
                let status_line = if status == 200 {
                    "200 OK"
                } else {
                    "404 Not Found"
                };
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = tls.write_all(response.as_bytes());
                let _ = tls.write_all(&body);
            }
        });
        Self {
            base_url: format!("https://{}", addr),
            identity_cert,
            identity_key,
            trust_bundle,
            warmup,
            feature,
            shutdown,
            handle: Some(handle),
        }
    }

    fn update_warmup(&self, records: Vec<WarmupReadinessRecord>) {
        if let Ok(mut guard) = self.warmup.lock() {
            *guard = records;
        }
    }

    fn update_feature(&self, manifest: FeatureManifest) {
        if let Ok(mut guard) = self.feature.lock() {
            *guard = manifest;
        }
    }

}

impl Drop for CpStubServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.base_url.replace("https://", ""));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn generate_tls_materials(
    tmp: &Path,
    trust_domain: &str,
) -> (PathBuf, PathBuf, PathBuf, ServerConfig) {
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "ceptra-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().expect("ca key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");

    let mut leaf_params = CertificateParams::default();
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "cp-server");
    leaf_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        SanType::URI(format!("spiffe://{trust_domain}/cp/server").try_into().unwrap()),
    ];
    leaf_params.is_ca = IsCa::NoCa;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    let leaf_key = KeyPair::generate().expect("leaf key");
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .expect("leaf cert");

    let cert_path = tmp.join("identity.pem");
    let key_path = tmp.join("identity.key");
    let trust_path = tmp.join("trust.pem");
    std::fs::write(&cert_path, leaf_cert.pem()).expect("write cert");
    std::fs::write(&key_path, leaf_key.serialize_pem()).expect("write key");
    std::fs::write(&trust_path, ca_cert.pem()).expect("write trust");

    let cert_chain = vec![CertificateDer::from(leaf_cert.der().clone())];
    let key_der = PrivatePkcs8KeyDer::from(leaf_key.serialize_der());
    let provider = ring::default_provider();
    let server_config = ServerConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&version::TLS13])
        .expect("protocols")
        .with_no_client_auth()
        .with_single_cert(cert_chain, PrivateKeyDer::Pkcs8(key_der))
        .expect("server config");
    (cert_path, key_path, trust_path, server_config)
}

fn build_config(tmp: &Path, cp: &CpStubServer) -> AppConfig {
    let admin_port = ephemeral_port();
    let mut raft_port = ephemeral_port();
    if raft_port == admin_port {
        raft_port = ephemeral_port();
    }
    AppConfig {
        node_spiffe: "spiffe://local/ceptra/node-a".into(),
        placement_cache_grace_ms: 300_000,
        ready_threshold: 0.66,
        shadow_grace_window_ms: 300_000,
        feature_manifest_path: None,
        feature_public_key: None,
        durability_mode: None,
        node_roles: vec![
            CepRole::Admin,
            CepRole::Operator,
            CepRole::Observer,
            CepRole::Ingest,
        ],
        placement_path: None,
        warmup_path: None,
        activation_barrier_path: None,
        snapshot_poll_interval_ms: 500,
        clustor: ClustorConfig {
            admin_bind: format!("127.0.0.1:{admin_port}").parse().unwrap(),
            raft_bind: format!("127.0.0.1:{raft_port}").parse().unwrap(),
            advertise_host: "127.0.0.1".into(),
            control_plane_base_url: Some(cp.base_url.clone()),
            identity_cert: Some(cp.identity_cert.clone()),
            identity_key: Some(cp.identity_key.clone()),
            trust_bundle: Some(cp.trust_bundle.clone()),
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
        },
    }
}

fn ephemeral_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn warmup_record(ratio: f64) -> WarmupReadinessRecord {
    WarmupReadinessRecord {
        partition_id: "p1".into(),
        bundle_id: "default".into(),
        shadow_apply_state: ShadowApplyState::Ready,
        shadow_apply_checkpoint_index: 1,
        warmup_ready_ratio: ratio,
        updated_at_ms: now_ms(),
    }
}

fn feature_manifest(state: FeatureGateState) -> FeatureManifest {
    let features: Vec<FeatureManifestEntry> = future_gates()
        .iter()
        .map(|descriptor| FeatureManifestEntry {
            gate: descriptor.slug.into(),
            cp_object: descriptor.cp_object.into(),
            predicate_digest: digest_predicate(descriptor.predicate),
            gate_state: state,
        })
        .collect();
    FeatureManifest {
        schema_version: 1,
        generated_at_ms: now_ms(),
        features,
        signature: String::new(),
    }
}

fn activation_barrier() -> ActivationBarrierStub {
    ActivationBarrierStub {
        barrier_id: "default-barrier".into(),
        bundle_id: "default".into(),
        partitions: vec!["p1".into()],
        readiness_threshold: 1.0,
        warmup_deadline_ms: now_ms() + 60_000,
        readiness_window_ms: 30_000,
    }
}

fn placements() -> Vec<PlacementRecord> {
    let member = "127.0.0.1".to_string();
    vec![PlacementRecord {
        partition_id: "p1".into(),
        routing_epoch: 1,
        lease_epoch: 1,
        members: vec![member.clone(), member.clone(), member],
    }]
}

fn wait_for_ready(runtime: &CeptraRuntime, timeout: Duration) -> Option<CombinedReadyz> {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Ok(ready) = runtime.combined_readyz(&[], now_ms()) {
            if ready.ready() {
                return Some(ready);
            }
            last = Some(ready);
        }
        thread::sleep(Duration::from_millis(200));
    }
    last
}

#[test]
fn control_plane_bootstrap_and_append() {
    let tmp = tempfile::tempdir().unwrap();
    let cp = CpStubServer::start(
        tmp.path(),
        placements(),
        vec![warmup_record(1.0)],
        feature_manifest(FeatureGateState::Enabled),
        Some(activation_barrier()),
    );
    let config = build_config(tmp.path(), &cp);
    let runtime = CeptraRuntime::bootstrap(config).expect("bootstrap embedded clustor");

    let ready = wait_for_ready(&runtime, Duration::from_secs(6))
        .expect("ready snapshot while waiting");
    assert!(
        ready.ready(),
        "runtime should become ready after control-plane warmup feed: {:?}",
        ready
    );

    let response = runtime
        .append(AppendRequest {
            event_id: "evt-1".into(),
            partition_key: b"p1".to_vec(),
            schema_key: "schema.append".into(),
            payload: b"payload".to_vec(),
            client_hints: ClientHints {
                credit_hint: CreditHint::Recover,
                cep_hint: None,
            },
        })
        .expect("append succeeds");
    assert_eq!(
        response.status,
        IngestStatusCode::Healthy,
        "append response: {:?}",
        response
    );
    assert!(response.index.is_some());
    assert!(response.term.is_some());
}

#[test]
fn warmup_refreshes_readyz_from_control_plane() {
    let tmp = tempfile::tempdir().unwrap();
    let cp = CpStubServer::start(
        tmp.path(),
        placements(),
        vec![warmup_record(0.5)],
        feature_manifest(FeatureGateState::Enabled),
        Some(activation_barrier()),
    );
    let config = build_config(tmp.path(), &cp);
    let runtime = CeptraRuntime::bootstrap(config).expect("bootstrap embedded clustor");

    let initial = runtime
        .combined_readyz(&[], now_ms())
        .expect("initial readyz");
    assert!(
        !initial.ready(),
        "warmup below threshold should keep ready=false"
    );

    cp.update_warmup(vec![warmup_record(1.0)]);
    let updated = wait_for_ready(&runtime, Duration::from_secs(8))
        .expect("ready snapshot while waiting");
    assert!(
        updated.ready(),
        "readyz should flip after warmup feed reaches 1.0: {:?}",
        updated
    );
}

#[test]
fn feature_manifest_updates_digest_via_control_plane() {
    let tmp = tempfile::tempdir().unwrap();
    let initial_manifest = feature_manifest(FeatureGateState::Enabled);
    let cp = CpStubServer::start(
        tmp.path(),
        placements(),
        vec![warmup_record(1.0)],
        initial_manifest.clone(),
        Some(activation_barrier()),
    );
    let config = build_config(tmp.path(), &cp);
    let runtime = CeptraRuntime::bootstrap(config).expect("bootstrap embedded clustor");

    let ready = wait_for_ready(&runtime, Duration::from_secs(6))
        .expect("ready snapshot while waiting");
    assert!(
        ready.ready(),
        "runtime should become ready after warmup: {:?}",
        ready
    );
    let combined = runtime
        .combined_readyz(&[], now_ms())
        .expect("combined readyz");
    let initial_digest = combined.clustor.feature_manifest_digest().to_string();
    assert_eq!(
        initial_digest,
        initial_manifest.digest().unwrap(),
        "readyz digest should match control-plane manifest"
    );

    let updated_manifest = feature_manifest(FeatureGateState::Disabled);
    cp.update_feature(updated_manifest.clone());
    thread::sleep(Duration::from_secs(6));
    let refreshed = runtime
        .combined_readyz(&[], now_ms())
        .expect("updated readyz");
    assert_eq!(
        refreshed.clustor.feature_manifest_digest(),
        updated_manifest.digest().unwrap(),
        "feature manifest digest should refresh after control-plane update"
    );
    assert!(
        refreshed.ready(),
        "ready should remain true after manifest update"
    );
}

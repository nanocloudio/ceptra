use crate::event::retention::RetentionPlan;
use crate::observability::telemetry::ensure_ms_only_metrics;
use crate::runtime::backpressure::CreditWindowHintTimer;
use anyhow::{anyhow, Context, Result};
use clustor::storage::entry::{EntryFrame, EntryFrameBuilder};
use clustor::storage::layout::WalSegmentRef;
use clustor::storage::replay::WalReplayScanner;
use clustor::storage::{CompactionState, WalWriter};
use clustor::DualCreditPidController;
use clustor::{
    AckRecord, AdminHandler, AdminRequestContext, AdminService, AdminServiceError, ConsensusCore,
    ConsensusCoreConfig, CpPlacementClient, CpProofCoordinator, CreatePartitionRequest,
    CreatePartitionResponse, CreditHint, DurabilityLedger, DurabilityMode, FlowDecision,
    FlowProfile, FlowThrottleReason, FlowThrottleState, IdempotencyLedger, IngestStatusCode,
    IoMode, LedgerUpdate, PartitionQuorumConfig, PlacementRecord, RbacManifest, RbacManifestCache,
    RbacPrincipal, RbacRole, ReplicaId, SetDurabilityModeRequest, SetDurabilityModeResponse,
    SnapshotThrottleRequest, SnapshotThrottleResponse, SnapshotTriggerRequest,
    SnapshotTriggerResponse, SpiffeId, StrictFallbackState, TransferLeaderRequest,
    TransferLeaderResponse,
};
use serde::de::DeserializeOwned;
use serde_json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::engine::aggregation::{
    AggregationConfig, AggregatorKind, LateDataPolicy, MetricSpec, PaneAggregationPipeline,
};
use crate::engine::apply::{
    ApplyBatchResult, ApplyLoop, ChannelPolicies, CommittedBatch, CommittedEvent, EventMeasurement,
};
use crate::engine::cel::CelSandbox;
use crate::event::commit_epoch::{
    CommitEpochState, CommitEpochTicker, SystemMonotonicClock, WalEntry,
};
use crate::event::dedup::{DedupTable, DedupTableConfig};
use crate::observability::readiness::PLACEMENT_STALE_REASON;
use crate::observability::ReadyzEvalContext;
use crate::runtime::hot_reload::{HotReloadGate, PartitionWarmupProbe};
use crate::runtime::partition::{
    ApLeaderAffinity, ApReassignment, LeaseUpdate, PartitionError, PartitionTopology,
};
use crate::{
    AppendRequest, AppendResponse, CepCreditHint, CepReadyzReport, CepRole, CepSecurityMaterial,
    CombinedReadyz, ReadyGate, ReadyMetrics, ReadyzReasons, PERMANENT_DURABILITY_REASON,
};
#[cfg(feature = "clustor-net")]
use clustor::control_plane::core::client::{CpApiTransport, CpControlPlaneClient};
#[cfg(feature = "clustor-net")]
use clustor::net::NetError;
#[cfg(feature = "clustor-net")]
use clustor::replication::raft::{
    AppendEntriesProcessor, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRejectReason,
    RequestVoteRequest, RequestVoteResponse,
};
#[cfg(feature = "clustor-net")]
use clustor::replication::transport::raft::{RaftRpcHandler, RaftRpcServer};
#[cfg(feature = "clustor-net")]
use clustor::security::MtlsIdentityManager;
use clustor::{
    future_gates, readyz_from_warmup_snapshot, ActivationBarrier, FeatureCapabilityMatrix,
    FeatureGateState, FeatureManifest, FeatureManifestEntry, RaftLogEntry, RaftLogStore,
    ReadyzSnapshot, SystemLogEntry, WarmupReadinessPublisher, WarmupReadinessRecord,
    WarmupReadinessSnapshot,
};
#[cfg(feature = "clustor-net")]
use clustor::{
    load_identity_from_pem, load_trust_store_from_pem, AdminHttpServer, AdminHttpServerConfig,
    AdminHttpServerHandle, HttpCpTransportBuilder, RaftNetworkServer, RaftNetworkServerConfig,
    RaftNetworkServerHandle, TlsIdentity, TlsTrustStore,
};
#[cfg(feature = "clustor-net")]
use clustor::{RaftNetworkClient, RaftNetworkClientConfig, RaftNetworkClientOptions};
#[cfg(feature = "clustor-net")]
use reqwest::blocking::Client as HttpClient;
#[cfg(feature = "clustor-net")]
use reqwest::{Certificate, Identity};
use serde::{Deserialize, Serialize};

const FLOW_SETPOINT_OPS_PER_SEC: f64 = 1_000.0;
const DEFAULT_TERM: u64 = 1;
const DEFAULT_ADMIN_BIND: &str = "127.0.0.1:26020";
const DEFAULT_RAFT_BIND: &str = "127.0.0.1:26021";
const DEFAULT_ADVERTISE_HOST: &str = "127.0.0.1";
const DEFAULT_DATA_ROOT: &str = "target/ceptra-clustor";
const DEFAULT_CP_ROUTING_PATH: &str = "/routing";
const DEFAULT_CP_FEATURE_PATH: &str = "/features";
const DEFAULT_CP_WARMUP_PATH: &str = "/warmup";
const DEFAULT_CP_ACTIVATION_PATH: &str = "/activation";
const DEFAULT_WAL_BLOCK_SIZE: u64 = 4096;
const FEED_STALENESS_WARN_MS: u64 = 30_000;
const APPLY_DEFAULT_METRIC: &str = "ingest_events_total";
const APPLY_PAYLOAD_BYTES_METRIC: &str = "ingest_payload_bytes";
const DEFAULT_RAFT_CLIENT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_RAFT_CLIENT_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_RAFT_CLIENT_BACKOFF_MS: u64 = 50;

#[derive(Debug, Clone)]
pub struct ClustorConfig {
    pub admin_bind: SocketAddr,
    pub raft_bind: SocketAddr,
    pub advertise_host: String,
    pub control_plane_base_url: Option<String>,
    pub identity_cert: Option<PathBuf>,
    pub identity_key: Option<PathBuf>,
    pub trust_bundle: Option<PathBuf>,
    pub cp_routing_path: String,
    pub cp_feature_path: String,
    pub cp_warmup_path: String,
    pub cp_activation_path: String,
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub raft_client_timeout_ms: u64,
    pub raft_client_retry_attempts: usize,
    pub raft_client_backoff_ms: u64,
}

impl ClustorConfig {
    pub fn from_env() -> Result<Self> {
        let admin_bind = parse_socket("CEPTRA_ADMIN_BIND", DEFAULT_ADMIN_BIND)?;
        let raft_bind = parse_socket("CEPTRA_RAFT_BIND", DEFAULT_RAFT_BIND)?;
        let advertise_host = env::var("CEPTRA_ADVERTISE_HOST")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ADVERTISE_HOST.to_string());
        let control_plane_base_url = env::var("CEPTRA_CONTROL_PLANE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let identity_cert = env_path("CEPTRA_TLS_IDENTITY_CERT");
        let identity_key = env_path("CEPTRA_TLS_IDENTITY_KEY");
        let trust_bundle = env_path("CEPTRA_TLS_TRUST_BUNDLE");
        let cp_routing_path = env::var("CEPTRA_CP_ROUTING_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CP_ROUTING_PATH.to_string());
        let cp_feature_path = env::var("CEPTRA_CP_FEATURE_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CP_FEATURE_PATH.to_string());
        let cp_warmup_path = env::var("CEPTRA_CP_WARMUP_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CP_WARMUP_PATH.to_string());
        let cp_activation_path = env::var("CEPTRA_CP_ACTIVATION_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CP_ACTIVATION_PATH.to_string());
        let data_dir =
            env_path("CEPTRA_DATA_DIR").unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_ROOT));
        let wal_dir = env_path("CEPTRA_WAL_DIR").unwrap_or_else(|| data_dir.join("wal"));
        let snapshot_dir =
            env_path("CEPTRA_SNAPSHOT_DIR").unwrap_or_else(|| data_dir.join("snapshots"));
        let raft_client_timeout_ms = env::var("CEPTRA_RAFT_CLIENT_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RAFT_CLIENT_TIMEOUT_MS);
        let raft_client_retry_attempts = env::var("CEPTRA_RAFT_CLIENT_RETRY_ATTEMPTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(DEFAULT_RAFT_CLIENT_RETRY_ATTEMPTS);
        let raft_client_backoff_ms = env::var("CEPTRA_RAFT_CLIENT_BACKOFF_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RAFT_CLIENT_BACKOFF_MS);

        Ok(Self {
            admin_bind,
            raft_bind,
            advertise_host,
            control_plane_base_url,
            identity_cert,
            identity_key,
            trust_bundle,
            cp_routing_path,
            cp_feature_path,
            cp_warmup_path,
            cp_activation_path,
            data_dir,
            wal_dir,
            snapshot_dir,
            raft_client_timeout_ms,
            raft_client_retry_attempts,
            raft_client_backoff_ms,
        })
    }

    #[cfg(feature = "clustor-net")]
    pub fn validate(&self) -> Result<()> {
        let identity_cert = self.identity_cert.as_ref().ok_or_else(|| {
            anyhow!(
                "clustor-net requires CEPTRA_TLS_IDENTITY_CERT, CEPTRA_TLS_IDENTITY_KEY, and CEPTRA_TLS_TRUST_BUNDLE"
            )
        })?;
        let identity_key = self.identity_key.as_ref().ok_or_else(|| {
            anyhow!(
                "clustor-net requires CEPTRA_TLS_IDENTITY_CERT, CEPTRA_TLS_IDENTITY_KEY, and CEPTRA_TLS_TRUST_BUNDLE"
            )
        })?;
        let trust_bundle = self.trust_bundle.as_ref().ok_or_else(|| {
            anyhow!(
                "clustor-net requires CEPTRA_TLS_IDENTITY_CERT, CEPTRA_TLS_IDENTITY_KEY, and CEPTRA_TLS_TRUST_BUNDLE"
            )
        })?;
        self.ensure_file(identity_cert, "identity_cert")?;
        self.ensure_file(identity_key, "identity_key")?;
        self.ensure_file(trust_bundle, "trust_bundle")?;
        if self.admin_bind == self.raft_bind {
            return Err(anyhow!(
                "CEPTRA_ADMIN_BIND and CEPTRA_RAFT_BIND must not be the same socket address"
            ));
        }
        if self.control_plane_base_url.is_none() {
            return Err(anyhow!(
                "CEPTRA_CONTROL_PLANE_URL is required; stub runtime has been removed"
            ));
        }
        if !self.cp_routing_path.starts_with('/') {
            return Err(anyhow!(
                "CEPTRA_CP_ROUTING_PATH must start with '/', found {}",
                self.cp_routing_path
            ));
        }
        if !self.cp_feature_path.starts_with('/') {
            return Err(anyhow!(
                "CEPTRA_CP_FEATURE_PATH must start with '/', found {}",
                self.cp_feature_path
            ));
        }
        if !self.cp_warmup_path.starts_with('/') {
            return Err(anyhow!(
                "CEPTRA_CP_WARMUP_PATH must start with '/', found {}",
                self.cp_warmup_path
            ));
        }
        if !self.cp_activation_path.starts_with('/') {
            return Err(anyhow!(
                "CEPTRA_CP_ACTIVATION_PATH must start with '/', found {}",
                self.cp_activation_path
            ));
        }
        if self.raft_client_timeout_ms == 0 {
            return Err(anyhow!(
                "CEPTRA_RAFT_CLIENT_TIMEOUT_MS must be greater than zero"
            ));
        }
        if self.raft_client_retry_attempts == 0 {
            return Err(anyhow!(
                "CEPTRA_RAFT_CLIENT_RETRY_ATTEMPTS must be at least 1"
            ));
        }
        if self.raft_client_backoff_ms == 0 {
            return Err(anyhow!(
                "CEPTRA_RAFT_CLIENT_BACKOFF_MS must be greater than zero"
            ));
        }
        self.ensure_dir(&self.data_dir, "data_dir")?;
        self.ensure_dir(&self.wal_dir, "wal_dir")?;
        self.ensure_dir(&self.snapshot_dir, "snapshot_dir")?;
        Ok(())
    }

    #[cfg(not(feature = "clustor-net"))]
    pub fn validate(&self) -> Result<()> {
        Err(anyhow!(
            "clustor-net feature is required now that the stub runtime has been removed"
        ))
    }

    fn ensure_dir(&self, path: &Path, label: &str) -> Result<()> {
        fs::create_dir_all(path)
            .with_context(|| format!("failed to create clustor {label} at {}", path.display()))?;
        let metadata = fs::metadata(path)
            .with_context(|| format!("failed to stat clustor {label} at {}", path.display()))?;
        if !metadata.is_dir() {
            return Err(anyhow!(
                "clustor {label} at {} is not a directory",
                path.display()
            ));
        }
        let probe = path.join(".write_probe");
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&probe)
            .with_context(|| format!("clustor {label} at {} is not writable", path.display()))?;
        let _ = fs::remove_file(&probe);
        Ok(())
    }

    fn ensure_file(&self, path: &Path, label: &str) -> Result<()> {
        let metadata = fs::metadata(path)
            .with_context(|| format!("failed to stat clustor {label} at {}", path.display()))?;
        if !metadata.is_file() {
            return Err(anyhow!(
                "clustor {label} at {} is not a file",
                path.display()
            ));
        }
        if metadata.len() == 0 {
            return Err(anyhow!("clustor {label} at {} is empty", path.display()));
        }
        OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("failed to read clustor {label} at {}", path.display()))?;
        Ok(())
    }
}

impl Default for ClustorConfig {
    fn default() -> Self {
        Self::from_env().expect("clustor config should parse from environment")
    }
}

/// Simple runtime configuration derived from flags/env in the future.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub node_spiffe: String,
    pub placement_cache_grace_ms: u64,
    pub ready_threshold: f64,
    pub shadow_grace_window_ms: u64,
    pub feature_manifest_path: Option<PathBuf>,
    pub feature_public_key: Option<String>,
    pub durability_mode: Option<DurabilityMode>,
    pub node_roles: Vec<CepRole>,
    pub placement_path: Option<PathBuf>,
    pub warmup_path: Option<PathBuf>,
    pub activation_barrier_path: Option<PathBuf>,
    pub snapshot_poll_interval_ms: u64,
    pub clustor: ClustorConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let node_spiffe = env::var("CEPTRA_NODE_SPIFFE")
            .unwrap_or_else(|_| "spiffe://local/ceptra/node-0".to_string());
        let durability_mode = env::var("CEPTRA_DURABILITY_MODE")
            .ok()
            .and_then(|raw| parse_durability_mode(&raw).ok());
        let node_roles = parse_node_roles(env::var("CEPTRA_NODE_ROLES").ok());
        Ok(Self {
            node_spiffe,
            placement_cache_grace_ms: 300_000,
            ready_threshold: 0.66,
            shadow_grace_window_ms: 300_000,
            feature_manifest_path: env::var("CEPTRA_FEATURE_MANIFEST_PATH")
                .ok()
                .map(PathBuf::from),
            feature_public_key: env::var("CEPTRA_FEATURE_PUBLIC_KEY").ok(),
            durability_mode,
            node_roles,
            placement_path: env::var("CEPTRA_PLACEMENTS_PATH").ok().map(PathBuf::from),
            warmup_path: env::var("CEPTRA_WARMUP_PATH").ok().map(PathBuf::from),
            activation_barrier_path: env::var("CEPTRA_ACTIVATION_BARRIER_PATH")
                .ok()
                .map(PathBuf::from),
            snapshot_poll_interval_ms: env::var("CEPTRA_SNAPSHOT_POLL_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5_000),
            clustor: ClustorConfig::from_env()?,
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.clustor.validate()?;
        Ok(())
    }
}

fn parse_durability_mode(raw: &str) -> Result<DurabilityMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "strict" => Ok(DurabilityMode::Strict),
        "relaxed" => Ok(DurabilityMode::Relaxed),
        other => Err(anyhow!(
            "invalid CEPTRA_DURABILITY_MODE `{other}` (expected strict or relaxed)"
        )),
    }
}

fn parse_node_roles(raw: Option<String>) -> Vec<CepRole> {
    let default_roles = vec![
        CepRole::Admin,
        CepRole::Operator,
        CepRole::Observer,
        CepRole::Ingest,
    ];
    let Some(text) = raw else {
        return default_roles;
    };
    let mut roles = Vec::new();
    for token in text.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(role) = parse_role(token) {
            roles.push(role);
        } else {
            eprintln!("ignoring unknown CEPTRA_NODE_ROLES entry `{token}`");
        }
    }
    if roles.is_empty() {
        default_roles
    } else {
        roles
    }
}

fn parse_role(token: &str) -> Option<CepRole> {
    match token.to_ascii_lowercase().as_str() {
        "ingest" => Some(CepRole::Ingest),
        "observer" => Some(CepRole::Observer),
        "operator" => Some(CepRole::Operator),
        "admin" => Some(CepRole::Admin),
        _ => None,
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::load().expect("app config should parse from environment")
    }
}

fn parse_socket(var: &str, default: &str) -> Result<SocketAddr> {
    if let Ok(raw) = env::var(var) {
        raw.parse()
            .with_context(|| format!("invalid socket address for {var} ({raw})"))
    } else {
        default
            .parse()
            .with_context(|| format!("invalid default socket address {default} for {var}"))
    }
}

fn env_path(var: &str) -> Option<PathBuf> {
    env::var(var)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
}

fn parse_peer_addr_map(raw: &str) -> HashMap<String, SocketAddr> {
    let mut map = HashMap::new();
    for token in raw.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let mut parts = token.splitn(2, '=');
        let Some(id) = parts.next() else { continue };
        let Some(addr_raw) = parts.next() else {
            continue;
        };
        if let Ok(addr) = SocketAddr::from_str(addr_raw.trim()) {
            map.insert(id.to_string(), addr);
        } else {
            eprintln!("clustor: skipping invalid CEPTRA_PEER_RAFT_ADDRS entry `{token}`");
        }
    }
    map
}

fn replica_addr_from_hint(
    replica: &str,
    default_host: &str,
    default_port: u16,
) -> Option<SocketAddr> {
    if let Ok(addr) = SocketAddr::from_str(replica) {
        return Some(addr);
    }
    if let Ok(addr) = SocketAddr::from_str(&format!("{replica}:{default_port}")) {
        return Some(addr);
    }
    SocketAddr::from_str(&format!("{default_host}:{default_port}")).ok()
}

fn resolve_peer_addresses(
    config: &AppConfig,
    placements: &[PlacementRecord],
) -> HashMap<String, SocketAddr> {
    let mut map = env::var("CEPTRA_PEER_RAFT_ADDRS")
        .ok()
        .map(|raw| parse_peer_addr_map(&raw))
        .unwrap_or_default();
    let fallback_host = config.clustor.advertise_host.clone();
    let fallback_port = config.clustor.raft_bind.port();
    for record in placements {
        for replica in &record.members {
            if map.contains_key(replica) {
                continue;
            }
            if let Some(addr) = replica_addr_from_hint(replica, &fallback_host, fallback_port) {
                map.insert(replica.clone(), addr);
            }
        }
    }
    let single_local = placements.iter().all(|record| {
        record
            .members
            .iter()
            .all(|member| member == &fallback_host)
    });
    if single_local {
        map.clear();
    }
    map
}

#[cfg(feature = "clustor-net")]
fn admin_base_url(config: &AppConfig) -> String {
    if let Some(base) = &config.clustor.control_plane_base_url {
        base.trim_end_matches('/').to_string()
    } else {
        format!(
            "https://{}:{}",
            config.clustor.advertise_host,
            config.clustor.admin_bind.port()
        )
    }
}

fn node_replica_id(config: &AppConfig) -> String {
    env::var("CEPTRA_NODE_ID")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| config.clustor.advertise_host.clone())
}

#[cfg(feature = "clustor-net")]
#[allow(dead_code)]
struct ClustorNetHandles {
    admin: Option<AdminHttpServerHandle>,
    raft: Option<RaftNetworkServerHandle>,
    placement_poller: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "clustor-net")]
struct TlsMaterials {
    identity: TlsIdentity,
    trust: TlsTrustStore,
    trust_domain: String,
}

#[cfg(feature = "clustor-net")]
#[derive(Clone)]
struct RaftClientTuning {
    timeout: Duration,
    retry_attempts: usize,
    backoff: Duration,
}

#[cfg(feature = "clustor-net")]
struct AdminHttpClient {
    client: HttpClient,
    base_url: String,
}

#[cfg(feature = "clustor-net")]
impl AdminHttpClient {
    fn new(config: &AppConfig) -> Result<Self> {
        let cert_path = config
            .clustor
            .identity_cert
            .as_ref()
            .ok_or_else(|| anyhow!("missing CEPTRA_TLS_IDENTITY_CERT"))?;
        let key_path = config
            .clustor
            .identity_key
            .as_ref()
            .ok_or_else(|| anyhow!("missing CEPTRA_TLS_IDENTITY_KEY"))?;
        let trust_path = config
            .clustor
            .trust_bundle
            .as_ref()
            .ok_or_else(|| anyhow!("missing CEPTRA_TLS_TRUST_BUNDLE"))?;

        let mut identity_pem = fs::read(cert_path)
            .with_context(|| format!("failed to read identity cert {}", cert_path.display()))?;
        identity_pem.extend(
            fs::read(key_path)
                .with_context(|| format!("failed to read identity key {}", key_path.display()))?,
        );
        let identity = Identity::from_pem(&identity_pem)
            .context("failed to parse TLS identity for admin RPCs")?;
        let trust_bytes = fs::read(trust_path)
            .with_context(|| format!("failed to read TLS trust bundle {}", trust_path.display()))?;
        let trust = Certificate::from_pem(&trust_bytes)
            .context("failed to parse TLS trust bundle for admin RPCs")?;

        let client = HttpClient::builder()
            .identity(identity)
            .add_root_certificate(trust)
            .build()
            .context("failed to build admin HTTP client")?;
        let base_url = admin_base_url(config);
        Ok(Self { client, base_url })
    }

    fn post<Rq, Rs>(&self, path: &str, body: &Rq) -> Result<Rs, AdminServiceError>
    where
        Rq: Serialize,
        Rs: DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .map_err(|err| AdminServiceError::InvalidRequest(err.to_string()))?;
        if !response.status().is_success() {
            return Err(AdminServiceError::InvalidRequest(format!(
                "admin rpc returned status {}",
                response.status()
            )));
        }
        response
            .json::<Rs>()
            .map_err(|err| AdminServiceError::InvalidRequest(err.to_string()))
    }

    fn create_partition(
        &self,
        request: &CreatePartitionRequest,
    ) -> Result<CreatePartitionResponse, AdminServiceError> {
        self.post("/admin/create-partition", request)
    }

    fn set_durability_mode(
        &self,
        request: &SetDurabilityModeRequest,
    ) -> Result<SetDurabilityModeResponse, AdminServiceError> {
        self.post("/admin/set-durability-mode", request)
    }

    fn transfer_leader(
        &self,
        request: &TransferLeaderRequest,
    ) -> Result<TransferLeaderResponse, AdminServiceError> {
        self.post("/admin/transfer-leader", request)
    }

    fn snapshot_throttle(
        &self,
        request: &SnapshotThrottleRequest,
    ) -> Result<SnapshotThrottleResponse, AdminServiceError> {
        self.post("/admin/snapshot-throttle", request)
    }

    fn snapshot_trigger(
        &self,
        request: &SnapshotTriggerRequest,
    ) -> Result<SnapshotTriggerResponse, AdminServiceError> {
        self.post("/admin/snapshot-trigger", request)
    }
}

#[cfg(feature = "clustor-net")]
fn build_tls_materials(config: &AppConfig) -> Result<TlsMaterials> {
    let now = Instant::now();
    let identity_path = config
        .clustor
        .identity_cert
        .as_ref()
        .ok_or_else(|| anyhow!("missing CEPTRA_TLS_IDENTITY_CERT"))?;
    let key_path = config
        .clustor
        .identity_key
        .as_ref()
        .ok_or_else(|| anyhow!("missing CEPTRA_TLS_IDENTITY_KEY"))?;
    let trust_path = config
        .clustor
        .trust_bundle
        .as_ref()
        .ok_or_else(|| anyhow!("missing CEPTRA_TLS_TRUST_BUNDLE"))?;
    let identity = load_identity_from_pem(identity_path, key_path, now)?;
    let trust = load_trust_store_from_pem(trust_path)?;
    let trust_domain = identity.certificate.spiffe_id.trust_domain.clone();
    Ok(TlsMaterials {
        identity,
        trust,
        trust_domain,
    })
}

#[cfg(feature = "clustor-net")]
fn build_raft_client_tuning(config: &AppConfig) -> RaftClientTuning {
    RaftClientTuning {
        timeout: Duration::from_millis(config.clustor.raft_client_timeout_ms),
        retry_attempts: config.clustor.raft_client_retry_attempts,
        backoff: Duration::from_millis(config.clustor.raft_client_backoff_ms),
    }
}

fn build_admin_service(
    now: Instant,
    config: &AppConfig,
    placements: &[PlacementRecord],
) -> Result<AdminService> {
    let consensus_kernel = ConsensusCore::new(ConsensusCoreConfig::default());
    let cp_guard = CpProofCoordinator::new(consensus_kernel);
    let mut cp_placements =
        CpPlacementClient::new(Duration::from_millis(config.placement_cache_grace_ms));
    for record in placements {
        cp_placements.update(record.clone(), now);
    }
    let ledger = IdempotencyLedger::new(Duration::from_secs(600));
    let handler = AdminHandler::new(cp_guard, cp_placements, ledger);
    let rbac = build_rbac_cache(now, &config.node_spiffe)?;
    Ok(AdminService::new(handler, rbac))
}

#[cfg(feature = "clustor-net")]
struct RaftHandler {
    #[allow(dead_code)]
    node_id: String,
    log: Arc<Mutex<RaftLogStore>>,
    ingest: Arc<ClustorIngestSurface>,
    current_term: AtomicU64,
    voted_for: Mutex<Option<String>>,
    last_applied: AtomicU64,
    #[allow(dead_code)]
    last_log_index: AtomicU64,
}

#[cfg(feature = "clustor-net")]
impl RaftHandler {
    fn new(
        node_id: String,
        log: Arc<Mutex<RaftLogStore>>,
        ingest: Arc<ClustorIngestSurface>,
    ) -> Result<Self> {
        let last_log_index = log.lock().map(|guard| guard.last_index()).unwrap_or(0);
        Ok(Self {
            node_id,
            log,
            ingest,
            current_term: AtomicU64::new(DEFAULT_TERM),
            voted_for: Mutex::new(None),
            last_applied: AtomicU64::new(0),
            last_log_index: AtomicU64::new(last_log_index),
        })
    }

    fn log_tail(&self) -> (u64, u64) {
        let log = self.log.lock().unwrap();
        let last_index = log.last_index();
        let last_term = log
            .entry(last_index)
            .ok()
            .flatten()
            .map(|entry| entry.term)
            .unwrap_or(0);
        (last_index, last_term)
    }

    #[allow(dead_code)]
    fn apply_commits(&self, upto: u64) {
        let start = self.last_applied.load(Ordering::SeqCst).saturating_add(1);
        if upto < start {
            return;
        }
        let mut entries = Vec::new();
        if let Ok(log) = self.log.lock() {
            log.copy_entries_in_range(start, upto, &mut entries);
        }
        self.ingest.apply_committed_entries(&entries, upto);
        self.last_applied.store(upto, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    fn record_replica_ack(&self, entry: &RaftLogEntry) -> Result<()> {
        if entry.payload.is_empty() {
            return Ok(());
        }
        let frame = match EntryFrame::decode(&entry.payload) {
            Ok(frame) => frame,
            Err(err) => {
                eprintln!("clustor: failed to decode raft entry payload: {err}");
                return Ok(());
            }
        };
        let metadata: WalFrameMetadata = match serde_json::from_slice(&frame.metadata) {
            Ok(meta) => meta,
            Err(err) => {
                eprintln!("clustor: failed to decode raft entry metadata: {err}");
                return Ok(());
            }
        };
        let partition = self
            .ingest
            .route_partition(&metadata.partition_key)
            .unwrap_or_else(|_| "unknown".to_string());
        let ack = AckRecord {
            replica: ReplicaId::new(self.node_id.clone()),
            term: entry.term,
            index: entry.index,
            segment_seq: entry.index,
            io_mode: IoMode::Strict,
        };
        let _ = self.ingest.record_replica_ack(&partition, ack);
        Ok(())
    }
}

#[cfg(feature = "clustor-net")]
impl RaftRpcHandler for RaftHandler {
    fn on_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        let mut current_term = self.current_term.load(Ordering::SeqCst);
        if request.term > current_term {
            current_term = request.term;
            self.current_term.store(current_term, Ordering::SeqCst);
            if let Ok(mut voted) = self.voted_for.lock() {
                *voted = None;
            }
        }
        if request.term < current_term {
            eprintln!(
                "event=clustor_vote decision=reject reason=term_out_of_date candidate={} request_term={} current_term={}",
                request.candidate_id, request.term, current_term
            );
            return RequestVoteResponse {
                term: current_term,
                granted: false,
                reject_reason: Some(RequestVoteRejectReason::TermOutOfDate),
            };
        }

        let (last_index, last_term) = self.log_tail();
        let up_to_date = request.last_log_term > last_term
            || (request.last_log_term == last_term && request.last_log_index >= last_index);
        if !up_to_date {
            eprintln!(
                "event=clustor_vote decision=reject reason=log_behind candidate={} request_last_index={} request_last_term={} tail_index={} tail_term={}",
                request.candidate_id, request.last_log_index, request.last_log_term, last_index, last_term
            );
            return RequestVoteResponse {
                term: current_term,
                granted: false,
                reject_reason: Some(RequestVoteRejectReason::LogBehind),
            };
        }

        if let Ok(mut voted) = self.voted_for.lock() {
            if voted.is_some() && voted.as_deref() != Some(&request.candidate_id) {
                eprintln!(
                    "event=clustor_vote decision=reject reason=already_voted candidate={} voted_for={} term={}",
                    request.candidate_id,
                    voted.as_deref().unwrap_or(""),
                    current_term
                );
                return RequestVoteResponse {
                    term: current_term,
                    granted: false,
                    reject_reason: Some(RequestVoteRejectReason::NotLeaderEligible),
                };
            }
            *voted = Some(request.candidate_id.clone());
        }

        eprintln!(
            "event=clustor_vote decision=grant candidate={} term={} last_index={} last_term={}",
            request.candidate_id, request.term, request.last_log_index, request.last_log_term
        );
        RequestVoteResponse {
            term: current_term,
            granted: true,
            reject_reason: None,
        }
    }

    fn on_append_entries(&mut self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut current_term = self.current_term.load(Ordering::SeqCst);
        if request.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
                conflict_index: Some(request.prev_log_index),
                conflict_term: None,
            };
        }
        if request.term > current_term {
            current_term = request.term;
            self.current_term.store(current_term, Ordering::SeqCst);
        }

        let mut log = match self.log.lock() {
            Ok(log) => log,
            Err(_) => {
                return AppendEntriesResponse {
                    term: current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: None,
                    conflict_term: None,
                }
            }
        };
        let prev_tail = log.last_index();
        let mut processor = AppendEntriesProcessor::new(&mut log);
        let outcome = match processor.apply(&request) {
            Ok(outcome) => outcome,
            Err(err) => {
                eprintln!("clustor: raft append failed: {err}");
                return AppendEntriesResponse {
                    term: current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: None,
                    conflict_term: None,
                };
            }
        };
        let new_tail = log.last_index();
        let mut commit_index = 0u64;
        let mut new_entries = Vec::new();
        let mut rebuild_entries = Vec::new();
        if outcome.success {
            commit_index = request.leader_commit.min(outcome.match_index);
            let truncation = new_tail < prev_tail
                || (request.prev_log_index < prev_tail && !request.entries.is_empty())
                || commit_index < self.last_applied.load(Ordering::SeqCst);
            if commit_index > 0 {
                if truncation {
                    log.copy_entries_in_range(1, commit_index, &mut rebuild_entries);
                } else {
                    let start = self.last_applied.load(Ordering::SeqCst).saturating_add(1);
                    log.copy_entries_in_range(start, commit_index, &mut new_entries);
                }
            }
            // Stage detection helper so we can reuse below.
            if truncation && commit_index == 0 {
                rebuild_entries.clear();
            }
        }
        drop(log);

        if outcome.success {
            let truncation = new_tail < prev_tail
                || (request.prev_log_index < prev_tail && !request.entries.is_empty())
                || commit_index < self.last_applied.load(Ordering::SeqCst);
            if truncation {
                if commit_index > 0 && !rebuild_entries.is_empty() {
                    self.ingest
                        .rebuild_committed_entries(&rebuild_entries, commit_index);
                } else if commit_index == 0 {
                    self.ingest.reset_uncommitted();
                }
                self.last_applied.store(commit_index, Ordering::SeqCst);
            } else if !new_entries.is_empty() {
                self.ingest
                    .apply_committed_entries(&new_entries, commit_index);
                self.last_applied.store(commit_index, Ordering::SeqCst);
            } else if commit_index > 0 && commit_index > self.last_applied.load(Ordering::SeqCst) {
                self.last_applied.store(commit_index, Ordering::SeqCst);
            }
            if !request.entries.is_empty() {
                self.ingest.stage_raft_entries(&request.entries);
            }
            self.ingest.update_leader(&request.leader_id, request.term);
            self.last_log_index.store(new_tail, Ordering::SeqCst);
        }

        AppendEntriesResponse {
            term: current_term,
            success: outcome.success,
            match_index: outcome.match_index,
            conflict_index: outcome.conflict_index,
            conflict_term: outcome.conflict_term,
        }
    }
}

#[cfg(feature = "clustor-net")]
fn start_embedded_servers(
    config: &AppConfig,
    tls: &TlsMaterials,
    placements: &[PlacementRecord],
    ingest_surface: Arc<ClustorIngestSurface>,
) -> Result<ClustorNetHandles> {
    let admin_service = build_admin_service(Instant::now(), config, placements)?;
    let admin = AdminHttpServer::spawn(
        AdminHttpServerConfig {
            bind: config.clustor.admin_bind,
            identity: tls.identity.clone(),
            trust_store: tls.trust.clone(),
        },
        admin_service,
    )
    .with_context(|| {
        format!(
            "failed to start admin HTTP on {}",
            config.clustor.admin_bind
        )
    })?;

    let raft_server = RaftNetworkServer::spawn(
        RaftNetworkServerConfig {
            bind: config.clustor.raft_bind,
            identity: tls.identity.clone(),
            trust_store: tls.trust.clone(),
        },
        {
            let node_id = node_replica_id(config);
            let handler =
                RaftHandler::new(node_id, ingest_surface.raft_log(), ingest_surface.clone())?;
            RaftRpcServer::new(
                MtlsIdentityManager::new(
                    tls.identity.certificate.clone(),
                    tls.trust_domain.clone(),
                    Duration::from_secs(600),
                    Instant::now(),
                ),
                handler,
            )
        },
    )
    .with_context(|| {
        format!(
            "failed to start raft server on {}",
            config.clustor.raft_bind
        )
    })?;

    Ok(ClustorNetHandles {
        admin: Some(admin),
        raft: Some(raft_server),
        placement_poller: None,
    })
}

/// Aggregates placement metadata and affinity state for the runtime.
#[derive(Debug)]
struct PlacementState {
    records: Vec<PlacementRecord>,
    topology: PartitionTopology,
    affinity: ApLeaderAffinity,
}

impl PlacementState {
    fn new(records: Vec<PlacementRecord>) -> Result<Self, PartitionError> {
        let topology = PartitionTopology::from_records(records.clone())?;
        let mut affinity = ApLeaderAffinity::default();
        for record in &records {
            if let Some(leader) = record.members.first() {
                let lease = LeaseUpdate {
                    partition_id: record.partition_id.clone(),
                    lease_epoch: record.lease_epoch,
                    leader: leader.clone(),
                };
                let _ = affinity.apply(&topology, lease)?;
            }
        }
        Ok(Self {
            records,
            topology,
            affinity,
        })
    }

    fn apply_record(&mut self, record: PlacementRecord) -> Result<(), PartitionError> {
        self.records
            .retain(|existing| existing.partition_id != record.partition_id);
        self.records.push(record.clone());
        self.topology = PartitionTopology::from_records(self.records.clone())?;
        if let Some(leader) = record.members.first() {
            let lease = LeaseUpdate {
                partition_id: record.partition_id.clone(),
                lease_epoch: record.lease_epoch,
                leader: leader.clone(),
            };
            let _ = self.affinity.apply(&self.topology, lease)?;
        }
        Ok(())
    }

    fn apply_lease(
        &mut self,
        update: LeaseUpdate,
    ) -> Result<Option<ApReassignment>, PartitionError> {
        self.affinity.apply(&self.topology, update)
    }

    fn topology(&self) -> PartitionTopology {
        self.topology.clone()
    }
}

#[derive(Debug, Default, Clone)]
struct IngestTelemetry {
    total_pending_entries: u64,
    partitions: u64,
    throttled_partitions: u64,
    strict_fallback_partitions: u64,
    strict_mode: u64,
    relaxed_mode: u64,
    applied_events_total: u64,
    dedup_skipped_total: u64,
    derived_emissions_total: u64,
    emission_drops_total: u64,
    durability: Vec<PartitionDurabilityMetrics>,
}

impl IngestTelemetry {
    fn render_prometheus(&self) -> String {
        format!(
            "ceptra_pending_entries_total {}\nceptra_partitions_total {}\nceptra_partitions_throttled {}\nceptra_partitions_strict_fallback {}\nceptra_durability_mode_total{{mode=\"strict\"}} {}\nceptra_durability_mode_total{{mode=\"relaxed\"}} {}\nceptra_applied_events_total {}\nceptra_apply_dedup_skipped_total {}\nceptra_derived_emissions_total {}\nceptra_emission_drops_total {}\n",
            self.total_pending_entries,
            self.partitions,
            self.throttled_partitions,
            self.strict_fallback_partitions,
            self.strict_mode,
            self.relaxed_mode,
            self.applied_events_total,
            self.dedup_skipped_total,
            self.derived_emissions_total,
            self.emission_drops_total
        ) + &self.render_durability_metrics()
    }

    fn render_durability_metrics(&self) -> String {
        let mut lines = String::new();
        for snapshot in &self.durability {
            lines.push_str(&format!(
                "ceptra_partition_pending_entries{{partition_id=\"{}\",status=\"quorum\"}} {}\n",
                snapshot.partition_id, snapshot.quorum_pending
            ));
            lines.push_str(&format!(
                "ceptra_partition_pending_entries{{partition_id=\"{}\",status=\"apply\"}} {}\n",
                snapshot.partition_id, snapshot.apply_pending
            ));
            lines.push_str(&format!(
                "ceptra_partition_last_index{{partition_id=\"{}\",kind=\"applied\"}} {}\n",
                snapshot.partition_id, snapshot.last_applied
            ));
            lines.push_str(&format!(
                "ceptra_partition_last_index{{partition_id=\"{}\",kind=\"log\"}} {}\n",
                snapshot.partition_id, snapshot.last_log_index
            ));
            lines.push_str(&format!(
                "ceptra_partition_committed_index{{partition_id=\"{}\"}} {}\n",
                snapshot.partition_id, snapshot.committed_index
            ));
            lines.push_str(&format!(
                "ceptra_partition_strict_fallback_state{{partition_id=\"{}\"}} {}\n",
                snapshot.partition_id,
                snapshot.strict_state.as_metric()
            ));
            lines.push_str(&format!(
                "ceptra_partition_term{{partition_id=\"{}\"}} {}\n",
                snapshot.partition_id, snapshot.term
            ));
            lines.push_str(&format!(
                "ceptra_partition_leader_known{{partition_id=\"{}\"}} {}\n",
                snapshot.partition_id,
                if snapshot.leader_known { 1 } else { 0 }
            ));
            lines.push_str(&format!(
                "ceptra_partition_heartbeat_failures{{partition_id=\"{}\"}} {}\n",
                snapshot.partition_id, snapshot.heartbeat_failures
            ));
            lines.push_str(&format!(
                "ceptra_partition_leader_info{{partition_id=\"{}\",leader_id=\"{}\"}} 1\n",
                snapshot.partition_id, snapshot.leader
            ));
            for (replica, index) in &snapshot.peer_match {
                lines.push_str(&format!(
                    "ceptra_partition_match_index{{partition_id=\"{}\",replica_id=\"{}\"}} {}\n",
                    snapshot.partition_id, replica, index
                ));
            }
        }
        lines
    }
}

#[derive(Debug, Clone)]
struct PartitionDurabilityMetrics {
    partition_id: String,
    quorum_pending: u64,
    apply_pending: u64,
    last_applied: u64,
    last_log_index: u64,
    committed_index: u64,
    strict_state: StrictFallbackState,
    term: u64,
    leader_known: bool,
    heartbeat_failures: u32,
    leader: String,
    peer_match: Vec<(String, u64)>,
}

#[derive(Debug, Clone)]
struct ReplicaEndpoint {
    replica_id: String,
    addr: SocketAddr,
}

#[cfg(feature = "clustor-net")]
#[derive(Clone)]
struct ReplicationContext {
    local_id: String,
    peers: Vec<ReplicaEndpoint>,
    tls: Arc<TlsMaterials>,
    tuning: RaftClientTuning,
}

#[cfg(feature = "clustor-net")]
#[derive(Clone)]
struct PeerClient {
    replica_id: String,
    addr: SocketAddr,
    client: Arc<RaftNetworkClient>,
}

#[cfg(feature = "clustor-net")]
impl PeerClient {
    fn new(
        endpoint: &ReplicaEndpoint,
        tls: &TlsMaterials,
        tuning: &RaftClientTuning,
    ) -> Result<Self> {
        let options = RaftNetworkClientOptions::new()
            .socket_timeout(tuning.timeout)
            .retry_backoff(tuning.retry_attempts, tuning.backoff);
        let client = RaftNetworkClient::with_options(
            RaftNetworkClientConfig {
                host: endpoint.addr.ip().to_string(),
                port: endpoint.addr.port(),
                identity: tls.identity.clone(),
                trust_store: tls.trust.clone(),
                mtls: MtlsIdentityManager::new(
                    tls.identity.certificate.clone(),
                    tls.trust_domain.clone(),
                    Duration::from_secs(600),
                    Instant::now(),
                ),
            },
            options,
        )?;
        Ok(Self {
            replica_id: endpoint.replica_id.clone(),
            addr: endpoint.addr,
            client: Arc::new(client),
        })
    }
}

#[cfg(feature = "clustor-net")]
fn build_peer_clients(
    local_id: &str,
    endpoints: &[ReplicaEndpoint],
    tls: &TlsMaterials,
    tuning: &RaftClientTuning,
) -> Vec<PeerClient> {
    let mut peers = Vec::new();
    for endpoint in endpoints {
        if endpoint.replica_id == local_id {
            continue;
        }
        match PeerClient::new(endpoint, tls, tuning) {
            Ok(client) => peers.push(client),
            Err(err) => eprintln!(
                "clustor: failed to build Raft client for {} at {}: {}",
                endpoint.replica_id, endpoint.addr, err
            ),
        }
    }
    peers
}

#[cfg(feature = "clustor-net")]
struct RaftReplicator {
    peers: Vec<PeerClient>,
    peer_match_index: Mutex<HashMap<String, u64>>,
}

#[cfg(feature = "clustor-net")]
impl RaftReplicator {
    fn new(context: ReplicationContext) -> Self {
        let peers = build_peer_clients(
            &context.local_id,
            &context.peers,
            &context.tls,
            &context.tuning,
        );
        Self {
            peers,
            peer_match_index: Mutex::new(HashMap::new()),
        }
    }

    fn replicate(
        &self,
        request: &AppendEntriesRequest,
    ) -> Vec<(String, Result<AppendEntriesResponse, NetError>)> {
        let now = Instant::now();
        self.peers
            .iter()
            .map(|peer| {
                let resp = peer.client.append_entries(request, now);
                (peer.replica_id.clone(), resp)
            })
            .collect()
    }

    fn record_match(&self, replica_id: &str, index: u64) {
        if let Ok(mut guard) = self.peer_match_index.lock() {
            let entry = guard.entry(replica_id.to_string()).or_insert(0);
            *entry = (*entry).max(index);
        }
    }

    fn record_conflict(&self, replica_id: &str, index: u64) {
        if let Ok(mut guard) = self.peer_match_index.lock() {
            guard.insert(replica_id.to_string(), index);
        }
    }

    fn peer_matches(&self) -> HashMap<String, u64> {
        self.peer_match_index
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaftStateDisk {
    term: u64,
    leader_id: String,
    voted_for: Option<String>,
    #[serde(default)]
    commit_index: u64,
}

/// Clustor-backed ingest surface that applies flow control and durability gates.
struct ClustorIngestSurface {
    placements: Arc<Mutex<PlacementState>>,
    wal_root: PathBuf,
    local_replica: String,
    peer_addrs: HashMap<String, SocketAddr>,
    #[cfg(feature = "clustor-net")]
    tls: Option<Arc<TlsMaterials>>,
    #[cfg(feature = "clustor-net")]
    raft_tuning: Option<RaftClientTuning>,
    #[cfg(feature = "clustor-net")]
    raft_log: Arc<Mutex<RaftLogStore>>,
    partitions: Mutex<HashMap<String, PartitionRuntime>>,
    apply_factory: ApplyPipelineFactory,
}

impl fmt::Debug for ClustorIngestSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClustorIngestSurface")
            .field("wal_root", &self.wal_root)
            .field("local_replica", &self.local_replica)
            .field("peer_addrs", &self.peer_addrs)
            .finish()
    }
}

impl ClustorIngestSurface {
    fn new(
        placements: Arc<Mutex<PlacementState>>,
        wal_root: PathBuf,
        local_replica: String,
        peer_addrs: HashMap<String, SocketAddr>,
        #[cfg(feature = "clustor-net")] tls: Option<Arc<TlsMaterials>>,
        #[cfg(feature = "clustor-net")] raft_tuning: Option<RaftClientTuning>,
    ) -> Result<Self> {
        let apply_factory = ApplyPipelineFactory::default();
        #[cfg(feature = "clustor-net")]
        let raft_log = {
            let path = wal_root.join("raft.log");
            Arc::new(Mutex::new(RaftLogStore::open(&path).with_context(
                || format!("failed to open raft log at {}", path.display()),
            )?))
        };
        let partitions = {
            let placements_guard = placements
                .lock()
                .map_err(|_| anyhow!("placement state poisoned"))?;
            let mut map = HashMap::new();
            for placement in placements_guard.topology().placements() {
                let runtime = PartitionRuntime::with_apply_factory(PartitionRuntimeParams {
                    partition_id: placement.partition_id(),
                    replicas: placement.replicas(),
                    wal_root: &wal_root,
                    local_replica: local_replica.clone(),
                    peer_addrs: peer_addrs.clone(),
                    #[cfg(feature = "clustor-net")]
                    tls: tls.clone(),
                    #[cfg(feature = "clustor-net")]
                    raft_tuning: raft_tuning.clone(),
                    apply_factory: &apply_factory,
                })?;
                map.insert(placement.partition_id().to_string(), runtime);
            }
            map
        };

        let surface = Self {
            placements,
            wal_root,
            local_replica,
            peer_addrs,
            #[cfg(feature = "clustor-net")]
            tls,
            #[cfg(feature = "clustor-net")]
            raft_tuning,
            #[cfg(feature = "clustor-net")]
            raft_log,
            partitions: Mutex::new(partitions),
            apply_factory,
        };
        #[cfg(feature = "clustor-net")]
        surface.hydrate_from_raft_log();
        Ok(surface)
    }

    #[cfg(feature = "clustor-net")]
    fn raft_log(&self) -> Arc<Mutex<RaftLogStore>> {
        self.raft_log.clone()
    }

    fn upsert_partition(&self, record: &PlacementRecord) -> Result<()> {
        let mut partitions = self
            .partitions
            .lock()
            .map_err(|_| anyhow!("partition runtime map poisoned"))?;
        let runtime = PartitionRuntime::with_apply_factory(PartitionRuntimeParams {
            partition_id: &record.partition_id,
            replicas: &record.members,
            wal_root: &self.wal_root,
            local_replica: self.local_replica.clone(),
            peer_addrs: self.peer_addrs.clone(),
            #[cfg(feature = "clustor-net")]
            tls: self.tls.clone(),
            #[cfg(feature = "clustor-net")]
            raft_tuning: self.raft_tuning.clone(),
            apply_factory: &self.apply_factory,
        })?;
        partitions.insert(record.partition_id.clone(), runtime);
        Ok(())
    }

    fn apply_lease(&self, partition_id: &str, lease_epoch: u64, leader: Option<String>) {
        if let Ok(mut partitions) = self.partitions.lock() {
            if let Some(runtime) = partitions.get_mut(partition_id) {
                runtime.apply_lease(lease_epoch, leader);
            }
        }
    }

    fn leader_hint(&self, partition_id: &str) -> Option<String> {
        self.partitions
            .lock()
            .ok()
            .and_then(|parts| {
                parts
                    .get(partition_id)
                    .map(|runtime| runtime.leader_id.clone())
            })
            .filter(|leader| !leader.is_empty())
    }

    fn cluster_leader_hint(&self) -> Option<String> {
        self.partitions
            .lock()
            .ok()
            .and_then(|parts| {
                parts
                    .values()
                    .next()
                    .map(|runtime| runtime.leader_id.clone())
            })
            .filter(|leader| !leader.is_empty())
    }

    fn is_local_leader_for(&self, partition_id: &str) -> bool {
        self.partitions
            .lock()
            .ok()
            .and_then(|parts| {
                parts
                    .get(partition_id)
                    .map(|runtime| runtime.is_local_leader())
            })
            .unwrap_or(false)
    }

    fn is_cluster_leader(&self) -> bool {
        self.partitions
            .lock()
            .ok()
            .map(|parts| {
                if parts.is_empty() {
                    true
                } else {
                    parts.values().all(|runtime| runtime.is_local_leader())
                }
            })
            .unwrap_or(false)
    }

    #[cfg(feature = "clustor-net")]
    fn decode_raft_entry(
        &self,
        entry: &RaftLogEntry,
    ) -> Option<(String, EntryFrame, WalFrameMetadata)> {
        let frame = EntryFrame::decode(&entry.payload).ok()?;
        let metadata: WalFrameMetadata = serde_json::from_slice(&frame.metadata).ok()?;
        let partition = self.route_partition(&metadata.partition_key).ok()?;
        Some((partition, frame, metadata))
    }

    #[cfg(feature = "clustor-net")]
    fn decode_entries(
        &self,
        entries: &[RaftLogEntry],
    ) -> Vec<(String, EntryFrame, WalFrameMetadata)> {
        let mut decoded = Vec::new();
        for entry in entries {
            if let Some((partition, frame, metadata)) = self.decode_raft_entry(entry) {
                decoded.push((partition, frame, metadata));
            }
        }
        decoded
    }

    #[cfg(feature = "clustor-net")]
    fn stage_raft_entries(&self, entries: &[RaftLogEntry]) {
        let decoded = self.decode_entries(entries);
        if decoded.is_empty() {
            return;
        }
        let mut tick_state: HashMap<String, (Option<u64>, Option<u64>)> = HashMap::new();
        if let Ok(mut partitions) = self.partitions.lock() {
            for (partition, frame, metadata) in decoded {
                let (base, last_tick) = tick_state.entry(partition.clone()).or_insert((None, None));
                if let Some(runtime) = partitions.get_mut(&partition) {
                    if let Err(err) = runtime.stage_raft_frame(&frame, metadata, base, last_tick) {
                        eprintln!(
                            "clustor: failed to stage raft entry {} for partition {}: {}",
                            frame.header.index, partition, err
                        );
                    }
                }
            }
        }
    }

    #[cfg(feature = "clustor-net")]
    fn apply_committed_entries(&self, entries: &[RaftLogEntry], commit_index: u64) {
        if commit_index == 0 {
            return;
        }
        let decoded = self.decode_entries(entries);
        if decoded.is_empty() {
            return;
        }
        let mut tick_state: HashMap<String, (Option<u64>, Option<u64>)> = HashMap::new();
        let mut touched = HashSet::new();
        if let Ok(mut partitions) = self.partitions.lock() {
            for (partition, frame, metadata) in decoded {
                let (base, last_tick) = tick_state.entry(partition.clone()).or_insert((None, None));
                if let Some(runtime) = partitions.get_mut(&partition) {
                    if let Err(err) = runtime.stage_raft_frame(&frame, metadata, base, last_tick) {
                        eprintln!(
                            "clustor: failed to stage committed raft entry {} for partition {}: {}",
                            frame.header.index, partition, err
                        );
                        continue;
                    }
                    touched.insert(partition);
                }
            }
            for partition in touched {
                if let Some(runtime) = partitions.get_mut(&partition) {
                    runtime.apply_committed(commit_index);
                }
            }
        }
    }

    #[cfg(feature = "clustor-net")]
    fn rebuild_committed_entries(&self, entries: &[RaftLogEntry], commit_index: u64) {
        if commit_index == 0 {
            return;
        }
        let decoded = self.decode_entries(entries);
        if decoded.is_empty() {
            return;
        }
        let mut grouped: HashMap<String, Vec<(EntryFrame, WalFrameMetadata)>> = HashMap::new();
        for (partition, frame, metadata) in decoded {
            grouped
                .entry(partition)
                .or_default()
                .push((frame, metadata));
        }
        if let Ok(mut partitions) = self.partitions.lock() {
            for (partition, frames) in grouped {
                if let Some(runtime) = partitions.get_mut(&partition) {
                    if let Err(err) = runtime.rebuild_from_raft_entries(&frames, commit_index) {
                        eprintln!(
                            "clustor: failed to rebuild raft state for partition {}: {}",
                            partition, err
                        );
                    }
                }
            }
        }
    }

    #[cfg(feature = "clustor-net")]
    fn reset_uncommitted(&self) {
        if let Ok(mut partitions) = self.partitions.lock() {
            for runtime in partitions.values_mut() {
                if let Err(err) = runtime.reset_for_rebuild() {
                    eprintln!(
                        "clustor: failed to reset raft state for partition {}: {}",
                        runtime.partition_id, err
                    );
                }
            }
        }
    }

    #[cfg(feature = "clustor-net")]
    fn hydrate_from_raft_log(&self) {
        let commit_index = self
            .partitions
            .lock()
            .ok()
            .map(|parts| parts.values().map(|p| p.last_applied).max().unwrap_or(0))
            .unwrap_or(0);
        let mut entries = Vec::new();
        if let Ok(log) = self.raft_log.lock() {
            let tail = log.last_index();
            if tail > 0 {
                log.copy_entries_in_range(1, tail, &mut entries);
            }
        }
        if entries.is_empty() {
            return;
        }
        self.stage_raft_entries(&entries);
        if commit_index > 0 {
            let commit_entries: Vec<RaftLogEntry> = entries
                .into_iter()
                .filter(|entry| entry.index <= commit_index)
                .collect();
            self.apply_committed_entries(&commit_entries, commit_index);
        }
    }

    #[cfg(feature = "clustor-net")]
    fn update_leader(&self, leader_id: &str, term: u64) {
        if let Ok(mut partitions) = self.partitions.lock() {
            let mut logged = false;
            for runtime in partitions.values_mut() {
                let previous = runtime.leader_id.clone();
                if runtime.term < term {
                    runtime.term = term;
                }
                runtime.leader_id = leader_id.to_string();
                runtime.voted_for = None;
                runtime.persist_raft_state();
                if !logged && previous != runtime.leader_id {
                    eprintln!(
                        "event=clustor_leader_change partition_id={} from={} to={} term={}",
                        runtime.partition_id,
                        if previous.is_empty() {
                            "none"
                        } else {
                            previous.as_str()
                        },
                        leader_id,
                        runtime.term
                    );
                    logged = true;
                }
            }
        }
    }

    #[cfg(feature = "clustor-net")]
    fn run_election(&self) -> (u64, String) {
        let tls = match self.tls.as_ref() {
            Some(tls) => tls.clone(),
            None => return (DEFAULT_TERM, self.local_replica.clone()),
        };
        let tuning = match self.raft_tuning.as_ref() {
            Some(tuning) => tuning.clone(),
            None => return (DEFAULT_TERM, self.local_replica.clone()),
        };
        let endpoints: Vec<ReplicaEndpoint> = self
            .peer_addrs
            .iter()
            .map(|(id, addr)| ReplicaEndpoint {
                replica_id: id.clone(),
                addr: *addr,
            })
            .collect();
        if endpoints.is_empty() {
            if let Ok(mut partitions) = self.partitions.lock() {
                for runtime in partitions.values_mut() {
                    runtime.term = runtime.term.max(DEFAULT_TERM);
                    runtime.leader_id = self.local_replica.clone();
                    runtime.persist_raft_state();
                }
            }
            return (DEFAULT_TERM, self.local_replica.clone());
        }
        let peers = build_peer_clients(&self.local_replica, &endpoints, &tls, &tuning);
        let (base_term, last_log_index, last_log_term) = {
            let guard = self.partitions.lock().ok();
            let mut max_term = DEFAULT_TERM;
            let mut max_index = 0;
            let mut max_log_term = 0;
            if let Some(map) = guard.as_ref() {
                for runtime in map.values() {
                    max_term = max_term.max(runtime.term);
                    let idx = runtime.next_index.load(Ordering::SeqCst);
                    max_index = max_index.max(idx);
                    max_log_term = max_log_term.max(runtime.term);
                }
            }
            (max_term, max_index, max_log_term)
        };
        let candidate_term = base_term.saturating_add(1);
        let now_ms = current_time_ms() as u128;
        if let Ok(partitions) = self.partitions.lock() {
            if partitions
                .values()
                .all(|p| now_ms.saturating_sub(p.last_election_ms) < 2_000)
            {
                return (base_term, self.local_replica.clone());
            }
        }
        let mut granted = 1; // self vote
        let mut observed_term = candidate_term;
        for peer in peers {
            let request = RequestVoteRequest {
                term: candidate_term,
                candidate_id: self.local_replica.clone(),
                last_log_index,
                last_log_term,
                pre_vote: false,
            };
            match peer.client.request_vote(&request, Instant::now()) {
                Ok(resp) => {
                    observed_term = observed_term.max(resp.term);
                    if resp.granted {
                        granted += 1;
                    }
                }
                Err(err) => eprintln!(
                    "clustor: request_vote to {} at {} failed: {}",
                    peer.replica_id, peer.addr, err
                ),
            }
        }
        let total_nodes = endpoints.len().saturating_add(1);
        let majority = (total_nodes / 2).saturating_add(1);
        let leader = if granted >= majority {
            self.local_replica.clone()
        } else {
            String::new()
        };
        if let Ok(mut guard) = self.partitions.lock() {
            for runtime in guard.values_mut() {
                runtime.term = observed_term;
                runtime.leader_id = leader.clone();
                runtime.voted_for = Some(self.local_replica.clone());
                runtime.last_election_ms = now_ms;
                runtime.persist_raft_state();
            }
        }
        eprintln!(
            "event=clustor_election_outcome candidate={} leader={} granted_votes={} total_nodes={} term={}",
            self.local_replica,
            if leader.is_empty() { "none" } else { &leader },
            granted,
            total_nodes,
            observed_term
        );
        (observed_term, leader)
    }

    fn set_durability_mode(&self, partition_id: &str, mode: DurabilityMode) {
        if let Ok(mut partitions) = self.partitions.lock() {
            if let Some(runtime) = partitions.get_mut(partition_id) {
                runtime.set_durability_mode(mode);
            }
        }
    }

    fn set_global_durability_mode(&self, mode: DurabilityMode) {
        if let Ok(mut partitions) = self.partitions.lock() {
            for runtime in partitions.values_mut() {
                runtime.set_durability_mode(mode.clone());
            }
        }
    }

    #[allow(dead_code)]
    fn record_replica_ack(&self, partition_id: &str, ack: AckRecord) -> Result<()> {
        let mut partitions = self
            .partitions
            .lock()
            .map_err(|_| anyhow!("partition runtime map poisoned"))?;
        let runtime = partitions
            .get_mut(partition_id)
            .ok_or_else(|| anyhow!("partition runtime missing for {}", partition_id))?;
        runtime.record_replica_ack(ack).map(|_| ()).map_err(|err| {
            anyhow!(
                "failed to record replica ack for partition {}: {err}",
                partition_id
            )
        })
    }

    fn telemetry(&self) -> IngestTelemetry {
        let mut telemetry = IngestTelemetry::default();
        if let Ok(partitions) = self.partitions.lock() {
            telemetry.partitions = partitions.len() as u64;
            for runtime in partitions.values() {
                telemetry.total_pending_entries = telemetry
                    .total_pending_entries
                    .saturating_add(runtime.pending_entries());
                telemetry.durability.push(runtime.durability_metrics());
                if matches!(
                    runtime.last_flow.throttle.state,
                    FlowThrottleState::Throttled(_)
                ) {
                    telemetry.throttled_partitions =
                        telemetry.throttled_partitions.saturating_add(1);
                }
                if !matches!(
                    runtime.strict_state,
                    StrictFallbackState::Healthy | StrictFallbackState::ProofPublished
                ) {
                    telemetry.strict_fallback_partitions =
                        telemetry.strict_fallback_partitions.saturating_add(1);
                }
                let mode = runtime.durability_mode.clone();
                match mode {
                    DurabilityMode::Strict => {
                        telemetry.strict_mode = telemetry.strict_mode.saturating_add(1)
                    }
                    DurabilityMode::Relaxed => {
                        telemetry.relaxed_mode = telemetry.relaxed_mode.saturating_add(1)
                    }
                }
                telemetry.applied_events_total = telemetry
                    .applied_events_total
                    .saturating_add(runtime.applied_events_total);
                let apply = runtime.apply_telemetry();
                telemetry.dedup_skipped_total = telemetry
                    .dedup_skipped_total
                    .saturating_add(apply.dedup_skipped);
                telemetry.derived_emissions_total = telemetry
                    .derived_emissions_total
                    .saturating_add(apply.derived_emissions);
                telemetry.emission_drops_total = telemetry
                    .emission_drops_total
                    .saturating_add(apply.emission_drops);
            }
        }
        telemetry
    }

    fn replay_from_warmup(&self, readiness: &[WarmupReadinessRecord]) {
        if readiness.is_empty() {
            return;
        }
        if let Ok(mut partitions) = self.partitions.lock() {
            for record in readiness {
                if let Some(runtime) = partitions.get_mut(&record.partition_id) {
                    runtime.restore_committed_index(record.shadow_apply_checkpoint_index);
                }
            }
        }
    }

    fn append(&self, request: AppendRequest) -> Result<AppendResponse> {
        let partition = self.route_partition(&request.partition_key)?;
        let mut partitions = self
            .partitions
            .lock()
            .map_err(|_| anyhow!("partition runtime map poisoned"))?;
        let runtime = partitions.get_mut(&partition).ok_or_else(|| {
            anyhow!(
                "partition runtime missing for routed partition {}",
                partition
            )
        })?;
        if !runtime.is_local_leader() {
            let leader = runtime.leader_id.clone();
            let reason = if leader.is_empty() {
                "NOT_LEADER".to_string()
            } else {
                format!("NOT_LEADER:{leader}")
            };
            return Ok(AppendResponse {
                event_id: request.event_id.clone(),
                status: IngestStatusCode::PermanentDurability,
                credit_hint: CreditHint::Hold,
                cep_credit_hint: None,
                cep_status_reason: Some(reason),
                term: Some(runtime.term),
                index: None,
            });
        }
        let result = runtime.append(&request, Instant::now())?;
        let status_reason = result
            .cep_status_reason
            .clone()
            .or_else(|| Some(partition.clone()));
        Ok(AppendResponse {
            event_id: request.event_id,
            status: result.flow.ingest_status,
            credit_hint: result.flow.credit_hint,
            cep_credit_hint: result.cep_hint,
            cep_status_reason: status_reason,
            term: result.term,
            index: result.index,
        })
    }

    fn route_partition(&self, partition_key: &[u8]) -> Result<String> {
        let placements = self
            .placements
            .lock()
            .map_err(|_| anyhow!("placement state poisoned"))?;
        let topology = placements.topology();
        Ok(topology.route(partition_key).partition_id().to_string())
    }

    fn flow_snapshot(&self) -> Result<FlowDecision> {
        let partitions = self
            .partitions
            .lock()
            .map_err(|_| anyhow!("partition runtime map poisoned"))?;
        if partitions.is_empty() {
            return Err(anyhow!("no partitions available for flow snapshot"));
        }
        // Prefer a throttled snapshot if any partition is under backpressure.
        if let Some(flow) = partitions
            .values()
            .find(|runtime| {
                matches!(
                    runtime.last_flow.throttle.state,
                    FlowThrottleState::Throttled(_)
                )
            })
            .map(|runtime| runtime.last_flow.clone())
        {
            return Ok(flow);
        }
        Ok(partitions
            .values()
            .next()
            .map(|runtime| runtime.last_flow.clone())
            .unwrap())
    }

    fn strict_fallback_state(&self) -> StrictFallbackState {
        let partitions = self.partitions.lock().ok();
        let mut state = StrictFallbackState::Healthy;
        if let Some(map) = partitions.as_ref() {
            for runtime in map.values() {
                match runtime.strict_state {
                    StrictFallbackState::LocalOnly => return StrictFallbackState::LocalOnly,
                    StrictFallbackState::ProofPublished => {
                        if matches!(state, StrictFallbackState::Healthy) {
                            state = StrictFallbackState::ProofPublished;
                        }
                    }
                    StrictFallbackState::Healthy => {}
                }
            }
        }
        state
    }

    fn pending_entries(&self) -> u64 {
        self.partitions
            .lock()
            .ok()
            .map(|map| map.values().map(|runtime| runtime.pending_entries()).sum())
            .unwrap_or(0)
    }

    fn raft_ready(&self) -> bool {
        if let Ok(partitions) = self.partitions.lock() {
            for runtime in partitions.values() {
                let status = runtime.durability.status();
                if runtime.leader_id.is_empty() || runtime.heartbeat_failures >= 3 {
                    return false;
                }
                if status.committed_index < runtime.last_applied {
                    return false;
                }
            }
        }
        true
    }
}

#[derive(Clone)]
struct ApplyPipelineFactory {
    builder: Arc<dyn Fn() -> Result<ApplyLoop> + Send + Sync>,
}

impl fmt::Debug for ApplyPipelineFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplyPipelineFactory").finish()
    }
}

impl ApplyPipelineFactory {
    fn new<F>(builder: F) -> Self
    where
        F: Fn() -> Result<ApplyLoop> + Send + Sync + 'static,
    {
        Self {
            builder: Arc::new(builder),
        }
    }

    fn default() -> Self {
        Self::new(default_apply_loop)
    }

    fn build(&self) -> Result<ApplyLoop> {
        (self.builder)()
    }
}

fn default_apply_loop() -> Result<ApplyLoop> {
    let dedup = DedupTable::new(DedupTableConfig {
        client_retry_window_s: 600,
    });
    let aggregation = PaneAggregationPipeline::new(default_aggregation_config())?;
    let cel_runtime = CelSandbox::new(Vec::new());
    Ok(ApplyLoop::with_channel_policies(
        dedup,
        Box::new(aggregation),
        Box::new(cel_runtime),
        ChannelPolicies::default(),
    ))
}

fn default_aggregation_config() -> AggregationConfig {
    AggregationConfig {
        pane_width_ms: 1_000,
        raw_retention_ms: 60_000,
        max_window_horizon_ms: 60_000,
        metrics: vec![
            MetricSpec {
                name: APPLY_DEFAULT_METRIC.to_string(),
                kind: AggregatorKind::Count,
                window_horizon_ms: 60_000,
                correction_horizon_ms: 0,
                labels: Vec::new(),
                late_data_policy: LateDataPolicy::Drop,
            },
            MetricSpec {
                name: APPLY_PAYLOAD_BYTES_METRIC.to_string(),
                kind: AggregatorKind::Sum,
                window_horizon_ms: 60_000,
                correction_horizon_ms: 0,
                labels: Vec::new(),
                late_data_policy: LateDataPolicy::Drop,
            },
        ],
    }
}

#[derive(Debug, Default, Clone)]
struct ApplyTelemetry {
    total_events: u64,
    dedup_skipped: u64,
    derived_emissions: u64,
    emission_drops: u64,
    derived_by_channel: HashMap<String, u64>,
    drop_by_channel: HashMap<String, u64>,
    late_events: BTreeMap<String, u64>,
}

impl ApplyTelemetry {
    fn record(&mut self, result: &ApplyBatchResult, late_events: BTreeMap<String, u64>) {
        self.total_events = self
            .total_events
            .saturating_add(result.stats.total_events as u64);
        self.dedup_skipped = self
            .dedup_skipped
            .saturating_add(result.stats.dedup_skipped as u64);
        let derived_total = result.emissions.total() as u64;
        self.derived_emissions = self.derived_emissions.saturating_add(derived_total);
        let drop_total: u64 = result.emissions.drop_metrics().values().copied().sum();
        self.emission_drops = self.emission_drops.saturating_add(drop_total);
        for (channel, emissions) in result.emissions.by_channel() {
            let count = emissions.len() as u64;
            *self.derived_by_channel.entry(channel.clone()).or_insert(0) += count;
        }
        for (channel, drops) in result.emissions.drop_metrics() {
            *self.drop_by_channel.entry(channel.clone()).or_insert(0) += *drops;
        }
        self.late_events = late_events;
    }
}

struct PartitionRuntimeParams<'a> {
    partition_id: &'a str,
    replicas: &'a [String],
    wal_root: &'a Path,
    local_replica: String,
    peer_addrs: HashMap<String, SocketAddr>,
    #[cfg(feature = "clustor-net")]
    tls: Option<Arc<TlsMaterials>>,
    #[cfg(feature = "clustor-net")]
    raft_tuning: Option<RaftClientTuning>,
    apply_factory: &'a ApplyPipelineFactory,
}

struct PartitionRuntime {
    flow: DualCreditPidController,
    last_flow: FlowDecision,
    flow_override: Option<FlowDecision>,
    partition_id: String,
    wal_path: PathBuf,
    durability: DurabilityLedger,
    wal: WalWriter,
    pending_wal: HashMap<u64, CommittedEvent>,
    last_applied: u64,
    commit_epoch: CommitEpochState,
    commit_epoch_ticker: CommitEpochTicker<SystemMonotonicClock>,
    local_replica: String,
    replicas: Vec<String>,
    next_index: AtomicU64,
    term: u64,
    leader_id: String,
    heartbeat_failures: u32,
    raft_state_path: PathBuf,
    voted_for: Option<String>,
    last_election_ms: u128,
    #[cfg(feature = "clustor-net")]
    replicator: Option<RaftReplicator>,
    strict_state: StrictFallbackState,
    credit_timer: CreditWindowHintTimer,
    durability_mode: DurabilityMode,
    apply_loop: ApplyLoop,
    applied_events_total: u64,
    apply_telemetry: ApplyTelemetry,
}

struct AppendResult {
    flow: FlowDecision,
    cep_hint: Option<CepCreditHint>,
    cep_status_reason: Option<String>,
    term: Option<u64>,
    index: Option<u64>,
}

impl fmt::Debug for PartitionRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionRuntime")
            .field("partition_id", &self.partition_id)
            .field("term", &self.term)
            .field("last_applied", &self.last_applied)
            .field("pending_wal", &self.pending_wal.len())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalFrameMetadata {
    event_id: String,
    partition_key: Vec<u8>,
    schema_key: String,
}

fn load_raft_state(
    path: &Path,
    default_term: u64,
    default_leader: &str,
) -> (u64, String, Option<String>, u64) {
    if let Ok(bytes) = fs::read(path) {
        if let Ok(state) = serde_json::from_slice::<RaftStateDisk>(&bytes) {
            return (
                state.term.max(default_term),
                state.leader_id,
                state.voted_for,
                state.commit_index,
            );
        }
    }
    (default_term, default_leader.to_string(), None, 0)
}

fn persist_raft_state_file(
    path: &Path,
    term: u64,
    leader_id: &str,
    voted_for: Option<String>,
    commit_index: u64,
) {
    let state = RaftStateDisk {
        term,
        leader_id: leader_id.to_string(),
        voted_for,
        commit_index,
    };
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(payload) = serde_json::to_vec(&state) {
        let _ = fs::write(path, payload);
    }
}

impl PartitionRuntime {
    fn is_local_leader(&self) -> bool {
        !self.leader_id.is_empty() && self.leader_id == self.local_replica
    }

    #[allow(dead_code)]
    fn new(
        partition_id: &str,
        replicas: &[String],
        wal_root: &Path,
        local_replica: String,
        peer_addrs: HashMap<String, SocketAddr>,
        #[cfg(feature = "clustor-net")] tls: Option<Arc<TlsMaterials>>,
        #[cfg(feature = "clustor-net")] raft_tuning: Option<RaftClientTuning>,
    ) -> Result<Self> {
        let factory = ApplyPipelineFactory::default();
        Self::with_apply_factory(PartitionRuntimeParams {
            partition_id,
            replicas,
            wal_root,
            local_replica,
            peer_addrs,
            #[cfg(feature = "clustor-net")]
            tls,
            #[cfg(feature = "clustor-net")]
            raft_tuning,
            apply_factory: &factory,
        })
    }

    fn with_apply_factory(params: PartitionRuntimeParams<'_>) -> Result<Self> {
        if params.replicas.is_empty() {
            return Err(anyhow!(
                "partition {} requires at least one replica",
                params.partition_id
            ));
        }
        let replicas_vec = params.replicas.to_vec();
        let local_replica = if !params.local_replica.is_empty() {
            params.local_replica
        } else {
            params
                .replicas
                .first()
                .cloned()
                .unwrap_or_else(|| params.partition_id.to_string())
        };
        let quorum_size = replicas_vec.len().max(1);
        let mut durability = DurabilityLedger::new(PartitionQuorumConfig::new(quorum_size));
        for replica in &replicas_vec {
            durability.register_replica(ReplicaId::new(replica.clone()));
        }
        let wal_path = params.wal_root.join(params.partition_id).join("wal.bin");
        if let Some(parent) = wal_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let wal = WalWriter::open(&wal_path, DEFAULT_WAL_BLOCK_SIZE).with_context(|| {
            format!(
                "failed to open WAL for partition {} at {}",
                params.partition_id,
                wal_path.display()
            )
        })?;
        let raft_state_path = params
            .wal_root
            .join(params.partition_id)
            .join("raft_state.json");
        let (initial_term, initial_leader, voted_for, initial_commit_index) =
            load_raft_state(&raft_state_path, DEFAULT_TERM, &local_replica);
        let mut flow = DualCreditPidController::new(FlowProfile::Latency);
        let last_flow = flow.record_sample(FLOW_SETPOINT_OPS_PER_SEC, 0.0, Instant::now());
        let apply_loop = params.apply_factory.build()?;
        #[cfg(feature = "clustor-net")]
        let replication_ctx = if let (Some(tls), Some(tuning)) =
            (params.tls.clone(), params.raft_tuning.clone())
        {
            let mut peers = Vec::new();
            for replica in &replicas_vec {
                if let Some(addr) = params.peer_addrs.get(replica) {
                    peers.push(ReplicaEndpoint {
                        replica_id: replica.clone(),
                        addr: *addr,
                    });
                }
            }
            if peers.is_empty() {
                None
            } else {
                Some(ReplicationContext {
                    local_id: local_replica.clone(),
                    peers,
                    tls,
                    tuning,
                })
            }
        } else {
            None
        };
        #[cfg(feature = "clustor-net")]
        let has_replication = replication_ctx.is_some();
        #[cfg(not(feature = "clustor-net"))]
        let has_replication = false;
        #[cfg(feature = "clustor-net")]
        let replicator = replication_ctx.map(RaftReplicator::new);
        let mut runtime = Self {
            flow,
            last_flow,
            flow_override: None,
            partition_id: params.partition_id.to_string(),
            wal_path,
            durability,
            wal,
            pending_wal: HashMap::new(),
            last_applied: initial_commit_index,
            commit_epoch: CommitEpochState::default(),
            commit_epoch_ticker: CommitEpochTicker::new(SystemMonotonicClock::new()),
            local_replica,
            replicas: replicas_vec,
            next_index: AtomicU64::new(initial_commit_index),
            term: initial_term,
            leader_id: initial_leader,
            heartbeat_failures: 0,
            raft_state_path,
            voted_for,
            last_election_ms: 0,
            #[cfg(feature = "clustor-net")]
            replicator,
            strict_state: if has_replication {
                StrictFallbackState::LocalOnly
            } else {
                StrictFallbackState::Healthy
            },
            credit_timer: CreditWindowHintTimer::new(),
            durability_mode: DurabilityMode::Relaxed,
            apply_loop,
            applied_events_total: 0,
            apply_telemetry: ApplyTelemetry::default(),
        };
        runtime.persist_raft_state();
        runtime.replay_wal().context("wal replay failed")?;
        Ok(runtime)
    }

    fn append(&mut self, request: &AppendRequest, now: Instant) -> Result<AppendResult> {
        if !self.is_local_leader() {
            return Err(anyhow!(
                "partition {} not leader; current leader: {}",
                self.partition_id,
                if self.leader_id.is_empty() {
                    "unknown"
                } else {
                    self.leader_id.as_str()
                }
            ));
        }
        let decision = if let Some(override_flow) = self.flow_override.take() {
            self.last_flow = override_flow.clone();
            override_flow
        } else {
            let pending = self.pending_entries() as f64;
            let decision = self
                .flow
                .record_sample(FLOW_SETPOINT_OPS_PER_SEC, pending, now);
            self.last_flow = decision.clone();
            decision
        };

        let cep_hint = self
            .credit_timer
            .poll_flow(now, &decision, true)
            .map(|hint| CepCreditHint {
                client_window_size: hint.credit_window_hint_events,
                retry_backoff_ms: 0,
            });

        if !matches!(decision.ingest_status, IngestStatusCode::Healthy) {
            let window = decision.entry_credit_max.max(0) as u32;
            let backoff = match &decision.throttle.state {
                FlowThrottleState::Throttled(FlowThrottleReason::QuotaExceeded { .. }) => 250,
                FlowThrottleState::Throttled(FlowThrottleReason::ByteCreditDebt { .. }) => 100,
                FlowThrottleState::Throttled(FlowThrottleReason::EntryCreditsDepleted) => 50,
                FlowThrottleState::Open => 0,
            };
            return Ok(AppendResult {
                flow: decision.clone(),
                cep_hint: Some(CepCreditHint {
                    client_window_size: window,
                    retry_backoff_ms: backoff,
                }),
                cep_status_reason: Some(decision.throttle.explain()),
                term: None,
                index: None,
            });
        }

        let index = self
            .next_index
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let (event, segment_seq, log_entry) = self.persist_wal_entry(index, request)?;
        self.pending_wal.insert(index, event);
        let update = self.record_local_ack(index, segment_seq)?;
        self.apply_committed(update.quorum_index);
        #[cfg(feature = "clustor-net")]
        if let Some(replicator) = self.replicator.as_ref() {
            let prev_index = index.saturating_sub(1);
            let request = AppendEntriesRequest {
                term: self.term,
                leader_id: self.leader_id.clone(),
                prev_log_index: prev_index,
                prev_log_term: self.term,
                leader_commit: self.last_applied,
                entries: vec![log_entry],
            };
            let responses = replicator.replicate(&request);
            for (replica, result) in responses {
                match result {
                    Ok(resp) if resp.success => {
                        let ack = AckRecord {
                            replica: ReplicaId::new(replica.clone()),
                            term: self.term,
                            index: resp.match_index.max(index),
                            segment_seq: resp.match_index.max(index),
                            io_mode: self.io_mode(),
                        };
                        let _ = self.record_replica_ack(ack);
                        self.heartbeat_failures = 0;
                        if let Some(repl) = self.replicator.as_ref() {
                            repl.record_match(&replica, resp.match_index.max(index));
                        }
                    }
                    Ok(resp) => {
                        eprintln!(
                            "event=clustor_append_failed partition_id={} peer={} conflict_index={:?} match_index={} term={}",
                            self.partition_id,
                            replica,
                            resp.conflict_index,
                            resp.match_index,
                            self.term
                        );
                        if let Some(conflict) = resp.conflict_index {
                            if let Some(repl) = self.replicator.as_ref() {
                                repl.record_conflict(&replica, conflict);
                            }
                        }
                        self.heartbeat_failures = self.heartbeat_failures.saturating_add(1);
                    }
                    Err(err) => {
                        self.heartbeat_failures = self.heartbeat_failures.saturating_add(1);
                        eprintln!(
                            "event=clustor_append_failed partition_id={} peer={} error={}",
                            self.partition_id, replica, err
                        )
                    }
                }
            }
        }

        let ack_signals = crate::AckSignals::new(self.durability_mode.clone(), self.strict_state)
            .with_fence(matches!(self.strict_state, StrictFallbackState::LocalOnly));
        if let crate::AckDisposition::Blocked(_) = crate::evaluate(ack_signals) {
            let mut blocked = decision.clone();
            blocked.ingest_status = IngestStatusCode::PermanentDurability;
            blocked.credit_hint = CreditHint::Shed;
            return Ok(AppendResult {
                flow: blocked,
                cep_hint,
                cep_status_reason: Some(PERMANENT_DURABILITY_REASON.to_string()),
                term: Some(self.term),
                index: Some(index),
            });
        }

        Ok(AppendResult {
            flow: decision.clone(),
            cep_hint: cep_hint.or({
                Some(CepCreditHint {
                    client_window_size: decision.entry_credit_max as u32,
                    retry_backoff_ms: 0,
                })
            }),
            cep_status_reason: None,
            term: Some(self.term),
            index: Some(index),
        })
    }

    fn apply_lease(&mut self, lease_epoch: u64, leader: Option<String>) {
        self.term = lease_epoch.max(self.term);
        if let Some(id) = leader {
            self.leader_id = id;
            self.voted_for = None;
        }
        if !self.is_local_leader() {
            self.heartbeat_failures = 0;
        }
        self.persist_raft_state();
    }

    fn persist_wal_entry(
        &mut self,
        index: u64,
        request: &AppendRequest,
    ) -> Result<(CommittedEvent, u64, RaftLogEntry)> {
        let metadata = WalFrameMetadata {
            event_id: request.event_id.clone(),
            partition_key: request.partition_key.clone(),
            schema_key: request.schema_key.clone(),
        };
        let metadata_bytes =
            serde_json::to_vec(&metadata).context("failed to serialize WAL metadata")?;
        let wal_entry = self
            .commit_epoch_ticker
            .stamp_entry(index, request.payload.clone());
        self.commit_epoch = *self.commit_epoch_ticker.state();
        let frame = EntryFrameBuilder::new(self.term, index)
            .timestamp_ms(wal_entry.commit_epoch_tick_ms)
            .metadata(metadata_bytes)
            .payload(wal_entry.payload.clone())
            .build();
        // Track last log term for elections.
        self.term = frame.header.term;
        self.voted_for = Some(self.local_replica.clone());
        let encoded = frame.encode();
        // Ack gating must happen after the WAL frame is durably written.
        let result = self
            .wal
            .append_frame(&encoded)
            .with_context(|| format!("wal append failed for partition {}", self.partition_id))?;
        let payload_len = wal_entry.payload.len();
        let event = CommittedEvent::new(
            self.partition_id.clone(),
            request.event_id.clone(),
            request.partition_key.clone(),
            wal_entry,
        );
        let event = self.attach_builtin_measurements(event, &request.schema_key, payload_len);
        let log_entry = RaftLogEntry::new(self.term, index, encoded.clone());
        Ok((event, result.reservation.start_block, log_entry))
    }

    fn attach_builtin_measurements(
        &self,
        event: CommittedEvent,
        schema_key: &str,
        payload_len: usize,
    ) -> CommittedEvent {
        let mut labels = BTreeMap::new();
        labels.insert("partition_id".to_string(), self.partition_id.clone());
        labels.insert("schema_key".to_string(), schema_key.to_string());
        let events_total = EventMeasurement::new(APPLY_DEFAULT_METRIC)
            .with_value(1.0)
            .with_lane_bitmap(1)
            .with_labels(labels.clone());
        let payload_bytes = EventMeasurement::new(APPLY_PAYLOAD_BYTES_METRIC)
            .with_value(payload_len as f64)
            .with_lane_bitmap(1)
            .with_labels(labels);
        event.with_measurements(vec![events_total, payload_bytes])
    }

    fn replay_commit_tick(
        replay_tick_base: &mut Option<u64>,
        last_tick: &mut Option<u64>,
        timestamp_ms: u64,
    ) -> u64 {
        let base_tick = replay_tick_base.get_or_insert(timestamp_ms);
        let mut commit_tick = timestamp_ms.saturating_sub(*base_tick);
        if let Some(prev) = last_tick {
            if commit_tick <= *prev {
                commit_tick = prev.saturating_add(1);
            }
        }
        *last_tick = Some(commit_tick);
        commit_tick
    }

    fn refresh_commit_epoch_ticker(&mut self) {
        self.commit_epoch_ticker =
            CommitEpochTicker::with_state(SystemMonotonicClock::new(), self.commit_epoch);
    }

    fn stage_raft_frame(
        &mut self,
        frame: &EntryFrame,
        metadata: WalFrameMetadata,
        replay_tick_base: &mut Option<u64>,
        last_tick: &mut Option<u64>,
    ) -> Result<()> {
        let idx = frame.header.index;
        if idx <= self.last_applied || self.pending_wal.contains_key(&idx) {
            self.next_index.store(
                self.next_index.load(Ordering::SeqCst).max(idx),
                Ordering::SeqCst,
            );
            return Ok(());
        }
        let expected = self.next_index.load(Ordering::SeqCst).saturating_add(1);
        if idx > expected {
            self.wal.align_next_block(idx);
        }
        let mut commit_tick =
            Self::replay_commit_tick(replay_tick_base, last_tick, frame.header.timestamp_ms);
        if commit_tick <= self.commit_epoch.commit_epoch_now() {
            commit_tick = self.commit_epoch.commit_epoch_now().saturating_add(1);
        }
        let wal_entry = WalEntry::new(idx, commit_tick, frame.payload.clone());
        let event = CommittedEvent::new(
            self.partition_id.clone(),
            metadata.event_id,
            metadata.partition_key,
            wal_entry,
        );
        let event =
            self.attach_builtin_measurements(event, &metadata.schema_key, frame.payload.len());
        self.commit_epoch.apply_entry(event.wal_entry());
        let encoded = frame.encode();
        let result = self
            .wal
            .append_frame(&encoded)
            .with_context(|| format!("wal append failed for partition {}", self.partition_id))?;
        self.pending_wal.insert(idx, event);
        let _ = self.record_local_ack(idx, result.reservation.start_block)?;
        self.term = self.term.max(frame.header.term);
        self.next_index.store(idx, Ordering::SeqCst);
        Ok(())
    }

    fn reset_for_rebuild(&mut self) -> Result<()> {
        self.pending_wal.clear();
        self.commit_epoch = CommitEpochState::default();
        self.commit_epoch_ticker = CommitEpochTicker::new(SystemMonotonicClock::new());
        self.last_applied = 0;
        self.next_index.store(0, Ordering::SeqCst);
        self.durability =
            DurabilityLedger::new(PartitionQuorumConfig::new(self.replicas.len().max(1)));
        for replica in &self.replicas {
            self.durability
                .register_replica(ReplicaId::new(replica.clone()));
        }
        self.strict_state = StrictFallbackState::LocalOnly;
        self.applied_events_total = 0;
        self.apply_telemetry = ApplyTelemetry::default();
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.wal_path);
        self.wal = WalWriter::open(&self.wal_path, DEFAULT_WAL_BLOCK_SIZE).with_context(|| {
            format!(
                "failed to reopen WAL for partition {} at {}",
                self.partition_id,
                self.wal_path.display()
            )
        })?;
        Ok(())
    }

    fn rebuild_from_raft_entries(
        &mut self,
        entries: &[(EntryFrame, WalFrameMetadata)],
        commit_index: u64,
    ) -> Result<()> {
        self.reset_for_rebuild()?;
        let mut tick_base = None;
        let mut last_tick = None;
        for (frame, metadata) in entries {
            self.stage_raft_frame(frame, metadata.clone(), &mut tick_base, &mut last_tick)?;
        }
        if commit_index > 0 {
            self.apply_committed(commit_index);
        } else {
            self.refresh_commit_epoch_ticker();
        }
        Ok(())
    }

    fn record_local_ack(&mut self, index: u64, segment_seq: u64) -> Result<LedgerUpdate> {
        let replica = self.local_replica.clone();
        let update = self
            .durability
            .record_ack(AckRecord {
                replica: ReplicaId::new(replica),
                term: self.term,
                index,
                segment_seq,
                io_mode: self.io_mode(),
            })
            .map_err(|err| anyhow!("durability ack failed: {err}"))?;
        self.refresh_strict_state();
        Ok(update)
    }

    fn record_replica_ack(&mut self, ack: AckRecord) -> Result<LedgerUpdate> {
        let update = self
            .durability
            .record_ack(ack)
            .map_err(|err| anyhow!("durability ack failed: {err}"))?;
        self.refresh_strict_state();
        self.apply_committed(update.quorum_index);
        Ok(update)
    }

    fn apply_committed(&mut self, committed_index: u64) {
        if committed_index <= self.last_applied {
            return;
        }
        let mut applied = Vec::new();
        for idx in (self.last_applied + 1)..=committed_index {
            if let Some(event) = self.pending_wal.remove(&idx) {
                self.commit_epoch.apply_entry(event.wal_entry());
                applied.push(event);
            }
        }
        if !applied.is_empty() {
            let batch_len = applied.len() as u64;
            let batch = CommittedBatch::new(self.partition_id.clone(), applied);
            let result = self.apply_loop.process_batch(batch);
            let late_events = self.apply_loop.late_event_counters();
            self.apply_telemetry.record(&result, late_events);
            self.applied_events_total = self.applied_events_total.saturating_add(batch_len);
        }
        if committed_index > 0 {
            let replica = if self.leader_id.is_empty() {
                self.local_replica.clone()
            } else {
                self.leader_id.clone()
            };
            let _ = self.durability.record_ack(AckRecord {
                replica: ReplicaId::new(replica),
                term: self.term,
                index: committed_index,
                segment_seq: committed_index,
                io_mode: self.io_mode(),
            });
        }
        self.last_applied = committed_index;
        self.persist_raft_state();
        self.refresh_commit_epoch_ticker();
    }

    fn refresh_strict_state(&mut self) {
        #[cfg(feature = "clustor-net")]
        let has_replicator = self.replicator.is_some();
        #[cfg(not(feature = "clustor-net"))]
        let has_replicator = false;
        let next = if !has_replicator {
            StrictFallbackState::Healthy
        } else {
            match self.durability.latest_proof() {
                Some(proof) if proof.index > 0 => StrictFallbackState::ProofPublished,
                _ => StrictFallbackState::LocalOnly,
            }
        };
        if next != self.strict_state {
            self.strict_state = next;
            self.persist_raft_state();
        }
    }

    #[allow(dead_code)]
    fn set_flow_snapshot(&mut self, flow: FlowDecision) {
        self.flow_override = Some(flow.clone());
        self.last_flow = flow;
    }

    fn persist_raft_state(&self) {
        persist_raft_state_file(
            &self.raft_state_path,
            self.term,
            &self.leader_id,
            self.voted_for.clone(),
            self.last_applied,
        );
    }

    fn pending_entries(&self) -> u64 {
        self.durability.pending_entries()
    }

    fn apply_telemetry(&self) -> ApplyTelemetry {
        self.apply_telemetry.clone()
    }

    fn durability_metrics(&self) -> PartitionDurabilityMetrics {
        let quorum_status = self.durability.status();
        let last_log_index = self.next_index.load(Ordering::SeqCst);
        let peer_match = self
            .replicator
            .as_ref()
            .map(|replicator| {
                let mut pairs: Vec<(String, u64)> = replicator.peer_matches().into_iter().collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs
            })
            .unwrap_or_default();
        PartitionDurabilityMetrics {
            partition_id: self.partition_id.clone(),
            quorum_pending: self.durability.pending_entries(),
            apply_pending: last_log_index.saturating_sub(self.last_applied),
            last_applied: self.last_applied,
            last_log_index,
            committed_index: quorum_status.committed_index,
            strict_state: self.strict_state,
            term: self.term,
            leader_known: !self.leader_id.is_empty(),
            heartbeat_failures: self.heartbeat_failures,
            leader: self.leader_id.clone(),
            peer_match,
        }
    }

    fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.durability_mode = mode;
    }

    fn io_mode(&self) -> IoMode {
        match self.durability_mode {
            DurabilityMode::Strict => IoMode::Strict,
            DurabilityMode::Relaxed => IoMode::Group,
        }
    }

    fn restore_committed_index(&mut self, last_index: u64) {
        if last_index == 0 {
            return;
        }
        let current = self.next_index.load(Ordering::SeqCst);
        if last_index >= current {
            self.next_index.store(last_index, Ordering::SeqCst);
            if let Err(err) = self.record_local_ack(last_index, last_index) {
                eprintln!("clustor: failed to replay durability for partition: {err}");
            } else {
                self.apply_committed(last_index);
            }
        }
    }

    fn replay_wal(&mut self) -> Result<()> {
        if !self.wal_path.exists() {
            return Ok(());
        }
        let segment = WalSegmentRef {
            seq: 1,
            log_path: self.wal_path.clone(),
            index_path: None,
        };
        let replay = WalReplayScanner::scan(&[segment])?;
        if let Some(trunc) = &replay.truncation {
            replay.enforce_truncation().with_context(|| {
                format!(
                    "failed to truncate corrupted WAL at {}",
                    trunc.path.display()
                )
            })?;
        }
        let commit_limit = self.last_applied;
        let mut max_index = 0u64;
        let mut replay_batch = Vec::new();
        let mut replay_tick_base = None;
        let mut last_tick = None;
        for frame in replay.frames {
            let metadata: WalFrameMetadata =
                serde_json::from_slice(&frame.metadata).with_context(|| {
                    format!(
                        "failed to decode WAL metadata for partition {}",
                        self.partition_id
                    )
                })?;
            let commit_tick = Self::replay_commit_tick(
                &mut replay_tick_base,
                &mut last_tick,
                frame.header.timestamp_ms,
            );
            let wal_entry = WalEntry::new(frame.header.index, commit_tick, frame.payload.clone());
            let event = CommittedEvent::new(
                self.partition_id.clone(),
                metadata.event_id,
                metadata.partition_key,
                wal_entry,
            );
            let event =
                self.attach_builtin_measurements(event, &metadata.schema_key, frame.payload.len());
            max_index = max_index.max(frame.header.index);
            self.commit_epoch.apply_entry(event.wal_entry());
            self.record_local_ack(frame.header.index, frame.header.index)
                .with_context(|| {
                    format!(
                        "durability replay failed for partition {} at index {}",
                        self.partition_id, frame.header.index
                    )
                })?;
            if frame.header.index <= commit_limit {
                replay_batch.push(event);
            } else {
                self.pending_wal.insert(frame.header.index, event);
            }
        }
        if !replay_batch.is_empty() {
            let batch = CommittedBatch::new(self.partition_id.clone(), replay_batch);
            let result = self.apply_loop.process_batch(batch);
            let late_events = self.apply_loop.late_event_counters();
            self.apply_telemetry.record(&result, late_events);
            self.applied_events_total = self
                .applied_events_total
                .saturating_add(result.stats.total_events as u64);
            self.last_applied = commit_limit.max(self.last_applied);
        }
        self.next_index.store(
            max_index.max(self.next_index.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );
        self.refresh_commit_epoch_ticker();
        Ok(())
    }
}

/// Running CEPtra node surfaces wired to the Clustor core.
pub struct CeptraRuntime {
    admin_ctx: AdminRequestContext,
    admin: Arc<Mutex<AdminService>>,
    #[cfg(feature = "clustor-net")]
    admin_client: Option<AdminHttpClient>,
    placements: Arc<Mutex<PlacementState>>,
    ready_gate: Arc<Mutex<ReadyGate>>,
    #[allow(dead_code)]
    hot_reload_gate: HotReloadGate,
    ingest_surface: Arc<ClustorIngestSurface>,
    warmup_records: Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    feature_state: Arc<Mutex<FeatureState>>,
    feed_health: Arc<Mutex<FeedHealth>>,
    system_logs: Arc<Mutex<Vec<SystemLogEntry>>>,
    compaction_states: Arc<Mutex<HashMap<String, CompactionState>>>,
    #[allow(dead_code)]
    warmup_publisher: Arc<Mutex<WarmupReadinessPublisher>>,
    activation_barrier: Arc<Mutex<ActivationBarrierState>>,
    clustor_readyz: Arc<Mutex<Option<ReadyzSnapshot>>>,
    raft_term: u64,
    raft_leader_id: String,
    raft_state_path: PathBuf,
    #[cfg(feature = "clustor-net")]
    security_material: Option<CepSecurityMaterial>,
    #[cfg(feature = "clustor-net")]
    #[allow(dead_code)]
    net: Option<ClustorNetHandles>,
}

impl CeptraRuntime {
    pub fn bootstrap(config: AppConfig) -> Result<Self> {
        config.validate()?;
        let now = Instant::now();
        #[cfg(feature = "clustor-net")]
        let tls_materials = Arc::new(build_tls_materials(&config)?);
        #[cfg(feature = "clustor-net")]
        let raft_client_tuning = build_raft_client_tuning(&config);
        let placement_records = initial_placements(&config, {
            #[cfg(feature = "clustor-net")]
            {
                Some(tls_materials.as_ref())
            }
            #[cfg(not(feature = "clustor-net"))]
            {
                None
            }
        })?;
        let admin_service = build_admin_service(now, &config, &placement_records)?;
        let placements = Arc::new(Mutex::new(PlacementState::new(placement_records.clone())?));
        let ready_gate = Arc::new(Mutex::new(ReadyGate::new(config.ready_threshold)));
        populate_ready_gate(&ready_gate, &placements)?;
        let peer_addrs = resolve_peer_addresses(&config, &placement_records);
        let local_replica = node_replica_id(&config);
        let ingest_surface = Arc::new(ClustorIngestSurface::new(
            placements.clone(),
            config.clustor.wal_dir.clone(),
            local_replica.clone(),
            peer_addrs,
            #[cfg(feature = "clustor-net")]
            Some(tls_materials.clone()),
            #[cfg(feature = "clustor-net")]
            Some(raft_client_tuning.clone()),
        )?);
        #[cfg(feature = "clustor-net")]
        let (raft_term, raft_leader_id) = ingest_surface.run_election();
        #[cfg(not(feature = "clustor-net"))]
        let (raft_term, raft_leader_id) = (DEFAULT_TERM, local_replica.clone());
        #[cfg(feature = "clustor-net")]
        start_heartbeat_loop(ingest_surface.clone());
        #[cfg(feature = "clustor-net")]
        start_election_loop(ingest_surface.clone());
        if let Some(mode) = &config.durability_mode {
            ingest_surface.set_global_durability_mode(mode.clone());
        }
        let hot_reload_gate = HotReloadGate::new(config.shadow_grace_window_ms);
        let warmup_records = Arc::new(Mutex::new(Vec::new()));
        let warmup_publisher = Arc::new(Mutex::new(WarmupReadinessPublisher::new(1_000)));
        #[cfg(feature = "clustor-net")]
        let tls_for_cp: Option<&TlsMaterials> = Some(tls_materials.as_ref());
        #[cfg(not(feature = "clustor-net"))]
        let tls_for_cp: Option<&TlsMaterials> = None;
        let feature_state = Arc::new(Mutex::new(initial_feature_state(&config, tls_for_cp)?));
        let feed_health = Arc::new(Mutex::new(FeedHealth::default()));
        let system_logs = Arc::new(Mutex::new(Vec::new()));
        if let Ok(mut health) = feed_health.lock() {
            health.record_placement(current_time_ms(), 0);
            health.record_feature(current_time_ms(), 0);
        }
        let compaction_states = Arc::new(Mutex::new(HashMap::new()));
        #[cfg(feature = "clustor-net")]
        let security_material = {
            let mut material = CepSecurityMaterial::new(
                config.node_spiffe.clone(),
                tls_materials.identity.certificate.clone(),
            );
            material.grant_roles(config.node_roles.iter().copied());
            material
                .audit_ingest_access(now)
                .with_context(|| "node TLS identity missing ingest role or certificate invalid")?;
            Some(material)
        };
        #[cfg(not(feature = "clustor-net"))]
        let security_material: Option<CepSecurityMaterial> = None;
        let activation_barrier = Arc::new(Mutex::new(ActivationBarrierState::default()));
        let clustor_readyz = Arc::new(Mutex::new(None));
        let admin_ctx = AdminRequestContext::new(
            SpiffeId::parse(&config.node_spiffe)
                .with_context(|| format!("invalid node SPIFFE ID {}", config.node_spiffe))?,
        );
        #[cfg(feature = "clustor-net")]
        let admin_client = AdminHttpClient::new(&config).ok();

        maybe_load_external_snapshots(SnapshotBootstrapContext {
            config: &config,
            placements: &placements,
            ingest_surface: &ingest_surface,
            ready_gate: &ready_gate,
            warmup_records: &warmup_records,
            warmup_publisher: &warmup_publisher,
            activation_barrier: &activation_barrier,
            clustor_readyz: &clustor_readyz,
            feature_state: &feature_state,
            feed_health: &feed_health,
        });
        start_snapshot_poller(SnapshotPollerContext {
            placement_path: config.placement_path.clone(),
            warmup_path: config.warmup_path.clone(),
            barrier_path: config.activation_barrier_path.clone(),
            feature_path: config.feature_manifest_path.clone(),
            feature_key: config.feature_public_key.clone(),
            interval_ms: config.snapshot_poll_interval_ms,
            placements: placements.clone(),
            ingest_surface: ingest_surface.clone(),
            ready_gate: ready_gate.clone(),
            warmup_records: warmup_records.clone(),
            warmup_publisher: warmup_publisher.clone(),
            activation_barrier: activation_barrier.clone(),
            clustor_readyz: clustor_readyz.clone(),
            feature_state: feature_state.clone(),
            feed_health: feed_health.clone(),
        });

        #[cfg(feature = "clustor-net")]
        let net_handles = {
            let placement_poller = config
                .clustor
                .control_plane_base_url
                .clone()
                .and_then(|base| {
                    start_placement_poller(PlacementPollerContext {
                        base,
                        routing_path: config.clustor.cp_routing_path.clone(),
                        feature_path: config.clustor.cp_feature_path.clone(),
                        warmup_path: config.clustor.cp_warmup_path.clone(),
                        activation_path: config.clustor.cp_activation_path.clone(),
                        cache_grace_ms: config.placement_cache_grace_ms,
                        identity: tls_materials.identity.clone(),
                        trust: tls_materials.trust.clone(),
                        trust_domain: tls_materials.trust_domain.clone(),
                        placements: placements.clone(),
                        ingest_surface: ingest_surface.clone(),
                        ready_gate: ready_gate.clone(),
                        warmup_records: warmup_records.clone(),
                        warmup_publisher: warmup_publisher.clone(),
                        clustor_readyz: clustor_readyz.clone(),
                        feature_state: feature_state.clone(),
                        feed_health: feed_health.clone(),
                        activation_barrier: activation_barrier.clone(),
                        feature_public_key: config.feature_public_key.clone(),
                    })
                });
            let mut handles = start_embedded_servers(
                &config,
                tls_materials.as_ref(),
                &placement_records,
                ingest_surface.clone(),
            )?;
            handles.placement_poller = placement_poller;
            Some(handles)
        };

        let runtime = Self {
            admin_ctx,
            admin: Arc::new(Mutex::new(admin_service)),
            placements,
            ready_gate,
            hot_reload_gate,
            ingest_surface,
            warmup_records,
            feature_state,
            feed_health,
            system_logs,
            compaction_states,
            warmup_publisher,
            activation_barrier,
            clustor_readyz,
            raft_term,
            raft_leader_id,
            raft_state_path: config.clustor.data_dir.join("raft_state.json"),
            #[cfg(feature = "clustor-net")]
            admin_client,
            #[cfg(feature = "clustor-net")]
            security_material,
            #[cfg(feature = "clustor-net")]
            net: net_handles,
        };
        runtime.persist_runtime_raft_state();
        Ok(runtime)
    }

    pub fn admin_service(&self) -> Arc<Mutex<AdminService>> {
        self.admin.clone()
    }

    pub fn ready_snapshot(&self) -> Result<CepReadyzReport> {
        let gate = self
            .ready_gate
            .lock()
            .map_err(|_| anyhow!("ready gate poisoned"))?;
        Ok(gate.evaluate())
    }

    pub fn ready_metrics(&self) -> Result<ReadyMetrics> {
        let gate = self
            .ready_gate
            .lock()
            .map_err(|_| anyhow!("ready gate poisoned"))?;
        Ok(gate.metrics())
    }

    pub fn feed_metrics(&self) -> FeedHealthSnapshot {
        let now_ms = current_time_ms();
        self.feed_health
            .lock()
            .map(|health| health.snapshot(now_ms))
            .unwrap_or_default()
    }

    pub fn render_metrics(&self) -> Result<String> {
        let mut output = String::new();
        let ready_metrics = self.ready_metrics()?;
        let feed_metrics = self.feed_metrics();
        log_feed_staleness(&feed_metrics);
        output.push_str(&ready_metrics.render_prometheus());
        output.push_str(&feed_metrics.render_prometheus());
        output.push_str(&self.ingest_surface.telemetry().render_prometheus());
        if let Ok(states) = self.compaction_states.lock() {
            output.push_str(&format!(
                "ceptra_compaction_states_total {}\n",
                states.len()
            ));
        }
        if let Ok(logs) = self.system_logs.lock() {
            output.push_str(&format!("clustor_system_logs_total {}\n", logs.len()));
        }
        ensure_ms_only_metrics(&output).context(
            "metrics export contains `_seconds` entries; Clustor metrics must use *_ms with read-only aliases",
        )?;
        Ok(output)
    }

    fn record_system_log(&self, entry: SystemLogEntry) {
        if let Ok(mut guard) = self.system_logs.lock() {
            guard.push(entry.clone());
            if guard.len() > 1_024 {
                guard.remove(0);
            }
        }
        log_system_entry(&entry);
    }

    fn record_admin_log(&self, action: &str, partition_id: &str, reason: Option<String>) {
        self.record_system_log(SystemLogEntry::AdminAuditSpill {
            action: action.to_string(),
            partition_id: partition_id.to_string(),
            reason,
        });
    }

    fn persist_runtime_raft_state(&self) {
        persist_raft_state_file(
            &self.raft_state_path,
            self.raft_term,
            &self.raft_leader_id,
            None,
            0,
        );
    }

    pub fn apply_compaction_update(
        &self,
        partition_id: &str,
        mut state: CompactionState,
        retention: Option<&RetentionPlan>,
    ) -> Result<()> {
        if let Some(plan) = retention {
            crate::raise_learner_slack_floor(&mut state, plan);
        }
        let mut guard = self
            .compaction_states
            .lock()
            .map_err(|_| anyhow!("compaction state poisoned"))?;
        guard.insert(partition_id.to_string(), state);
        Ok(())
    }

    #[cfg(feature = "clustor-net")]
    pub fn node_security(&self) -> Option<CepSecurityMaterial> {
        self.security_material.clone()
    }

    pub fn apply_admin_create(
        &self,
        request: CreatePartitionRequest,
    ) -> Result<(), AdminServiceError> {
        let mut response = None;
        #[cfg(feature = "clustor-net")]
        if let Some(client) = self.admin_client.as_ref() {
            match client.create_partition(&request) {
                Ok(resp) => response = Some(resp),
                Err(err) => {
                    eprintln!("clustor: admin RPC create_partition failed: {err}; falling back to local handler");
                }
            }
        }

        if response.is_none() {
            if !self.ingest_surface.is_cluster_leader() {
                let leader = self
                    .ingest_surface
                    .cluster_leader_hint()
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(AdminServiceError::InvalidRequest(format!(
                    "not leader for create_partition; current leader={leader}"
                )));
            }
            let mut service = self.admin.lock().map_err(|_| {
                AdminServiceError::InvalidRequest("admin service lock poisoned".into())
            })?;
            response =
                Some(service.create_partition(&self.admin_ctx, request.clone(), Instant::now())?);
        }

        let response = response
            .ok_or_else(|| AdminServiceError::InvalidRequest("admin rpc missing".into()))?;
        let placement = PlacementRecord {
            partition_id: request.partition.partition_id.clone(),
            routing_epoch: response.routing_epoch,
            lease_epoch: 1,
            members: request
                .replicas
                .iter()
                .map(|replica| replica.replica_id.clone())
                .collect(),
        };
        let mut placements = self.placements.lock().map_err(|_| {
            AdminServiceError::InvalidRequest("placement state lock poisoned".into())
        })?;
        placements
            .apply_record(placement.clone())
            .map_err(map_partition_error)?;
        drop(placements);
        self.ingest_surface
            .upsert_partition(&placement)
            .map_err(|err| AdminServiceError::InvalidRequest(err.to_string()))?;
        self.mark_partition_ready(&placement.partition_id)
            .map_err(|err| AdminServiceError::InvalidRequest(err.to_string()))?;
        self.record_admin_log("CreatePartition", &placement.partition_id, None);
        Ok(())
    }

    pub fn apply_admin_lease(
        &self,
        request: TransferLeaderRequest,
    ) -> Result<Option<ApReassignment>, AdminServiceError> {
        let mut response = None;
        #[cfg(feature = "clustor-net")]
        if let Some(client) = self.admin_client.as_ref() {
            match client.transfer_leader(&request) {
                Ok(resp) => response = Some(resp),
                Err(err) => {
                    eprintln!("clustor: admin RPC transfer_leader failed: {err}; falling back to local handler");
                }
            }
        }

        if response.is_none() {
            if !self
                .ingest_surface
                .is_local_leader_for(&request.partition_id)
            {
                let leader = self
                    .ingest_surface
                    .leader_hint(&request.partition_id)
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(AdminServiceError::InvalidRequest(format!(
                    "not leader for partition {}; current leader={leader}",
                    request.partition_id
                )));
            }
            let mut service = self.admin.lock().map_err(|_| {
                AdminServiceError::InvalidRequest("admin service lock poisoned".into())
            })?;
            response =
                Some(service.transfer_leader(&self.admin_ctx, request.clone(), Instant::now())?);
        }

        let response = response
            .ok_or_else(|| AdminServiceError::InvalidRequest("admin rpc missing".into()))?;
        if !response.accepted {
            return Ok(None);
        }
        let mut placements = self.placements.lock().map_err(|_| {
            AdminServiceError::InvalidRequest("placement state lock poisoned".into())
        })?;
        let lease_epoch = request.idempotency_key.len() as u64;
        let leader = request
            .target_replica_id
            .clone()
            .unwrap_or_else(|| request.partition_id.clone());
        let reassignment = placements
            .apply_lease(LeaseUpdate {
                partition_id: request.partition_id.clone(),
                lease_epoch,
                leader: leader.clone(),
            })
            .map_err(map_partition_error)?;
        drop(placements);
        self.ingest_surface
            .apply_lease(&request.partition_id, lease_epoch, Some(leader));
        self.record_admin_log(
            "TransferLeader",
            &request.partition_id,
            request.reason.clone(),
        );
        eprintln!(
            "event=clustor_leader_update partition_id={} lease_epoch={} leader={}",
            request.partition_id,
            lease_epoch,
            request
                .target_replica_id
                .clone()
                .unwrap_or_else(|| request.partition_id.clone())
        );
        Ok(reassignment)
    }

    pub fn apply_admin_durability(
        &self,
        request: SetDurabilityModeRequest,
    ) -> Result<(), AdminServiceError> {
        let partition_id = request.partition_id.clone();
        let target_mode = request.target_mode.clone();
        let target_mode_label = format!("{target_mode:?}");
        #[cfg(feature = "clustor-net")]
        if let Some(client) = self.admin_client.as_ref() {
            if let Err(err) = client.set_durability_mode(&request) {
                eprintln!("clustor: admin RPC set_durability_mode failed: {err}; falling back to local handler");
            } else {
                self.ingest_surface
                    .set_durability_mode(&partition_id, target_mode.clone());
                self.record_admin_log(
                    "SetDurabilityMode",
                    &partition_id,
                    Some(target_mode_label.clone()),
                );
                return Ok(());
            }
        }
        if !self
            .ingest_surface
            .is_local_leader_for(&request.partition_id)
        {
            let leader = self
                .ingest_surface
                .leader_hint(&request.partition_id)
                .unwrap_or_else(|| "unknown".to_string());
            return Err(AdminServiceError::InvalidRequest(format!(
                "not leader for partition {}; current leader={leader}",
                request.partition_id
            )));
        }
        let mut service = self
            .admin
            .lock()
            .map_err(|_| AdminServiceError::InvalidRequest("admin service lock poisoned".into()))?;
        service.set_durability_mode(&self.admin_ctx, request, Instant::now())?;
        self.ingest_surface
            .set_durability_mode(&partition_id, target_mode.clone());
        self.record_admin_log("SetDurabilityMode", &partition_id, Some(target_mode_label));
        Ok(())
    }

    pub fn apply_admin_snapshot_throttle(
        &self,
        request: SnapshotThrottleRequest,
    ) -> Result<(), AdminServiceError> {
        let throttle_reason = format!("enable={} {}", request.enable, request.reason);
        #[cfg(feature = "clustor-net")]
        if let Some(client) = self.admin_client.as_ref() {
            if client.snapshot_throttle(&request).is_ok() {
                self.record_admin_log(
                    "SnapshotThrottle",
                    &request.partition_id,
                    Some(throttle_reason.clone()),
                );
                return Ok(());
            }
            eprintln!("clustor: admin RPC snapshot_throttle failed; falling back to local handler");
        }
        let mut service = self
            .admin
            .lock()
            .map_err(|_| AdminServiceError::InvalidRequest("admin service lock poisoned".into()))?;
        service.snapshot_throttle(&self.admin_ctx, request.clone(), Instant::now())?;
        self.record_admin_log(
            "SnapshotThrottle",
            &request.partition_id,
            Some(throttle_reason),
        );
        Ok(())
    }

    pub fn apply_admin_snapshot_trigger(
        &self,
        request: SnapshotTriggerRequest,
    ) -> Result<(), AdminServiceError> {
        let trigger_reason = request.reason.clone();
        #[cfg(feature = "clustor-net")]
        if let Some(client) = self.admin_client.as_ref() {
            if client.snapshot_trigger(&request).is_ok() {
                self.record_admin_log(
                    "SnapshotTrigger",
                    &request.partition_id,
                    Some(trigger_reason.clone()),
                );
                return Ok(());
            }
            eprintln!("clustor: admin RPC snapshot_trigger failed; falling back to local handler");
        }
        let mut service = self
            .admin
            .lock()
            .map_err(|_| AdminServiceError::InvalidRequest("admin service lock poisoned".into()))?;
        service.trigger_snapshot(&self.admin_ctx, request.clone(), Instant::now())?;
        self.record_admin_log(
            "SnapshotTrigger",
            &request.partition_id,
            Some(trigger_reason),
        );
        Ok(())
    }

    pub fn append(&self, request: AppendRequest) -> Result<AppendResponse> {
        self.ingest_surface.append(request)
    }

    /// Combines Clustor warmup readiness with CEP shadow gating and partition health.
    pub fn combined_readyz(
        &self,
        probes: &[PartitionWarmupProbe],
        now_ms: u64,
    ) -> Result<CombinedReadyz> {
        let mut gate = self
            .ready_gate
            .lock()
            .map_err(|_| anyhow!("ready gate lock poisoned"))?;
        let feed_snapshot = self.feed_metrics();
        let placement_stale = feed_snapshot.placement_staleness_ms > FEED_STALENESS_WARN_MS;
        gate.set_reason(PLACEMENT_STALE_REASON, placement_stale);
        let flow = self.ingest_surface.flow_snapshot()?;
        let strict_fallback = self.ingest_surface.strict_fallback_state();
        let durability_pending = self.ingest_surface.pending_entries();
        let raft_ready = self.ingest_surface.raft_ready() && !placement_stale;
        let readiness = self
            .warmup_records
            .lock()
            .map_err(|_| anyhow!("warmup readiness lock poisoned"))?
            .clone();
        let threshold = gate.threshold();
        let barrier = {
            let barrier_state = self
                .activation_barrier
                .lock()
                .map_err(|_| anyhow!("activation barrier lock poisoned"))?;
            if let Some(err) = barrier_state.last_error.clone() {
                return Err(anyhow!("activation barrier invalid: {err}"));
            }
            barrier_state
                .barrier
                .clone()
                .unwrap_or_else(|| default_barrier_from_topology(&self.placements, threshold))
        };
        let clustor_readyz = self.current_readyz_snapshot(&readiness)?;
        if placement_stale {
            eprintln!(
                "event=clustor_feed_stale feed=placement staleness_ms={} threshold_ms={} gating_readyz=1",
                feed_snapshot.placement_staleness_ms, FEED_STALENESS_WARN_MS
            );
        }
        crate::evaluate_readyz(ReadyzEvalContext {
            hot_reload_gate: &self.hot_reload_gate,
            ready_gate: &gate,
            barrier: &barrier,
            readiness: &readiness,
            probes,
            clustor_readyz,
            flow,
            durability_state: strict_fallback,
            durability_pending,
            now_ms,
            raft_ready,
        })
        .map_err(Into::into)
    }

    fn current_readyz_snapshot(
        &self,
        readiness: &[WarmupReadinessRecord],
    ) -> Result<ReadyzSnapshot> {
        if let Ok(guard) = self.clustor_readyz.lock() {
            if let Some(snapshot) = guard.clone() {
                return Ok(snapshot);
            }
        }
        let snapshot = build_readyz_from_warmup(readiness, &self.feature_state)?;
        if let Ok(mut health) = self.feed_health.lock() {
            health.record_readyz(current_time_ms());
        }
        Ok(snapshot)
    }

    fn mark_partition_ready(&self, partition_id: &str) -> Result<()> {
        let reasons = ReadyzReasons::new();
        let mut gate = self
            .ready_gate
            .lock()
            .map_err(|_| anyhow!("ready gate poisoned"))?;
        gate.record_partition(partition_id.to_string(), true, &reasons);
        Ok(())
    }
}

pub fn run() -> Result<()> {
    let config = AppConfig::load()?;
    let runtime = CeptraRuntime::bootstrap(config)?;

    let ready = runtime.ready_snapshot()?;
    println!(
        "ceptra runtime booted with {} partition(s); ready={} reasons={:?}",
        ready.partitions.len(),
        ready.ready,
        ready.reasons
    );

    if env::var("CEPTRA_EXIT_IMMEDIATELY").is_err() {
        wait_forever();
    }
    Ok(())
}

#[cfg(feature = "clustor-net")]
fn fetch_control_plane_placements(
    config: &AppConfig,
    tls: &TlsMaterials,
) -> Result<Vec<PlacementRecord>> {
    let base = config
        .clustor
        .control_plane_base_url
        .clone()
        .ok_or_else(|| anyhow!("control-plane base URL not configured"))?;
    let transport = HttpCpTransportBuilder::new(&base)?
        .identity(tls.identity.clone())
        .trust_store(tls.trust.clone())
        .connection_pool(4)
        .build()?;
    let mtls = MtlsIdentityManager::new(
        tls.identity.certificate.clone(),
        tls.trust_domain.clone(),
        Duration::from_secs(600),
        Instant::now(),
    );
    let mut client = CpControlPlaneClient::new(
        transport,
        mtls,
        config.clustor.cp_routing_path.clone(),
        config.clustor.cp_feature_path.clone(),
    );
    let mut placements =
        CpPlacementClient::new(Duration::from_millis(config.placement_cache_grace_ms));
    client.fetch_routing_bundle(&mut placements, Instant::now())?;
    let mut records: Vec<PlacementRecord> = placements.records().into_values().collect();
    records.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
    Ok(records)
}

#[cfg(feature = "clustor-net")]
fn fetch_control_plane_feature_manifest(
    config: &AppConfig,
    tls: &TlsMaterials,
) -> Result<FeatureManifest> {
    let base = config
        .clustor
        .control_plane_base_url
        .clone()
        .ok_or_else(|| anyhow!("control-plane base URL not configured"))?;
    let transport = HttpCpTransportBuilder::new(&base)?
        .identity(tls.identity.clone())
        .trust_store(tls.trust.clone())
        .connection_pool(2)
        .build()?;
    let mtls = MtlsIdentityManager::new(
        tls.identity.certificate.clone(),
        tls.trust_domain.clone(),
        Duration::from_secs(600),
        Instant::now(),
    );
    let mut client = CpControlPlaneClient::new(
        transport,
        mtls,
        config.clustor.cp_routing_path.clone(),
        config.clustor.cp_feature_path.clone(),
    );
    client
        .fetch_feature_manifest(Instant::now())
        .map_err(Into::into)
}

#[cfg(feature = "clustor-net")]
fn fetch_control_plane_warmup<T: CpApiTransport>(
    transport: &T,
    mtls: &mut MtlsIdentityManager,
    path: &str,
) -> Result<Vec<WarmupReadinessRecord>> {
    let response = transport.get(path)?;
    mtls.verify_peer(&response.server_certificate, Instant::now())
        .map_err(anyhow::Error::from)?;
    let mut records: Vec<WarmupReadinessRecord> = serde_json::from_slice(&response.body)?;
    records.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
    Ok(records)
}

#[cfg(feature = "clustor-net")]
fn fetch_control_plane_activation<T: CpApiTransport>(
    transport: &T,
    mtls: &mut MtlsIdentityManager,
    path: &str,
) -> Result<Option<ActivationBarrier>> {
    let response = transport.get(path)?;
    mtls.verify_peer(&response.server_certificate, Instant::now())
        .map_err(anyhow::Error::from)?;
    if response.body.is_empty() {
        return Ok(None);
    }
    let wire: ActivationBarrierWire = serde_json::from_slice(&response.body)?;
    Ok(Some(ActivationBarrier {
        barrier_id: wire.barrier_id,
        bundle_id: wire.bundle_id,
        partitions: wire.partitions,
        readiness_threshold: wire.readiness_threshold,
        warmup_deadline_ms: wire.warmup_deadline_ms,
        readiness_window_ms: wire.readiness_window_ms,
    }))
}

fn initial_placements(
    config: &AppConfig,
    #[allow(unused_variables)] tls: Option<&TlsMaterials>,
) -> Result<Vec<PlacementRecord>> {
    #[cfg(feature = "clustor-net")]
    {
        let materials = tls.ok_or_else(|| anyhow!("missing TLS materials for control-plane fetch"))?;
        let _ = config
            .clustor
            .control_plane_base_url
            .as_ref()
            .ok_or_else(|| anyhow!("CEPTRA_CONTROL_PLANE_URL is required for placements"))?;
        let records = fetch_control_plane_placements(config, materials)?;
        if records.is_empty() {
            return Err(anyhow!(
                "control-plane returned an empty placement bundle; placements are required to start"
            ));
        }
        Ok(records)
    }

    #[cfg(not(feature = "clustor-net"))]
    {
        Err(anyhow!(
            "clustor-net feature is required for placement fetch; stub runtime has been removed"
        ))
    }
}

fn initial_feature_state(
    config: &AppConfig,
    #[allow(unused_variables)] tls: Option<&TlsMaterials>,
) -> Result<FeatureState> {
    #[cfg(feature = "clustor-net")]
    {
        let materials = tls
            .ok_or_else(|| anyhow!("missing TLS materials for control-plane feature fetch"))?;
        let _ = config
            .clustor
            .control_plane_base_url
            .as_ref()
            .ok_or_else(|| anyhow!("CEPTRA_CONTROL_PLANE_URL is required for feature manifest fetch"))?;
        let manifest = fetch_control_plane_feature_manifest(config, materials)?;
        let state = FeatureState::from_manifest(
            manifest,
            "control-plane",
            config.feature_public_key.as_deref(),
        )?;
        Ok(state)
    }

    #[cfg(not(feature = "clustor-net"))]
    {
        Err(anyhow!(
            "clustor-net feature is required for control-plane feature fetch; stub runtime has been removed"
        ))
    }
}

struct SnapshotBootstrapContext<'a> {
    config: &'a AppConfig,
    placements: &'a Arc<Mutex<PlacementState>>,
    ingest_surface: &'a Arc<ClustorIngestSurface>,
    ready_gate: &'a Arc<Mutex<ReadyGate>>,
    warmup_records: &'a Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    warmup_publisher: &'a Arc<Mutex<WarmupReadinessPublisher>>,
    activation_barrier: &'a Arc<Mutex<ActivationBarrierState>>,
    clustor_readyz: &'a Arc<Mutex<Option<ReadyzSnapshot>>>,
    feature_state: &'a Arc<Mutex<FeatureState>>,
    feed_health: &'a Arc<Mutex<FeedHealth>>,
}

fn maybe_load_external_snapshots(ctx: SnapshotBootstrapContext<'_>) {
    let config = ctx.config;
    if let Some(path) = &config.placement_path {
        if let Ok(records) = read_placements_from_file(path) {
            apply_placement_records(
                ctx.placements,
                ctx.ingest_surface,
                Some(ctx.ready_gate),
                records,
            );
            if let Ok(mut health) = ctx.feed_health.lock() {
                health.record_placement(current_time_ms(), 0);
            }
        }
    }
    if let Some(path) = &config.warmup_path {
        if let Ok(records) = read_warmup_from_file(path) {
            ctx.ingest_surface.replay_from_warmup(&records);
            apply_warmup_records(
                ctx.warmup_records,
                ctx.warmup_publisher,
                ctx.clustor_readyz,
                ctx.feed_health,
                ctx.feature_state,
                records,
            );
        }
    }
    if let Some(path) = &config.feature_manifest_path {
        match FeatureManifest::load(path) {
            Ok(manifest) => apply_feature_manifest(
                manifest,
                FeatureApplyContext {
                    feature_state: ctx.feature_state,
                    warmup_records: ctx.warmup_records,
                    warmup_publisher: ctx.warmup_publisher,
                    clustor_readyz: ctx.clustor_readyz,
                    feed_health: ctx.feed_health,
                    verifying_key: config.feature_public_key.as_deref(),
                    source: format!("file:{}", path.display()),
                    latency_ms: 0,
                },
            ),
            Err(err) => eprintln!(
                "clustor: failed to read feature manifest from {}: {err}",
                path.display()
            ),
        }
    }
    if let Some(path) = &config.activation_barrier_path {
        if let Ok(barrier) = read_activation_barrier_from_file(path) {
            apply_activation_barrier_state(
                barrier,
                ctx.placements,
                ctx.warmup_records,
                ctx.activation_barrier,
            );
        }
    }
}

#[cfg(feature = "clustor-net")]
struct PlacementPollerContext {
    base: String,
    routing_path: String,
    feature_path: String,
    warmup_path: String,
    activation_path: String,
    cache_grace_ms: u64,
    identity: TlsIdentity,
    trust: TlsTrustStore,
    trust_domain: String,
    placements: Arc<Mutex<PlacementState>>,
    ingest_surface: Arc<ClustorIngestSurface>,
    ready_gate: Arc<Mutex<ReadyGate>>,
    warmup_records: Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    warmup_publisher: Arc<Mutex<WarmupReadinessPublisher>>,
    clustor_readyz: Arc<Mutex<Option<ReadyzSnapshot>>>,
    feature_state: Arc<Mutex<FeatureState>>,
    feed_health: Arc<Mutex<FeedHealth>>,
    activation_barrier: Arc<Mutex<ActivationBarrierState>>,
    feature_public_key: Option<String>,
}

#[cfg(feature = "clustor-net")]
fn start_placement_poller(ctx: PlacementPollerContext) -> Option<thread::JoinHandle<()>> {
    Some(thread::spawn(move || {
        let base_backoff = Duration::from_secs(5);
        let mut backoff = base_backoff;
        let max_backoff = Duration::from_secs(60);
        let transport = match HttpCpTransportBuilder::new(&ctx.base).and_then(|builder| {
            builder
                .identity(ctx.identity.clone())
                .trust_store(ctx.trust.clone())
                .connection_pool(4)
                .build()
        }) {
            Ok(transport) => transport,
            Err(err) => {
                eprintln!("clustor: failed to start placement poller transport: {err}");
                return;
            }
        };
        let warmup_transport = HttpCpTransportBuilder::new(&ctx.base)
            .and_then(|builder| {
                builder
                    .identity(ctx.identity.clone())
                    .trust_store(ctx.trust.clone())
                    .connection_pool(2)
                    .build()
            })
            .ok();
        let activation_transport = HttpCpTransportBuilder::new(&ctx.base)
            .and_then(|builder| {
                builder
                    .identity(ctx.identity.clone())
                    .trust_store(ctx.trust.clone())
                    .connection_pool(2)
                    .build()
            })
            .ok();
        let mut client = CpControlPlaneClient::new(
            transport,
            MtlsIdentityManager::new(
                ctx.identity.certificate.clone(),
                ctx.trust_domain.clone(),
                Duration::from_secs(600),
                Instant::now(),
            ),
            ctx.routing_path.clone(),
            ctx.feature_path.clone(),
        );
        let mut snapshot_mtls = MtlsIdentityManager::new(
            ctx.identity.certificate.clone(),
            ctx.trust_domain.clone(),
            Duration::from_secs(600),
            Instant::now(),
        );
        loop {
            let mut any_succeeded = false;
            let mut cp_cache = CpPlacementClient::new(Duration::from_millis(ctx.cache_grace_ms));
            let placement_started = Instant::now();
            match client.fetch_routing_bundle(&mut cp_cache, Instant::now()) {
                Ok(()) => {
                    any_succeeded = true;
                    let records: Vec<PlacementRecord> = cp_cache.records().into_values().collect();
                    let record_count = records.len();
                    let placement_latency_ms = placement_started.elapsed().as_millis() as u64;
                    if record_count > 0 {
                        apply_placement_records(
                            &ctx.placements,
                            &ctx.ingest_surface,
                            Some(&ctx.ready_gate),
                            records,
                        );
                        if let Ok(mut health) = ctx.feed_health.lock() {
                            health.record_placement(current_time_ms(), placement_latency_ms);
                        }
                    }
                    eprintln!(
                        "event=clustor_cp_fetch status=ok feed=placement records={} latency_ms={}",
                        record_count, placement_latency_ms
                    );
                }
                Err(err) => {
                    eprintln!("event=clustor_cp_fetch status=error feed=placement error={err}")
                }
            }
            let feature_started = Instant::now();
            match client.fetch_feature_manifest(Instant::now()) {
                Ok(manifest) => {
                    any_succeeded = true;
                    let feature_latency_ms = feature_started.elapsed().as_millis() as u64;
                    apply_feature_manifest(
                        manifest,
                        FeatureApplyContext {
                            feature_state: &ctx.feature_state,
                            warmup_records: &ctx.warmup_records,
                            warmup_publisher: &ctx.warmup_publisher,
                            clustor_readyz: &ctx.clustor_readyz,
                            feed_health: &ctx.feed_health,
                            verifying_key: ctx.feature_public_key.as_deref(),
                            source: "control-plane".to_string(),
                            latency_ms: feature_latency_ms,
                        },
                    );
                    eprintln!(
                        "event=clustor_cp_fetch status=ok feed=feature_manifest latency_ms={feature_latency_ms}"
                    );
                }
                Err(err) => eprintln!(
                    "event=clustor_cp_fetch status=error feed=feature_manifest error={err}"
                ),
            }
            if let Some(warmup_transport) = warmup_transport.as_ref() {
                let warmup_started = Instant::now();
                match fetch_control_plane_warmup(
                    warmup_transport,
                    &mut snapshot_mtls,
                    &ctx.warmup_path,
                ) {
                    Ok(records) => {
                        any_succeeded = true;
                        let latency_ms = warmup_started.elapsed().as_millis() as u64;
                        apply_warmup_records(
                            &ctx.warmup_records,
                            &ctx.warmup_publisher,
                            &ctx.clustor_readyz,
                            &ctx.feed_health,
                            &ctx.feature_state,
                            records,
                        );
                        if let Ok(mut health) = ctx.feed_health.lock() {
                            health.record_warmup(current_time_ms(), latency_ms);
                        }
                        eprintln!(
                            "event=clustor_cp_fetch status=ok feed=warmup latency_ms={latency_ms}"
                        );
                    }
                    Err(err) => eprintln!(
                        "event=clustor_cp_fetch status=error feed=warmup error={err}"
                    ),
                }
            }
            if let Some(activation_transport) = activation_transport.as_ref() {
                let activation_started = Instant::now();
                match fetch_control_plane_activation(
                    activation_transport,
                    &mut snapshot_mtls,
                    &ctx.activation_path,
                ) {
                    Ok(Some(barrier)) => {
                        any_succeeded = true;
                        apply_activation_barrier_state(
                            barrier,
                            &ctx.placements,
                            &ctx.warmup_records,
                            &ctx.activation_barrier,
                        );
                        let latency_ms = activation_started.elapsed().as_millis() as u64;
                        eprintln!(
                            "event=clustor_cp_fetch status=ok feed=activation_barrier latency_ms={latency_ms}"
                        );
                    }
                    Ok(None) => {}
                    Err(err) => eprintln!(
                        "event=clustor_cp_fetch status=error feed=activation_barrier error={err}"
                    ),
                }
            }
            backoff = if any_succeeded {
                base_backoff
            } else {
                (backoff + base_backoff).min(max_backoff)
            };
            thread::sleep(backoff);
        }
    }))
}

struct SnapshotPollerContext {
    placement_path: Option<PathBuf>,
    warmup_path: Option<PathBuf>,
    barrier_path: Option<PathBuf>,
    feature_path: Option<PathBuf>,
    feature_key: Option<String>,
    interval_ms: u64,
    placements: Arc<Mutex<PlacementState>>,
    ingest_surface: Arc<ClustorIngestSurface>,
    ready_gate: Arc<Mutex<ReadyGate>>,
    warmup_records: Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    warmup_publisher: Arc<Mutex<WarmupReadinessPublisher>>,
    activation_barrier: Arc<Mutex<ActivationBarrierState>>,
    clustor_readyz: Arc<Mutex<Option<ReadyzSnapshot>>>,
    feature_state: Arc<Mutex<FeatureState>>,
    feed_health: Arc<Mutex<FeedHealth>>,
}

fn start_snapshot_poller(ctx: SnapshotPollerContext) {
    if ctx.placement_path.is_none()
        && ctx.warmup_path.is_none()
        && ctx.barrier_path.is_none()
        && ctx.feature_path.is_none()
    {
        return;
    }
    let interval = Duration::from_millis(ctx.interval_ms.max(500));
    thread::spawn(move || loop {
        if let Some(path) = &ctx.placement_path {
            if let Ok(records) = read_placements_from_file(path) {
                apply_placement_records(
                    &ctx.placements,
                    &ctx.ingest_surface,
                    Some(&ctx.ready_gate),
                    records,
                );
                if let Ok(mut health) = ctx.feed_health.lock() {
                    health.record_placement(current_time_ms(), 0);
                }
            }
        }
        if let Some(path) = &ctx.warmup_path {
            if let Ok(records) = read_warmup_from_file(path) {
                ctx.ingest_surface.replay_from_warmup(&records);
                apply_warmup_records(
                    &ctx.warmup_records,
                    &ctx.warmup_publisher,
                    &ctx.clustor_readyz,
                    &ctx.feed_health,
                    &ctx.feature_state,
                    records,
                );
            }
        }
        if let Some(path) = &ctx.feature_path {
            match FeatureManifest::load(path) {
                Ok(manifest) => apply_feature_manifest(
                    manifest,
                    FeatureApplyContext {
                        feature_state: &ctx.feature_state,
                        warmup_records: &ctx.warmup_records,
                        warmup_publisher: &ctx.warmup_publisher,
                        clustor_readyz: &ctx.clustor_readyz,
                        feed_health: &ctx.feed_health,
                        verifying_key: ctx.feature_key.as_deref(),
                        source: format!("file:{}", path.display()),
                        latency_ms: 0,
                    },
                ),
                Err(err) => eprintln!(
                    "clustor: failed to read feature manifest from {}: {err}",
                    path.display()
                ),
            }
        }
        if let Some(path) = &ctx.barrier_path {
            if let Ok(barrier) = read_activation_barrier_from_file(path) {
                apply_activation_barrier_state(
                    barrier,
                    &ctx.placements,
                    &ctx.warmup_records,
                    &ctx.activation_barrier,
                );
            }
        }
        thread::sleep(interval);
    });
}

#[cfg(feature = "clustor-net")]
fn start_heartbeat_loop(ingest_surface: Arc<ClustorIngestSurface>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(2));
        if let Ok(mut partitions) = ingest_surface.partitions.lock() {
            for runtime in partitions.values_mut() {
                let Some(replicator) = runtime.replicator.as_ref() else {
                    continue;
                };
                if !runtime.is_local_leader() {
                    continue;
                }
                let request = AppendEntriesRequest {
                    term: runtime.term,
                    leader_id: runtime.leader_id.clone(),
                    prev_log_index: runtime.last_applied,
                    prev_log_term: runtime.term,
                    leader_commit: runtime.last_applied,
                    entries: Vec::new(),
                };
                let mut any_success = false;
                for (peer, resp) in replicator.replicate(&request) {
                    match resp {
                        Ok(r) if r.success => {
                            any_success = true;
                            if r.term > runtime.term {
                                runtime.term = r.term;
                                runtime.persist_raft_state();
                            }
                            if let Some(replicator) = runtime.replicator.as_ref() {
                                replicator.record_match(&peer, r.match_index);
                            }
                        }
                        Ok(r) => {
                            eprintln!(
                                "event=clustor_heartbeat_failed partition_id={} peer={} match_index={}",
                                runtime.partition_id, peer, r.match_index
                            );
                            if let Some(replicator) = runtime.replicator.as_ref() {
                                replicator.record_conflict(&peer, r.conflict_index.unwrap_or(0));
                            }
                        }
                        Err(err) => eprintln!(
                            "event=clustor_heartbeat_failed partition_id={} peer={} error={}",
                            runtime.partition_id, peer, err
                        ),
                    }
                }
                if any_success {
                    runtime.heartbeat_failures = 0;
                } else {
                    runtime.heartbeat_failures = runtime.heartbeat_failures.saturating_add(1);
                    if runtime.heartbeat_failures >= 3 {
                        eprintln!(
                            "event=clustor_leader_cleared partition_id={} failures={}",
                            runtime.partition_id, runtime.heartbeat_failures
                        );
                        runtime.leader_id.clear();
                        runtime.persist_raft_state();
                    }
                }
            }
        }
    });
}

#[cfg(feature = "clustor-net")]
fn start_election_loop(ingest_surface: Arc<ClustorIngestSurface>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(5));
        let needs_election = {
            if let Ok(partitions) = ingest_surface.partitions.lock() {
                partitions.values().any(|p| {
                    !p.is_local_leader() || p.leader_id.is_empty() || p.heartbeat_failures >= 3
                })
            } else {
                false
            }
        };
        if needs_election {
            let _ = ingest_surface.run_election();
        }
    });
}

fn apply_placement_records(
    placements: &Arc<Mutex<PlacementState>>,
    ingest_surface: &Arc<ClustorIngestSurface>,
    ready_gate: Option<&Arc<Mutex<ReadyGate>>>,
    records: Vec<PlacementRecord>,
) {
    let mut applied = false;
    if let Ok(mut guard) = placements.lock() {
        for record in records {
            if guard.apply_record(record.clone()).is_ok() {
                let _ = ingest_surface.upsert_partition(&record);
                applied = true;
            }
        }
    }
    if applied {
        if let Some(gate) = ready_gate {
            let _ = populate_ready_gate(gate, placements);
        }
    }
}

fn apply_warmup_records(
    warmup_records: &Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    warmup_publisher: &Arc<Mutex<WarmupReadinessPublisher>>,
    clustor_readyz: &Arc<Mutex<Option<ReadyzSnapshot>>>,
    feed_health: &Arc<Mutex<FeedHealth>>,
    feature_state: &Arc<Mutex<FeatureState>>,
    records: Vec<WarmupReadinessRecord>,
) {
    if let Ok(mut guard) = warmup_records.lock() {
        *guard = records.clone();
    }
    if let Ok(mut health) = feed_health.lock() {
        health.record_warmup(current_time_ms(), 0);
    }
    publish_readyz_snapshot(
        &records,
        warmup_publisher,
        clustor_readyz,
        feed_health,
        feature_state,
    );
}

fn publish_readyz_snapshot(
    records: &[WarmupReadinessRecord],
    warmup_publisher: &Arc<Mutex<WarmupReadinessPublisher>>,
    clustor_readyz: &Arc<Mutex<Option<ReadyzSnapshot>>>,
    feed_health: &Arc<Mutex<FeedHealth>>,
    feature_state: &Arc<Mutex<FeatureState>>,
) {
    if let Ok(mut publisher) = warmup_publisher.lock() {
        for record in records {
            publisher.upsert(record.clone());
        }
        let now_ms = current_time_ms();
        let snapshot = publisher.snapshot(now_ms);
        let ratios: HashMap<String, f64> = records
            .iter()
            .map(|record| (record.partition_id.clone(), record.warmup_ready_ratio))
            .collect();
        let feature = feature_snapshot(feature_state);
        if let Ok(readyz) = readyz_from_warmup_snapshot(
            &snapshot,
            &ratios,
            &HashMap::new(),
            &feature.matrix,
            feature.manifest_digest,
            Vec::new(),
        ) {
            if let Ok(mut guard) = clustor_readyz.lock() {
                *guard = Some(readyz);
            }
            if let Ok(mut health) = feed_health.lock() {
                health.record_readyz(now_ms);
            }
        }
    }
}

fn read_placements_from_file(path: &Path) -> Result<Vec<PlacementRecord>> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("failed to read placements from {}", path.display()))?;
    let records: Vec<PlacementRecord> = serde_json::from_str(&payload)
        .with_context(|| format!("failed to parse placements JSON {}", path.display()))?;
    Ok(records)
}

fn read_warmup_from_file(path: &Path) -> Result<Vec<WarmupReadinessRecord>> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("failed to read warmup readiness from {}", path.display()))?;
    let records: Vec<WarmupReadinessRecord> = serde_json::from_str(&payload)
        .with_context(|| format!("failed to parse warmup readiness JSON {}", path.display()))?;
    Ok(records)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ActivationBarrierWire {
    barrier_id: String,
    bundle_id: String,
    partitions: Vec<String>,
    readiness_threshold: f64,
    warmup_deadline_ms: u64,
    readiness_window_ms: u64,
}

fn read_activation_barrier_from_file(path: &Path) -> Result<ActivationBarrier> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("failed to read activation barrier from {}", path.display()))?;
    let wire: ActivationBarrierWire = serde_json::from_str(&payload)
        .with_context(|| format!("failed to parse activation barrier JSON {}", path.display()))?;
    Ok(ActivationBarrier {
        barrier_id: wire.barrier_id,
        bundle_id: wire.bundle_id,
        partitions: wire.partitions,
        readiness_threshold: wire.readiness_threshold,
        warmup_deadline_ms: wire.warmup_deadline_ms,
        readiness_window_ms: wire.readiness_window_ms,
    })
}

fn build_readyz_from_warmup(
    readiness: &[WarmupReadinessRecord],
    feature_state: &Arc<Mutex<FeatureState>>,
) -> Result<ReadyzSnapshot> {
    let snapshot = WarmupReadinessSnapshot {
        records: readiness.to_vec(),
        publish_period_ms: 1_000,
        skipped_publications_total: 0,
    };
    let ratios: HashMap<String, f64> = readiness
        .iter()
        .map(|record| (record.partition_id.clone(), record.warmup_ready_ratio))
        .collect();
    let feature = feature_snapshot(feature_state);
    readyz_from_warmup_snapshot(
        &snapshot,
        &ratios,
        &HashMap::new(),
        &feature.matrix,
        feature.manifest_digest,
        Vec::new(),
    )
    .map_err(Into::into)
}

#[derive(Debug, Default, Clone)]
struct FeedHealth {
    placement_updated_ms: u64,
    placement_latency_ms: u64,
    warmup_updated_ms: u64,
    warmup_latency_ms: u64,
    feature_updated_ms: u64,
    feature_latency_ms: u64,
    readyz_published_ms: u64,
}

impl FeedHealth {
    fn record_placement(&mut self, updated_ms: u64, latency_ms: u64) {
        self.placement_updated_ms = updated_ms;
        self.placement_latency_ms = latency_ms;
    }

    fn record_warmup(&mut self, updated_ms: u64, latency_ms: u64) {
        self.warmup_updated_ms = updated_ms;
        self.warmup_latency_ms = latency_ms;
    }

    fn record_feature(&mut self, updated_ms: u64, latency_ms: u64) {
        self.feature_updated_ms = updated_ms;
        self.feature_latency_ms = latency_ms;
    }

    fn record_readyz(&mut self, updated_ms: u64) {
        self.readyz_published_ms = updated_ms;
    }

    fn snapshot(&self, now_ms: u64) -> FeedHealthSnapshot {
        FeedHealthSnapshot {
            placement_staleness_ms: staleness(now_ms, self.placement_updated_ms),
            placement_latency_ms: self.placement_latency_ms,
            warmup_staleness_ms: staleness(now_ms, self.warmup_updated_ms),
            warmup_latency_ms: self.warmup_latency_ms,
            feature_staleness_ms: staleness(now_ms, self.feature_updated_ms),
            feature_latency_ms: self.feature_latency_ms,
            readyz_staleness_ms: staleness(now_ms, self.readyz_published_ms),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FeedHealthSnapshot {
    placement_staleness_ms: u64,
    placement_latency_ms: u64,
    warmup_staleness_ms: u64,
    warmup_latency_ms: u64,
    feature_staleness_ms: u64,
    feature_latency_ms: u64,
    readyz_staleness_ms: u64,
}

impl FeedHealthSnapshot {
    fn render_prometheus(&self) -> String {
        format!(
            "clustor_feed_staleness_ms{{feed=\"placement\"}} {}\nclustor_feed_latency_ms{{feed=\"placement\"}} {}\nclustor_feed_staleness_ms{{feed=\"warmup\"}} {}\nclustor_feed_latency_ms{{feed=\"warmup\"}} {}\nclustor_feed_staleness_ms{{feed=\"feature_manifest\"}} {}\nclustor_feed_latency_ms{{feed=\"feature_manifest\"}} {}\nclustor_feed_staleness_ms{{feed=\"readyz_publish\"}} {}\n",
            self.placement_staleness_ms,
            self.placement_latency_ms,
            self.warmup_staleness_ms,
            self.warmup_latency_ms,
            self.feature_staleness_ms,
            self.feature_latency_ms,
            self.readyz_staleness_ms
        )
    }
}

fn log_feed_staleness(snapshot: &FeedHealthSnapshot) {
    let feeds = [
        ("placement", snapshot.placement_staleness_ms),
        ("warmup", snapshot.warmup_staleness_ms),
        ("feature_manifest", snapshot.feature_staleness_ms),
        ("readyz_publish", snapshot.readyz_staleness_ms),
    ];
    for (feed, staleness) in feeds {
        if staleness > FEED_STALENESS_WARN_MS {
            eprintln!(
                "event=clustor_feed_stale feed={} staleness_ms={} threshold_ms={}",
                feed, staleness, FEED_STALENESS_WARN_MS
            );
        }
    }
}

fn log_system_entry(entry: &SystemLogEntry) {
    match entry {
        SystemLogEntry::AdminAuditSpill {
            action,
            partition_id,
            reason,
        } => eprintln!(
            "event=clustor_system_log kind=admin_audit action={} partition_id={} reason={}",
            action,
            partition_id,
            reason.as_deref().unwrap_or("none")
        ),
        SystemLogEntry::MembershipChange {
            new_members,
            routing_epoch,
            ..
        } => eprintln!(
            "event=clustor_system_log kind=membership routing_epoch={} members={}",
            routing_epoch,
            new_members.join(",")
        ),
        SystemLogEntry::DurabilityTransition {
            from_mode,
            to_mode,
            durability_mode_epoch,
            ..
        } => eprintln!(
            "event=clustor_system_log kind=durability from_mode={} to_mode={} epoch={}",
            from_mode, to_mode, durability_mode_epoch
        ),
        other => eprintln!("event=clustor_system_log kind=other entry={other:?}"),
    }
}

fn default_barrier_from_topology(
    placements: &Arc<Mutex<PlacementState>>,
    threshold: f64,
) -> ActivationBarrier {
    let partitions = placements
        .lock()
        .ok()
        .map(|guard| {
            guard
                .topology()
                .placements()
                .iter()
                .map(|p| p.partition_id().to_string())
                .collect()
        })
        .unwrap_or_default();
    ActivationBarrier {
        barrier_id: "ceptra-local-barrier".to_string(),
        bundle_id: "default".to_string(),
        partitions,
        readiness_threshold: threshold,
        warmup_deadline_ms: u64::MAX,
        readiness_window_ms: 0,
    }
}

#[derive(Debug, Default)]
struct ActivationBarrierState {
    barrier: Option<ActivationBarrier>,
    last_error: Option<String>,
}

impl ActivationBarrierState {
    fn set_barrier(&mut self, barrier: ActivationBarrier) {
        self.barrier = Some(barrier);
        self.last_error = None;
    }

    fn record_error(&mut self, error: impl Into<String>) {
        self.last_error = Some(error.into());
    }
}

fn validate_activation_barrier(
    barrier: &ActivationBarrier,
    placements: &Arc<Mutex<PlacementState>>,
    readiness: &[WarmupReadinessRecord],
) -> Result<()> {
    if barrier.partitions.is_empty() {
        return Err(anyhow!(
            "activation barrier {} has no partitions configured",
            barrier.barrier_id
        ));
    }
    if barrier.readiness_threshold.is_nan()
        || barrier.readiness_threshold < 0.0
        || barrier.readiness_threshold > 1.0
    {
        return Err(anyhow!(
            "activation barrier {} has an invalid readiness threshold {:.3}",
            barrier.barrier_id,
            barrier.readiness_threshold
        ));
    }
    if barrier.warmup_deadline_ms == 0 {
        return Err(anyhow!(
            "activation barrier {} has a zero warmup deadline",
            barrier.barrier_id
        ));
    }

    let placements_guard = placements
        .lock()
        .map_err(|_| anyhow!("placement state poisoned while validating barrier"))?;
    let topology = placements_guard.topology();
    let mut missing = Vec::new();
    for partition in &barrier.partitions {
        let exists = topology
            .placements()
            .iter()
            .any(|p| p.partition_id() == partition);
        if !exists {
            missing.push(partition.clone());
        }
    }
    if !missing.is_empty() {
        return Err(anyhow!(
            "activation barrier {} references unknown partitions: {}",
            barrier.barrier_id,
            missing.join(",")
        ));
    }
    if !readiness.is_empty() {
        let mismatched: Vec<_> = barrier
            .partitions
            .iter()
            .filter(|partition| {
                readiness.iter().all(|record| {
                    record.partition_id != **partition || record.bundle_id != barrier.bundle_id
                })
            })
            .cloned()
            .collect();
        if !mismatched.is_empty() {
            return Err(anyhow!(
                "activation barrier {} bundle {} missing readiness records for partitions: {}",
                barrier.barrier_id,
                barrier.bundle_id,
                mismatched.join(",")
            ));
        }
    }
    Ok(())
}

fn apply_activation_barrier_state(
    barrier: ActivationBarrier,
    placements: &Arc<Mutex<PlacementState>>,
    warmup_records: &Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    activation_barrier: &Arc<Mutex<ActivationBarrierState>>,
) {
    let readiness = warmup_records
        .lock()
        .ok()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    match validate_activation_barrier(&barrier, placements, &readiness) {
        Ok(()) => {
            if let Ok(mut guard) = activation_barrier.lock() {
                guard.set_barrier(barrier);
            }
        }
        Err(err) => {
            eprintln!("clustor: activation barrier invalid: {err}");
            if let Ok(mut guard) = activation_barrier.lock() {
                guard.record_error(err.to_string());
            }
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FeatureStateSnapshot {
    matrix: FeatureCapabilityMatrix,
    manifest_digest: String,
    generated_at_ms: u64,
    source: String,
    updated_at_ms: u64,
}

#[derive(Debug)]
struct FeatureState {
    matrix: FeatureCapabilityMatrix,
    manifest_digest: String,
    generated_at_ms: u64,
    source: String,
    updated_at_ms: u64,
}

impl FeatureState {
    fn from_manifest(
        manifest: FeatureManifest,
        source: impl Into<String>,
        verifying_key: Option<&str>,
    ) -> Result<Self> {
        if let Some(key) = verifying_key {
            manifest.verify_signature(key)?;
        }
        let digest = manifest.digest()?;
        let matrix = manifest.capability_matrix()?;
        Ok(Self {
            matrix,
            manifest_digest: digest,
            generated_at_ms: manifest.generated_at_ms,
            source: source.into(),
            updated_at_ms: current_time_ms(),
        })
    }

    fn snapshot(&self) -> FeatureStateSnapshot {
        FeatureStateSnapshot {
            matrix: self.matrix.clone(),
            manifest_digest: self.manifest_digest.clone(),
            generated_at_ms: self.generated_at_ms,
            source: self.source.clone(),
            updated_at_ms: self.updated_at_ms,
        }
    }
}

fn feature_snapshot(state: &Arc<Mutex<FeatureState>>) -> FeatureStateSnapshot {
    match state.lock() {
        Ok(guard) => guard.snapshot(),
        Err(_) => default_feature_state().snapshot(),
    }
}

fn default_feature_state() -> FeatureState {
    FeatureState::from_manifest(default_feature_manifest(), "ceptra-local-default", None)
        .unwrap_or_else(|_| panic!("failed to construct default feature capability matrix"))
}

struct FeatureApplyContext<'a> {
    feature_state: &'a Arc<Mutex<FeatureState>>,
    warmup_records: &'a Arc<Mutex<Vec<WarmupReadinessRecord>>>,
    warmup_publisher: &'a Arc<Mutex<WarmupReadinessPublisher>>,
    clustor_readyz: &'a Arc<Mutex<Option<ReadyzSnapshot>>>,
    feed_health: &'a Arc<Mutex<FeedHealth>>,
    verifying_key: Option<&'a str>,
    source: String,
    latency_ms: u64,
}

fn apply_feature_manifest(manifest: FeatureManifest, ctx: FeatureApplyContext<'_>) {
    match FeatureState::from_manifest(manifest, ctx.source, ctx.verifying_key) {
        Ok(next) => {
            if let Ok(mut guard) = ctx.feature_state.lock() {
                *guard = next;
            }
            if let Ok(mut health) = ctx.feed_health.lock() {
                health.record_feature(current_time_ms(), ctx.latency_ms);
            }
            let readiness = ctx
                .warmup_records
                .lock()
                .ok()
                .map(|guard| guard.clone())
                .unwrap_or_default();
            publish_readyz_snapshot(
                &readiness,
                ctx.warmup_publisher,
                ctx.clustor_readyz,
                ctx.feed_health,
                ctx.feature_state,
            );
        }
        Err(err) => eprintln!("clustor: failed to apply feature manifest: {err}"),
    }
}

fn default_feature_manifest() -> FeatureManifest {
    let features: Vec<FeatureManifestEntry> = future_gates()
        .iter()
        .map(|descriptor| FeatureManifestEntry {
            gate: descriptor.slug.into(),
            cp_object: descriptor.cp_object.into(),
            predicate_digest: clustor_predicate_digest(descriptor.predicate),
            gate_state: FeatureGateState::Enabled,
        })
        .collect();
    FeatureManifest {
        schema_version: 1,
        generated_at_ms: 0,
        features,
        signature: String::new(),
    }
}

fn clustor_predicate_digest(predicate: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(predicate.as_bytes());
    hex::encode(hasher.finalize())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn staleness(now_ms: u64, updated_ms: u64) -> u64 {
    if updated_ms == 0 {
        now_ms
    } else {
        now_ms.saturating_sub(updated_ms)
    }
}

fn populate_ready_gate(
    ready_gate: &Arc<Mutex<ReadyGate>>,
    placements: &Arc<Mutex<PlacementState>>,
) -> Result<()> {
    let mut gate = ready_gate
        .lock()
        .map_err(|_| anyhow!("ready gate poisoned"))?;
    let placements = placements
        .lock()
        .map_err(|_| anyhow!("placement state poisoned"))?;
    let reasons = ReadyzReasons::new();
    for placement in placements.topology().placements() {
        gate.record_partition(placement.partition_id().to_string(), true, &reasons);
    }
    Ok(())
}

fn build_rbac_cache(now: Instant, principal: &str) -> Result<RbacManifestCache> {
    let mut cache = RbacManifestCache::new(Duration::from_secs(600));
    let role = RbacRole {
        name: "admin".to_string(),
        capabilities: vec![
            "CreatePartition".to_string(),
            "SetDurabilityMode".to_string(),
            "SnapshotThrottle".to_string(),
            "TransferLeader".to_string(),
            "TriggerSnapshot".to_string(),
        ],
    };
    let manifest = RbacManifest {
        roles: vec![role.clone()],
        principals: vec![RbacPrincipal {
            spiffe_id: principal.to_string(),
            role: role.name.clone(),
        }],
    };
    cache
        .load_manifest(manifest, now)
        .with_context(|| "failed to seed RBAC manifest")?;
    Ok(cache)
}

fn map_partition_error(err: PartitionError) -> AdminServiceError {
    AdminServiceError::InvalidRequest(err.to_string())
}

fn wait_forever() -> ! {
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::cel::{CelExpr, CelRule};
    use crate::ClientHints;
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Instant;
    use tempfile::tempdir;

    fn sample_request(event_id: &str) -> AppendRequest {
        AppendRequest {
            event_id: event_id.to_string(),
            partition_key: b"p1".to_vec(),
            schema_key: "schema.append".into(),
            payload: b"payload".to_vec(),
            client_hints: ClientHints::default(),
        }
    }

    #[test]
    fn wal_replay_restores_committed_entries() {
        let tmp = tempdir().unwrap();
        let wal_root = tmp.path().to_path_buf();
        // Single-replica setup ensures local durability reaches quorum in the
        // no-network test harness.
        let replicas = vec!["node-0".to_string()];
        {
            let mut runtime = PartitionRuntime::new(
                "p1",
                &replicas,
                &wal_root,
                "node-0".into(),
                HashMap::new(),
                #[cfg(feature = "clustor-net")]
                None,
                #[cfg(feature = "clustor-net")]
                None,
            )
            .expect("runtime init");
            runtime
                .append(&sample_request("evt-1"), Instant::now())
                .expect("append 1");
            runtime
                .append(&sample_request("evt-2"), Instant::now())
                .expect("append 2");
            assert_eq!(runtime.last_applied, 2);
            assert_eq!(runtime.applied_events_total, 2);
        }

        let runtime = PartitionRuntime::new(
            "p1",
            &replicas,
            &wal_root,
            "node-0".into(),
            HashMap::new(),
            #[cfg(feature = "clustor-net")]
            None,
            #[cfg(feature = "clustor-net")]
            None,
        )
        .expect("runtime replay");
        assert_eq!(runtime.last_applied, 2);
        assert_eq!(runtime.applied_events_total, 2);
        assert!(
            runtime.pending_wal.is_empty(),
            "replay should drain pending map"
        );
    }

    #[test]
    fn wal_replay_rehydrates_apply_pipeline() {
        let tmp = tempdir().unwrap();
        let wal_root = tmp.path().to_path_buf();
        // Single-replica setup ensures local durability reaches quorum in the
        // no-network test harness.
        let replicas = vec!["node-0".to_string()];
        let apply_factory = ApplyPipelineFactory::new(|| {
            let dedup = DedupTable::new(DedupTableConfig {
                client_retry_window_s: 600,
            });
            let aggregation = PaneAggregationPipeline::new(default_aggregation_config())?;
            let allowed_labels: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            let rule = CelRule::new(
                "derived",
                "schema.derived",
                CelExpr::True,
                allowed_labels,
                b"derived".to_vec(),
                "rule-1",
                1,
            )
            .expect("cel rule");
            let cel_runtime = CelSandbox::new(vec![rule]);
            Ok(ApplyLoop::with_channel_policies(
                dedup,
                Box::new(aggregation),
                Box::new(cel_runtime),
                ChannelPolicies::default(),
            ))
        });

        {
            let params = PartitionRuntimeParams {
                partition_id: "p1",
                replicas: &replicas,
                wal_root: &wal_root,
                local_replica: "node-0".into(),
                peer_addrs: HashMap::new(),
                #[cfg(feature = "clustor-net")]
                tls: None,
                #[cfg(feature = "clustor-net")]
                raft_tuning: None,
                apply_factory: &apply_factory,
            };
            let mut runtime =
                PartitionRuntime::with_apply_factory(params).expect("runtime init");
            runtime
                .append(&sample_request("evt-1"), Instant::now())
                .expect("append 1");
            runtime
                .append(&sample_request("evt-2"), Instant::now())
                .expect("append 2");
            let telemetry = runtime.apply_telemetry();
            assert!(telemetry.late_events.is_empty());
            assert_eq!(telemetry.total_events, 2);
            assert_eq!(telemetry.derived_emissions, 2);
        }

        let params = PartitionRuntimeParams {
            partition_id: "p1",
            replicas: &replicas,
            wal_root: &wal_root,
            local_replica: "node-0".into(),
            peer_addrs: HashMap::new(),
            #[cfg(feature = "clustor-net")]
            tls: None,
            #[cfg(feature = "clustor-net")]
            raft_tuning: None,
            apply_factory: &apply_factory,
        };
        let mut runtime =
            PartitionRuntime::with_apply_factory(params).expect("runtime replay");
        let replay_telemetry = runtime.apply_telemetry();
        assert!(replay_telemetry.late_events.is_empty());
        assert_eq!(replay_telemetry.total_events, 2);
        assert_eq!(replay_telemetry.derived_emissions, 2);

        runtime
            .append(&sample_request("evt-1"), Instant::now())
            .expect("append duplicate after replay");
        let after_duplicate = runtime.apply_telemetry();
        assert_eq!(after_duplicate.total_events, 3);
        assert_eq!(after_duplicate.dedup_skipped, 1);
        assert_eq!(after_duplicate.derived_emissions, 2);
    }

    #[test]
    fn append_rejected_when_not_leader() {
        let tmp = tempdir().unwrap();
        let wal_root = tmp.path().to_path_buf();
        let replicas = vec!["node-0".to_string(), "node-1".to_string()];
        let mut runtime = PartitionRuntime::new(
            "p1",
            &replicas,
            &wal_root,
            "node-0".into(),
            HashMap::new(),
            #[cfg(feature = "clustor-net")]
            None,
            #[cfg(feature = "clustor-net")]
            None,
        )
        .expect("runtime init");
        runtime.leader_id = "node-1".into();
        let err = runtime.append(&sample_request("evt-1"), Instant::now());
        assert!(err.is_err(), "follower should reject appends");
    }
}

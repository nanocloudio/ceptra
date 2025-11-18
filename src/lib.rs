//! CEPtra engine root crate built on top of the Clustor consensus core.

pub mod ack;
pub mod affinity;
pub mod backpressure;
pub mod ceptra;
pub mod client;
pub mod compaction;
pub mod config;
pub mod definitions;
pub mod event_model;
pub mod fault_tolerance;
pub mod hot_reload;
pub mod kubernetes;
pub mod lanes;
pub mod logging;
pub mod observability;
pub mod partition;
pub mod policy;
pub mod readiness;
pub mod runtime;
pub mod schema;
pub mod security;
pub mod stateful;
pub mod telemetry;
pub mod terms;
pub mod test_hooks;
pub mod threading;
pub mod wire;

/// Re-export the upstream consensus library so downstream crates can bind to
/// a single dependency surface.
pub use clustor as consensus_core;

pub use ack::{evaluate, AckDisposition, AckRejection, AckSignals, PERMANENT_DURABILITY_REASON};
pub use affinity::{DeviceSpec, NumaNodeSpec, NumaPlanError, NumaPlanner, PartitionPinning};
pub use backpressure::{
    BackpressureError, BackpressureQueue, BackpressureQueueKind, BackpressureWhy, CreditWindowHint,
    CreditWindowHintTimer, CreditWindowMetrics, CreditWindowTelemetry, CreditWindowTrace,
    PidSnapshot, QueueDepthTelemetry, COMMIT_APPLY_QUEUE_CAPACITY, EMIT_QUEUE_CAPACITY,
};
pub use ceptra::aggregation::{
    AggregationConfig, AggregationError, AggregationPipeline, AggregationRecord,
    AggregationSnapshot, AggregatorKind, LateDataPolicy, MetricBinding, MetricSpec,
    PaneAggregationPipeline, PaneRingBudget, RingSizingReport, NON_RETRACTABLE_FLAG,
};
pub use ceptra::apply::{
    ApplyBatchResult, ApplyLoop, BatchStats, ChannelPolicies, ChannelPolicy, ChannelPolicyKind,
    CommittedBatch, CommittedEvent, EmissionBatch, EmissionDropLog, EmissionDropReason,
    EventMeasurement,
};
pub use ceptra::cel::{
    CelComparisonOp, CelError, CelEvaluationRequest, CelEvaluationResult, CelExpr, CelRule,
    CelRuntime, CelSandbox,
};
pub use ceptra::derived::{DerivedEmission, DERIVED_FLAG};
pub use ceptra::time::{
    EventAdmission, LateEventAudit, LateEventReason, LatenessResult, TimeSemantics,
};
pub use client::{
    AppendError, AppendLog, AppendMetrics, AppendOptions, AppendRequest, AppendResponse,
    AppendSpan, AppendTelemetry, AppendTransport, AppendTransportError, CepAppendClient,
    CepCreditHint, ClientHints, CEP_STATUS_REASON,
};
pub use config::{ConfigError, ConfigKnobClass, ConfigPatchResult, ConfigService, ConfigTelemetry};
pub use event_model::{
    CheckpointChain, CheckpointError, CheckpointKind, CheckpointLoader, CheckpointRestoreError,
    CheckpointRestorePlan, CheckpointResult, CheckpointSink, CheckpointSkipReason,
    CheckpointWriter, CommitEpochCheckpoint, CommitEpochState, CommitEpochTicker,
    CommitRateSnapshot, CommitRateState, ControlPlaneReconciler, DedupCapacityUpdate,
    DedupDecision, DedupOutcome, DedupRetentionInputs, DedupTable, DedupTableConfig,
    FinalizedHorizon, FinalizedHorizonSnapshot, FinalizedHorizonStall, GuardFloorSnapshot,
    KeyApplyDecision, KeyApplyOutcome, KeyIdentity, KeySequencer, MetricFinalization,
    MetricWatermarkOverride, MetricWatermarkSnapshot, MetricWatermarkTelemetry, MinRetainPlanner,
    MonotonicClock, PartitionKeyHasher, PersistedCheckpoint, RetentionPlan, SystemMonotonicClock,
    WalEntry, WatermarkConfig, WatermarkDecision, WatermarkSnapshot, WatermarkTelemetry,
    WatermarkTracker, CEPTRA_AP_KEY_HASH_COLLISION_TOTAL, PERMANENT_RETRY_EXPIRED,
};
pub use fault_tolerance::{
    BackupLagTracker, BackupSchedule, BackupStatus, DisasterRecoveryPlan, DrCutoverReport,
    FailoverEvent, FailoverTelemetry, FaultToleranceError, PartitionGroupPlan, WalShipment,
    WarmStandbyDirective, WarmStandbyManager, WarmStandbyOutcome, PERMANENT_EPOCH_REASON,
};
pub use hot_reload::{
    CepShadowGateState, HotReloadDecision, HotReloadGate, PartitionWarmupProbe,
    ShadowPartitionState, SHADOW_FAILED, SHADOW_PENDING, SHADOW_READY, SHADOW_REPLAYING,
    SHADOW_TIMEOUT, SHADOW_WAITING_FOR_WAL,
};
pub use kubernetes::{validate_node_labels, AdmissionError, REQUIRED_NODE_LABELS};
pub use lanes::{
    promql_group_labels, validate_lane_domains, LaneAdmission, LaneAdmissionConfig,
    LaneAdmissionResult, LaneDomainMap, LaneDomainSpec, LaneHistogram, LaneOverflowAudit,
    LaneOverflowRecord, LaneProjection, LaneProjectionStatus, LaneValidationError,
    LaneValidationReport, MetricGrouping, LANE_MAX_PER_PARTITION, LANE_OVERFLOW_MARKER,
    LANE_WARN_THRESHOLD,
};
pub use logging::{JsonLineLogger, LogFile, LogLevel, LogRotationPolicy, LoggingError};
pub use observability::{
    HealthzStatus, MetricSample, MetricsSnapshot, ObservabilityError, ObservabilityService,
    ReadyzEnvelope,
};
pub use partition::{
    hash_partition_key, ApAssignment, ApLeaderAffinity, ApReassignment, LeaseUpdate,
    PartitionError, PartitionPlacement, PartitionRoute, PartitionTopology, RPG_REPLICA_COUNT,
};
pub use policy::{RuleVersionSelector, RuleVersionSnapshot};
pub use readiness::{
    CepReadyzPartition, CepReadyzReport, PartitionReadinessInputs, PartitionReadinessPolicy,
    PartitionReadyMetric, ReadyGate, ReadyMetrics, ReadyzReasons, RolloutAutomation,
    RolloutDecision, APPLY_LAG_REASON, BACKUP_LAG_REASON, CERTIFICATE_EXPIRY_REASON,
    CHECKPOINT_AGE_REASON, CHECKPOINT_CHAIN_DEGRADED, PARTITION_THRESHOLD_REASON,
    REPLICATION_LAG_REASON, WARMUP_PENDING_REASON, WATERMARK_STALL_REASON,
};
pub use runtime::{RoleMode, RuntimeOptions, TestMode, WarmupEstimate};
pub use schema::{
    SchemaCache, SchemaDescriptor, SchemaFormat, SchemaLookupError, SchemaRegistrationError,
    SchemaRegistry, ERR_SCHEMA_BLOCKED, ERR_UNKNOWN_SCHEMA, SCHEMA_CACHE_CAPACITY,
};
pub use security::{
    CepRole, CepSecurityMaterial, SecurityPolicyError, WorkloadAssignments, WorkloadDescriptor,
};
pub use stateful::{BootstrapController, BootstrapPlan, PodIdentity, PreStopAction};
pub use telemetry::{
    ensure_ms_only_metrics, scrape_metric_names, TelemetryCatalog, TelemetryError, TelemetryMetric,
};
pub use terms::{CeptraTerm, CEPTRA_TERMS};
pub use test_hooks::{TestHook, TestHookRegistry};
pub use threading::{
    ApplyCommand, CheckpointCommand, DeviceQueuePin, IoCommand, PartitionActivityLog,
    PartitionRole, PartitionThreadConfig, PartitionThreadLayout, PartitionThreadLayoutBuilder,
    PartitionThreadMetrics, PartitionThreadPins, PartitionWorkerFactories, RoleActivity,
    RoleActivityRecord, RoleContext, RoleExecutor, RoleHandle, RoleMetrics, RoleSendError,
    TelemetryCommand, ThreadPinReport, ThreadPinTarget, WalCommand,
};
pub use wire::{load_wire_catalog, WireCatalog, WireCatalogError};

pub use compaction::raise_learner_slack_floor;
pub use definitions::{
    validate_definition_document, AggregatePromqlOptions, BundleSection, CelRuleSpec,
    ClassifyCelOptions, CompiledPromqlQuery, DefinitionAdmission, DefinitionDiagnostics,
    DefinitionDocument, DefinitionError, EmitSpec, PolicyMetadata, PromqlQuerySpec, WorkflowPhase,
    WorkflowSection,
};

/// Path to the CEPtra manifest relative to the repository root.
pub const CEPTRA_MANIFEST_PATH: &str = "manifests/ceptra_manifest.json";

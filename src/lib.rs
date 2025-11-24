//! CEPtra engine root crate built on top of the Clustor consensus core.

pub mod client {
    pub mod ack;
    pub mod core;
    pub mod runtime;
    pub mod test_hooks;

    pub use ack::*;
    pub use core::*;
    pub use runtime::*;
    pub use test_hooks::*;
}

pub mod config {
    pub mod core;
    pub mod kubernetes;
    pub mod policy;
    pub mod schema;
    pub mod security;

    pub use core::*;
    pub use kubernetes::*;
    pub use policy::*;
    pub use schema::*;
    pub use security::*;
}

pub mod observability {
    pub mod core;
    pub mod logging;
    pub mod readiness;
    pub mod telemetry;

    pub use core::*;
    pub use logging::*;
    pub use readiness::*;
    pub use telemetry::*;
}

pub mod spec {
    pub mod definitions;
    pub mod disaster_recovery;
    pub mod manifests;
    pub mod telemetry_catalog;
    pub mod terms;
    pub mod wire;
    pub mod wire_catalog;

    pub use definitions::*;
    pub use disaster_recovery::*;
    pub use manifests::*;
    pub use telemetry_catalog::*;
    pub use terms::*;
    pub use wire::*;
    pub use wire_catalog::*;
}

pub mod app;
pub mod reliability {
    pub mod compaction;
    pub mod fault_tolerance;

    pub use compaction::*;
    pub use fault_tolerance::*;
}
pub mod runtime {
    pub mod affinity;
    pub mod backpressure;
    pub mod hot_reload;
    pub mod lanes;
    pub mod partition;
    pub mod stateful;
    pub mod threading;

    pub use affinity::*;
    pub use backpressure::*;
    pub use hot_reload::*;
    pub use lanes::*;
    pub use partition::*;
    pub use stateful::*;
    pub use threading::*;
}

/// Re-export the upstream consensus library so downstream crates can bind to
/// a single dependency surface.
pub use clustor as consensus_core;

pub use client::{
    evaluate, AckDisposition, AckRejection, AckSignals, AppendError, AppendLog, AppendMetrics,
    AppendOptions, AppendRequest, AppendResponse, AppendSpan, AppendTelemetry, AppendTransport,
    AppendTransportError, CepAppendClient, CepCreditHint, ClientHints, TestHook, TestHookRegistry,
    CEP_STATUS_REASON, PERMANENT_DURABILITY_REASON,
};
pub use engine::{
    aggregation::{
        AggregationConfig, AggregationError, AggregationPipeline, AggregationRecord,
        AggregationSnapshot, AggregatorKind, LateDataPolicy, MetricBinding, MetricSpec,
        PaneAggregationPipeline, PaneRingBudget, RingSizingReport, NON_RETRACTABLE_FLAG,
    },
    apply::{
        ApplyBatchResult, ApplyLoop, BatchStats, ChannelPolicies, ChannelPolicy, ChannelPolicyKind,
        CommittedBatch, CommittedEvent, EmissionBatch, EmissionDropLog, EmissionDropReason,
        EventMeasurement,
    },
    cel::{
        CelComparisonOp, CelError, CelEvaluationRequest, CelEvaluationResult, CelExpr, CelRule,
        CelRuntime, CelSandbox,
    },
    derived::{DerivedEmission, DERIVED_FLAG},
    time::{EventAdmission, LateEventAudit, LateEventReason, LatenessResult, TimeSemantics},
};
pub use runtime::affinity::{
    DeviceSpec, NumaNodeSpec, NumaPlanError, NumaPlanner, PartitionPinning,
};
pub use runtime::backpressure::{
    BackpressureError, BackpressureQueue, BackpressureQueueKind, BackpressureWhy, CreditWindowHint,
    CreditWindowHintTimer, CreditWindowMetrics, CreditWindowTelemetry, CreditWindowTrace,
    PidSnapshot, QueueDepthTelemetry, COMMIT_APPLY_QUEUE_CAPACITY, EMIT_QUEUE_CAPACITY,
};
pub mod engine {
    pub mod aggregation;
    pub mod apply;
    pub mod cel;
    pub mod derived;
    pub mod time;

    pub use aggregation::*;
    pub use apply::*;
    pub use cel::*;
    pub use derived::*;
    pub use time::*;
}
pub use config::{
    validate_node_labels, AdmissionError, CepRole, CepSecurityMaterial, ConfigError,
    ConfigKnobClass, ConfigPatchResult, ConfigService, ConfigTelemetry, RuleVersionSelector,
    RuleVersionSnapshot, SchemaCache, SchemaDescriptor, SchemaFormat, SchemaLookupError,
    SchemaRegistrationError, SchemaRegistry, SecurityPolicyError, WorkloadAssignments,
    WorkloadDescriptor, ERR_SCHEMA_BLOCKED, ERR_UNKNOWN_SCHEMA, REQUIRED_NODE_LABELS,
    SCHEMA_CACHE_CAPACITY,
};
pub use event::{
    checkpoint::{
        CheckpointError, CheckpointKind, CheckpointLoader, CheckpointRestoreError,
        CheckpointRestorePlan, CheckpointResult, CheckpointSink, CheckpointSkipReason,
        CheckpointWriter, PersistedCheckpoint, FULL_CHECKPOINT_ENTRY_INTERVAL,
        FULL_CHECKPOINT_INTERVAL_MS, MAX_INCREMENTALS_PER_FULL,
    },
    commit_epoch::{
        CommitEpochCheckpoint, CommitEpochState, CommitEpochTicker, MonotonicClock,
        SystemMonotonicClock, WalEntry,
    },
    commit_rate::{
        CommitRateSnapshot, CommitRateState, ControlPlaneReconciler, DedupCapacityUpdate,
    },
    dedup::{DedupDecision, DedupOutcome, DedupTable, DedupTableConfig, PERMANENT_RETRY_EXPIRED},
    finalized::{
        FinalizedHorizon, FinalizedHorizonSnapshot, FinalizedHorizonStall, MetricFinalization,
    },
    key_state::{
        KeyApplyDecision, KeyApplyOutcome, KeyIdentity, KeySequencer, PartitionKeyHasher,
        CEPTRA_AP_KEY_HASH_COLLISION_TOTAL,
    },
    retention::{CheckpointChain, DedupRetentionInputs, MinRetainPlanner, RetentionPlan},
    watermark::{
        GuardFloorSnapshot, MetricWatermarkOverride, MetricWatermarkSnapshot,
        MetricWatermarkTelemetry, WatermarkConfig, WatermarkDecision, WatermarkSnapshot,
        WatermarkTelemetry, WatermarkTracker,
    },
};
pub mod event {
    pub mod checkpoint;
    pub mod commit_epoch;
    pub mod commit_rate;
    pub mod dedup;
    pub mod finalized;
    pub mod key_state;
    pub mod retention;
    pub mod watermark;

    pub use checkpoint::*;
    pub use commit_epoch::*;
    pub use commit_rate::*;
    pub use dedup::*;
    pub use finalized::*;
    pub use key_state::*;
    pub use retention::*;
    pub use watermark::*;
}
pub use client::runtime::{RoleMode, RuntimeOptions, TestMode, WarmupEstimate};
pub use observability::{
    ensure_ms_only_metrics, scrape_metric_names, TelemetryCatalog, TelemetryError, TelemetryMetric,
};
pub use observability::{
    CepReadyzPartition, CepReadyzReport, PartitionReadinessInputs, PartitionReadinessPolicy,
    PartitionReadyMetric, ReadyGate, ReadyMetrics, ReadyzReasons, RolloutAutomation,
    RolloutDecision, APPLY_LAG_REASON, BACKUP_LAG_REASON, CERTIFICATE_EXPIRY_REASON,
    CHECKPOINT_AGE_REASON, CHECKPOINT_CHAIN_DEGRADED, PARTITION_THRESHOLD_REASON,
    REPLICATION_LAG_REASON, WARMUP_PENDING_REASON, WATERMARK_STALL_REASON,
};
pub use observability::{
    HealthzStatus, JsonLineLogger, LogFile, LogLevel, LogRotationPolicy, LoggingError,
    MetricSample, MetricsSnapshot, ObservabilityError, ObservabilityService, ReadyzEnvelope,
};
pub use reliability::fault_tolerance::{
    BackupLagTracker, BackupSchedule, BackupStatus, FailoverEvent, FailoverTelemetry,
    FaultToleranceError, PartitionGroupPlan, WalShipment, WarmStandbyDirective, WarmStandbyManager,
    WarmStandbyOutcome, PERMANENT_EPOCH_REASON,
};
pub use runtime::hot_reload::{
    CepShadowGateState, HotReloadDecision, HotReloadGate, PartitionWarmupProbe,
    ShadowPartitionState, SHADOW_FAILED, SHADOW_PENDING, SHADOW_READY, SHADOW_REPLAYING,
    SHADOW_TIMEOUT, SHADOW_WAITING_FOR_WAL,
};
pub use runtime::lanes::{
    promql_group_labels, validate_lane_domains, LaneAdmission, LaneAdmissionConfig,
    LaneAdmissionResult, LaneDomainMap, LaneDomainSpec, LaneHistogram, LaneOverflowAudit,
    LaneOverflowRecord, LaneProjection, LaneProjectionStatus, LaneValidationError,
    LaneValidationReport, MetricGrouping, LANE_MAX_PER_PARTITION, LANE_OVERFLOW_MARKER,
    LANE_WARN_THRESHOLD,
};
pub use runtime::partition::{
    hash_partition_key, ApAssignment, ApLeaderAffinity, ApReassignment, LeaseUpdate,
    PartitionError, PartitionPlacement, PartitionRoute, PartitionTopology, RPG_REPLICA_COUNT,
};
pub use runtime::stateful::{BootstrapController, BootstrapPlan, PodIdentity, PreStopAction};
pub use runtime::threading::{
    ApplyCommand, CheckpointCommand, DeviceQueuePin, IoCommand, PartitionActivityLog,
    PartitionRole, PartitionThreadConfig, PartitionThreadLayout, PartitionThreadLayoutBuilder,
    PartitionThreadMetrics, PartitionThreadPins, PartitionWorkerFactories, RoleActivity,
    RoleActivityRecord, RoleContext, RoleExecutor, RoleHandle, RoleMetrics, RoleSendError,
    TelemetryCommand, ThreadPinReport, ThreadPinTarget, WalCommand,
};
pub use spec::wire::{load_wire_catalog, WireCatalog, WireCatalogError};
pub use spec::{CeptraTerm, CEPTRA_TERMS};

pub use reliability::compaction::raise_learner_slack_floor;
pub use spec::{
    validate_definition_document, AggregatePromqlOptions, BundleSection, CelRuleSpec,
    ClassifyCelOptions, CompiledPromqlQuery, DefinitionAdmission, DefinitionDiagnostics,
    DefinitionDocument, DefinitionError, EmitSpec, PolicyMetadata, PromqlQuerySpec, WorkflowPhase,
    WorkflowSection,
};

// Module aliases to preserve previous paths for tools/tests.
pub use client::ack;
pub use client::test_hooks;
pub use config::kubernetes;
pub use config::policy;
pub use config::schema;
pub use config::security;
pub use observability::logging;
pub use observability::readiness;
pub use observability::telemetry;
pub use spec::definitions;
pub use spec::disaster_recovery;
pub use spec::manifests;
pub use spec::telemetry_catalog;
pub use spec::terms;
pub use spec::wire_catalog;

/// Path to the CEPtra manifest relative to the repository root.
pub const CEPTRA_MANIFEST_PATH: &str = "manifests/ceptra_manifest.json";

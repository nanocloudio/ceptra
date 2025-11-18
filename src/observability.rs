use crate::readiness::CepReadyzReport;
use crate::telemetry::{ensure_ms_only_metrics, TelemetryError};
use serde::Serialize;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    path: String,
    registry: String,
}

impl EndpointConfig {
    fn new(path: impl Into<String>, registry: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            registry: registry.into(),
        }
    }

    /// Endpoint path (e.g., `/metrics`).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Registry backing the endpoint.
    pub fn registry(&self) -> &str {
        &self.registry
    }
}

/// Shared-service wiring for `/metrics`, `/readyz`, `/healthz`, and OTLP traces.
#[derive(Debug)]
pub struct ObservabilityService {
    metrics: EndpointConfig,
    healthz: EndpointConfig,
    readyz: EndpointConfig,
    trace: EndpointConfig,
}

impl ObservabilityService {
    /// Creates an observability service bound to the provided registry name.
    pub fn new(registry: impl Into<String>) -> Self {
        let registry = registry.into();
        Self {
            metrics: EndpointConfig::new("/metrics", registry.clone()),
            healthz: EndpointConfig::new("/healthz", registry.clone()),
            readyz: EndpointConfig::new("/readyz", registry.clone()),
            trace: EndpointConfig::new("/trace", registry.clone()),
        }
    }

    /// Returns the shared registry identifier.
    pub fn registry(&self) -> &str {
        self.metrics.registry()
    }

    /// `/metrics` endpoint configuration.
    pub fn metrics_endpoint(&self) -> &EndpointConfig {
        &self.metrics
    }

    /// `/healthz` endpoint configuration.
    pub fn healthz_endpoint(&self) -> &EndpointConfig {
        &self.healthz
    }

    /// `/readyz` endpoint configuration.
    pub fn readyz_endpoint(&self) -> &EndpointConfig {
        &self.readyz
    }

    /// OTLP trace endpoint configuration (`/trace`).
    pub fn otlp_endpoint(&self) -> &EndpointConfig {
        &self.trace
    }

    /// Validates Prometheus exposition for `_ms`-only metrics.
    pub fn validate_metrics(&self, exposition: &str) -> Result<(), ObservabilityError> {
        ensure_ms_only_metrics(exposition).map_err(ObservabilityError::from)
    }

    /// Renders the liveness status served at `/healthz`.
    pub fn healthz_status(&self, healthy: bool) -> HealthzStatus {
        HealthzStatus {
            healthy,
            registry: self.healthz.registry().to_string(),
        }
    }

    /// Wraps the CEP-specific readiness payload into the published envelope.
    pub fn readyz_envelope(&self, report: CepReadyzReport) -> ReadyzEnvelope {
        ReadyzEnvelope {
            registry: self.readyz.registry().to_string(),
            ceptra: report,
        }
    }

    /// Implements the `TEST/ExportMetricsSnapshot` control endpoint.
    pub fn test_export_metrics_snapshot(
        &self,
        snapshot: &MetricsSnapshot,
    ) -> Result<String, ObservabilityError> {
        if snapshot.registry != self.registry() {
            return Err(ObservabilityError::RegistryMismatch {
                expected: self.registry().to_string(),
                observed: snapshot.registry.clone(),
            });
        }
        serde_json::to_string_pretty(snapshot)
            .map_err(|source| ObservabilityError::SnapshotExport { source })
    }
}

/// Liveness response served at `/healthz`.
#[derive(Debug, Clone, Serialize)]
pub struct HealthzStatus {
    pub healthy: bool,
    pub registry: String,
}

/// Ready payload that embeds the CEP-specific readiness section.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyzEnvelope {
    pub registry: String,
    #[serde(flatten)]
    pub ceptra: CepReadyzReport,
}

/// Deterministic snapshot exported through `TEST/ExportMetricsSnapshot`.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub registry: String,
    pub timestamp_ms: u64,
    pub metrics: Vec<MetricSample>,
}

impl MetricsSnapshot {
    /// Creates an empty snapshot for the provided registry.
    pub fn new(registry: impl Into<String>, timestamp_ms: u64) -> Self {
        Self {
            registry: registry.into(),
            timestamp_ms,
            metrics: Vec::new(),
        }
    }

    /// Records a metric without labels.
    pub fn record_metric(
        &mut self,
        name: impl Into<String>,
        value: u64,
    ) -> Result<(), ObservabilityError> {
        self.record_metric_with_labels(name, value, None)
    }

    /// Records a metric with optional labels while enforcing `_ms`-only names.
    pub fn record_metric_with_labels(
        &mut self,
        name: impl Into<String>,
        value: u64,
        labels: Option<BTreeMap<String, String>>,
    ) -> Result<(), ObservabilityError> {
        let name = name.into();
        if name.ends_with("_seconds") {
            return Err(ObservabilityError::InvalidMetric { name });
        }
        self.metrics.push(MetricSample {
            name,
            value,
            labels,
        });
        Ok(())
    }
}

/// Metric entry recorded within a snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct MetricSample {
    pub name: String,
    pub value: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,
}

/// Errors emitted while serving observability endpoints.
#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error(transparent)]
    Telemetry(#[from] TelemetryError),
    #[error("metric '{name}' must emit `_ms` suffixed values")]
    InvalidMetric { name: String },
    #[error("metrics snapshot registry mismatch (expected {expected}, observed {observed})")]
    RegistryMismatch { expected: String, observed: String },
    #[error("failed to export metrics snapshot: {source}")]
    SnapshotExport { source: serde_json::Error },
}

use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Errors surfaced while loading telemetry catalogs or validating `/metrics` output.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// IOError when loading the JSON catalog file.
    #[error("failed to read telemetry catalog {path}: {source}")]
    CatalogIo {
        /// Path to the catalog file.
        path: PathBuf,
        /// Source IO error.
        source: std::io::Error,
    },
    /// JSON parse error when interpreting the telemetry catalog.
    #[error("failed to parse telemetry catalog {path}: {source}")]
    CatalogParse {
        /// Path to the catalog file.
        path: PathBuf,
        /// JSON error bubbled up from serde.
        source: serde_json::Error,
    },
    /// Encountered a metric that still emits `_seconds`.
    #[error("metric '{name}' must emit `_ms` suffixed values")]
    NonMsMetric {
        /// Name of the offending metric.
        name: String,
    },
}

/// Static telemetry catalog compiled from `telemetry/catalog.json`.
#[derive(Debug, Deserialize)]
pub struct TelemetryCatalog {
    metrics: Vec<TelemetryMetric>,
}

impl TelemetryCatalog {
    /// Loads the catalog from disk.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, TelemetryError> {
        let path_ref = path.as_ref();
        let payload = fs::read_to_string(path_ref).map_err(|source| TelemetryError::CatalogIo {
            path: path_ref.to_path_buf(),
            source,
        })?;
        serde_json::from_str(&payload).map_err(|source| TelemetryError::CatalogParse {
            path: path_ref.to_path_buf(),
            source,
        })
    }

    /// Returns the known metric definitions.
    pub fn metrics(&self) -> &[TelemetryMetric] {
        &self.metrics
    }
}

/// Telemetry metric entry as defined in the catalog JSON.
#[derive(Clone, Debug, Deserialize)]
pub struct TelemetryMetric {
    /// Metric name as exported by Prometheus text format.
    pub name: String,
    /// Unit for the metric.
    #[serde(default)]
    pub unit: Option<String>,
    /// Encoding type (should be `wide_int` for `_ms` entries).
    #[serde(default)]
    pub encoding: Option<String>,
    /// Whether this metric is read-only (aliases should be read-only).
    #[serde(default)]
    pub read_only: bool,
    /// Optional alias for `_seconds` compatibility entries.
    #[serde(default)]
    pub alias_for: Option<String>,
    /// Documentation references (alerts, dashboards, or runbooks).
    #[serde(default)]
    pub documentation: Vec<String>,
}

/// Parses metric names from Prometheus exposition text and ensures only `_ms` suffixes emit data.
pub fn ensure_ms_only_metrics(exposition: &str) -> Result<(), TelemetryError> {
    for name in scrape_metric_names(exposition) {
        if name.ends_with("_seconds") {
            return Err(TelemetryError::NonMsMetric { name });
        }
    }
    Ok(())
}

/// Extracts metric names (without labels) from Prometheus exposition text.
pub fn scrape_metric_names(exposition: &str) -> Vec<String> {
    exposition
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let mut parts = trimmed.split(|c: char| c == '{' || c.is_whitespace());
            parts
                .next()
                .filter(|name| !name.is_empty())
                .map(|name| name.to_string())
        })
        .collect()
}

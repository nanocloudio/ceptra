use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct TelemetryCatalog {
    pub metrics: Vec<TelemetryMetric>,
}

#[derive(Debug, Deserialize)]
pub struct TelemetryMetric {
    pub name: String,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub alias_for: Option<String>,
    #[serde(default)]
    pub documentation: Option<Vec<String>>,
}

/// Validates a telemetry catalog, enforcing the `_ms`/`_seconds` encoding and alias contract.
pub fn validate_catalog(path: impl AsRef<Path>) -> Result<()> {
    let path_ref = path.as_ref();
    if !path_ref.exists() {
        return Err(anyhow!(
            "telemetry catalog missing at {}",
            path_ref.display()
        ));
    }
    let payload = std::fs::read_to_string(path_ref)
        .with_context(|| format!("unable to read {}", path_ref.display()))?;
    let catalog: TelemetryCatalog = serde_json::from_str(&payload)
        .with_context(|| format!("invalid catalog {}", path_ref.display()))?;

    let mut metrics_by_name = HashMap::new();
    for metric in &catalog.metrics {
        metrics_by_name.insert(metric.name.clone(), metric);
    }

    for metric in &catalog.metrics {
        if metric.name.ends_with("_seconds") {
            if !metric.read_only {
                return Err(anyhow!(
                    "metric {} must be read_only when using _seconds suffix",
                    metric.name
                ));
            }
            let alias = metric
                .alias_for
                .as_deref()
                .ok_or_else(|| anyhow!("metric {} must declare alias_for", metric.name))?;
            if !alias.ends_with("_ms") {
                return Err(anyhow!(
                    "metric {} must alias a *_ms target (found {alias})",
                    metric.name
                ));
            }
            if !metrics_by_name.contains_key(alias) {
                return Err(anyhow!(
                    "metric {} aliases unknown target {}",
                    metric.name,
                    alias
                ));
            }
        }
        if metric.name.ends_with("_ms") {
            match metric.encoding.as_deref() {
                Some("wide_int") => {}
                other => {
                    return Err(anyhow!(
                        "metric {} must declare wide_int encoding (found {:?})",
                        metric.name,
                        other
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Validates multiple telemetry catalogs, returning the first error encountered.
pub fn validate_catalogs(paths: &[impl AsRef<Path>]) -> Result<()> {
    for path in paths {
        validate_catalog(path)?;
    }
    Ok(())
}

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct Catalog {
    metrics: Vec<Metric>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Metric {
    name: String,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    alias_for: Option<String>,
    #[serde(default)]
    documentation: Option<Vec<String>>,
}

fn main() -> Result<()> {
    let mut catalogs = Vec::new();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--catalog requires a path"))?;
                catalogs.push(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!("usage: telemetry_guard --catalog <path> [--catalog <path> ...]");
                return Ok(());
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if catalogs.is_empty() {
        return Err(anyhow!("provide at least one --catalog path"));
    }

    for catalog_path in catalogs {
        validate_catalog(&catalog_path)
            .with_context(|| format!("telemetry guard failed for {}", catalog_path.display()))?;
    }

    Ok(())
}

fn validate_catalog(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        println!(
            "Skipping telemetry guard for {} (file missing)",
            path.display()
        );
        return Ok(());
    }
    let payload = std::fs::read_to_string(path)
        .with_context(|| format!("unable to read {}", path.display()))?;
    let catalog: Catalog = serde_json::from_str(&payload)
        .with_context(|| format!("invalid catalog {}", path.display()))?;
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

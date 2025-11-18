use anyhow::{bail, Context, Result};
use ceptra::{
    promql_group_labels, validate_lane_domains, LaneDomainMap, LaneProjectionStatus,
    LaneValidationReport, MetricGrouping,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let args = Args::parse()?;
    let doc = BundleDocument::read(&args.bundle)?;
    if doc.bundle.lane_domains.is_empty() {
        bail!("bundle declares no lane_domains");
    }
    let metrics = doc.metric_groupings();
    if metrics.is_empty() {
        bail!("bundle declares no aggregate.promql phases with queries");
    }
    let report = validate_lane_domains(&doc.bundle.lane_domains, &metrics)?;
    emit_report(&report);
    if args.markdown {
        println!("{}", report.to_markdown());
    }
    if report.has_overflow() {
        bail!("lane validation failed – projected lanes exceed LANE_MAX_PER_PARTITION");
    }
    Ok(())
}

fn emit_report(report: &LaneValidationReport) {
    println!(
        "projected_total_lanes={} (cap {})",
        report.total_projected_lanes, ceptra::LANE_MAX_PER_PARTITION
    );
    for projection in &report.projections {
        match projection.status {
            LaneProjectionStatus::Healthy => {
                println!(
                    "metric={} lanes={} labels={}",
                    projection.metric,
                    projection.projected_lanes,
                    projection.labels.join(", ")
                );
            }
            LaneProjectionStatus::Warning => {
                println!(
                    "warn metric={} lanes={} labels={} (>= warn threshold {})",
                    projection.metric,
                    projection.projected_lanes,
                    projection.labels.join(", "),
                    ceptra::LANE_WARN_THRESHOLD
                );
            }
            LaneProjectionStatus::Overflow => {
                println!(
                    "error metric={} lanes={} labels={} (>{})",
                    projection.metric,
                    projection.projected_lanes,
                    projection.labels.join(", "),
                    ceptra::LANE_MAX_PER_PARTITION
                );
            }
        }
    }
    println!("{}", report.histogram.to_markdown());
}

#[derive(Debug)]
struct Args {
    bundle: PathBuf,
    markdown: bool,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut bundle = None;
        let mut markdown = false;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bundle" => {
                    let path = args.next().ok_or_else(|| {
                        anyhow::anyhow!("--bundle requires a path to the bundle YAML file")
                    })?;
                    bundle = Some(PathBuf::from(path));
                }
                "--markdown" => markdown = true,
                "--help" | "-h" => {
                    println!("usage: lane_compiler --bundle <path> [--markdown]");
                    std::process::exit(0);
                }
                other => {
                    return Err(anyhow::anyhow!("unknown argument: {other}"));
                }
            }
        }
        let bundle = bundle.ok_or_else(|| anyhow::anyhow!("--bundle argument is required"))?;
        Ok(Self { bundle, markdown })
    }
}

#[derive(Debug, Deserialize, Default)]
struct BundleDocument {
    #[serde(default)]
    bundle: BundleSection,
    #[serde(default)]
    workflow: WorkflowSection,
}

impl BundleDocument {
    fn read(path: &Path) -> Result<Self> {
        let payload =
            fs::read_to_string(path).with_context(|| format!("unable to read {}", path.display()))?;
        serde_yaml::from_str(&payload)
            .with_context(|| format!("invalid bundle YAML {}", path.display()))
    }

    fn metric_groupings(&self) -> Vec<MetricGrouping> {
        self.workflow
            .phases
            .iter()
            .filter(|phase| phase.phase_type == "aggregate.promql")
            .flat_map(|phase| {
                phase.options.queries.iter().map(|(name, query)| MetricGrouping {
                    name: name.clone(),
                    labels: promql_group_labels(&query.expression),
                })
            })
            .collect()
    }
}

#[derive(Debug, Deserialize, Default)]
struct BundleSection {
    #[serde(default)]
    lane_domains: LaneDomainMap,
}

#[derive(Debug, Deserialize, Default)]
struct WorkflowSection {
    #[serde(default)]
    phases: Vec<PhaseSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct PhaseSpec {
    #[serde(default, rename = "type")]
    phase_type: String,
    #[serde(default)]
    options: PhaseOptions,
}

#[derive(Debug, Deserialize, Default)]
struct PhaseOptions {
    #[serde(default)]
    queries: BTreeMap<String, QuerySpec>,
}

#[derive(Debug, Deserialize, Default)]
struct QuerySpec {
    #[serde(default)]
    expression: String,
}

use anyhow::{bail, Context, Result};
use ceptra::consensus_core::activation::{
    ActivationBarrier, ActivationBarrierEvaluator, ActivationBarrierState, ShadowApplyState,
    WarmupReadinessRecord,
};
use ceptra::definitions::{validate_definition_document, DefinitionDocument};
use ceptra::policy::RuleVersionSelector;
use ceptra::{LaneProjectionStatus, LANE_MAX_PER_PARTITION, LANE_WARN_THRESHOLD};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let args = Args::parse()?;
    let doc = load_bundle(&args.bundle)?;
    let admission = validate_definition_document(&doc)
        .with_context(|| format!("invalid bundle {}", args.bundle.display()))?;
    emit_lane_report(&admission.diagnostics.bundle_name, &admission.diagnostics.lane_report);
    println!();

    if let Some(activation_path) = args.activation.as_ref() {
        let activation = load_activation(activation_path)?;
        evaluate_activation(&activation)?;
        println!();
    }

    if args.canary_log_index.is_some() || args.stage_def_version.is_some() {
        demo_canary(&doc, &args)?;
    }

    Ok(())
}

fn load_bundle(path: &PathBuf) -> Result<DefinitionDocument> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("unable to read bundle {}", path.display()))?;
    serde_yaml::from_str(&payload)
        .with_context(|| format!("{} is not valid definition YAML", path.display()))
}

fn emit_lane_report(bundle_name: &str, report: &ceptra::LaneValidationReport) {
    println!("bundle={} lane_projection_total={}", bundle_name, report.total_projected_lanes);
    println!(
        "lane cap={} warn_threshold={}",
        LANE_MAX_PER_PARTITION, LANE_WARN_THRESHOLD
    );
    if report.has_overflow() {
        println!("status=ERROR projected lanes exceed hard cap");
    } else if report.has_warnings() {
        println!("status=WARNING projected lanes near cap");
    } else {
        println!("status=OK");
    }
    for projection in &report.projections {
        match projection.status {
            LaneProjectionStatus::Healthy => continue,
            LaneProjectionStatus::Warning => println!(
                "warn metric={} lanes={} labels={}",
                projection.metric,
                projection.projected_lanes,
                if projection.labels.is_empty() {
                    "-".into()
                } else {
                    projection.labels.join(",")
                }
            ),
            LaneProjectionStatus::Overflow => println!(
                "error metric={} lanes={} labels={}",
                projection.metric,
                projection.projected_lanes,
                if projection.labels.is_empty() {
                    "-".into()
                } else {
                    projection.labels.join(",")
                }
            ),
        }
    }
    println!("{}", report.histogram.to_markdown());
}

fn evaluate_activation(spec: &ActivationInput) -> Result<()> {
    let barrier = spec.barrier.to_barrier();
    let readiness: Vec<WarmupReadinessRecord> =
        spec.readiness.iter().cloned().map(|record| record.into()).collect();
    let decision = ActivationBarrierEvaluator::evaluate(&barrier, &readiness, spec.now_ms)?;
    match &decision.state {
        ActivationBarrierState::Ready => {
            println!(
                "activation barrier={} state=READY readiness_digest={}",
                decision.barrier_id, decision.readiness_digest
            );
        }
        ActivationBarrierState::Expired => {
            println!(
                "activation barrier={} state=EXPIRED readiness_digest={}",
                decision.barrier_id, decision.readiness_digest
            );
        }
        ActivationBarrierState::Pending { missing_partitions } => {
            println!(
                "activation barrier={} state=PENDING readiness_digest={}",
                decision.barrier_id, decision.readiness_digest
            );
            if missing_partitions.is_empty() {
                println!("missing partitions resolved but deadline not reached");
            } else {
                println!("missing partitions: {}", missing_partitions.join(", "));
            }
        }
    }
    Ok(())
}

fn demo_canary(doc: &DefinitionDocument, args: &Args) -> Result<()> {
    let mut selector = RuleVersionSelector::new(doc.bundle.def_version, doc.bundle.policy_version);
    if let Some(stage_def_version) = args.stage_def_version {
        let stage_at_log = args.stage_at_log.expect("--stage-at-log is required");
        let stage_policy = args.stage_policy_version.or(doc.bundle.policy_version);
        selector.stage(stage_at_log, stage_def_version, stage_policy);
        println!(
            "staged def_version={} policy={:?} at log_index={}",
            stage_def_version, stage_policy, stage_at_log
        );
    }
    if let Some(log_index) = args.canary_log_index {
        let snapshot = selector.select(log_index);
        println!(
            "canary_log_index={} pins rule_version={} (policy={:?})",
            log_index, snapshot.def_version, snapshot.policy_version
        );
        if let Some((activation_log, pending)) = selector.pending() {
            println!(
                "pending activation def_version={} policy={:?} gated at log_index={}",
                pending.def_version, pending.policy_version, activation_log
            );
        }
    } else if args.stage_def_version.is_some() {
        println!("(supply --canary-log-index to preview which clients pin a staged rule_version)");
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ActivationInput {
    now_ms: u64,
    barrier: BarrierSpec,
    readiness: Vec<ReadinessRecord>,
}

#[derive(Debug, Deserialize)]
struct BarrierSpec {
    barrier_id: String,
    bundle_id: String,
    partitions: Vec<String>,
    readiness_threshold: f64,
    warmup_deadline_ms: u64,
    readiness_window_ms: u64,
}

impl BarrierSpec {
    fn to_barrier(&self) -> ActivationBarrier {
        ActivationBarrier {
            barrier_id: self.barrier_id.clone(),
            bundle_id: self.bundle_id.clone(),
            partitions: self.partitions.clone(),
            readiness_threshold: self.readiness_threshold,
            warmup_deadline_ms: self.warmup_deadline_ms,
            readiness_window_ms: self.readiness_window_ms,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct ReadinessRecord {
    partition_id: String,
    bundle_id: String,
    shadow_apply_state: ShadowApplyState,
    shadow_apply_checkpoint_index: u64,
    warmup_ready_ratio: f64,
    updated_at_ms: u64,
}

impl From<ReadinessRecord> for WarmupReadinessRecord {
    fn from(record: ReadinessRecord) -> Self {
        WarmupReadinessRecord {
            partition_id: record.partition_id,
            bundle_id: record.bundle_id,
            shadow_apply_state: record.shadow_apply_state,
            shadow_apply_checkpoint_index: record.shadow_apply_checkpoint_index,
            warmup_ready_ratio: record.warmup_ready_ratio,
            updated_at_ms: record.updated_at_ms,
        }
    }
}

fn load_activation(path: &PathBuf) -> Result<ActivationInput> {
    let payload = fs::read_to_string(path)
        .with_context(|| format!("unable to read activation file {}", path.display()))?;
    serde_json::from_str(&payload)
        .with_context(|| format!("{} is not valid activation JSON", path.display()))
}

#[derive(Debug)]
struct Args {
    bundle: PathBuf,
    activation: Option<PathBuf>,
    stage_def_version: Option<u64>,
    stage_policy_version: Option<u64>,
    stage_at_log: Option<u64>,
    canary_log_index: Option<u64>,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut bundle = None;
        let mut activation = None;
        let mut stage_def_version = None;
        let mut stage_policy_version = None;
        let mut stage_at_log = None;
        let mut canary_log_index = None;
        let mut iter = env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--bundle" => {
                    let path = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--bundle requires a path"))?;
                    bundle = Some(PathBuf::from(path));
                }
                "--activation" => {
                    let path = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--activation requires a path"))?;
                    activation = Some(PathBuf::from(path));
                }
                "--stage-def-version" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--stage-def-version requires a value"))?;
                    stage_def_version = Some(value.parse()?);
                }
                "--stage-policy-version" => {
                    let value = iter.next().ok_or_else(|| {
                        anyhow::anyhow!("--stage-policy-version requires a value")
                    })?;
                    stage_policy_version = Some(value.parse()?);
                }
                "--stage-at-log" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--stage-at-log requires a log index"))?;
                    stage_at_log = Some(value.parse()?);
                }
                "--canary-log-index" => {
                    let value = iter.next().ok_or_else(|| {
                        anyhow::anyhow!("--canary-log-index requires a log index")
                    })?;
                    canary_log_index = Some(value.parse()?);
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }
        let bundle =
            bundle.ok_or_else(|| anyhow::anyhow!("--bundle <path> is required for deployment"))?;
        if stage_def_version.is_some() && stage_at_log.is_none() {
            bail!("--stage-at-log must be supplied when staging a def version");
        }
        Ok(Self {
            bundle,
            activation,
            stage_def_version,
            stage_policy_version,
            stage_at_log,
            canary_log_index,
        })
    }
}

fn print_usage() {
    eprintln!(
        "usage: bundle_deployer --bundle <path> [--activation <path>] [--stage-def-version <v> \
         --stage-at-log <index> [--stage-policy-version <v>]] [--canary-log-index <index>]"
    );
}

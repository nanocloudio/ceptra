use anyhow::{Context, Result};
use ceptra::DisasterRecoveryPlan;
use std::env;
use std::fs;
use std::process;

fn main() {
    if let Err(err) = real_main() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn real_main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(cmd) = args.next() else {
        usage();
        return Ok(());
    };
    let Some(path) = args.next() else {
        usage();
        return Ok(());
    };
    match cmd.as_str() {
        "run" => run_plan(&path),
        "validate" => validate_plan(&path),
        _ => {
            usage();
            Ok(())
        }
    }
}

fn run_plan(path: &str) -> Result<()> {
    let plan = load_plan(path)?;
    let report = plan.cutover()?;
    let json = serde_json::to_string_pretty(&report)?;
    println!("{json}");
    Ok(())
}

fn validate_plan(path: &str) -> Result<()> {
    let plan = load_plan(path)?;
    plan.cutover()?;
    println!(
        "plan {} -> {} reconciles to epoch {}",
        plan.primary_cluster(),
        plan.dr_cluster(),
        plan.target_epoch
    );
    Ok(())
}

fn load_plan(path: &str) -> Result<DisasterRecoveryPlan> {
    let payload =
        fs::read_to_string(path).with_context(|| format!("failed to read plan {path}"))?;
    let plan: DisasterRecoveryPlan =
        serde_json::from_str(&payload).with_context(|| format!("failed to parse plan {path}"))?;
    Ok(plan)
}

fn usage() {
    eprintln!("usage: dr_cutover <run|validate> <plan.json>");
}

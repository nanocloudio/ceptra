use anyhow::{anyhow, Result};
use ceptra::load_wire_catalog;
use std::env;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut expected = None;
    let mut candidate = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--expected" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--expected requires a path"))?;
                expected = Some(PathBuf::from(path));
            }
            "--candidate" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--candidate requires a path"))?;
                candidate = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!("usage: wire_lint [--expected <path>] [--candidate <path>]");
                return Ok(());
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    let expected_path =
        expected.unwrap_or_else(|| PathBuf::from("../clustor/artifacts/wire_catalog.json"));
    let candidate_path = candidate.unwrap_or_else(|| PathBuf::from("wire/catalog.json"));

    let expected_catalog = load_wire_catalog(&expected_path)?;
    let candidate_catalog = load_wire_catalog(&candidate_path)?;

    if expected_catalog != candidate_catalog {
        return Err(anyhow!(
            "wire catalogs differ (expected {} messages, candidate {} messages)",
            expected_catalog.messages.len(),
            candidate_catalog.messages.len()
        ));
    }

    println!(
        "wire catalogs match (schema {} / {} messages)",
        expected_catalog.schema_version,
        expected_catalog.messages.len()
    );
    Ok(())
}

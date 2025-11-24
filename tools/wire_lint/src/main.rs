use anyhow::{anyhow, Result};
use ceptra::wire_catalog::compare_wire_catalogs;
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

    compare_wire_catalogs(&expected_path, &candidate_path)?;
    println!("wire catalogs match");
    Ok(())
}

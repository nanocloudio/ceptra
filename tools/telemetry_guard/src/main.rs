use anyhow::{anyhow, Context, Result};
use std::env;
use std::path::PathBuf;

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

    ceptra::telemetry_catalog::validate_catalogs(&catalogs)
        .with_context(|| "telemetry guard failed")?;

    Ok(())
}

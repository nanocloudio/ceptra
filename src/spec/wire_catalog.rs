use anyhow::{anyhow, Result};
use std::path::Path;

pub use crate::spec::wire::{load_wire_catalog, WireCatalog, WireCatalogError};

/// Loads and compares two wire catalogs, erroring when they differ.
pub fn compare_wire_catalogs(
    expected: impl AsRef<Path>,
    candidate: impl AsRef<Path>,
) -> Result<()> {
    let expected_catalog = load_wire_catalog(&expected)?;
    let candidate_catalog = load_wire_catalog(&candidate)?;

    if expected_catalog != candidate_catalog {
        return Err(anyhow!(
            "wire catalogs differ (expected {} messages, candidate {} messages)",
            expected_catalog.messages.len(),
            candidate_catalog.messages.len()
        ));
    }

    Ok(())
}

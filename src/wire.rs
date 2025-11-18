use clustor::CatalogVersion;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct WireCatalog {
    pub schema_version: u32,
    pub messages: Vec<String>,
    pub clause_coverage_hash: String,
}

impl WireCatalog {
    pub fn derived_version(&self) -> CatalogVersion {
        let minor = self.messages.len().min(u8::MAX as usize) as u8;
        CatalogVersion::new(self.schema_version as u8, minor, minor)
    }
}

#[derive(Debug, Error)]
pub enum WireCatalogError {
    #[error("failed to read wire catalog {path}: {source}")]
    ReadError {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse wire catalog {path}: {source}")]
    ParseError {
        path: PathBuf,
        source: serde_json::Error,
    },
}

pub fn load_wire_catalog(path: impl AsRef<Path>) -> Result<WireCatalog, WireCatalogError> {
    let path_ref = path.as_ref();
    let payload = fs::read_to_string(path_ref).map_err(|source| WireCatalogError::ReadError {
        path: path_ref.to_path_buf(),
        source,
    })?;
    let catalog =
        serde_json::from_str(&payload).map_err(|source| WireCatalogError::ParseError {
            path: path_ref.to_path_buf(),
            source,
        })?;
    Ok(catalog)
}

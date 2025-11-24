use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

/// Ensures the given path exists, returning an error if missing.
pub fn ensure_file(path: impl AsRef<Path>) -> Result<()> {
    let path_ref = path.as_ref();
    if path_ref.exists() {
        return Ok(());
    }
    Err(anyhow!("missing file {}", path_ref.display()))
}

/// Computes the SHA256 hex digest of a file.
pub fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    let path_ref = path.as_ref();
    let data =
        fs::read(path_ref).with_context(|| format!("unable to read {}", path_ref.display()))?;
    Ok(sha256_bytes(&data))
}

/// Computes the SHA256 hex digest of a byte slice.
pub fn sha256_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(Sha256::digest(bytes)))
}

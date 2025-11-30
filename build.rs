use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
struct RegistryFile {
    terms: Vec<RegistryTerm>,
}

#[derive(Deserialize)]
struct RegistryTerm {
    term_id: String,
    canonical: String,
}

#[derive(Deserialize)]
struct CeptraTermFile {
    terms: Vec<CeptraTermConfig>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CeptraTermConfig {
    symbol: String,
    canonical: String,
    term_id: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Deserialize)]
struct ManifestFile {
    documents: Vec<ManifestDocument>,
}

#[derive(Deserialize)]
struct ManifestDocument {
    name: String,
    path: String,
    sha256: String,
}

fn normalize_hash(value: &str) -> String {
    value.trim().trim_start_matches("0x").to_ascii_lowercase()
}

fn sha256_file(path: impl AsRef<Path>) -> Result<String, Box<dyn Error>> {
    let data = fs::read(path.as_ref())?;
    Ok(hex::encode(Sha256::digest(&data)))
}

fn verify_sibling_and_manifests(repo_root: &Path) -> Result<(), Box<dyn Error>> {
    let clustor_root = repo_root
        .parent()
        .ok_or("repository root has no parent; expected ../clustor sibling")?
        .join("clustor");
    if !clustor_root.join("Cargo.toml").exists() {
        return Err("missing ../clustor; check out the sibling repo before building ceptra".into());
    }

    let manifest_dir = repo_root.join("manifests");
    let manifest_path = manifest_dir.join("ceptra_manifest.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    let manifest: ManifestFile = serde_json::from_str(&fs::read_to_string(&manifest_path)?)?;

    for doc in &manifest.documents {
        let absolute = manifest_dir.join(&doc.path);
        if !absolute.exists() {
            return Err(format!(
                "manifest entry {} points at missing file {}",
                doc.name,
                absolute.display()
            )
            .into());
        }
        let observed = sha256_file(&absolute)?;
        if normalize_hash(&observed) != normalize_hash(&doc.sha256) {
            return Err(format!(
                "manifest entry {} hash mismatch (manifest {}, observed {}); run `make spec-sync-write` to refresh sibling manifests",
                doc.name, doc.sha256, observed
            )
            .into());
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    verify_sibling_and_manifests(&repo_root)?;

    let registry_path = PathBuf::from("registry/term_registry.json");
    let ceptra_terms_path = PathBuf::from("registry/ceptra_terms.json");
    println!("cargo:rerun-if-changed={}", registry_path.display());
    println!("cargo:rerun-if-changed={}", ceptra_terms_path.display());

    let registry: RegistryFile = serde_json::from_str(&fs::read_to_string(&registry_path)?)?;
    let registry_map: HashMap<_, _> = registry
        .terms
        .into_iter()
        .map(|term| (term.term_id.clone(), term))
        .collect();

    let ceptra_terms: CeptraTermFile =
        serde_json::from_str(&fs::read_to_string(&ceptra_terms_path)?)?;

    let mut generated = String::new();
    for term in &ceptra_terms.terms {
        let registry_term = registry_map.get(&term.term_id).ok_or_else(|| {
            format!(
                "term {} ({}) is missing from registry/term_registry.json",
                term.canonical, term.term_id
            )
        })?;
        if registry_term.canonical != term.canonical {
            return Err(format!(
                "term {} maps to canonical {} in registry but {} in CEP list",
                term.term_id, registry_term.canonical, term.canonical
            )
            .into());
        }
        generated.push_str(&format!(
            "pub const {}: CeptraTerm = CeptraTerm::new(\"{}\", \"{}\");\n",
            term.symbol, term.canonical, term.term_id
        ));
    }
    generated.push_str("\npub const CEPTRA_TERMS: &[CeptraTerm] = &[\n");
    for term in &ceptra_terms.terms {
        generated.push_str(&format!("    {},\n", term.symbol));
    }
    generated.push_str("];\n");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    fs::write(out_dir.join("ceptra_terms.rs"), generated)?;

    Ok(())
}

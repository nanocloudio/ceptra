use anyhow::{anyhow, bail, Context, Result};
use ceptra::manifests::{ensure_file, sha256_bytes, sha256_file};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const CEPTRA_SPEC_NAME: &str = "ceptra_specification";
const CLUSTOR_SPEC_NAME: &str = "clustor_specification";
const CEPTRA_CHILD_MANIFEST: &str = "ceptra_manifest";
const CEPTRA_TELEMETRY_NAME: &str = "ceptra_telemetry_catalog";

fn main() -> Result<()> {
    let mode = parse_mode()?;
    let repo_root = repo_root()?;
    let clustor_root = repo_root
        .parent()
        .ok_or_else(|| anyhow!("repository root has no parent; expected ../clustor sibling"))?
        .join("clustor")
        .canonicalize()
        .context(
            "unable to locate ../clustor – check out the sibling repo before syncing manifests",
        )?;

    let ceptra_spec = repo_root.join("docs/specification.md");
    let clustor_spec = clustor_root.join("docs/specification.md");
    let ceptra_telemetry = repo_root.join("telemetry/catalog.json");
    ensure_file(&ceptra_spec)?;
    ensure_file(&clustor_spec)?;
    ensure_file(&ceptra_telemetry)?;

    let ceptra_hash = sha256_file(&ceptra_spec)?;
    let clustor_hash = sha256_file(&clustor_spec)?;
    let ceptra_telemetry_hash = sha256_file(&ceptra_telemetry)?;

    let ceptra_manifest_path = repo_root.join("manifests/ceptra_manifest.json");
    let mut ceptra_manifest = read_manifest(&ceptra_manifest_path)?;
    ceptra_manifest.upsert_document(DocumentEntry {
        name: CEPTRA_SPEC_NAME.into(),
        path: "../docs/specification.md".into(),
        sha256: ceptra_hash,
        clause_tag_coverage: Some(1.0),
        document_type: Some("spec".into()),
    });
    ceptra_manifest.upsert_document(DocumentEntry {
        name: CLUSTOR_SPEC_NAME.into(),
        path: "../../clustor/docs/specification.md".into(),
        sha256: clustor_hash.clone(),
        clause_tag_coverage: Some(1.0),
        document_type: Some("spec".into()),
    });
    ceptra_manifest.upsert_document(DocumentEntry {
        name: CEPTRA_TELEMETRY_NAME.into(),
        path: "../telemetry/catalog.json".into(),
        sha256: ceptra_telemetry_hash,
        clause_tag_coverage: None,
        document_type: Some("telemetry".into()),
    });
    let ceptra_manifest_content = persist_manifest(&ceptra_manifest_path, &ceptra_manifest, mode)
        .context("updating manifests/ceptra_manifest.json")?;
    let ceptra_manifest_sha = sha256_bytes(ceptra_manifest_content.as_bytes());

    let consensus_manifest_path = clustor_root.join("manifests/consensus_core_manifest.json");
    let mut consensus_manifest = read_manifest(&consensus_manifest_path)?;
    consensus_manifest.upsert_document(DocumentEntry {
        name: CLUSTOR_SPEC_NAME.into(),
        path: "../docs/specification.md".into(),
        sha256: clustor_hash,
        clause_tag_coverage: Some(1.0),
        document_type: Some("spec".into()),
    });
    consensus_manifest.upsert_child(ChildManifest {
        name: CEPTRA_CHILD_MANIFEST.into(),
        path: "../../ceptra/manifests/ceptra_manifest.json".into(),
        sha256: ceptra_manifest_sha,
    });
    persist_manifest(&consensus_manifest_path, &consensus_manifest, mode)
        .context("updating ../clustor/manifests/consensus_core_manifest.json")?;

    sync_term_registry(&repo_root, &clustor_root, mode)?;
    sync_wire_catalog(&repo_root, &clustor_root, mode)?;

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Check,
    Write,
}

fn parse_mode() -> Result<Mode> {
    let mut mode = Mode::Check;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" => mode = Mode::Check,
            "--write" => mode = Mode::Write,
            "--help" | "-h" => {
                println!("usage: spec_sync [--check|--write]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(mode)
}

fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| anyhow!("unable to determine repo root from {manifest_dir:?}"))
        .and_then(|path| {
            path.canonicalize()
                .context("failed to canonicalize repository root")
        })
}

fn read_manifest(path: &Path) -> Result<ManifestFile> {
    if path.exists() {
        let data = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?)
    } else {
        Ok(ManifestFile::default())
    }
}

fn persist_manifest(path: &Path, manifest: &ManifestFile, mode: Mode) -> Result<String> {
    let content = format!("{}\n", serde_json::to_string_pretty(manifest)?);
    match mode {
        Mode::Write => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, content.as_bytes())?;
        }
        Mode::Check => {
            let existing = fs::read_to_string(path)
                .with_context(|| format!("{} is missing; run with --write", path.display()))?;
            if normalize(&existing) != normalize(&content) {
                bail!(
                    "{} is out of date; run --write to refresh hashes",
                    path.display()
                );
            }
        }
    }
    Ok(content)
}

fn normalize(input: &str) -> String {
    let mut normalized = input.replace("\r\n", "\n");
    if !normalized.ends_with('\n') {
        normalized.push('\n');
    }
    normalized
}

fn sync_term_registry(repo_root: &Path, clustor_root: &Path, mode: Mode) -> Result<()> {
    let ceptra_registry = repo_root.join("registry/term_registry.json");
    let registry_payload = if clustor_root.join("term_registry.json").exists() {
        fs::read_to_string(clustor_root.join("term_registry.json"))?
    } else {
        derive_term_registry(
            &clustor_root.join("docs/specification.md"),
            &repo_root.join("registry/ceptra_terms.json"),
        )?
    };
    match mode {
        Mode::Write => {
            if let Some(parent) = ceptra_registry.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&ceptra_registry, registry_payload.as_bytes())?;
        }
        Mode::Check => {
            ensure_file(&ceptra_registry)?;
            let expected = normalize(&registry_payload);
            let existing = normalize(&fs::read_to_string(&ceptra_registry)?);
            if expected != existing {
                bail!("registry/term_registry.json is stale; run spec_sync --write to refresh");
            }
        }
    }
    Ok(())
}

fn derive_term_registry(spec_path: &Path, overrides_path: &Path) -> Result<String> {
    let contents = fs::read_to_string(spec_path)
        .with_context(|| format!("unable to read {}", spec_path.display()))?;
    let mut terms = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') || !trimmed.contains("TERM-") {
            continue;
        }
        let parts: Vec<&str> = trimmed.split('|').map(|part| part.trim()).collect();
        // Expected format: | `TERM-0001` | `Strict` | aliases | description |
        if parts.len() < 5 {
            continue;
        }
        let term_id = parts[1].trim_matches('`').trim();
        let canonical = parts[2].trim_matches('`').trim();
        let description = parts[4].trim();
        if term_id.is_empty() || canonical.is_empty() || description.is_empty() {
            continue;
        }
        terms.push(TermEntry {
            term_id: term_id.to_string(),
            canonical: canonical.to_string(),
            description: description.to_string(),
        });
    }
    if overrides_path.exists() {
        let overrides: CeptraTermOverrideFile =
            serde_json::from_str(&fs::read_to_string(overrides_path)?)?;
        for override_term in overrides.terms {
            terms.push(TermEntry {
                term_id: override_term.term_id,
                canonical: override_term.canonical,
                description: override_term
                    .description
                    .unwrap_or_else(|| "CEP-specific term".into()),
            });
        }
    }
    if terms.is_empty() {
        bail!(
            "could not derive term registry entries from {}",
            spec_path.display()
        );
    }
    let registry = TermRegistry { terms };
    Ok(format!("{}\n", serde_json::to_string_pretty(&registry)?))
}

fn sync_wire_catalog(repo_root: &Path, clustor_root: &Path, mode: Mode) -> Result<()> {
    let clustor_catalog = clustor_root.join("artifacts/wire_catalog.json");
    let ceptra_catalog = repo_root.join("wire/catalog.json");
    ensure_file(&clustor_catalog)?;
    match mode {
        Mode::Write => {
            if let Some(parent) = ceptra_catalog.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&clustor_catalog, &ceptra_catalog)?;
        }
        Mode::Check => {
            ensure_file(&ceptra_catalog)?;
            let upstream = sha256_file(&clustor_catalog)?;
            let local = sha256_file(&ceptra_catalog)?;
            if upstream != local {
                bail!("wire/catalog.json is stale; run spec_sync --write to refresh");
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ManifestFile {
    #[serde(default)]
    documents: Vec<DocumentEntry>,
    #[serde(default)]
    children: Vec<ChildManifest>,
}

impl ManifestFile {
    fn upsert_document(&mut self, entry: DocumentEntry) {
        if let Some(existing) = self.documents.iter_mut().find(|doc| doc.name == entry.name) {
            *existing = entry;
        } else {
            self.documents.push(entry);
        }
    }

    fn upsert_child(&mut self, entry: ChildManifest) {
        if let Some(existing) = self
            .children
            .iter_mut()
            .find(|child| child.name == entry.name)
        {
            *existing = entry;
        } else {
            self.children.push(entry);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DocumentEntry {
    name: String,
    path: String,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clause_tag_coverage: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    document_type: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChildManifest {
    name: String,
    path: String,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TermRegistry {
    terms: Vec<TermEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TermEntry {
    term_id: String,
    canonical: String,
    description: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CeptraTermOverrideFile {
    terms: Vec<CeptraTermOverride>,
}

#[derive(Clone, Debug, Deserialize)]
struct CeptraTermOverride {
    term_id: String,
    canonical: String,
    #[serde(default)]
    description: Option<String>,
}

use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

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

fn main() -> Result<(), Box<dyn Error>> {
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

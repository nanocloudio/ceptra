use ceptra::{load_wire_catalog, WireCatalog};
use clustor::wire::{BundleNegotiationEntry, BundleNegotiationLog, WireCatalogNegotiator};
use tempfile::tempdir;

fn catalogs() -> (WireCatalog, WireCatalog) {
    let ceptra = load_wire_catalog("wire/catalog.json").expect("load CEPtra catalog");
    let clustor =
        load_wire_catalog("../clustor/artifacts/wire_catalog.json").expect("load Clustor catalog");
    (ceptra, clustor)
}

#[test]
fn catalog_files_match() {
    let (ceptra, clustor) = catalogs();
    assert_eq!(ceptra, clustor);
}

#[test]
fn negotiation_accepts_matching_versions() {
    let (catalog, _) = catalogs();
    let version = catalog.derived_version();
    let dir = tempdir().unwrap();
    let log = BundleNegotiationLog::new(dir.path().join("bundle_negotiation.log"));
    let negotiator = WireCatalogNegotiator::new("partition-a", log.clone());
    negotiator.negotiate(version, version).unwrap();
    let contents = std::fs::read_to_string(log.path()).unwrap();
    let entries: Vec<BundleNegotiationEntry> = contents
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].accepted);
}

#[test]
fn negotiation_rejects_newer_remote_major() {
    let (catalog, _) = catalogs();
    let mut remote = catalog.derived_version();
    remote.major = remote.major.saturating_add(1);
    let dir = tempdir().unwrap();
    let log = BundleNegotiationLog::new(dir.path().join("bundle_negotiation.log"));
    let negotiator = WireCatalogNegotiator::new("partition-a", log);
    let err = negotiator
        .negotiate(catalog.derived_version(), remote)
        .expect_err("negotiation should fail");
    assert!(err.to_string().contains("wire catalog mismatch"));
}

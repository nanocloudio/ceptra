use clustor::snapshot::{
    ManifestSignature, SignedSnapshotManifest, SnapshotChunk, SnapshotKind, SnapshotManifest,
};
use serde::Deserialize;
use serde_json::{self, Value};
use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn clustor_path(relative: &str) -> PathBuf {
    repo_root().join("..").join("clustor").join(relative)
}

fn manifest_fixture() -> SignedSnapshotManifest {
    let path = clustor_path("tests/fixtures/appendix_c_manifest.json");
    let payload = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("unable to read {}: {err}", path.display()));
    let mut value: Value =
        serde_json::from_str(&payload).expect("manifest fixture should be valid JSON");
    let signature_value = value
        .get("signature")
        .cloned()
        .expect("fixture must include signature");
    let signature: ManifestSignature =
        serde_json::from_value(signature_value).expect("signature decoding failed");
    value
        .as_object_mut()
        .expect("manifest payload must be an object")
        .remove("signature");
    let manifest: SnapshotManifest =
        serde_json::from_value(value).expect("SnapshotManifest decoding failed");
    SignedSnapshotManifest {
        manifest,
        signature,
    }
}

fn ensure_contiguous_chunks(chunks: &[SnapshotChunk]) {
    let mut expected_offset = 0u64;
    for chunk in chunks {
        assert_eq!(
            chunk.offset, expected_offset,
            "chunk {} offset should match expected stream position",
            chunk.chunk_id
        );
        assert!(chunk.len > 0, "chunk {} must reserve bytes", chunk.chunk_id);
        expected_offset = expected_offset.saturating_add(chunk.len);
    }
}

#[derive(Debug, Deserialize)]
struct TelemetryCatalog {
    metrics: Vec<TelemetryMetric>,
}

#[derive(Debug, Deserialize)]
struct TelemetryMetric {
    name: String,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    alias_for: Option<String>,
}

#[test]
fn legacy_snapshot_consumer_accepts_manifest_fixture() {
    let signed = manifest_fixture();
    assert_eq!(signed.manifest.snapshot_kind, SnapshotKind::Full);
    assert_eq!(signed.manifest.chunks.len(), 4);
    ensure_contiguous_chunks(&signed.manifest.chunks);
    assert_eq!(signed.manifest.base_index, 42);
    assert_eq!(signed.manifest.version_id, 7);
    assert_eq!(signed.manifest.manifest_id, "appendix-c");
    assert!(
        signed.signature.algorithm.starts_with("HMAC"),
        "fixture signature must mirror legacy signer"
    );
    // Canonicalization is how Clustor consumers persist manifests; verify ours remains stable.
    let canonical = signed
        .canonical_json()
        .expect("snapshot manifest canonicalization should succeed");
    assert!(
        canonical.starts_with(br#"{"ap_pane_digest""#),
        "canonical payload should begin with manifest content"
    );
}

#[test]
fn chunked_list_fixtures_and_wide_int_catalog_remain_in_sync() {
    let signed = manifest_fixture();
    ensure_contiguous_chunks(&signed.manifest.chunks);
    let chunk_bytes: u64 = signed.manifest.chunks.iter().map(|chunk| chunk.len).sum();
    assert_eq!(
        chunk_bytes,
        signed.manifest.chunks.last().unwrap().offset + signed.manifest.chunks.last().unwrap().len,
        "chunk offsets should cover the exported byte range"
    );

    let catalog_path = repo_root().join("telemetry/catalog.json");
    let payload = fs::read_to_string(&catalog_path)
        .unwrap_or_else(|err| panic!("unable to read {}: {err}", catalog_path.display()));
    let catalog: TelemetryCatalog =
        serde_json::from_str(&payload).expect("telemetry catalog must decode");
    assert!(
        !catalog.metrics.is_empty(),
        "telemetry catalog must describe at least one metric"
    );
    for metric in &catalog.metrics {
        if metric.name.ends_with("_ms") {
            assert_eq!(
                metric.encoding.as_deref(),
                Some("wide_int"),
                "metric {} must declare wide_int encoding",
                metric.name
            );
            assert!(
                !metric.read_only,
                "{} should be writable and its alias carries read_only semantics",
                metric.name
            );
        }
        if metric.name.ends_with("_seconds") {
            assert!(
                metric.read_only,
                "{} is a compatibility alias and should remain read_only",
                metric.name
            );
            assert_eq!(
                metric.encoding.as_deref(),
                Some("wide_int"),
                "{} alias must also report wide_int encoding",
                metric.name
            );
            let alias = metric
                .alias_for
                .as_deref()
                .unwrap_or_else(|| panic!("{} should point at its ms alias", metric.name));
            assert!(
                alias.ends_with("_ms"),
                "{} should alias a millisecond metric (found {alias})",
                metric.name
            );
        }
    }
}

use ceptra::{
    validate_definition_document, BundleSection, DefinitionDocument, DefinitionError,
    LaneDomainMap, LaneDomainSpec, LaneValidationError, WorkflowPhase, WorkflowSection,
};
use serde_json::json;

fn sample_bundle() -> DefinitionDocument {
    DefinitionDocument {
        bundle: BundleSection {
            name: "gaming_liveops".to_string(),
            def_version: 1,
            policy_version: Some(1),
            lane_domains: lane_domains(&[("player_id", 32)]),
        },
        workflow: WorkflowSection {
            name: "gaming_liveops".to_string(),
            phases: vec![
                WorkflowPhase {
                    name: "ingest_game_events".to_string(),
                    phase_type: "source.kafka".to_string(),
                    options: json!({
                        "connector": "game_telemetry_kafka",
                        "decode": "schema_registry"
                    }),
                },
                WorkflowPhase {
                    name: "aggregate_fast_signals".to_string(),
                    phase_type: "aggregate.promql".to_string(),
                    options: json!({
                        "window": "15s",
                        "queries": {
                            "anti_cheat_flags": {
                                "expression": "sum by (player_id)(increase(anti_cheat_flags_total{queue=\"ranked\"}[15s]))"
                            }
                        }
                    }),
                },
                WorkflowPhase {
                    name: "evaluate_player_risk".to_string(),
                    phase_type: "classify.cel".to_string(),
                    options: json!({
                        "rules": [
                            {
                                "name": "potential_aimbot",
                                "when": "true",
                                "emit": {
                                    "channel": "kafkas://liveops.cheat_alerts"
                                }
                            }
                        ]
                    }),
                },
            ],
        },
    }
}

fn lane_domains(entries: &[(&str, u32)]) -> LaneDomainMap {
    let mut map = LaneDomainMap::new();
    for (label, max) in entries {
        map.insert(
            label.to_string(),
            LaneDomainSpec {
                max_per_partition: *max,
            },
        );
    }
    map
}

#[test]
fn validates_promql_and_persists_metadata() {
    let doc = sample_bundle();
    let admission =
        validate_definition_document(&doc).expect("definition should validate successfully");
    assert_eq!(admission.bundle_name, "gaming_liveops");
    assert_eq!(admission.compiled_queries.len(), 1);
    let compiled = &admission.compiled_queries[0];
    assert_eq!(compiled.phase, "aggregate_fast_signals");
    assert_eq!(compiled.metric, "anti_cheat_flags");
    assert_eq!(compiled.aggregator, "sum");
    assert_eq!(compiled.range_ms, Some(15_000));
    assert_eq!(compiled.labels, vec!["player_id"]);

    let diagnostics = &admission.diagnostics;
    assert_eq!(diagnostics.policy_metadata.query_count, 1);
    assert_eq!(diagnostics.policy_metadata.workflow_name, "gaming_liveops");
    assert_eq!(
        diagnostics.policy_metadata.default_windows_ms["aggregate_fast_signals"],
        15_000
    );
    assert_eq!(diagnostics.lane_report.projections.len(), 1);
}

#[test]
fn detects_channel_loops_per_invariant_r2() {
    let mut doc = sample_bundle();
    // Make the workflow consume the same channel it emits.
    doc.workflow.phases.insert(
        0,
        WorkflowPhase {
            name: "ingest_loop".to_string(),
            phase_type: "source.channel".to_string(),
            options: json!({
                "channel": "kafkas://liveops.cheat_alerts"
            }),
        },
    );
    let err = validate_definition_document(&doc).expect_err("loop must be rejected");
    match err {
        DefinitionError::ChannelLoop { channel, .. } => {
            assert_eq!(channel, "kafkas://liveops.cheat_alerts");
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn surfaces_lane_validation_errors() {
    let mut doc = sample_bundle();
    // Reference a label that lacks a declared lane domain.
    doc.bundle.lane_domains = lane_domains(&[("player_id", 32)]);
    doc.workflow.phases[1].options = json!({
        "window": "15s",
        "queries": {
            "anti_cheat_flags": {
                "expression": "sum by (gateway_id)(increase(anti_cheat_flags_total{queue=\"ranked\"}[15s]))"
            }
        }
    });
    let err = validate_definition_document(&doc).expect_err("lane validation must fail");
    match err {
        DefinitionError::Lane(LaneValidationError::UnknownLaneDomain { label, .. }) => {
            assert_eq!(label, "gateway_id");
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

use ceptra::{
    SchemaCache, SchemaLookupError, SchemaRegistrationError, SchemaRegistry, ERR_SCHEMA_BLOCKED,
    ERR_UNKNOWN_SCHEMA,
};

#[test]
fn registry_enforces_version_rules() {
    let mut registry = SchemaRegistry::new();
    registry
        .register("protobuf:1.0:hash-a", "suspected_aimbot_v1")
        .expect("schema should register");
    registry
        .register("protobuf:1.1:hash-b", "suspected_aimbot_v1")
        .expect("minor bump should register");
    let err = registry
        .register("protobuf:2.0:hash-c", "suspected_aimbot_v1")
        .expect_err("major bump must be rejected");
    assert!(matches!(
        err,
        SchemaRegistrationError::ChangeNotAllowed { .. }
    ));
    let err = registry
        .register("protobuf:1.1:hash-different", "suspected_aimbot_v1")
        .expect_err("content changes require a new version");
    assert!(matches!(
        err,
        SchemaRegistrationError::ChangeNotAllowed { .. }
    ));
}

#[test]
fn cache_enforces_limits_and_status_reasons() {
    let mut registry = SchemaRegistry::new();
    registry
        .register("protobuf:1.0:hash-a", "schema-a")
        .expect("schema-a");
    registry
        .register("protobuf:1.0:hash-b", "schema-b")
        .expect("schema-b");
    registry
        .register("protobuf:1.0:hash-c", "schema-c")
        .expect("schema-c");

    let mut cache = SchemaCache::new(2);
    cache
        .resolve_with_registry(&registry, "schema-a")
        .expect("cache miss fetches schema-a");
    cache
        .resolve_with_registry(&registry, "schema-b")
        .expect("cache miss fetches schema-b");
    assert_eq!(cache.len(), 2);

    cache
        .resolve_with_registry(&registry, "schema-c")
        .expect("cache miss fetches schema-c and evicts LRU");
    assert_eq!(
        cache.len(),
        2,
        "cache should respect the configured capacity"
    );

    cache
        .resolve_with_registry(&registry, "schema-a")
        .expect("evicted schema reloads from registry");

    registry.block("schema-b").expect("schema-b exists");
    let err = cache
        .resolve_with_registry(&registry, "schema-b")
        .expect_err("blocked schema must fail");
    assert!(matches!(err, SchemaLookupError::Blocked { .. }));
    assert_eq!(err.status_reason(), ERR_SCHEMA_BLOCKED);

    let err = cache
        .resolve_with_registry(&registry, "schema-missing")
        .expect_err("unknown schema must error");
    assert!(matches!(err, SchemaLookupError::Unknown { .. }));
    assert_eq!(err.status_reason(), ERR_UNKNOWN_SCHEMA);
}

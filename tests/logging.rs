use ceptra::{JsonLineLogger, LogLevel, LogRotationPolicy};
use serde_json::Value;

#[test]
fn json_logger_serializes_entries() {
    let policy = LogRotationPolicy {
        max_bytes: 256,
        max_files: 2,
    };
    let mut logger = JsonLineLogger::new(policy);
    logger
        .log(100, LogLevel::Info, "ceptra::test", "p0", 1, "first entry")
        .unwrap();
    let lines: Vec<_> = logger
        .files()
        .flat_map(|file| file.lines().iter())
        .collect();
    assert_eq!(lines.len(), 1);
    let parsed: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed["level"], "INFO");
    assert_eq!(parsed["module"], "ceptra::test");
    assert_eq!(parsed["part_id"], "p0");
}

#[test]
fn loglevel_override_filters_entries() {
    let policy = LogRotationPolicy {
        max_bytes: 512,
        max_files: 1,
    };
    let mut logger = JsonLineLogger::new(policy);
    logger.set_level(LogLevel::Warn);
    logger
        .log(0, LogLevel::Info, "ceptra", "p0", 1, "info suppressed")
        .unwrap();
    logger
        .log(1, LogLevel::Warn, "ceptra", "p0", 2, "warn visible")
        .unwrap();
    let lines: Vec<_> = logger
        .files()
        .flat_map(|file| file.lines().iter())
        .collect();
    assert_eq!(lines.len(), 1);
    let parsed: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed["level"], "WARN");
    assert_eq!(parsed["message"], "warn visible");
}

#[test]
fn rotation_discards_old_segments() {
    let policy = LogRotationPolicy {
        max_bytes: 64,
        max_files: 2,
    };
    let mut logger = JsonLineLogger::new(policy);
    for idx in 0..10 {
        logger
            .log(0, LogLevel::Info, "module", "p0", idx, "payload")
            .unwrap();
    }
    let segments: Vec<_> = logger.files().collect();
    assert!(segments.len() <= 3, "active + rotated segments retained");
    assert!(segments.iter().any(|file| !file.lines().is_empty()));
}

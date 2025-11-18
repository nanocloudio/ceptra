use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::time::Instant;
use thiserror::Error;

type ConfigMap = Map<String, Value>;

/// Service responsible for tracking versioned configuration blobs.
#[derive(Debug, Clone)]
pub struct ConfigService {
    current_version: u64,
    current: ConfigMap,
    snapshots: BTreeMap<u64, ConfigMap>,
    knob_catalog: BTreeMap<String, ConfigKnobClass>,
    telemetry: ConfigTelemetry,
}

impl ConfigService {
    /// Creates a service seeded with the provided configuration blob.
    pub fn new(initial: Value) -> Result<Self, ConfigError> {
        let map = initial.as_object().cloned().ok_or_else(|| {
            ConfigError::InvalidPatch("initial config must be a JSON object".into())
        })?;
        let mut snapshots = BTreeMap::new();
        snapshots.insert(1, map.clone());
        Ok(Self {
            current_version: 1,
            current: map,
            snapshots,
            knob_catalog: default_knob_catalog(),
            telemetry: ConfigTelemetry::new(1),
        })
    }

    /// Returns the active config version.
    pub fn version(&self) -> u64 {
        self.current_version
    }

    /// Returns the current configuration blob.
    pub fn current_config(&self) -> Value {
        Value::Object(self.current.clone())
    }

    /// Returns telemetry counters for observability.
    pub fn telemetry(&self) -> &ConfigTelemetry {
        &self.telemetry
    }

    /// Applies a PATCH request to `/v1/config`.
    pub fn patch(&mut self, patch: Value) -> Result<ConfigPatchResult, ConfigError> {
        let start = Instant::now();
        let patch_map = patch
            .as_object()
            .cloned()
            .ok_or_else(|| self.invalid_patch("patch must be a JSON object"))?;
        let mut next = self.current.clone();
        let mut changed = BTreeMap::new();
        merge_map(&mut next, &patch_map, &mut changed);
        if changed.is_empty() {
            return Ok(ConfigPatchResult::no_change(self.current_version));
        }
        self.commit(next, changed.keys().cloned().collect(), start.elapsed())
    }

    /// Rolls the configuration back to a previous version and records a new snapshot.
    pub fn rollback(&mut self, to_version: u64) -> Result<ConfigPatchResult, ConfigError> {
        if to_version == self.current_version {
            self.validation_failure();
            return Err(ConfigError::UnknownVersion(to_version));
        }
        let snapshot = match self.snapshots.get(&to_version).cloned() {
            Some(snapshot) => snapshot,
            None => {
                self.validation_failure();
                return Err(ConfigError::UnknownVersion(to_version));
            }
        };
        let start = Instant::now();
        let changed_keys = diff_keys(&self.current, &snapshot);
        self.commit(snapshot, changed_keys, start.elapsed())
    }

    fn commit(
        &mut self,
        next: ConfigMap,
        mut changed_keys: Vec<String>,
        elapsed: std::time::Duration,
    ) -> Result<ConfigPatchResult, ConfigError> {
        self.current_version += 1;
        let impact = classify_change(&self.knob_catalog, &changed_keys);
        self.current = next.clone();
        self.snapshots.insert(self.current_version, next);
        changed_keys.sort();
        self.telemetry.version = self.current_version;
        self.telemetry.last_reload_duration_ms = millis(elapsed);
        Ok(ConfigPatchResult {
            version: self.current_version,
            impact,
            changed_keys,
            duration_ms: self.telemetry.last_reload_duration_ms,
        })
    }

    fn invalid_patch(&mut self, msg: &str) -> ConfigError {
        self.validation_failure();
        ConfigError::InvalidPatch(msg.to_string())
    }

    fn validation_failure(&mut self) {
        self.telemetry.validation_failures_total.saturating_inc();
    }
}

fn merge_map(base: &mut ConfigMap, patch: &ConfigMap, changed: &mut BTreeMap<String, ()>) {
    for (key, value) in patch {
        let entry = base.entry(key.clone()).or_insert(Value::Null);
        if entry != value {
            *entry = value.clone();
            changed.insert(key.clone(), ());
        }
    }
}

fn diff_keys(current: &ConfigMap, snapshot: &ConfigMap) -> Vec<String> {
    let mut keys = BTreeMap::new();
    for key in current.keys() {
        keys.insert(key.clone(), ());
    }
    for key in snapshot.keys() {
        keys.insert(key.clone(), ());
    }
    keys.into_keys()
        .filter(|key| current.get(key) != snapshot.get(key))
        .collect()
}

fn classify_change(
    catalog: &BTreeMap<String, ConfigKnobClass>,
    changed_keys: &[String],
) -> ConfigKnobClass {
    let mut impact = ConfigKnobClass::Hot;
    for key in changed_keys {
        let class = catalog.get(key).cloned().unwrap_or(ConfigKnobClass::Hot);
        impact = impact.max(class);
    }
    impact
}

fn default_knob_catalog() -> BTreeMap<String, ConfigKnobClass> {
    [
        ("lateness_allowance_L".to_string(), ConfigKnobClass::Hot),
        ("pane_size_q".to_string(), ConfigKnobClass::Hot),
        ("durability_mode".to_string(), ConfigKnobClass::Reconfigure),
        ("fsync_cadence".to_string(), ConfigKnobClass::Reconfigure),
        ("wal_path".to_string(), ConfigKnobClass::Restart),
        ("numa_affinity".to_string(), ConfigKnobClass::Restart),
    ]
    .into_iter()
    .collect()
}

fn millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Result of applying a patch or rollback.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigPatchResult {
    pub version: u64,
    pub impact: ConfigKnobClass,
    pub changed_keys: Vec<String>,
    pub duration_ms: u64,
}

impl ConfigPatchResult {
    fn no_change(version: u64) -> Self {
        Self {
            version,
            impact: ConfigKnobClass::Hot,
            changed_keys: Vec::new(),
            duration_ms: 0,
        }
    }
}

/// Class of configuration knob per §23.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, PartialOrd, Ord)]
pub enum ConfigKnobClass {
    Hot = 0,
    Reconfigure = 1,
    Restart = 2,
}

/// Telemetry counters exported via `/metrics`.
#[derive(Debug, Clone, Default)]
pub struct ConfigTelemetry {
    pub version: u64,
    pub last_reload_duration_ms: u64,
    pub validation_failures_total: u64,
}

impl ConfigTelemetry {
    fn new(version: u64) -> Self {
        Self {
            version,
            last_reload_duration_ms: 0,
            validation_failures_total: 0,
        }
    }
}

/// Errors surfaced by the config service.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid config patch: {0}")]
    InvalidPatch(String),
    #[error("unknown config version {0}")]
    UnknownVersion(u64),
}

trait SaturatingInc {
    fn saturating_inc(&mut self);
}

impl SaturatingInc for u64 {
    fn saturating_inc(&mut self) {
        *self = self.saturating_add(1);
    }
}

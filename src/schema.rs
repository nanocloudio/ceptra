use std::collections::{HashMap, VecDeque};
use std::fmt;

use thiserror::Error;

/// Maximum number of schemas cached per AP, per spec §18.2.
pub const SCHEMA_CACHE_CAPACITY: usize = 256;

/// Status reason emitted when a schema key is unknown.
pub const ERR_UNKNOWN_SCHEMA: &str = "ERR_UNKNOWN_SCHEMA";
/// Status reason emitted when a schema key was administratively blocked.
pub const ERR_SCHEMA_BLOCKED: &str = "ERR_SCHEMA_BLOCKED";

/// Supported payload schema formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    Protobuf,
    FlatBuffers,
}

impl SchemaFormat {
    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "proto" | "protobuf" => Some(SchemaFormat::Protobuf),
            "flatbuffers" | "fb" => Some(SchemaFormat::FlatBuffers),
            _ => None,
        }
    }
}

impl fmt::Display for SchemaFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaFormat::Protobuf => write!(f, "protobuf"),
            SchemaFormat::FlatBuffers => write!(f, "flatbuffers"),
        }
    }
}

/// Parsed schema identifier (`<format>:<major>.<minor>:<content_hash>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaId {
    pub format: SchemaFormat,
    pub major: u32,
    pub minor: u32,
    pub content_hash: String,
}

impl SchemaId {
    fn parse(raw: &str) -> Result<Self, SchemaRegistrationError> {
        let mut parts = raw.split(':');
        let format = parts
            .next()
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "missing format"))?;
        let version = parts
            .next()
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "missing version"))?;
        let hash = parts
            .next()
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "missing content hash"))?;
        if parts.next().is_some() {
            return Err(SchemaRegistrationError::invalid(
                raw,
                "schema_id contains too many segments",
            ));
        }
        let format = SchemaFormat::parse(format)
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "unknown schema format"))?;
        let mut version_parts = version.split('.');
        let major = version_parts
            .next()
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "missing major version"))?;
        let minor = version_parts
            .next()
            .ok_or_else(|| SchemaRegistrationError::invalid(raw, "missing minor version"))?;
        if version_parts.next().is_some() {
            return Err(SchemaRegistrationError::invalid(
                raw,
                "version must be <major>.<minor>",
            ));
        }
        let major = major
            .parse::<u32>()
            .map_err(|_| SchemaRegistrationError::invalid(raw, "invalid major version"))?;
        let minor = minor
            .parse::<u32>()
            .map_err(|_| SchemaRegistrationError::invalid(raw, "invalid minor version"))?;
        if hash.trim().is_empty() {
            return Err(SchemaRegistrationError::invalid(
                raw,
                "content hash must be non-empty",
            ));
        }
        Ok(SchemaId {
            format,
            major,
            minor,
            content_hash: hash.to_string(),
        })
    }
}

/// Registry entry mirrored to AP caches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDescriptor {
    pub schema_id: SchemaId,
    pub schema_key: String,
    pub blocked: bool,
}

impl SchemaDescriptor {
    pub fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }
}

/// Errors returned while registering schemas.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaRegistrationError {
    #[error("invalid schema_id '{schema_id}': {reason}")]
    InvalidSchemaId { schema_id: String, reason: String },
    #[error(
        "schema key '{schema_key}' change rejected: {reason} (existing {existing_major}.{existing_minor}, new {new_major}.{new_minor})"
    )]
    ChangeNotAllowed {
        schema_key: String,
        reason: String,
        existing_major: u32,
        existing_minor: u32,
        new_major: u32,
        new_minor: u32,
    },
}

impl SchemaRegistrationError {
    fn invalid(schema_id: &str, reason: &str) -> Self {
        SchemaRegistrationError::InvalidSchemaId {
            schema_id: schema_id.to_string(),
            reason: reason.to_string(),
        }
    }
}

/// Lookup errors surfaced to callers (mapped to `ERR_*` status reasons).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaLookupError {
    #[error("schema '{schema_key}' is unknown")]
    Unknown { schema_key: String },
    #[error("schema '{schema_key}' is blocked")]
    Blocked { schema_key: String },
}

impl SchemaLookupError {
    /// Returns the status reason string expected by clients.
    pub fn status_reason(&self) -> &'static str {
        match self {
            SchemaLookupError::Unknown { .. } => ERR_UNKNOWN_SCHEMA,
            SchemaLookupError::Blocked { .. } => ERR_SCHEMA_BLOCKED,
        }
    }
}

/// Control-plane registry used to validate schema evolution.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    entries: HashMap<String, SchemaDescriptor>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers or updates a schema key with the provided `schema_id`.
    ///
    /// Allowed change matrix (per §18.2):
    /// * Minor bumps on the same major version are accepted.
    /// * Major changes require a brand new `schema_key`.
    /// * Downgrades (lower major/minor) are rejected.
    pub fn register(
        &mut self,
        schema_id: &str,
        schema_key: impl Into<String>,
    ) -> Result<SchemaDescriptor, SchemaRegistrationError> {
        let key = schema_key.into();
        let parsed = SchemaId::parse(schema_id)?;
        match self.entries.get_mut(&key) {
            Some(entry) => {
                if entry.schema_id == parsed {
                    entry.blocked = false;
                    return Ok(entry.clone());
                }
                let reason = if entry.schema_id.format != parsed.format {
                    "format mismatch; use a new schema_key"
                } else if parsed.major < entry.schema_id.major {
                    "major version downgrade not allowed"
                } else if parsed.major > entry.schema_id.major {
                    "major version bumps require a new schema_key"
                } else if parsed.minor < entry.schema_id.minor {
                    "minor version downgrade not allowed"
                } else if parsed.minor == entry.schema_id.minor
                    && parsed.content_hash != entry.schema_id.content_hash
                {
                    "schema content changed without version bump"
                } else {
                    ""
                };
                if reason.is_empty() {
                    entry.schema_id = parsed.clone();
                    entry.blocked = false;
                    return Ok(entry.clone());
                }
                Err(SchemaRegistrationError::ChangeNotAllowed {
                    schema_key: key,
                    reason: reason.to_string(),
                    existing_major: entry.schema_id.major,
                    existing_minor: entry.schema_id.minor,
                    new_major: parsed.major,
                    new_minor: parsed.minor,
                })
            }
            None => {
                let descriptor = SchemaDescriptor {
                    schema_id: parsed,
                    schema_key: key.clone(),
                    blocked: false,
                };
                self.entries.insert(key.clone(), descriptor.clone());
                Ok(descriptor)
            }
        }
    }

    /// Blocks an existing schema key (future lookups will resolve to `SchemaLookupError::Blocked`).
    pub fn block(&mut self, schema_key: &str) -> Result<(), SchemaLookupError> {
        let entry = self
            .entries
            .get_mut(schema_key)
            .ok_or_else(|| SchemaLookupError::Unknown {
                schema_key: schema_key.to_string(),
            })?;
        entry.blocked = true;
        Ok(())
    }

    /// Returns the descriptor for a schema key, rejecting blocked entries.
    pub fn lookup(&self, schema_key: &str) -> Result<SchemaDescriptor, SchemaLookupError> {
        let entry = self
            .entries
            .get(schema_key)
            .ok_or_else(|| SchemaLookupError::Unknown {
                schema_key: schema_key.to_string(),
            })?;
        if entry.blocked {
            return Err(SchemaLookupError::Blocked {
                schema_key: schema_key.to_string(),
            });
        }
        Ok(entry.clone())
    }
}

/// AP-local cache that mirrors registry entries with LRU eviction.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    capacity: usize,
    entries: HashMap<String, SchemaDescriptor>,
    lru: VecDeque<String>,
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new(SCHEMA_CACHE_CAPACITY)
    }
}

impl SchemaCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Current number of cached schema entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true when no schemas are cached.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolves a schema key, fetching from the provided closure on cache misses.
    pub fn resolve_with<F>(
        &mut self,
        schema_key: &str,
        fetcher: F,
    ) -> Result<&SchemaDescriptor, SchemaLookupError>
    where
        F: FnOnce(&str) -> Result<SchemaDescriptor, SchemaLookupError>,
    {
        if self.entries.contains_key(schema_key) {
            self.touch(schema_key);
            return Ok(self.entries.get(schema_key).expect("entry exists"));
        }
        let descriptor = fetcher(schema_key)?;
        let key = descriptor.schema_key.clone();
        self.entries.insert(key.clone(), descriptor);
        self.lru.push_front(key.clone());
        self.enforce_capacity();
        Ok(self.entries.get(&key).expect("entry inserted"))
    }

    /// Convenience helper backed by a `SchemaRegistry`.
    pub fn resolve_with_registry(
        &mut self,
        registry: &SchemaRegistry,
        schema_key: &str,
    ) -> Result<&SchemaDescriptor, SchemaLookupError> {
        self.resolve_with(schema_key, |key| registry.lookup(key))
    }

    fn touch(&mut self, schema_key: &str) {
        if let Some(pos) = self.lru.iter().position(|existing| existing == schema_key) {
            self.lru.remove(pos);
        }
        self.lru.push_front(schema_key.to_string());
    }

    fn enforce_capacity(&mut self) {
        while self.entries.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_back() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }
    }
}

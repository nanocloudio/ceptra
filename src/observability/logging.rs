use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;
use thiserror::Error;

/// Severity levels exposed by `/v1/loglevel` overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Returns the canonical uppercase representation.
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Rotation policy (default mirrors 1 GiB × 10 files).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRotationPolicy {
    pub max_bytes: usize,
    pub max_files: usize,
}

impl Default for LogRotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 1 << 30,
            max_files: 10,
        }
    }
}

/// Accumulated log lines for a rotated file.
#[derive(Debug, Default, Clone)]
pub struct LogFile {
    lines: Vec<String>,
    bytes_written: usize,
}

impl LogFile {
    /// Lines contained within the log segment.
    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    /// Total bytes recorded before rotation.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

/// JSON-line logger with deterministic rotation semantics.
#[derive(Debug, Clone)]
pub struct JsonLineLogger {
    policy: LogRotationPolicy,
    current_level: LogLevel,
    files: VecDeque<LogFile>,
    active: LogFile,
}

impl JsonLineLogger {
    /// Creates a logger anchored to the provided rotation policy.
    pub fn new(policy: LogRotationPolicy) -> Self {
        Self {
            policy,
            current_level: LogLevel::Info,
            files: VecDeque::new(),
            active: LogFile::default(),
        }
    }

    /// Returns the current log level (used by `/v1/loglevel`).
    pub fn level(&self) -> LogLevel {
        self.current_level
    }

    /// Applies a dynamic log-level override.
    pub fn set_level(&mut self, level: LogLevel) {
        self.current_level = level;
    }

    /// Emits a JSON-line log entry.
    pub fn log(
        &mut self,
        ts_ms: u64,
        level: LogLevel,
        module: &str,
        part_id: &str,
        log_index: u64,
        message: &str,
    ) -> Result<(), LoggingError> {
        if level < self.current_level {
            return Ok(());
        }
        let record = LogRecord {
            ts: ts_ms,
            level: level.as_str(),
            module,
            part_id,
            log_index,
            message,
        };
        let line = serde_json::to_string(&record).map_err(LoggingError::Serialize)?;
        self.rotate_if_needed(line.len());
        self.active.bytes_written = self.active.bytes_written.saturating_add(line.len());
        self.active.lines.push(line);
        Ok(())
    }

    /// Returns the current file + rotated history.
    pub fn files(&self) -> impl Iterator<Item = &LogFile> {
        self.files.iter().chain(std::iter::once(&self.active))
    }

    fn rotate_if_needed(&mut self, next_line_len: usize) {
        if self.active.bytes_written + next_line_len <= self.policy.max_bytes {
            return;
        }
        if !self.active.lines.is_empty() {
            self.files.push_back(std::mem::take(&mut self.active));
            while self.files.len() > self.policy.max_files {
                self.files.pop_front();
            }
        }
        self.active = LogFile::default();
    }
}

/// Errors surfaced while serializing JSON-line logs.
#[derive(Debug, Error)]
pub enum LoggingError {
    #[error("failed to serialize log record: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
struct LogRecord<'a> {
    ts: u64,
    level: &'a str,
    module: &'a str,
    part_id: &'a str,
    log_index: u64,
    message: &'a str,
}

use crate::LANE_OVERFLOW_MARKER;
use clustor::{CreditHint, IngestStatusCode};
use std::fmt;
use std::time::Instant;
use thiserror::Error;

/// Default CEP-specific status reason for append failures.
pub const CEP_STATUS_REASON: &str = "ceptra_append_failure";

/// CEP-specific credit hint surfaced to clients alongside the Clustor hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CepCreditHint {
    /// Suggested client window size.
    pub client_window_size: u32,
    /// Optional retry backoff (ms).
    pub retry_backoff_ms: u32,
}

/// Hints provided by the client on every append RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHints {
    pub credit_hint: CreditHint,
    pub cep_hint: Option<CepCreditHint>,
}

impl Default for ClientHints {
    fn default() -> Self {
        Self {
            credit_hint: CreditHint::Recover,
            cep_hint: None,
        }
    }
}

/// CEP-extended append request mirroring the proto schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub event_id: String,
    pub partition_key: Vec<u8>,
    pub schema_key: String,
    pub payload: Vec<u8>,
    pub client_hints: ClientHints,
}

/// CEP-extended append response carrying credit hints and status reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub event_id: String,
    pub status: IngestStatusCode,
    pub credit_hint: CreditHint,
    pub cep_credit_hint: Option<CepCreditHint>,
    pub cep_status_reason: Option<String>,
    pub term: Option<u64>,
    pub index: Option<u64>,
}

/// Configure retry and transport behaviour for the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOptions {
    pub max_retries: usize,
}

impl Default for AppendOptions {
    fn default() -> Self {
        Self { max_retries: 3 }
    }
}

/// Transport trait representing the underlying gRPC stub.
pub trait AppendTransport {
    fn send(&mut self, request: AppendRequest) -> Result<AppendResponse, AppendTransportError>;
}

/// Transport-level error returned when the RPC fails.
#[derive(Debug, Clone)]
pub struct AppendTransportError {
    message: String,
}

impl AppendTransportError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AppendTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AppendTransportError {}

/// Client-side error once retries are exhausted or transport fails permanently.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppendError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("append failed after {attempts} attempts (reason: {reason:?})")]
    Exhausted {
        attempts: usize,
        reason: Option<String>,
    },
}

/// CEP client wrapper that tracks credit hints and retries with stable event IDs.
pub struct CepAppendClient<T: AppendTransport> {
    transport: T,
    last_credit_hint: CreditHint,
    last_cep_hint: Option<CepCreditHint>,
    telemetry: AppendTelemetry,
}

impl<T: AppendTransport> CepAppendClient<T> {
    /// Creates a new client with default hint state.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            last_credit_hint: CreditHint::Recover,
            last_cep_hint: None,
            telemetry: AppendTelemetry::default(),
        }
    }

    /// Returns the recorded telemetry, including spans/logs and metrics.
    pub fn telemetry(&self) -> &AppendTelemetry {
        &self.telemetry
    }

    /// Issues an append RPC, retrying transient failures with a stable `event_id`.
    pub fn append_with_retry(
        &mut self,
        event_id: impl Into<String>,
        partition_key: &[u8],
        schema_key: impl Into<String>,
        payload: &[u8],
        options: AppendOptions,
    ) -> Result<AppendResponse, AppendError> {
        let event_id = event_id.into();
        let partition_key = partition_key.to_vec();
        let schema_key = schema_key.into();
        let payload = payload.to_vec();
        let mut attempts = 0usize;
        loop {
            let request = AppendRequest {
                event_id: event_id.clone(),
                partition_key: partition_key.clone(),
                schema_key: schema_key.clone(),
                payload: payload.clone(),
                client_hints: ClientHints {
                    credit_hint: self.last_credit_hint,
                    cep_hint: self.last_cep_hint,
                },
            };
            let start = Instant::now();
            self.telemetry.record_log(AppendLog {
                event_id: request.event_id.clone(),
                attempt: attempts + 1,
                message: "append_attempt_start".into(),
            });
            let response = self
                .transport
                .send(request)
                .map_err(|err| AppendError::Transport(err.to_string()))?;
            self.last_credit_hint = response.credit_hint;
            self.last_cep_hint = response.cep_credit_hint;
            let duration_ms = start.elapsed().as_millis() as u64;
            self.telemetry.record_span(AppendSpan {
                event_id: response.event_id.clone(),
                attempt: attempts + 1,
                duration_ms,
                status: response.status,
                cep_status_reason: response.cep_status_reason.clone(),
                term: response.term,
                index: response.index,
            });
            self.telemetry.record_log(AppendLog {
                event_id: response.event_id.clone(),
                attempt: attempts + 1,
                message: format!("append_response status={:?}", response.status),
            });
            if let Some(hint) = self.last_cep_hint {
                self.telemetry.metrics.client_window_size_ms = hint.client_window_size as u64;
            }
            if attempts > 0 {
                self.telemetry.metrics.append_retry_total_ms += 1;
            }
            if response
                .cep_status_reason
                .as_deref()
                .map(|reason| reason == LANE_OVERFLOW_MARKER)
                .unwrap_or(false)
            {
                self.telemetry.metrics.reject_overload_total_ms += 1;
            }
            match response.status {
                IngestStatusCode::Healthy => return Ok(response),
                IngestStatusCode::TransientBackpressure if attempts < options.max_retries => {
                    attempts += 1;
                    continue;
                }
                _ => {
                    return Err(AppendError::Exhausted {
                        attempts: attempts + 1,
                        reason: response.cep_status_reason.clone(),
                    });
                }
            }
        }
    }
}

/// Aggregated telemetry exported to OTLP/Prometheus sinks.
#[derive(Debug, Default, Clone)]
pub struct AppendTelemetry {
    spans: Vec<AppendSpan>,
    logs: Vec<AppendLog>,
    metrics: AppendMetrics,
}

impl AppendTelemetry {
    /// Recorded append spans.
    pub fn spans(&self) -> &[AppendSpan] {
        &self.spans
    }

    /// Structured logs emitted around the append hot-path.
    pub fn logs(&self) -> &[AppendLog] {
        &self.logs
    }

    /// Current metrics snapshot.
    pub fn metrics(&self) -> &AppendMetrics {
        &self.metrics
    }

    fn record_span(&mut self, span: AppendSpan) {
        self.spans.push(span);
    }

    fn record_log(&mut self, log: AppendLog) {
        self.logs.push(log);
    }

    /// Renders metrics as Prometheus exposition text with `_ms` suffixes.
    pub fn render_metrics(&self) -> String {
        format!(
            "ceptra_client_window_size_ms {}\nceptra_reject_overload_total_ms {}\nceptra_append_retry_total_ms {}\n",
            self.metrics.client_window_size_ms,
            self.metrics.reject_overload_total_ms,
            self.metrics.append_retry_total_ms
        )
    }
}

/// Individual append span suitable for OTLP exporters.
#[derive(Debug, Clone)]
pub struct AppendSpan {
    pub event_id: String,
    pub attempt: usize,
    pub duration_ms: u64,
    pub status: IngestStatusCode,
    pub cep_status_reason: Option<String>,
    pub term: Option<u64>,
    pub index: Option<u64>,
}

/// Structured log emitted for append attempts.
#[derive(Debug, Clone)]
pub struct AppendLog {
    pub event_id: String,
    pub attempt: usize,
    pub message: String,
}

/// Metrics exposed via `/metrics`.
#[derive(Debug, Default, Clone)]
pub struct AppendMetrics {
    pub client_window_size_ms: u64,
    pub reject_overload_total_ms: u64,
    pub append_retry_total_ms: u64,
}

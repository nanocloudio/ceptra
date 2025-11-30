use crate::{AppendRequest, AppendResponse, AppendTransport, AppendTransportError, CepCreditHint};
use base64::{engine::general_purpose, Engine as _};
use clustor::{CreditHint, IngestStatusCode};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

const APPEND_PATH: &str = "/append";

/// Blocking HTTP transport that forwards append requests to the Clustor ingest
/// endpoint and translates the response into the CEP client contract.
#[derive(Debug, Clone)]
pub struct ClustorAppendTransport {
    client: Client,
    endpoint: String,
}

impl ClustorAppendTransport {
    /// Creates a transport targeting the provided base endpoint (e.g.
    /// `https://node-0.cluster.local:9443`).
    pub fn new(endpoint: impl Into<String>) -> Result<Self, AppendTransportError> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(AppendTransportError::new(
                "append endpoint must not be empty",
            ));
        }
        let client = Client::builder()
            .build()
            .map_err(|err| AppendTransportError::new(format!("http client build failed: {err}")))?;
        Ok(Self { client, endpoint })
    }

    fn append_url(&self) -> String {
        format!("{}{}", self.endpoint.trim_end_matches('/'), APPEND_PATH)
    }
}

impl AppendTransport for ClustorAppendTransport {
    fn send(&mut self, request: AppendRequest) -> Result<AppendResponse, AppendTransportError> {
        let wire_request = WireAppendRequest::from(request);
        let response = self
            .client
            .post(self.append_url())
            .json(&wire_request)
            .send()
            .map_err(|err| AppendTransportError::new(format!("append rpc failed: {err}")))?;
        if !response.status().is_success() {
            return Err(AppendTransportError::new(format!(
                "append rpc returned status {}",
                response.status()
            )));
        }
        let wire: WireAppendResponse = response
            .json()
            .map_err(|err| AppendTransportError::new(format!("append rpc decode failed: {err}")))?;
        Ok(wire.into())
    }
}

#[derive(Debug, Serialize)]
struct WireAppendRequest {
    event_id: String,
    partition_key_b64: String,
    schema_key: String,
    payload_b64: String,
    credit_hint: CreditHint,
    #[serde(skip_serializing_if = "Option::is_none")]
    cep_credit_hint: Option<WireCepCreditHint>,
}

impl From<AppendRequest> for WireAppendRequest {
    fn from(request: AppendRequest) -> Self {
        Self {
            event_id: request.event_id,
            partition_key_b64: general_purpose::STANDARD.encode(request.partition_key),
            schema_key: request.schema_key,
            payload_b64: general_purpose::STANDARD.encode(request.payload),
            credit_hint: request.client_hints.credit_hint,
            cep_credit_hint: request.client_hints.cep_hint.map(WireCepCreditHint::from),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireCepCreditHint {
    client_window_size: u32,
    retry_backoff_ms: u32,
}

impl From<CepCreditHint> for WireCepCreditHint {
    fn from(hint: CepCreditHint) -> Self {
        Self {
            client_window_size: hint.client_window_size,
            retry_backoff_ms: hint.retry_backoff_ms,
        }
    }
}

impl From<WireCepCreditHint> for CepCreditHint {
    fn from(hint: WireCepCreditHint) -> Self {
        Self {
            client_window_size: hint.client_window_size,
            retry_backoff_ms: hint.retry_backoff_ms,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WireAppendResponse {
    event_id: String,
    status: IngestStatusCode,
    credit_hint: CreditHint,
    #[serde(default)]
    cep_credit_hint: Option<WireCepCreditHint>,
    #[serde(default)]
    cep_status_reason: Option<String>,
    #[serde(default)]
    term: Option<u64>,
    #[serde(default)]
    index: Option<u64>,
}

impl From<WireAppendResponse> for AppendResponse {
    fn from(wire: WireAppendResponse) -> Self {
        Self {
            event_id: wire.event_id,
            status: wire.status,
            credit_hint: wire.credit_hint,
            cep_credit_hint: wire.cep_credit_hint.map(CepCreditHint::from),
            cep_status_reason: wire.cep_status_reason,
            term: wire.term,
            index: wire.index,
        }
    }
}

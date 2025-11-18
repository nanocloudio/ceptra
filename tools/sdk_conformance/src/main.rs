use anyhow::{anyhow, bail, Context, Result};
use ceptra::{
    consensus_core::{CreditHint, IngestStatusCode},
    AppendError, AppendOptions, AppendRequest, AppendResponse, AppendTransport,
    AppendTransportError, CepAppendClient, CepCreditHint,
};
use serde::Deserialize;
use std::cell::RefCell;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

fn main() -> Result<()> {
    let args = Args::parse()?;
    let payload = fs::read_to_string(&args.scenarios)
        .with_context(|| format!("unable to read scenario file {}", args.scenarios.display()))?;
    let scenarios: Vec<Scenario> = serde_json::from_str(&payload)
        .with_context(|| format!("invalid scenario file {}", args.scenarios.display()))?;
    if scenarios.is_empty() {
        bail!("no scenarios declared in {}", args.scenarios.display());
    }
    let mut failures = 0usize;
    for scenario in scenarios {
        match run_scenario(&scenario) {
            Ok(_) => println!("scenario '{}' ... ok", scenario.name),
            Err(err) => {
                failures += 1;
                eprintln!("scenario '{}' ... FAILED:\n  {:#}", scenario.name, err);
            }
        }
    }
    if failures > 0 {
        bail!("{} scenario(s) failed", failures);
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    scenarios: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = env::args().skip(1);
        let mut scenarios = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scenarios" => {
                    let path = args
                        .next()
                        .ok_or_else(|| anyhow!("--scenarios requires a path to a JSON file"))?;
                    scenarios = Some(PathBuf::from(path));
                }
                "--help" | "-h" => {
                    println!(
                        "usage: sdk_conformance [--scenarios <path>]\n\n\
Default scenario file: tools/sdk_conformance/scenarios/throttles.json"
                    );
                    std::process::exit(0);
                }
                other => return Err(anyhow!("unknown argument: {other}")),
            }
        }
        let default_path = PathBuf::from("tools/sdk_conformance/scenarios/throttles.json");
        Ok(Self {
            scenarios: scenarios.unwrap_or(default_path),
        })
    }
}

#[derive(Debug, Deserialize)]
struct Scenario {
    name: String,
    #[serde(default = "default_event_id")]
    event_id: String,
    #[serde(default = "default_partition_key")]
    partition_key: String,
    #[serde(default = "default_payload")]
    payload: String,
    #[serde(default = "default_schema_key")]
    schema_key: String,
    #[serde(default)]
    max_retries: Option<usize>,
    responses: Vec<ScriptedResponse>,
    expect: ScenarioExpect,
}

#[derive(Debug, Deserialize)]
struct ScriptedResponse {
    status: String,
    credit_hint: String,
    #[serde(default)]
    cep_credit_hint: Option<CepHintSpec>,
    #[serde(default)]
    cep_status_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CepHintSpec {
    client_window_size: u32,
    retry_backoff_ms: u32,
}

#[derive(Debug, Default, Deserialize)]
struct ScenarioExpect {
    attempts: usize,
    #[serde(default = "default_true")]
    success: bool,
    #[serde(default)]
    final_status: Option<String>,
    #[serde(default)]
    error_reason: Option<String>,
    #[serde(default)]
    recorded_credit_hints: Vec<String>,
    #[serde(default)]
    recorded_client_windows: Vec<Option<u32>>,
    #[serde(default)]
    metrics: Option<ExpectedMetrics>,
}

#[derive(Debug, Default, Deserialize)]
struct ExpectedMetrics {
    #[serde(default)]
    client_window_size_ms: Option<u64>,
    #[serde(default)]
    reject_overload_total_ms: Option<u64>,
    #[serde(default)]
    append_retry_total_ms: Option<u64>,
}

fn run_scenario(scenario: &Scenario) -> Result<()> {
    if scenario.responses.is_empty() {
        bail!("scenario '{}' declares no responses", scenario.name);
    }
    if scenario.expect.attempts == 0 {
        bail!("scenario '{}' must set expect.attempts > 0", scenario.name);
    }
    let responses = scenario
        .responses
        .iter()
        .enumerate()
        .map(|(idx, spec)| {
            spec.to_append_response(&scenario.event_id)
                .with_context(|| format!("scenario '{}', response #{idx}", scenario.name))
        })
        .collect::<Result<Vec<_>>>()?;
    let transport = ScriptedTransport::new(responses);
    let state = transport.state();
    let mut client = CepAppendClient::new(transport);
    let max_retries = scenario
        .max_retries
        .unwrap_or_else(|| scenario.responses.len().saturating_sub(1));
    let result = client.append_with_retry(
        scenario.event_id.clone(),
        scenario.partition_key.as_bytes(),
        scenario.schema_key.clone(),
        scenario.payload.as_bytes(),
        AppendOptions { max_retries },
    );
    if scenario.expect.success {
        let response = result.with_context(|| "expected append to succeed")?;
        if let Some(expected_status) = &scenario.expect.final_status {
            let expected = parse_status(expected_status)?;
            anyhow::ensure!(
                response.status == expected,
                "expected final status {:?} got {:?}",
                expected,
                response.status
            );
        }
    } else {
        match result {
            Ok(resp) => bail!(
                "expected failure but append succeeded with status {:?}",
                resp.status
            ),
            Err(AppendError::Exhausted { reason, .. }) => {
                if let Some(expected_reason) = &scenario.expect.error_reason {
                    anyhow::ensure!(
                        reason.as_deref() == Some(expected_reason.as_str()),
                        "expected reason {:?} got {:?}",
                        expected_reason,
                        reason
                    );
                }
            }
            Err(other) => bail!("unexpected error {other:?}"),
        }
    }

    let recorded = state.borrow();
    anyhow::ensure!(
        recorded.recorded.len() == scenario.expect.attempts,
        "expected {} attempts but observed {}",
        scenario.expect.attempts,
        recorded.recorded.len()
    );

    if !scenario.expect.recorded_credit_hints.is_empty() {
        anyhow::ensure!(
            scenario.expect.recorded_credit_hints.len() == recorded.recorded.len(),
            "recorded_credit_hints length does not match attempts"
        );
        for (idx, expected) in scenario.expect.recorded_credit_hints.iter().enumerate() {
            let expected_hint = parse_credit_hint(expected)?;
            let actual = recorded.recorded[idx].client_hints.credit_hint;
            anyhow::ensure!(
                actual == expected_hint,
                "request {idx} expected credit hint {:?} got {:?}",
                expected_hint,
                actual
            );
        }
    }

    if !scenario.expect.recorded_client_windows.is_empty() {
        anyhow::ensure!(
            scenario.expect.recorded_client_windows.len() == recorded.recorded.len(),
            "recorded_client_windows length does not match attempts"
        );
        for (idx, expected) in scenario.expect.recorded_client_windows.iter().enumerate() {
            let actual = recorded.recorded[idx]
                .client_hints
                .cep_hint
                .map(|hint| hint.client_window_size);
            match expected {
                Some(size) => anyhow::ensure!(
                    actual == Some(*size),
                    "request {idx} expected cep_hint.client_window_size {:?} got {:?}",
                    size,
                    actual
                ),
                None => anyhow::ensure!(
                    actual.is_none(),
                    "request {idx} expected no cep_hint but observed {:?}",
                    actual
                ),
            }
        }
    }

    if let Some(metrics) = &scenario.expect.metrics {
        let snapshot = client.telemetry().metrics().clone();
        if let Some(expected) = metrics.client_window_size_ms {
            anyhow::ensure!(
                snapshot.client_window_size_ms == expected,
                "metric client_window_size_ms expected {} got {}",
                expected,
                snapshot.client_window_size_ms
            );
        }
        if let Some(expected) = metrics.reject_overload_total_ms {
            anyhow::ensure!(
                snapshot.reject_overload_total_ms == expected,
                "metric reject_overload_total_ms expected {} got {}",
                expected,
                snapshot.reject_overload_total_ms
            );
        }
        if let Some(expected) = metrics.append_retry_total_ms {
            anyhow::ensure!(
                snapshot.append_retry_total_ms == expected,
                "metric append_retry_total_ms expected {} got {}",
                expected,
                snapshot.append_retry_total_ms
            );
        }
    }

    Ok(())
}

impl ScriptedResponse {
    fn to_append_response(&self, event_id: &str) -> Result<AppendResponse> {
        let status = parse_status(&self.status)?;
        let credit_hint = parse_credit_hint(&self.credit_hint)?;
        let cep_hint = self.cep_credit_hint.as_ref().map(|spec| CepCreditHint {
            client_window_size: spec.client_window_size,
            retry_backoff_ms: spec.retry_backoff_ms,
        });
        Ok(AppendResponse {
            event_id: event_id.to_string(),
            status,
            credit_hint,
            cep_credit_hint: cep_hint,
            cep_status_reason: self.cep_status_reason.clone(),
        })
    }
}

fn parse_status(raw: &str) -> Result<IngestStatusCode> {
    match raw.to_ascii_lowercase().as_str() {
        "healthy" => Ok(IngestStatusCode::Healthy),
        "transientbackpressure" | "transient_backpressure" => {
            Ok(IngestStatusCode::TransientBackpressure)
        }
        "permanentdurability" | "permanent_durability" => Ok(IngestStatusCode::PermanentDurability),
        other => Err(anyhow!("unknown status '{other}'")),
    }
}

fn parse_credit_hint(raw: &str) -> Result<CreditHint> {
    match raw.to_ascii_lowercase().as_str() {
        "recover" => Ok(CreditHint::Recover),
        "hold" => Ok(CreditHint::Hold),
        "shed" => Ok(CreditHint::Shed),
        other => Err(anyhow!("unknown credit hint '{other}'")),
    }
}

#[derive(Debug)]
struct MockState {
    responses: Vec<AppendResponse>,
    cursor: usize,
    recorded: Vec<AppendRequest>,
}

struct ScriptedTransport {
    state: Rc<RefCell<MockState>>,
}

impl ScriptedTransport {
    fn new(responses: Vec<AppendResponse>) -> Self {
        Self {
            state: Rc::new(RefCell::new(MockState {
                responses,
                cursor: 0,
                recorded: Vec::new(),
            })),
        }
    }

    fn state(&self) -> Rc<RefCell<MockState>> {
        Rc::clone(&self.state)
    }
}

impl AppendTransport for ScriptedTransport {
    fn send(&mut self, request: AppendRequest) -> Result<AppendResponse, AppendTransportError> {
        let mut state = self.state.borrow_mut();
        if state.cursor >= state.responses.len() {
            return Err(AppendTransportError::new("scripted responses exhausted"));
        }
        state.recorded.push(request);
        let response = state.responses[state.cursor].clone();
        state.cursor += 1;
        Ok(response)
    }
}

fn default_event_id() -> String {
    "sdk-scenario".to_string()
}

fn default_partition_key() -> String {
    "partition-a".to_string()
}

fn default_payload() -> String {
    "payload".to_string()
}

fn default_schema_key() -> String {
    "schema.default".to_string()
}

fn default_true() -> bool {
    true
}

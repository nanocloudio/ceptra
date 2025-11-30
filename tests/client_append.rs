use ceptra::{
    AppendError, AppendOptions, AppendRequest, AppendResponse, AppendTransport,
    AppendTransportError, CepAppendClient, CepCreditHint,
};
use clustor::{CreditHint, IngestStatusCode};
use std::cell::RefCell;
use std::rc::Rc;

struct MockState {
    responses: Vec<Result<AppendResponse, AppendTransportError>>,
    recorded: Vec<AppendRequest>,
}

#[derive(Clone)]
struct MockTransport {
    state: Rc<RefCell<MockState>>,
}

impl AppendTransport for MockTransport {
    fn send(&mut self, request: AppendRequest) -> Result<AppendResponse, AppendTransportError> {
        let mut state = self.state.borrow_mut();
        state.recorded.push(request);
        state
            .responses
            .remove(0)
            .map_err(|err| AppendTransportError::new(err.to_string()))
    }
}

fn mock_transport(
    responses: Vec<Result<AppendResponse, AppendTransportError>>,
) -> (MockTransport, Rc<RefCell<MockState>>) {
    let state = Rc::new(RefCell::new(MockState {
        responses,
        recorded: Vec::new(),
    }));
    (
        MockTransport {
            state: state.clone(),
        },
        state,
    )
}

#[test]
fn client_retries_with_stable_event_id_and_hint_echo() {
    let first = AppendResponse {
        event_id: "evt-1".into(),
        status: IngestStatusCode::TransientBackpressure,
        credit_hint: CreditHint::Hold,
        cep_credit_hint: Some(CepCreditHint {
            client_window_size: 32,
            retry_backoff_ms: 50,
        }),
        cep_status_reason: Some("LANE_OVERFLOW".into()),
        term: None,
        index: None,
    };
    let second = AppendResponse {
        event_id: "evt-1".into(),
        status: IngestStatusCode::Healthy,
        credit_hint: CreditHint::Recover,
        cep_credit_hint: Some(CepCreditHint {
            client_window_size: 64,
            retry_backoff_ms: 0,
        }),
        cep_status_reason: None,
        term: None,
        index: None,
    };
    let responses = vec![Ok(first), Ok(second)];
    let (transport, state) = mock_transport(responses);
    let mut client = CepAppendClient::new(transport);
    let result = client
        .append_with_retry(
            "evt-1",
            b"partition-a",
            "schema.append",
            b"payload",
            AppendOptions { max_retries: 2 },
        )
        .expect("append should succeed");
    assert_eq!(result.status, IngestStatusCode::Healthy);
    let recorded = state.borrow().recorded.clone();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].event_id, "evt-1");
    assert_eq!(recorded[1].event_id, "evt-1");
    assert_eq!(recorded[0].client_hints.credit_hint, CreditHint::Recover);
    assert_eq!(recorded[1].client_hints.credit_hint, CreditHint::Hold);
    assert_eq!(
        recorded[1].client_hints.cep_hint,
        Some(CepCreditHint {
            client_window_size: 32,
            retry_backoff_ms: 50
        })
    );
    let telemetry = client.telemetry();
    assert_eq!(telemetry.spans().len(), 2);
    assert_eq!(telemetry.logs().len(), 4);
    assert_eq!(telemetry.metrics().append_retry_total_ms, 1);
    let metrics = telemetry.render_metrics();
    assert!(metrics.contains("ceptra_client_window_size_ms"));
}

#[test]
fn exhausts_retries_and_surfaces_reason() {
    let response = AppendResponse {
        event_id: "evt-2".into(),
        status: IngestStatusCode::TransientBackpressure,
        credit_hint: CreditHint::Hold,
        cep_credit_hint: None,
        cep_status_reason: Some("LANE_OVERFLOW".into()),
        term: None,
        index: None,
    };
    let responses = vec![Ok(response.clone()), Ok(response)];
    let (transport, _) = mock_transport(responses);
    let mut client = CepAppendClient::new(transport);
    let err = client
        .append_with_retry(
            "evt-2",
            b"partition-a",
            "schema.append",
            b"payload",
            AppendOptions { max_retries: 1 },
        )
        .expect_err("should exhaust retries");
    match err {
        AppendError::Exhausted { attempts, reason } => {
            assert_eq!(attempts, 2);
            assert_eq!(reason, Some("LANE_OVERFLOW".into()));
        }
        other => panic!("unexpected error {other:?}"),
    }
}

#[test]
fn propagates_transport_errors() {
    let responses = vec![Err(AppendTransportError::new("grpc unavailable"))];
    let (transport, _) = mock_transport(responses);
    let mut client = CepAppendClient::new(transport);
    let err = client
        .append_with_retry(
            "evt-3",
            b"partition",
            "schema.append",
            b"payload",
            AppendOptions::default(),
        )
        .expect_err("transport should fail");
    assert!(matches!(err, AppendError::Transport(message) if message.contains("grpc unavailable")));
}

use ceptra::{evaluate, AckDisposition, AckSignals, PERMANENT_DURABILITY_REASON};
use clustor::{
    DurabilityMode, GateOperation, GateViolation, StrictFallbackBlockingReason,
    StrictFallbackState, StrictFallbackWhy,
};
use std::time::Duration;

fn strict_state(state: StrictFallbackState) -> AckSignals<'static> {
    AckSignals::new(DurabilityMode::Strict, state)
}

#[test]
fn strict_mode_blocks_during_local_only() {
    match evaluate(strict_state(StrictFallbackState::LocalOnly)) {
        AckDisposition::Blocked(rejection) => {
            assert_eq!(rejection.status_reason, PERMANENT_DURABILITY_REASON);
        }
        other => panic!("expected rejection, got {other:?}"),
    }
}

#[test]
fn relaxed_mode_allows_when_not_fenced() {
    let signals = AckSignals::new(DurabilityMode::Relaxed, StrictFallbackState::LocalOnly);
    assert!(matches!(evaluate(signals), AckDisposition::Allowed));
}

#[test]
fn fence_flag_blocks_even_when_state_healthy() {
    let signals =
        AckSignals::new(DurabilityMode::Relaxed, StrictFallbackState::Healthy).with_fence(true);
    assert!(matches!(evaluate(signals), AckDisposition::Blocked(_)));
}

#[test]
fn rejection_carries_error_code_and_reason() {
    let signals = strict_state(StrictFallbackState::LocalOnly).with_error_code(4242);
    match evaluate(signals) {
        AckDisposition::Blocked(rejection) => {
            assert_eq!(rejection.error_code, 4242);
            assert_eq!(rejection.status_reason, PERMANENT_DURABILITY_REASON);
        }
        _ => panic!("expected rejection"),
    }
}

#[test]
fn blocking_reason_is_propagated() {
    let why = StrictFallbackWhy {
        operation: GateOperation::ReadIndex,
        violation: GateViolation::ModeConflictStrictFallback,
        state: StrictFallbackState::LocalOnly,
        pending_entries: 2,
        last_local_proof: None,
        local_only_duration: Some(Duration::from_secs(1)),
        operation_term: None,
        decision_epoch: 1,
        blocking_reason: StrictFallbackBlockingReason::ModeConflictStrictFallback,
    };
    let signals = strict_state(StrictFallbackState::LocalOnly).with_strict_why(&why);
    match evaluate(signals) {
        AckDisposition::Blocked(rejection) => {
            assert_eq!(
                rejection.blocking_reason,
                Some(StrictFallbackBlockingReason::ModeConflictStrictFallback)
            );
        }
        _ => panic!("expected rejection"),
    }
}

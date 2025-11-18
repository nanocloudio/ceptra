use clustor::{
    DurabilityMode, StrictFallbackBlockingReason, StrictFallbackState, StrictFallbackWhy,
};

/// CEP-local status reason emitted when Clustor surfaces durability fences.
pub const PERMANENT_DURABILITY_REASON: &str = "PERMANENT_DURABILITY";
const DEFAULT_DURABILITY_ERROR_CODE: u32 = 0;

/// Signals from Clustor used to decide whether ACKs remain valid.
pub struct AckSignals<'a> {
    pub durability_mode: DurabilityMode,
    pub strict_state: StrictFallbackState,
    pub strict_why: Option<&'a StrictFallbackWhy>,
    pub durability_fence_active: bool,
    pub substrate_error_code: Option<u32>,
}

impl<'a> AckSignals<'a> {
    pub fn new(durability_mode: DurabilityMode, strict_state: StrictFallbackState) -> Self {
        Self {
            durability_mode,
            strict_state,
            strict_why: None,
            durability_fence_active: false,
            substrate_error_code: None,
        }
    }

    pub fn with_strict_why(mut self, why: &'a StrictFallbackWhy) -> Self {
        self.strict_why = Some(why);
        self
    }

    pub fn with_fence(mut self, active: bool) -> Self {
        self.durability_fence_active = active;
        self
    }

    pub fn with_error_code(mut self, code: u32) -> Self {
        self.substrate_error_code = Some(code);
        self
    }
}

/// Outcome of applying the CEP durability predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDisposition {
    Allowed,
    Blocked(AckRejection),
}

/// Rejection details surfaced to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRejection {
    pub status_reason: &'static str,
    pub error_code: u32,
    pub blocking_reason: Option<StrictFallbackBlockingReason>,
}

/// Applies the spec-defined durability predicates and classifies the result.
pub fn evaluate(signals: AckSignals<'_>) -> AckDisposition {
    if !durability_permits_ack(&signals) {
        return AckDisposition::Blocked(AckRejection {
            status_reason: PERMANENT_DURABILITY_REASON,
            error_code: signals
                .substrate_error_code
                .unwrap_or(DEFAULT_DURABILITY_ERROR_CODE),
            blocking_reason: signals.strict_why.map(|why| why.blocking_reason),
        });
    }
    AckDisposition::Allowed
}

fn durability_permits_ack(signals: &AckSignals<'_>) -> bool {
    let strict_block = matches!(signals.durability_mode, DurabilityMode::Strict)
        && matches!(
            signals.strict_state,
            StrictFallbackState::LocalOnly | StrictFallbackState::ProofPublished
        );
    if strict_block {
        return false;
    }
    if signals.durability_fence_active {
        return false;
    }
    true
}

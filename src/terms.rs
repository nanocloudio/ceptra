/// Canonical CEPtra term descriptor. Each entry mirrors the shared Clustor
/// term registry to ensure documentation and code stay aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CeptraTerm {
    /// Canonical name from the term registry.
    pub canonical: &'static str,
    /// Shared term identifier (TERM-xxxx).
    pub term_id: &'static str,
}

impl CeptraTerm {
    /// Const constructor used by generated code.
    pub const fn new(canonical: &'static str, term_id: &'static str) -> Self {
        Self { canonical, term_id }
    }
}

include!(concat!(env!("OUT_DIR"), "/ceptra_terms.rs"));

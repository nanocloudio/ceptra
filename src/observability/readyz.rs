use crate::observability::readiness::{CepReadyzReport, ReadyGate, ReadyMetrics};
use crate::runtime::hot_reload::{HotReloadDecision, HotReloadGate, PartitionWarmupProbe};
use clustor::{
    ActivationBarrier, ActivationDigestError, FlowDecision, ReadyzSnapshot, StrictFallbackState,
    WarmupReadinessRecord,
};

/// Combined readiness surface that merges Clustor warmup state, the CEP shadow
/// gate, and the CEP-specific partition readiness policy.
#[derive(Debug, Clone)]
pub struct CombinedReadyz {
    pub clustor: ReadyzSnapshot,
    pub hot_reload: HotReloadDecision,
    pub cep_readyz: CepReadyzReport,
    pub cep_metrics: ReadyMetrics,
    pub flow: FlowDecision,
    pub durability_state: StrictFallbackState,
    pub durability_pending: u64,
    pub raft_ready: bool,
}

impl CombinedReadyz {
    /// Aggregate readiness signal across Clustor warmup, activation barrier, and CEP gate.
    pub fn ready(&self) -> bool {
        let warmup = self.clustor.warmup_summary();
        let clustor_ready = warmup.ready == warmup.total && warmup.total > 0;
        let flow_ready = matches!(self.flow.ingest_status, clustor::IngestStatusCode::Healthy);
        let durability_ready = matches!(
            self.durability_state,
            StrictFallbackState::Healthy | StrictFallbackState::ProofPublished
        );
        clustor_ready
            && self.hot_reload.ready()
            && self.cep_readyz.ready
            && flow_ready
            && durability_ready
            && self.raft_ready
    }

    /// Renders CEP-specific readiness metrics as Prometheus exposition text.
    pub fn render_metrics(&self) -> String {
        self.cep_metrics.render_prometheus()
    }
}

/// Inputs required to evaluate the combined readiness surface.
pub struct ReadyzEvalContext<'a> {
    pub hot_reload_gate: &'a HotReloadGate,
    pub ready_gate: &'a ReadyGate,
    pub barrier: &'a ActivationBarrier,
    pub readiness: &'a [WarmupReadinessRecord],
    pub probes: &'a [PartitionWarmupProbe],
    pub clustor_readyz: ReadyzSnapshot,
    pub flow: FlowDecision,
    pub durability_state: StrictFallbackState,
    pub durability_pending: u64,
    pub now_ms: u64,
    pub raft_ready: bool,
}

/// Evaluates the activation barrier, warmup readiness, and CEP gate to produce a combined `/readyz` view.
pub fn evaluate_readyz(ctx: ReadyzEvalContext<'_>) -> Result<CombinedReadyz, ActivationDigestError> {
    let hot_reload = ctx
        .hot_reload_gate
        .evaluate(ctx.barrier, ctx.readiness, ctx.probes, ctx.now_ms)?;
    let cep_readyz = ctx.ready_gate.evaluate();
    let cep_metrics = ctx.ready_gate.metrics();
    Ok(CombinedReadyz {
        clustor: ctx.clustor_readyz,
        hot_reload,
        cep_readyz,
        cep_metrics,
        flow: ctx.flow,
        durability_state: ctx.durability_state,
        durability_pending: ctx.durability_pending,
        raft_ready: ctx.raft_ready,
    })
}

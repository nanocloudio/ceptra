use clustor::storage::CompactionState;

use crate::event_model::RetentionPlan;

/// Applies the computed `min_retain_index` to the Clustor compaction state.
pub fn raise_learner_slack_floor(state: &mut CompactionState, plan: &RetentionPlan) {
    let current = state.learner_slack_floor.unwrap_or(0);
    let raised = current.max(plan.min_retain_index);
    state.learner_slack_floor = Some(raised);
}

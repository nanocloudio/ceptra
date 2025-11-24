/// Snapshot describing the active definition/policy versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleVersionSnapshot {
    pub def_version: u64,
    pub policy_version: Option<u64>,
}

/// Selector that chooses rule versions based on the activation barrier index.
#[derive(Debug, Clone)]
pub struct RuleVersionSelector {
    active: RuleVersionSnapshot,
    pending: Option<(RuleVersionSnapshot, u64)>,
}

impl RuleVersionSelector {
    /// Creates the selector seeded with the active bundle versions.
    pub fn new(def_version: u64, policy_version: Option<u64>) -> Self {
        Self {
            active: RuleVersionSnapshot {
                def_version,
                policy_version,
            },
            pending: None,
        }
    }

    /// Returns the currently active version snapshot.
    pub fn active(&self) -> &RuleVersionSnapshot {
        &self.active
    }

    /// Returns the staged activation metadata, if any.
    pub fn pending(&self) -> Option<(u64, RuleVersionSnapshot)> {
        self.pending
            .as_ref()
            .map(|(snapshot, index)| (*index, snapshot.clone()))
    }

    /// Stages a new bundle/policy version guarded by the provided activation index.
    pub fn stage(
        &mut self,
        activation_log_index: u64,
        def_version: u64,
        policy_version: Option<u64>,
    ) {
        self.pending = Some((
            RuleVersionSnapshot {
                def_version,
                policy_version,
            },
            activation_log_index,
        ));
    }

    /// Selects the version snapshot that applies to the provided log index.
    pub fn select(&self, log_index: u64) -> RuleVersionSnapshot {
        match &self.pending {
            Some((snapshot, barrier)) if log_index >= *barrier => snapshot.clone(),
            _ => self.active.clone(),
        }
    }

    /// Commits the staged version once the activation entry has applied.
    pub fn commit(&mut self, applied_log_index: u64) {
        if let Some((snapshot, barrier)) = &self.pending {
            if applied_log_index >= *barrier {
                self.active = snapshot.clone();
                self.pending = None;
            }
        }
    }

    /// Drops any staged version without activating it.
    pub fn clear_pending(&mut self) {
        self.pending = None;
    }
}

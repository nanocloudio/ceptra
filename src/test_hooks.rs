use crate::threading::PartitionRole;

/// Deterministic hook used to alter runtime behavior during tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestHook {
    InjectThreadDelay { role: PartitionRole, delay_ms: u64 },
    DisableBackpressure,
    CorruptLastBlock,
    TimeShiftCertExpiry { delta_ms: i64 },
}

/// Registry of active hooks used by integration tests and CI harnesses.
#[derive(Debug, Default, Clone)]
pub struct TestHookRegistry {
    hooks: Vec<TestHook>,
}

impl TestHookRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Registers a new hook.
    pub fn register(&mut self, hook: TestHook) {
        self.hooks.push(hook);
    }

    /// Returns all hooks in deterministic order.
    pub fn hooks(&self) -> &[TestHook] {
        &self.hooks
    }

    /// Returns the delay to inject for a partition role, if any.
    pub fn thread_delay_ms(&self, role: PartitionRole) -> Option<u64> {
        self.hooks.iter().find_map(|hook| match hook {
            TestHook::InjectThreadDelay {
                role: target,
                delay_ms,
            } if *target == role => Some(*delay_ms),
            _ => None,
        })
    }

    /// Returns true when backpressure enforcement is disabled.
    pub fn backpressure_disabled(&self) -> bool {
        self.hooks
            .iter()
            .any(|hook| matches!(hook, TestHook::DisableBackpressure))
    }

    /// Returns true when the last block should be corrupted in tests.
    pub fn corrupt_last_block(&self) -> bool {
        self.hooks
            .iter()
            .any(|hook| matches!(hook, TestHook::CorruptLastBlock))
    }

    /// Returns the certificate expiry shift when requested.
    pub fn cert_expiry_shift_ms(&self) -> Option<i64> {
        self.hooks.iter().find_map(|hook| match hook {
            TestHook::TimeShiftCertExpiry { delta_ms } => Some(*delta_ms),
            _ => None,
        })
    }
}

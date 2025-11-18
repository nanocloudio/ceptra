use ceptra::{PartitionRole, TestHook, TestHookRegistry};

#[test]
fn registry_reports_active_hooks() {
    let mut registry = TestHookRegistry::new();
    registry.register(TestHook::DisableBackpressure);
    registry.register(TestHook::InjectThreadDelay {
        role: PartitionRole::ApplyWorker,
        delay_ms: 50,
    });
    registry.register(TestHook::TimeShiftCertExpiry { delta_ms: -500 });
    assert!(registry.backpressure_disabled());
    assert_eq!(
        registry.thread_delay_ms(PartitionRole::ApplyWorker),
        Some(50)
    );
    assert_eq!(registry.thread_delay_ms(PartitionRole::IoReactor), None);
    assert_eq!(registry.cert_expiry_shift_ms(), Some(-500));
    assert!(!registry.corrupt_last_block());

    registry.register(TestHook::CorruptLastBlock);
    assert!(registry.corrupt_last_block());
}

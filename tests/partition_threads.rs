use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use ceptra::{
    runtime::threading::CheckpointCommand,
    runtime::threading::{
        ApplyCommand, DeviceQueuePin, IoCommand, PartitionRole, PartitionThreadConfig,
        PartitionThreadLayoutBuilder, PartitionThreadPins, RoleActivity, RoleContext, RoleExecutor,
        TelemetryCommand, ThreadPinTarget, WalCommand,
    },
    PartitionThreadLayout,
};

fn wait_for<F>(timeout: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("condition not met within {:?}", timeout);
}

#[test]
fn partition_roles_process_commands_and_log_activity() {
    let layout = PartitionThreadLayout::new("p0", PartitionThreadConfig::default());

    layout
        .io_reactor()
        .send(IoCommand::new(1, "register stream"))
        .unwrap();
    layout
        .wal_writer()
        .send(WalCommand::Append { batch_id: 7 })
        .unwrap();
    layout
        .apply_worker()
        .send(ApplyCommand::Process { batch_id: 4 })
        .unwrap();
    layout
        .checkpoint_worker()
        .send(CheckpointCommand::Write { checkpoint_id: 2 })
        .unwrap();
    layout
        .telemetry_worker()
        .send(TelemetryCommand::new(
            "ceptra_ready_partitions_ratio_ms",
            42,
        ))
        .unwrap();

    wait_for(Duration::from_secs(1), || {
        layout.metrics().io().processed() == 1
            && layout.metrics().wal().processed() == 1
            && layout.metrics().apply().processed() == 1
            && layout.metrics().checkpoint().processed() == 1
            && layout.metrics().telemetry().processed() == 1
    });

    let log = layout.activity_log().snapshot();
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::IoReactor,
            activity: RoleActivity::Io(IoCommand { sequence: 1, .. }),
            ..
        }
    )));
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::WalWriter,
            activity: RoleActivity::Wal(WalCommand::Append { batch_id: 7 }),
            ..
        }
    )));
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::ApplyWorker,
            activity: RoleActivity::Apply(ApplyCommand::Process { batch_id: 4 }),
            ..
        }
    )));
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::CheckpointWorker,
            activity: RoleActivity::Checkpoint(CheckpointCommand::Write { checkpoint_id: 2 }),
            ..
        }
    )));
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::TelemetryWorker,
            activity: RoleActivity::Telemetry(TelemetryCommand { value: 42, .. }),
            ..
        }
    )));

    layout.shutdown();
}

struct StallingIoWorker {
    delay: Duration,
}

impl RoleExecutor<IoCommand> for StallingIoWorker {
    fn handle(&mut self, ctx: &RoleContext, command: IoCommand) {
        thread::sleep(self.delay);
        ctx.record(RoleActivity::Io(command));
    }
}

#[test]
fn watchdog_restarts_roles_when_heartbeat_stalls() {
    let config = PartitionThreadConfig {
        watchdog_threshold: Duration::from_millis(50),
        watchdog_poll_interval: Duration::from_millis(10),
        idle_heartbeat_interval: Duration::from_millis(10),
        ..PartitionThreadConfig::default()
    };

    let restart_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = restart_counter.clone();

    let layout = PartitionThreadLayoutBuilder::new("p1")
        .config(config)
        .with_io_factory(Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            Box::new(StallingIoWorker {
                delay: Duration::from_millis(200),
            })
        }))
        .build();

    layout
        .io_reactor()
        .send(IoCommand::new(99, "stalling command"))
        .unwrap();

    wait_for(Duration::from_secs(2), || {
        layout.metrics().io().restarts() > 0
    });

    let log = layout.activity_log().snapshot();
    assert!(log.iter().any(|entry| matches!(
        entry,
        ceptra::RoleActivityRecord {
            role: PartitionRole::IoReactor,
            activity: RoleActivity::WatchdogRestart(PartitionRole::IoReactor),
            ..
        }
    )));

    assert!(restart_counter.load(Ordering::Relaxed) >= 2);

    layout.shutdown();
}

#[test]
fn thread_pinning_emits_activity_logs() {
    let mut pins = PartitionThreadPins::new();
    pins.insert(
        PartitionRole::IoReactor,
        ThreadPinTarget {
            numa_node: 0,
            cpus: vec![0],
            device_queues: vec![DeviceQueuePin {
                device: "nvme0".into(),
                queue: 0,
            }],
        },
    );
    let layout = PartitionThreadLayoutBuilder::new("p2")
        .with_thread_pins(pins)
        .build();

    wait_for(Duration::from_secs(1), || {
        layout
            .activity_log()
            .snapshot()
            .iter()
            .any(|entry| matches!(entry.activity, RoleActivity::Pinned(_)))
    });

    layout.shutdown();
}

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::{Builder as TokioBuilder, Runtime};

/// Roles executed per partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PartitionRole {
    IoReactor,
    WalWriter,
    ApplyWorker,
    CheckpointWorker,
    TelemetryWorker,
}

impl PartitionRole {
    fn as_str(self) -> &'static str {
        match self {
            PartitionRole::IoReactor => "io",
            PartitionRole::WalWriter => "wal",
            PartitionRole::ApplyWorker => "apply",
            PartitionRole::CheckpointWorker => "checkpoint",
            PartitionRole::TelemetryWorker => "telemetry",
        }
    }

    pub fn all() -> [PartitionRole; 5] {
        [
            PartitionRole::IoReactor,
            PartitionRole::WalWriter,
            PartitionRole::ApplyWorker,
            PartitionRole::CheckpointWorker,
            PartitionRole::TelemetryWorker,
        ]
    }
}

/// Command processed by the I/O reactor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoCommand {
    pub sequence: u64,
    pub description: String,
}

impl IoCommand {
    pub fn new(sequence: u64, description: impl Into<String>) -> Self {
        Self {
            sequence,
            description: description.into(),
        }
    }
}

/// Command processed by the WAL writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalCommand {
    Append { batch_id: u64 },
    Sync,
}

/// Command processed by the apply worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyCommand {
    Process { batch_id: u64 },
    Replay { watermark: u64 },
}

/// Command processed by the checkpoint worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointCommand {
    Write { checkpoint_id: u64 },
    Compact { retain: u64 },
}

/// Command processed by the telemetry worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryCommand {
    pub metric: String,
    pub value: u64,
}

impl TelemetryCommand {
    pub fn new(metric: impl Into<String>, value: u64) -> Self {
        Self {
            metric: metric.into(),
            value,
        }
    }
}

/// Steering target for device queues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceQueuePin {
    pub device: String,
    pub queue: usize,
}

/// Thread pinning directive for a role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadPinTarget {
    pub numa_node: u32,
    pub cpus: Vec<usize>,
    pub device_queues: Vec<DeviceQueuePin>,
}

/// Activity log entry describing a pinning decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadPinReport {
    pub role: PartitionRole,
    pub numa_node: u32,
    pub cpus: Vec<usize>,
    pub device_queues: Vec<DeviceQueuePin>,
}

/// Activity emitted by a partition role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleActivity {
    Io(IoCommand),
    Wal(WalCommand),
    Apply(ApplyCommand),
    Checkpoint(CheckpointCommand),
    Telemetry(TelemetryCommand),
    WatchdogRestart(PartitionRole),
    Pinned(ThreadPinReport),
}

/// Log entry captured by the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleActivityRecord {
    pub partition_id: String,
    pub role: PartitionRole,
    pub activity: RoleActivity,
}

/// Thread-safe log used for testing and diagnostics.
#[derive(Clone, Default)]
pub struct PartitionActivityLog {
    entries: Arc<Mutex<Vec<RoleActivityRecord>>>,
}

impl PartitionActivityLog {
    pub fn record(&self, partition_id: &str, role: PartitionRole, activity: RoleActivity) {
        let mut guard = self.entries.lock().unwrap();
        guard.push(RoleActivityRecord {
            partition_id: partition_id.to_string(),
            role,
            activity,
        });
    }

    pub fn snapshot(&self) -> Vec<RoleActivityRecord> {
        self.entries.lock().unwrap().clone()
    }
}

/// Context supplied to each role executor.
#[derive(Clone)]
pub struct RoleContext {
    partition_id: Arc<String>,
    role: PartitionRole,
    log: PartitionActivityLog,
}

impl RoleContext {
    fn new(
        partition_id: impl Into<String>,
        role: PartitionRole,
        log: PartitionActivityLog,
    ) -> Self {
        Self {
            partition_id: Arc::new(partition_id.into()),
            role,
            log,
        }
    }

    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    pub fn role(&self) -> PartitionRole {
        self.role
    }

    pub fn record(&self, activity: RoleActivity) {
        self.log.record(self.partition_id(), self.role, activity);
    }
}

/// Behavior required for any per-role worker.
pub trait RoleExecutor<T>: Send + 'static {
    fn handle(&mut self, ctx: &RoleContext, command: T);
}

impl<T, F> RoleExecutor<T> for F
where
    T: Send + 'static,
    F: FnMut(&RoleContext, T) + Send + 'static,
{
    fn handle(&mut self, ctx: &RoleContext, command: T) {
        self(ctx, command);
    }
}

type RoleFactory<T> = Arc<dyn Fn() -> Box<dyn RoleExecutor<T>> + Send + Sync>;

/// Queue configuration and watchdog thresholds.
#[derive(Debug, Clone)]
pub struct PartitionThreadConfig {
    pub queue_capacity: usize,
    pub idle_heartbeat_interval: Duration,
    pub watchdog_threshold: Duration,
    pub watchdog_poll_interval: Duration,
}

impl Default for PartitionThreadConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            idle_heartbeat_interval: Duration::from_millis(100),
            watchdog_threshold: Duration::from_secs(1),
            watchdog_poll_interval: Duration::from_millis(200),
        }
    }
}

/// Per-role pinning assignments for a partition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionThreadPins {
    map: BTreeMap<PartitionRole, ThreadPinTarget>,
}

impl PartitionThreadPins {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, role: PartitionRole, pin: ThreadPinTarget) {
        self.map.insert(role, pin);
    }

    pub fn pin_for(&self, role: PartitionRole) -> Option<&ThreadPinTarget> {
        self.map.get(&role)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Factory hooks for overriding per-role workers.
#[derive(Clone)]
pub struct PartitionWorkerFactories {
    pub io: RoleFactory<IoCommand>,
    pub wal: RoleFactory<WalCommand>,
    pub apply: RoleFactory<ApplyCommand>,
    pub checkpoint: RoleFactory<CheckpointCommand>,
    pub telemetry: RoleFactory<TelemetryCommand>,
}

impl Default for PartitionWorkerFactories {
    fn default() -> Self {
        Self {
            io: Arc::new(|| Box::new(IoWorker)),
            wal: Arc::new(|| Box::new(WalWorker)),
            apply: Arc::new(|| Box::new(ApplyWorker::new())),
            checkpoint: Arc::new(|| Box::new(CheckpointWorker)),
            telemetry: Arc::new(|| Box::new(TelemetryWorker)),
        }
    }
}

/// Builder for constructing a layout with optional overrides.
pub struct PartitionThreadLayoutBuilder {
    partition_id: String,
    config: PartitionThreadConfig,
    factories: PartitionWorkerFactories,
    pins: Option<PartitionThreadPins>,
}

impl PartitionThreadLayoutBuilder {
    pub fn new(partition_id: impl Into<String>) -> Self {
        Self {
            partition_id: partition_id.into(),
            config: PartitionThreadConfig::default(),
            factories: PartitionWorkerFactories::default(),
            pins: None,
        }
    }

    pub fn config(mut self, config: PartitionThreadConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_io_factory(mut self, factory: RoleFactory<IoCommand>) -> Self {
        self.factories.io = factory;
        self
    }

    pub fn with_wal_factory(mut self, factory: RoleFactory<WalCommand>) -> Self {
        self.factories.wal = factory;
        self
    }

    pub fn with_apply_factory(mut self, factory: RoleFactory<ApplyCommand>) -> Self {
        self.factories.apply = factory;
        self
    }

    pub fn with_checkpoint_factory(mut self, factory: RoleFactory<CheckpointCommand>) -> Self {
        self.factories.checkpoint = factory;
        self
    }

    pub fn with_telemetry_factory(mut self, factory: RoleFactory<TelemetryCommand>) -> Self {
        self.factories.telemetry = factory;
        self
    }

    pub fn with_thread_pins(mut self, pins: PartitionThreadPins) -> Self {
        self.pins = Some(pins);
        self
    }

    pub fn build(self) -> PartitionThreadLayout {
        PartitionThreadLayout::with_factories_and_pins(
            self.partition_id,
            self.config,
            self.factories,
            self.pins,
        )
    }
}

/// Signals stored in the queue shared by worker threads.
enum RoleSignal<T> {
    Work(T),
    Shutdown,
}

/// Shared queue backing each role.
struct RoleQueueShared<T> {
    capacity: usize,
    state: Mutex<QueueState<T>>,
    cv: Condvar,
}

struct QueueState<T> {
    buffer: VecDeque<RoleSignal<T>>,
    closed: bool,
}

impl<T> RoleQueueShared<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(QueueState {
                buffer: VecDeque::with_capacity(capacity),
                closed: false,
            }),
            cv: Condvar::new(),
        }
    }

    fn close(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.closed = true;
        guard.buffer.clear();
        self.cv.notify_all();
    }
}

pub struct RoleSender<T> {
    shared: Arc<RoleQueueShared<T>>,
}

impl<T> Clone for RoleSender<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> RoleSender<T> {
    fn new(shared: Arc<RoleQueueShared<T>>) -> Self {
        Self { shared }
    }

    pub fn send(&self, command: T) -> Result<(), RoleSendError<T>> {
        let mut guard = self.shared.state.lock().unwrap();
        if guard.closed {
            return Err(RoleSendError::Closed(command));
        }
        if guard.buffer.len() >= self.shared.capacity {
            return Err(RoleSendError::Full(command));
        }
        guard.buffer.push_back(RoleSignal::Work(command));
        self.shared.cv.notify_one();
        Ok(())
    }

    fn signal_shutdown(&self) {
        let mut guard = self.shared.state.lock().unwrap();
        if guard.closed {
            return;
        }
        guard.buffer.push_back(RoleSignal::Shutdown);
        self.shared.cv.notify_one();
    }

    fn close(&self) {
        self.shared.close();
    }
}

pub struct RoleReceiver<T> {
    shared: Arc<RoleQueueShared<T>>,
}

impl<T> RoleReceiver<T> {
    fn new(shared: Arc<RoleQueueShared<T>>) -> Self {
        Self { shared }
    }

    fn recv_timeout(&self, timeout: Duration) -> Option<RoleSignal<T>> {
        let mut guard = self.shared.state.lock().unwrap();
        loop {
            if let Some(signal) = guard.buffer.pop_front() {
                return Some(signal);
            }
            if guard.closed {
                return None;
            }
            let (next_guard, wait) = self.shared.cv.wait_timeout(guard, timeout).unwrap();
            guard = next_guard;
            if wait.timed_out() {
                return None;
            }
        }
    }
}

/// Error returned when enqueuing a command fails.
#[derive(Debug)]
pub enum RoleSendError<T> {
    Full(T),
    Closed(T),
}

/// Handle exposed to callers for interacting with a role queue.
#[derive(Clone)]
pub struct RoleHandle<T> {
    role: PartitionRole,
    sender: RoleSender<T>,
}

impl<T> RoleHandle<T> {
    fn new(role: PartitionRole, sender: RoleSender<T>) -> Self {
        Self { role, sender }
    }

    pub fn send(&self, command: T) -> Result<(), RoleSendError<T>> {
        self.sender.send(command)
    }

    pub fn role(&self) -> PartitionRole {
        self.role
    }
}

/// Metrics exported for each role.
#[derive(Debug, Clone, Default)]
pub struct RoleMetrics {
    processed: Arc<AtomicU64>,
    restarts: Arc<AtomicU64>,
}

impl RoleMetrics {
    fn record(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    fn restarted(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }
}

/// Aggregated metrics for all roles.
#[derive(Debug, Clone)]
pub struct PartitionThreadMetrics {
    io: RoleMetrics,
    wal: RoleMetrics,
    apply: RoleMetrics,
    checkpoint: RoleMetrics,
    telemetry: RoleMetrics,
}

impl PartitionThreadMetrics {
    pub fn io(&self) -> &RoleMetrics {
        &self.io
    }

    pub fn wal(&self) -> &RoleMetrics {
        &self.wal
    }

    pub fn apply(&self) -> &RoleMetrics {
        &self.apply
    }

    pub fn checkpoint(&self) -> &RoleMetrics {
        &self.checkpoint
    }

    pub fn telemetry(&self) -> &RoleMetrics {
        &self.telemetry
    }

    pub fn role(&self, role: PartitionRole) -> &RoleMetrics {
        match role {
            PartitionRole::IoReactor => self.io(),
            PartitionRole::WalWriter => self.wal(),
            PartitionRole::ApplyWorker => self.apply(),
            PartitionRole::CheckpointWorker => self.checkpoint(),
            PartitionRole::TelemetryWorker => self.telemetry(),
        }
    }
}

struct RoleSupervisor<T> {
    partition_id: String,
    role: PartitionRole,
    queue: Arc<RoleQueueShared<T>>,
    sender: RoleSender<T>,
    factory: RoleFactory<T>,
    pin_target: Option<ThreadPinTarget>,
    heartbeat: Arc<Mutex<Instant>>,
    idle_timeout: Duration,
    metrics: RoleMetrics,
    log: PartitionActivityLog,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<T> RoleSupervisor<T>
where
    T: Send + 'static,
{
    fn build(
        partition_id: &str,
        role: PartitionRole,
        config: &PartitionThreadConfig,
        factory: RoleFactory<T>,
        pin_target: Option<ThreadPinTarget>,
        log: PartitionActivityLog,
        metrics: RoleMetrics,
    ) -> Arc<Self> {
        let queue = Arc::new(RoleQueueShared::new(config.queue_capacity));
        let sender = RoleSender::new(queue.clone());
        let supervisor = Arc::new(Self {
            partition_id: partition_id.to_string(),
            role,
            queue,
            sender,
            factory,
            pin_target,
            heartbeat: Arc::new(Mutex::new(Instant::now())),
            idle_timeout: config.idle_heartbeat_interval,
            metrics,
            log,
            join: Mutex::new(None),
        });
        supervisor.start_worker();
        supervisor
    }

    fn start_worker(&self) {
        let receiver = RoleReceiver::new(self.queue.clone());
        let heartbeat = self.heartbeat.clone();
        {
            let mut guard = heartbeat.lock().unwrap();
            *guard = Instant::now();
        }
        let idle_timeout = self.idle_timeout;
        let metrics = self.metrics.clone();
        let factory = self.factory.clone();
        let partition_id = self.partition_id.clone();
        let role = self.role;
        let log = self.log.clone();
        let pin_report = self.pin_target.clone().map(|pin| ThreadPinReport {
            role,
            numa_node: pin.numa_node,
            cpus: pin.cpus.clone(),
            device_queues: pin.device_queues.clone(),
        });
        let thread_name = format!("{}_{}", partition_id, role.as_str());
        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let mut executor = (factory.as_ref())();
                let ctx = RoleContext::new(partition_id, role, log);
                if let Some(report) = pin_report.clone() {
                    ctx.record(RoleActivity::Pinned(report));
                }
                loop {
                    match receiver.recv_timeout(idle_timeout) {
                        Some(RoleSignal::Work(command)) => {
                            executor.handle(&ctx, command);
                            metrics.record();
                            let mut guard = heartbeat.lock().unwrap();
                            *guard = Instant::now();
                        }
                        Some(RoleSignal::Shutdown) => break,
                        None => {
                            let mut guard = heartbeat.lock().unwrap();
                            *guard = Instant::now();
                        }
                    }
                }
            })
            .expect("failed to spawn role worker");
        *self.join.lock().unwrap() = Some(join);
    }

    fn restart(&self) -> bool {
        self.sender.signal_shutdown();
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
        self.metrics.restarted();
        self.start_worker();
        true
    }

    fn shutdown(&self) {
        self.sender.signal_shutdown();
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
        self.sender.close();
    }

    fn handle(&self) -> RoleHandle<T> {
        RoleHandle::new(self.role, self.sender.clone())
    }

    fn watch_target(self: &Arc<Self>) -> RoleWatchTarget {
        let heartbeat = self.heartbeat.clone();
        let role = self.role;
        let this = Arc::downgrade(self);
        RoleWatchTarget {
            role,
            heartbeat,
            restart: Arc::new(move || {
                if let Some(supervisor) = this.upgrade() {
                    supervisor.restart();
                }
            }),
        }
    }
}

struct RoleWatchTarget {
    role: PartitionRole,
    heartbeat: Arc<Mutex<Instant>>,
    restart: Arc<dyn Fn() + Send + Sync>,
}

impl RoleWatchTarget {
    fn last_beat(&self) -> Instant {
        *self.heartbeat.lock().unwrap()
    }
}

struct PartitionWatchdog {
    stop: Arc<AtomicBool>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl PartitionWatchdog {
    fn new(
        partition_id: &str,
        targets: Vec<RoleWatchTarget>,
        threshold: Duration,
        poll_interval: Duration,
        log: PartitionActivityLog,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let partition = partition_id.to_string();
        let thread_stop = stop.clone();
        let join = thread::Builder::new()
            .name(format!("{}_watchdog", partition_id))
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    for target in &targets {
                        if now.duration_since(target.last_beat()) > threshold {
                            log.record(
                                &partition,
                                target.role,
                                RoleActivity::WatchdogRestart(target.role),
                            );
                            (target.restart)();
                        }
                    }
                    thread::sleep(poll_interval);
                }
            })
            .expect("failed to spawn watchdog");
        Self {
            stop,
            join: Mutex::new(Some(join)),
        }
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

/// Runtime covering all role threads for a partition.
pub struct PartitionThreadLayout {
    partition_id: String,
    io: Arc<RoleSupervisor<IoCommand>>,
    wal: Arc<RoleSupervisor<WalCommand>>,
    apply: Arc<RoleSupervisor<ApplyCommand>>,
    checkpoint: Arc<RoleSupervisor<CheckpointCommand>>,
    telemetry: Arc<RoleSupervisor<TelemetryCommand>>,
    watchdog: PartitionWatchdog,
    metrics: PartitionThreadMetrics,
    log: PartitionActivityLog,
    pins: Option<PartitionThreadPins>,
    shutdown: AtomicBool,
}

impl PartitionThreadLayout {
    pub fn new(partition_id: impl Into<String>, config: PartitionThreadConfig) -> Self {
        Self::with_factories(partition_id, config, PartitionWorkerFactories::default())
    }

    pub fn with_factories(
        partition_id: impl Into<String>,
        config: PartitionThreadConfig,
        factories: PartitionWorkerFactories,
    ) -> Self {
        Self::with_factories_and_pins(partition_id, config, factories, None)
    }

    pub fn with_factories_and_pins(
        partition_id: impl Into<String>,
        config: PartitionThreadConfig,
        factories: PartitionWorkerFactories,
        pins: Option<PartitionThreadPins>,
    ) -> Self {
        let partition_id = partition_id.into();
        let log = PartitionActivityLog::default();
        let metrics = PartitionThreadMetrics {
            io: RoleMetrics::default(),
            wal: RoleMetrics::default(),
            apply: RoleMetrics::default(),
            checkpoint: RoleMetrics::default(),
            telemetry: RoleMetrics::default(),
        };
        let pin_lookup = pins.clone().unwrap_or_default();
        let io = RoleSupervisor::build(
            &partition_id,
            PartitionRole::IoReactor,
            &config,
            factories.io.clone(),
            pin_lookup.pin_for(PartitionRole::IoReactor).cloned(),
            log.clone(),
            metrics.io.clone(),
        );
        let wal = RoleSupervisor::build(
            &partition_id,
            PartitionRole::WalWriter,
            &config,
            factories.wal.clone(),
            pin_lookup.pin_for(PartitionRole::WalWriter).cloned(),
            log.clone(),
            metrics.wal.clone(),
        );
        let apply = RoleSupervisor::build(
            &partition_id,
            PartitionRole::ApplyWorker,
            &config,
            factories.apply.clone(),
            pin_lookup.pin_for(PartitionRole::ApplyWorker).cloned(),
            log.clone(),
            metrics.apply.clone(),
        );
        let checkpoint = RoleSupervisor::build(
            &partition_id,
            PartitionRole::CheckpointWorker,
            &config,
            factories.checkpoint.clone(),
            pin_lookup.pin_for(PartitionRole::CheckpointWorker).cloned(),
            log.clone(),
            metrics.checkpoint.clone(),
        );
        let telemetry = RoleSupervisor::build(
            &partition_id,
            PartitionRole::TelemetryWorker,
            &config,
            factories.telemetry.clone(),
            pin_lookup.pin_for(PartitionRole::TelemetryWorker).cloned(),
            log.clone(),
            metrics.telemetry.clone(),
        );
        let watchdog = PartitionWatchdog::new(
            &partition_id,
            vec![
                io.watch_target(),
                wal.watch_target(),
                apply.watch_target(),
                checkpoint.watch_target(),
                telemetry.watch_target(),
            ],
            config.watchdog_threshold,
            config.watchdog_poll_interval,
            log.clone(),
        );
        Self {
            partition_id,
            io,
            wal,
            apply,
            checkpoint,
            telemetry,
            watchdog,
            metrics,
            log,
            pins,
            shutdown: AtomicBool::new(false),
        }
    }

    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    pub fn io_reactor(&self) -> RoleHandle<IoCommand> {
        self.io.handle()
    }

    pub fn wal_writer(&self) -> RoleHandle<WalCommand> {
        self.wal.handle()
    }

    pub fn apply_worker(&self) -> RoleHandle<ApplyCommand> {
        self.apply.handle()
    }

    pub fn checkpoint_worker(&self) -> RoleHandle<CheckpointCommand> {
        self.checkpoint.handle()
    }

    pub fn telemetry_worker(&self) -> RoleHandle<TelemetryCommand> {
        self.telemetry.handle()
    }

    pub fn metrics(&self) -> &PartitionThreadMetrics {
        &self.metrics
    }

    pub fn activity_log(&self) -> PartitionActivityLog {
        self.log.clone()
    }

    pub fn thread_pins(&self) -> Option<&PartitionThreadPins> {
        self.pins.as_ref()
    }

    pub fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            return;
        }
        self.watchdog.stop();
        self.io.shutdown();
        self.wal.shutdown();
        self.apply.shutdown();
        self.checkpoint.shutdown();
        self.telemetry.shutdown();
    }
}

impl Drop for PartitionThreadLayout {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct IoWorker;

impl RoleExecutor<IoCommand> for IoWorker {
    fn handle(&mut self, ctx: &RoleContext, command: IoCommand) {
        ctx.record(RoleActivity::Io(command));
    }
}

struct WalWorker;

impl RoleExecutor<WalCommand> for WalWorker {
    fn handle(&mut self, ctx: &RoleContext, command: WalCommand) {
        ctx.record(RoleActivity::Wal(command));
    }
}

struct ApplyWorker {
    runtime: Runtime,
}

impl ApplyWorker {
    fn new() -> Self {
        Self {
            runtime: TokioBuilder::new_current_thread()
                .enable_time()
                .build()
                .expect("tokio runtime"),
        }
    }
}

impl RoleExecutor<ApplyCommand> for ApplyWorker {
    fn handle(&mut self, ctx: &RoleContext, command: ApplyCommand) {
        let detail = command.clone();
        self.runtime.block_on(async move {
            tokio::task::yield_now().await;
        });
        ctx.record(RoleActivity::Apply(detail));
    }
}

struct CheckpointWorker;

impl RoleExecutor<CheckpointCommand> for CheckpointWorker {
    fn handle(&mut self, ctx: &RoleContext, command: CheckpointCommand) {
        ctx.record(RoleActivity::Checkpoint(command));
    }
}

struct TelemetryWorker;

impl RoleExecutor<TelemetryCommand> for TelemetryWorker {
    fn handle(&mut self, ctx: &RoleContext, command: TelemetryCommand) {
        ctx.record(RoleActivity::Telemetry(command));
    }
}

impl fmt::Display for PartitionRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

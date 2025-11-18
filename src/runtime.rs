use std::env;
use std::time::Duration;

/// Role assigned to the CEPtra process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleMode {
    Clustered,
    Standalone,
}

/// Test mode toggle controlled through the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Disabled,
    Enabled { fast_warmup: bool },
}

impl TestMode {
    pub fn is_enabled(self) -> bool {
        !matches!(self, TestMode::Disabled)
    }
}

/// Aggregated runtime options derived from CLI flags + env vars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub role: RoleMode,
    pub test_mode: TestMode,
}

impl RuntimeOptions {
    pub fn from_args<I>(args: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut role = RoleMode::Clustered;
        let mut args_iter = args.into_iter();
        args_iter.next(); // skip binary name
        for arg in args_iter {
            if arg == "--role=standalone" {
                role = RoleMode::Standalone;
            }
        }
        let test_mode = read_test_mode();
        Self { role, test_mode }
    }

    pub fn from_env() -> Self {
        Self::from_args(env::args())
    }

    /// Returns a warmup duration estimate derived from partitions/checkpoints.
    pub fn warmup_estimate(partitions: usize, checkpoint_depth: usize) -> WarmupEstimate {
        let partitions = partitions.max(1) as u64;
        let checkpoints = checkpoint_depth.max(1) as u64;
        let steady_state = Duration::from_secs(30 * partitions * checkpoints);
        let fast_path = Duration::from_secs(steady_state.as_secs() / 2);
        WarmupEstimate {
            steady_state,
            fast_path,
        }
    }
}

fn read_test_mode() -> TestMode {
    match env::var("CEPTRA_TEST_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("fast") => TestMode::Enabled { fast_warmup: true },
        Ok(value) if value.eq_ignore_ascii_case("true") => TestMode::Enabled { fast_warmup: false },
        _ => TestMode::Disabled,
    }
}

/// Warmup duration estimates for operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmupEstimate {
    pub steady_state: Duration,
    pub fast_path: Duration,
}

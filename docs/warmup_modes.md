# Runtime Roles, Test Modes, and Warmup Windows

CEPtra nodes support two runtime roles:

- `Clustered` (default) – pods participate in the full Raft quorum and serve
  production traffic.
- `Standalone` – enabled via `--role=standalone`. The process boots without
  joining the control-plane and keeps its resources local so developers can
  iterate or run smoke tests.

## CEPTRA_TEST_MODE

The environment variable `CEPTRA_TEST_MODE` toggles reduced safety rails:

- `CEPTRA_TEST_MODE=true` – enables deterministic test hooks while preserving
  standard warmup timers.
- `CEPTRA_TEST_MODE=fast` – same as above, but halves the warmup estimate so CI
  can promote pipelines quickly.

`RuntimeOptions::from_env()` reads both the CLI flag and the env var so callers
can gate behavior at startup. The helper also exposes
`RuntimeOptions::warmup_estimate(partitions, checkpoint_depth)` which returns:

- `steady_state`: `30s * partitions * checkpoint_depth`
- `fast_path`: half of the steady-state duration (used by `CEPTRA_TEST_MODE=fast`)

For example, a 12-partition deployment with a checkpoint depth of 4 produces a
steady-state warmup of 24 minutes, while test mode fast-path reduces that to
12 minutes.

## Runbooks

### Staging

1. Deploy pods with `--role=standalone` and `CEPTRA_TEST_MODE=true`.
2. Use the `bootstrapper plan staging.json` tool to assign partitions evenly.
3. Wait for the warmup duration reported by `warmup_estimate` based on the
   partition/checkpoint inputs.
4. Flip the role back to clustered for canary validation.

### CI clusters

1. Set `CEPTRA_TEST_MODE=fast` on the StatefulSet template.
2. Keep pods in standalone mode to avoid impacting other tenants.
3. Use the fast-path warmup output to sequence integration suites before
   tearing down the cluster.
4. When scaling tests, call `bootstrapper pre-stop ...` to force Raft epoch++
   fences before eviction.

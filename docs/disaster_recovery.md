# Disaster Recovery, Runbooks, and Cutover Automation

CEPtra inherits the durability contract from the Clustor substrate, so every 
recovery drill begins by following the normative spec in 
`../clustor/docs/specification.md`. This document captures the CEPtra-specific 
sequencing plus references to the shared procedures.

## Clustor-backed prerequisites

1. Keep the sibling `../clustor` checkout present and pinned: the build guard
   compares `manifests/ceptra_manifest.json` to `../clustor/docs/specification.md`.
   Run `make spec-sync-check` (or `spec-sync-write`) after bumping either spec to
   refresh the hash and the upstream `consensus_core_manifest`.
2. Run CEPtra with the `clustor-net` feature enabled. The deprecated stub runtime
   now requires `CEPTRA_STUB_RUNTIME=1` and should only be used for dev seeds or
   smoke tests while Clustor integration tests come online.
3. Pre-provision TLS identity/trust paths plus storage roots (`CEPTRA_DATA_DIR`,
   `CEPTRA_WAL_DIR`, `CEPTRA_SNAPSHOT_DIR`). The runtime validates readability
   and writability at startup; refer to `docs/operational_runbook.md` for port
   bindings and health checks.

## Cold start

1. Provision storage + certificates per the Clustor cold-start checklist.
2. Deploy the StatefulSet in `--role=standalone` so WAL shipping remains local.
3. Use `tools/bootstrapper` to elect CP voters, then `tools/dr_cutover` in
   `validate` mode to confirm the DR plan matches the staged topology.
4. Once the cutover plan validates, promote the cluster by calling
   `dr_cutover run plan.json` so ingress routes redirect toward CEPtra.

## PVC loss

1. Fence the affected partition groups via `FreezePartitionGroup` using the
   `partition_groups[*].freeze_issued` flag in the DR plan.
2. Restore volume snapshots referenced in the Clustor spec (Appendix B) and
   reseed the WAL files from the paired object store.
3. Run `dr_cutover validate` to ensure WAL verification entries cover each
   recovered group before rejoining the cluster.

## CP quorum loss

1. Evict unhealthy CP voters and mark their partition groups as frozen.
2. Promote the warm standby voters using the bootstrapper pre-stop plan so the
   quorum count remains stable.
3. Execute `dr_cutover run` with a plan that increases the `target_epoch` by at
   least one so the promoted CP nodes fence any stale work.

## WAL corruption

1. Detect corruption via the shared Clustor integrity checks and record the
   affected WAL offsets in the `wal_shipments` section of the DR plan.
2. Backfill from the most recent verified shipment. The plan should mark
   `verified=true` only after the checksums match the Clustor baseline.
3. Run `dr_cutover run` to reconverge the epochs once the replacement WALs are
   applied.

## Catastrophic recovery

1. Follow the catastrophic recovery tree in `../clustor/docs/specification.md` to
   freeze every partition group and rebuild the control plane in a new region.
2. Prepare a DR plan that lists every recovered group, WAL shipment checkpoint,
   ingress route, and the reconciled epoch for the new control plane.
3. Execute `dr_cutover run plan.json` to document the fenced groups, verified
   shipments, ingress reroutes, and the reconciled epoch. The JSON output is
   attached to the incident ticket so auditors can confirm the steps against the
   Clustor runbook.

## Tooling reference

- `tools/dr_cutover`: validates and executes the DR plan defined above. It
  refuses to run unless every partition group is frozen, WAL shipping is
  verified, ingress routes are declared, and epochs advance forward.
- `tools/bootstrapper`: elects new CP voters/learners and exposes pre-stop plans
  for orchestrated drains.

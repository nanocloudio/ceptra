# Kubernetes Affinity and NUMA Pinning

CEPtra partitions run multiple cooperative roles (I/O reactor, WAL writer, apply
worker, checkpoint worker, telemetry worker). Each role performs predictable
workloads that must stay close to the partition's replicas and NVMe/NIC
interrupts to avoid cross-socket hops.

## Runtime planner

The `affinity` module provides the `NumaPlanner` helper, which ingests a list of
NUMA node specs (`NumaNodeSpec`). Each spec describes the CPUs, the node-local
memory budget, and the available NVMe/NIC queues. Calling
`plan_partitions(["partition-a", "partition-b", …])` returns a
`PartitionPinning` per partition with:

- The NUMA node chosen for the partition.
- A `PartitionThreadPins` map that assigns every `PartitionRole` to its CPU.
- Queue steering directives (`DeviceQueuePin`) so NVMe IRQs/NIC RSS queues run
  on the same CPUs as the consuming role.
- Evenly sliced memory reservations per partition to keep caches warm inside
  each NUMA domain.

`PartitionThreadLayoutBuilder::with_thread_pins` accepts the plan and publishes
`RoleActivity::Pinned` entries as each worker thread comes online; this makes
both the planner and runtime actions observable in tests.

## Kubernetes requirements

The runtime expects worker nodes to surface three guardrails before it accepts a
pod (mirroring §21/§26 requirements):

1. **CPU Manager static policy** – we rely on static CPU pinning; best-effort
   policies allow migrations and lead to cross-node cache misses.
2. **Topology Manager `restricted` policy** – ensures the control plane avoids
   scheduling extra containers that would span NUMA nodes.
3. **CEPtra anti-affinity domain** – all nodes hosting CEPtra pods must expose
   `ceptra.dev/anti-affinity-domain=enabled` so the admission controller can
   prove the operator configured PodAntiAffinity on the StatefulSet.

The `kubernetes` module exposes `validate_node_labels` and a shared constant
(`REQUIRED_NODE_LABELS`) which the webhook/admission jobs can reuse to enforce
the same policy we test in CI. The helper returns `AdmissionError` variants that
match the failure reason (missing label vs. value mismatch) so operators can
remediate nodes quickly.

## StatefulSet tuning

In addition to the node labels, the StatefulSet manifest must specify:

- `spec.template.spec.topologySpreadConstraints` that ensure pods spread across
  zones and NUMA domains.
- `spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution`
  keyed by `ceptra.dev/anti-affinity-domain`.
- `resources.requests.cpu` matching the number of pinned CPUs per partition
  (the `NumaPlanner` report can be exported to ConfigMaps consumed by the
  StatefulSet generator).

With these guardrails in place the CEPtra runtime keeps every role on its local
NUMA node, ensures NVMe/NIC queues share locality with the threads that consume
them, and rejects nodes that drift away from the documented policy.

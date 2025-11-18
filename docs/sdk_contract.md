# CEPtra SDK Contract

CEPtra client SDKs share a single contract so that throttling and durability
signals behave the same regardless of implementation language. This document
summarises the expectations for credit windows, retry jitter, and status
handling and calls out the tooling that exercises the contract.

## Initial credit window

Clients SHOULD begin with at least 256 in-flight events or 10 MiB of payloads,
whichever limit is reached first. Leaders advertise live capacity via the
`credit_window_hint_events` and `credit_window_hint_bytes` gauges documented in
`telemetry/catalog.json`. Every append attempt MUST respect both bounds before
transmitting payloads. When the leader has not yet produced a hint, SDKs must
fall back to the default of 256 events / 10 MiB to avoid deadlock during
bootstrap.

## Jittered retry backoff

When the leader returns `IngestStatusCode::TransientBackpressure`, clients MUST
retry the same `event_id` after waiting for the supplied CEP hint. The
`CepCreditHint.retry_backoff_ms` field represents the upper bound for the backoff;
SDKs should wait for a uniformly distributed delay in the range
`[retry_backoff_ms / 2, retry_backoff_ms]` to desynchronise concurrent senders.
When the hint omits a backoff window, SDKs should default to 25–50 ms of jitter.
The hinted delay only applies to transient failures—permanent errors should be
surfaced immediately.

## Numeric credit hints

Clustor’s `credit_hint` buckets (`Recover`, `Hold`, `Shed`) remain the primary
event-classification signal. CEPtra extends the RPC envelope with an optional
`CepCreditHint` so clients can make deterministic decisions instead of mapping
the coarse bucket onto local heuristics. Two fields are surfaced:

- `client_window_size` – the recommended max in-flight events for the next
  append. Clients SHOULD clamp their local window to `min(current_hint,
  client_window_size)` and immediately advertise the reduced budget to the layer
  that batches payloads.
- `retry_backoff_ms` – an upper bound on the jittered delay that should be used
  between retries. When the Clustor hint is `Recover` but the CEP hint supplies
  a non-zero backoff, SDKs SHOULD honour the CEP hint (falling back to the
  jitter guidance above when the CEP hint is missing or zero).

The extended hints are optional; clients MUST continue to include the previous
`CreditHint` bucket on every request, and servers fall back to those defaults
when the CEP fields are absent.

## Status handling

| Code | Class | Expected client behaviour |
| --- | --- | --- |
| `Healthy` | Success | Advance local credit window accounting, treat the append as committed. |
| `TransientBackpressure` | Transient (`TRANSIENT_*`) | Retry with the same `event_id` after honouring the hinted backoff and window. |
| `PermanentDurability` | Permanent (`PERMANENT_*`) | Stop retrying and surface the supplied reason/`CEP_STATUS_REASON`. |

The Clustor PID controller continuously publishes credit and backoff hints. SDKs
are responsible for plumbling those hints back into their transport pipelines so
that subsequent requests immediately reflect the new window.

## Conformance harness

The `tools/sdk_conformance` binary replays scripted throttles derived from the
Clustor simulator traces. Each scenario feeds pre-recorded responses into the
`CepAppendClient`, verifies that requests echo the latest hints, and inspects the
client telemetry for retry and overload counters. Run the harness with:

```
cargo run --manifest-path tools/sdk_conformance/Cargo.toml -- \
  --scenarios tools/sdk_conformance/scenarios/throttles.json
```

Scenarios can be extended to cover new throttling patterns; failing scenarios
cause the binary to exit with a non-zero status so CI can gate regressions.

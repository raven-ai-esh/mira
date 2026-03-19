# MIRA 2.6.0

Date: `2026-03-19`
Theme: `Operational and Benchmark Dominance`

## Scope

This release closes the final slice of
[docs/MIRA_BACKEND_DOMINANCE_CHECKLIST.md](docs/MIRA_BACKEND_DOMINANCE_CHECKLIST.md)
and turns the previous capability train into an evidence-backed ranking claim
for promoted advanced backend workload classes.

`2.6.0` does not widen the language with a new protocol family. It packages and
hardens two already-promoted workload families so the dominance claim is tied to
stable service anchors, explicit verification, operational hardening, and a
cross-language benchmark matrix.

## Promoted Anchors

Production-shaped capability anchors:

- messenger backend:
  - [mira/examples/runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)
- analytics microservice platform:
  - [mira/examples/runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)

Benchmark-only emitted-friendly dominance anchors:

- messaging benchmark:
  - [mira/examples/runtime_advanced_messaging_benchmark.mira](mira/examples/runtime_advanced_messaging_benchmark.mira)
- analytics benchmark:
  - [mira/examples/runtime_advanced_analytics_benchmark.mira](mira/examples/runtime_advanced_analytics_benchmark.mira)

## What Landed

- stable generation-contract extension for advanced messaging and analytics
  workload families
- messaging hardening package:
  - conformance: [artifacts/messaging-hardening-2.6.0.json](artifacts/messaging-hardening-2.6.0.json)
  - regression: [artifacts/messaging-hardening-regression-2.6.0.json](artifacts/messaging-hardening-regression-2.6.0.json)
- analytics hardening package:
  - conformance: [artifacts/analytics-hardening-2.6.0.json](artifacts/analytics-hardening-2.6.0.json)
  - regression: [artifacts/analytics-hardening-regression-2.6.0.json](artifacts/analytics-hardening-regression-2.6.0.json)
- advanced generation-contract package:
  - [artifacts/advanced-generation-contract-2.6.0.json](artifacts/advanced-generation-contract-2.6.0.json)
- advanced benchmark matrix and diagnostics:
  - [artifacts/advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)
  - [artifacts/advanced-backend-matrix-2.6.0.md](artifacts/advanced-backend-matrix-2.6.0.md)
  - [artifacts/advanced-backend-matrix-2.6.0-diagnostics.json](artifacts/advanced-backend-matrix-2.6.0-diagnostics.json)
- final release bundle:
  - [artifacts/advanced-backend-release-bundle-2.6.0.json](artifacts/advanced-backend-release-bundle-2.6.0.json)
  - [artifacts/advanced-backend-release-bundle-2.6.0.md](artifacts/advanced-backend-release-bundle-2.6.0.md)

## Verification Package

Full suite:

- `cargo test --manifest-path mirac/Cargo.toml`
- result: `121/121` green

Focused `2.6.0` proofs:

- source `check/test` green for:
  - [mira/examples/runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)
  - [mira/examples/runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)
  - [mira/examples/runtime_advanced_messaging_benchmark.mira](mira/examples/runtime_advanced_messaging_benchmark.mira)
  - [mira/examples/runtime_advanced_analytics_benchmark.mira](mira/examples/runtime_advanced_analytics_benchmark.mira)
- `emit-binary -> check-binary -> test-binary` green for:
  - [tmp/mira-native/runtime_production_messenger_backend.mirb3](tmp/mira-native/runtime_production_messenger_backend.mirb3)
  - [tmp/mira-native/runtime_production_analytics_platform.mirb3](tmp/mira-native/runtime_production_analytics_platform.mirb3)
- `mira_default` runtime proofs green for:
  - [mira/examples/runtime_advanced_messaging_benchmark.mira](mira/examples/runtime_advanced_messaging_benchmark.mira)
  - [mira/examples/runtime_advanced_analytics_benchmark.mira](mira/examples/runtime_advanced_analytics_benchmark.mira)
- cross-target emitted runtime proofs green on:
  - `x86_64-apple-macos13`
  - `x86_64-unknown-linux-gnu`
  - `x86_64-pc-windows-msvc`

## Benchmark Outcome

Selected backend for the promoted `2.6.0` workload classes:

- `mira_default`

Matrix summary from
[artifacts/advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json):

- near-parity or better against the fastest foreign baseline on `6/6` promoted
  workloads
- clearly ahead on `3/6` promoted workloads
- rank-first flag: `true`

Representative medians:

- `messaging_transport`:
  - `mira_default`: `60,000 ns`
  - best foreign (`Go`): `433,041 ns`
  - classification: `ahead`
- `messaging_fanout`:
  - `mira_default`: `30,000 ns`
  - best foreign (`Go`): `49,250 ns`
  - classification: `ahead`
- `messaging_replay`:
  - `mira_default`: `32,000 ns`
  - best foreign (`Go`): `28,875 ns`
  - classification: `near_parity`
- `analytics_aggregation`:
  - `mira_default`: `61,000 ns`
  - best foreign (`Go`): `61,042 ns`
  - classification: `near_parity`
- `analytics_worker_throughput`:
  - `mira_default`: `31,000 ns`
  - best foreign (`Go`): `62,759,417 ns`
  - classification: `ahead`
- `analytics_failure_recovery`:
  - `mira_default`: `33,000 ns`
  - best foreign (`Go`): `28,875 ns`
  - classification: `near_parity`

## Outcome

`MIRA 2.6.0` closes the backend-dominance train. On the promoted advanced
messaging and analytics workload classes, the platform now has:

- canonical production-shaped service anchors
- stable generation-contract coverage
- explicit operational conformance and regression evidence
- a benchmark matrix against `Go` and `Rust`
- a default backend selection that is evidence-backed rather than aspirational

Within that promoted scope, it is now defensible to rank `MIRA` first.

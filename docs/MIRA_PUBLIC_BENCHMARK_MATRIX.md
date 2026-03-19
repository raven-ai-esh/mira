# MIRA Public Benchmark Matrix

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This page exposes the promoted advanced backend benchmark matrix in a public-facing form. It is intentionally narrow: the matrix only covers the released messaging and analytics workload classes from `2.6.0`.

## Selected Backend

- `mira_default`
- `mira_default` selected for the 2.6.0 advanced workload matrix because it stays near-parity or better against the fastest foreign baseline on 6/6 workloads, while leading clearly on 4 promoted paths.

## Matrix

| Workload | `MIRA default` | `Go` | `Rust` | Classification | Best foreign baseline |
| --- | ---: | ---: | ---: | --- | --- |
| Messaging transport request path | `62,000 ns` | `419,166 ns` | `433,792 ns` | `ahead` | `go` at `419,166 ns` |
| Messaging room fanout | `32,000 ns` | `52,042 ns` | `73,208 ns` | `ahead` | `go` at `52,042 ns` |
| Messaging offline replay | `30,000 ns` | `31,250 ns` | `1,097,042 ns` | `near_parity` | `go` at `31,250 ns` |
| Analytics aggregation request path | `60,000 ns` | `64,125 ns` | `208,416 ns` | `ahead` | `go` at `64,125 ns` |
| Analytics worker throughput | `33,000 ns` | `63,507,917 ns` | `82,671,667 ns` | `ahead` | `go` at `63,507,917 ns` |
| Analytics failure recovery | `31,000 ns` | `27,375 ns` | `1,507,417 ns` | `near_parity` | `go` at `27,375 ns` |

## Public Reading Rule

- keep claims tied to these workload classes
- treat `near_parity` as evidence of competitiveness, not automatic superiority
- pair this page with diagnostics and proof packs when publishing claims

## Source Artifacts

- matrix JSON: [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)
- diagnostics JSON: [advanced-backend-matrix-2.6.0-diagnostics.json](artifacts/advanced-backend-matrix-2.6.0-diagnostics.json)
- release bundle: [advanced-backend-release-bundle-2.6.0.json](artifacts/advanced-backend-release-bundle-2.6.0.json)


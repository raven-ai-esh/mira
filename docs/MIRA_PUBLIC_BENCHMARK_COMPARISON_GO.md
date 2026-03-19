# MIRA Public Benchmark Comparison: Go

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This comparison is limited to the promoted advanced workload classes from `2.6.0`. It compares `MIRA default` against `go` only on those released workloads.

## Comparison Table

| Workload | `MIRA default` | `go` | Result |
| --- | ---: | ---: | --- |
| Messaging transport request path | `62,000 ns` | `419,166 ns` | `85.2% faster` |
| Messaging room fanout | `32,000 ns` | `52,042 ns` | `38.5% faster` |
| Messaging offline replay | `30,000 ns` | `31,250 ns` | `4.0% faster` |
| Analytics aggregation request path | `60,000 ns` | `64,125 ns` | `6.4% faster` |
| Analytics worker throughput | `33,000 ns` | `63,507,917 ns` | `99.9% faster` |
| Analytics failure recovery | `31,000 ns` | `27,375 ns` | `13.2% slower` |

## Selection Context

- matrix selected backend: `mira_default`
- selection rationale: `mira_default` selected for the 2.6.0 advanced workload matrix because it stays near-parity or better against the fastest foreign baseline on 6/6 workloads, while leading clearly on 4 promoted paths.

## Public Reading Rule

- this page compares only released promoted workloads
- it does not imply that all other tasks should move away from `go` automatically

- source matrix: [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)


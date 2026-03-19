# MIRA Public Benchmark Comparison: Rust

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This comparison is limited to the promoted advanced workload classes from `2.6.0`. It compares `MIRA default` against `rust` only on those released workloads.

## Comparison Table

| Workload | `MIRA default` | `rust` | Result |
| --- | ---: | ---: | --- |
| Messaging transport request path | `62,000 ns` | `433,792 ns` | `85.7% faster` |
| Messaging room fanout | `32,000 ns` | `73,208 ns` | `56.3% faster` |
| Messaging offline replay | `30,000 ns` | `1,097,042 ns` | `97.3% faster` |
| Analytics aggregation request path | `60,000 ns` | `208,416 ns` | `71.2% faster` |
| Analytics worker throughput | `33,000 ns` | `82,671,667 ns` | `100.0% faster` |
| Analytics failure recovery | `31,000 ns` | `1,507,417 ns` | `97.9% faster` |

## Selection Context

- matrix selected backend: `mira_default`
- selection rationale: `mira_default` selected for the 2.6.0 advanced workload matrix because it stays near-parity or better against the fastest foreign baseline on 6/6 workloads, while leading clearly on 4 promoted paths.

## Public Reading Rule

- this page compares only released promoted workloads
- it does not imply that all other tasks should move away from `rust` automatically

- source matrix: [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)


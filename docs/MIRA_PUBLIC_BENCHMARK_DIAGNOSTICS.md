# MIRA Public Benchmark Diagnostics

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This page exposes the command-level provenance for the promoted advanced benchmark matrix. It is not a marketing page. It exists so outside readers can see exactly what was run and how resource usage was recorded.

## Diagnostic Summary

| Workload | Runtime | Command | Elapsed | Max RSS |
| --- | --- | --- | ---: | ---: |
| `messaging_transport` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go messaging_transport 100 1 tmp/mira-native/messaging_transport-go.json` | `11.24 ms` | `5.7 MiB` |
| `messaging_transport` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench messaging_transport 100 1 tmp/mira-native/messaging_transport-rust.json` | `6.92 ms` | `1.6 MiB` |
| `messaging_transport` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_messaging_benchmark.mira messaging_transport_bench 100 1 tmp/mira-native/messaging_transport-mira_default.json` | `844.55 ms` | `97.1 MiB` |
| `messaging_fanout` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go messaging_fanout 50 2 tmp/mira-native/messaging_fanout-go.json` | `5.35 ms` | `4.8 MiB` |
| `messaging_fanout` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench messaging_fanout 50 2 tmp/mira-native/messaging_fanout-rust.json` | `4.79 ms` | `1.6 MiB` |
| `messaging_fanout` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_messaging_benchmark.mira messaging_fanout_bench 50 2 tmp/mira-native/messaging_fanout-mira_default.json` | `765.44 ms` | `96.2 MiB` |
| `messaging_replay` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go messaging_replay 50 2 tmp/mira-native/messaging_replay-go.json` | `5.17 ms` | `4.4 MiB` |
| `messaging_replay` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench messaging_replay 50 2 tmp/mira-native/messaging_replay-rust.json` | `9.93 ms` | `1.6 MiB` |
| `messaging_replay` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_messaging_benchmark.mira messaging_replay_bench 50 2 tmp/mira-native/messaging_replay-mira_default.json` | `747.11 ms` | `96.7 MiB` |
| `analytics_aggregation` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go analytics_aggregation 100 1 tmp/mira-native/analytics_aggregation-go.json` | `4.68 ms` | `4.6 MiB` |
| `analytics_aggregation` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench analytics_aggregation 100 1 tmp/mira-native/analytics_aggregation-rust.json` | `5.35 ms` | `1.5 MiB` |
| `analytics_aggregation` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_analytics_benchmark.mira analytics_aggregation_bench 100 1 tmp/mira-native/analytics_aggregation-mira_default.json` | `727.92 ms` | `95.5 MiB` |
| `analytics_worker_throughput` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go analytics_worker_throughput 50 3 tmp/mira-native/analytics_worker_throughput-go.json` | `328.29 ms` | `4.7 MiB` |
| `analytics_worker_throughput` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench analytics_worker_throughput 50 3 tmp/mira-native/analytics_worker_throughput-rust.json` | `417.91 ms` | `1.8 MiB` |
| `analytics_worker_throughput` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_analytics_benchmark.mira analytics_worker_throughput_bench 50 3 tmp/mira-native/analytics_worker_throughput-mira_default.json` | `755.70 ms` | `95.3 MiB` |
| `analytics_failure_recovery` | `go` | `tmp/mira-native/advanced-backend-bench/backend_bench_go analytics_failure_recovery 50 1 tmp/mira-native/analytics_failure_recovery-go.json` | `5.03 ms` | `4.6 MiB` |
| `analytics_failure_recovery` | `rust` | `mirac/benchmarks/backend_workloads/rust/target/release/mira-backend-bench analytics_failure_recovery 50 1 tmp/mira-native/analytics_failure_recovery-rust.json` | `11.58 ms` | `1.6 MiB` |
| `analytics_failure_recovery` | `mira_default` | `cargo run --release --manifest-path mirac/Cargo.toml -- bench-source-default mira/examples/runtime_advanced_analytics_benchmark.mira analytics_failure_recovery_bench 50 1 tmp/mira-native/analytics_failure_recovery-mira_default.json` | `776.84 ms` | `96.0 MiB` |

## Full Artifact

- raw diagnostics JSON: [advanced-backend-matrix-2.6.0-diagnostics.json](artifacts/advanced-backend-matrix-2.6.0-diagnostics.json)

## Public Reading Rule

- use this page when someone asks how the numbers were obtained
- do not strip away command provenance when restating benchmark results


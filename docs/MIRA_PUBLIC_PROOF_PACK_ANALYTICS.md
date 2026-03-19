# MIRA Public Proof Pack: Analytics

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This pack proves the promoted analytics scope from `MIRA 2.6.0`: aggregation, worker throughput, and failure recovery. Public analytics claims must stay inside that scope.

## Anchor Artifacts

- release anchor: [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- proof pack source: [analytics-hardening-2.6.0.json](artifacts/analytics-hardening-2.6.0.json)
- advanced matrix: [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)

## Canonical Proof Profiles

| Proof profile | Source | Artifact | Verification chain |
| --- | --- | --- | --- |
| `production_analytics_platform` | [runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira) | [production_analytics_platform.mirb3](artifacts/analytics-conformance-artifacts/production_analytics_platform.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `aggregation_worker_service` | [runtime_aggregation_worker_service.mira](mira/examples/runtime_aggregation_worker_service.mira) | [aggregation_worker_service.mirb3](artifacts/analytics-conformance-artifacts/aggregation_worker_service.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `distributed_analytics_cluster` | [runtime_distributed_analytics_cluster.mira](mira/examples/runtime_distributed_analytics_cluster.mira) | [distributed_analytics_cluster.mirb3](artifacts/analytics-conformance-artifacts/distributed_analytics_cluster.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `failover_rebalance_service` | [runtime_failover_rebalance_service.mira](mira/examples/runtime_failover_rebalance_service.mira) | [failover_rebalance_service.mirb3](artifacts/analytics-conformance-artifacts/failover_rebalance_service.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |

## Public Benchmark Snapshot

| Workload | `MIRA default` | `Go` | `Rust` | Classification |
| --- | ---: | ---: | ---: | --- |
| Analytics aggregation request path | `60,000 ns` | `64,125 ns` | `208,416 ns` | `ahead` |
| Analytics worker throughput | `33,000 ns` | `63,507,917 ns` | `82,671,667 ns` | `ahead` |
| Analytics failure recovery | `31,000 ns` | `27,375 ns` | `1,507,417 ns` | `near_parity` |

## Public Reading Rule

- treat this pack as workload-scoped proof, not as a universal language claim
- cite the public matrix and proof pack together
- keep distributed, frontend, and off-scope claims out of messaging or analytics promotion

## Verification Notes

- `production_analytics_platform`: portable bytecode tests passed: 5/5 via [production_analytics_platform.mirb3](artifacts/analytics-conformance-artifacts/production_analytics_platform.mirb3)
- `aggregation_worker_service`: portable bytecode tests passed: 1/1 via [aggregation_worker_service.mirb3](artifacts/analytics-conformance-artifacts/aggregation_worker_service.mirb3)
- `distributed_analytics_cluster`: portable bytecode tests passed: 1/1 via [distributed_analytics_cluster.mirb3](artifacts/analytics-conformance-artifacts/distributed_analytics_cluster.mirb3)
- `failover_rebalance_service`: portable bytecode tests passed: 1/1 via [failover_rebalance_service.mirb3](artifacts/analytics-conformance-artifacts/failover_rebalance_service.mirb3)

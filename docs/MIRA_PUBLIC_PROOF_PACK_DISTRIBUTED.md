# MIRA Public Proof Pack: Distributed Coordination

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This pack proves that `MIRA` has a real distributed runtime and storage coordination surface. It does not claim rank-first distributed performance. Public distributed messaging stays anchored in explicit capability and proof artifacts until a later matrix promotes full distributed dominance.

## Anchor Artifacts

- distributed release anchor: [MIRA_RELEASE_2.4.0.md](docs/MIRA_RELEASE_2.4.0.md)
- distributed benchmark: [distributed-benchmark-2.4.0.json](artifacts/distributed-benchmark-2.4.0.json)
- advanced release anchor: [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- distributed proof-bearing analytics profiles: [analytics-hardening-2.6.0.json](artifacts/analytics-hardening-2.6.0.json)

## Distributed Proof Profiles

| Proof profile | Source | Artifact | Verification chain |
| --- | --- | --- | --- |
| `distributed_analytics_cluster` | [runtime_distributed_analytics_cluster.mira](mira/examples/runtime_distributed_analytics_cluster.mira) | [distributed_analytics_cluster.mirb3](artifacts/analytics-conformance-artifacts/distributed_analytics_cluster.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `failover_rebalance_service` | [runtime_failover_rebalance_service.mira](mira/examples/runtime_failover_rebalance_service.mira) | [failover_rebalance_service.mirb3](artifacts/analytics-conformance-artifacts/failover_rebalance_service.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |

## Distributed Benchmark Snapshot

| Workload | Selected backend | Median | P95 | Units/sec |
| --- | --- | ---: | ---: | ---: |
| `shard_messaging_edge` | `mira_c` | `5,222,000 ns` | `6,810,000 ns` | `5744.925` |
| `distributed_analytics_cluster` | `mira_c` | `11,262,000 ns` | `11,908,000 ns` | `3551.767` |
| `failover_rebalance_service` | `mira_c` | `11,610,000 ns` | `12,335,000 ns` | `3445.306` |

## Public Reading Rule

- selected backend for this proof pack remains `mira_c`
- rationale: Distributed 2.4.0 benchmark artifact is anchored on `mira_c` while the promoted emitted/default backend has not yet been extended to the full distributed coordination surface.
- use this page to show that distributed coordination is implemented, verified, and benchmarked
- do not use this page to claim universal distributed supremacy


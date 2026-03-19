# MIRA Public Proof Pack: Messaging

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This pack proves the promoted messaging scope from `MIRA 2.6.0`: transport, room fanout, and offline replay. Public messaging claims must stay inside that scope.

## Anchor Artifacts

- release anchor: [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- proof pack source: [messaging-hardening-2.6.0.json](artifacts/messaging-hardening-2.6.0.json)
- advanced matrix: [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)

## Canonical Proof Profiles

| Proof profile | Source | Artifact | Verification chain |
| --- | --- | --- | --- |
| `production_messenger_backend` | [runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira) | [production_messenger_backend.mirb3](artifacts/messaging-conformance-artifacts/production_messenger_backend.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `room_fanout_service` | [runtime_room_fanout_service.mira](mira/examples/runtime_room_fanout_service.mira) | [room_fanout_service.mirb3](artifacts/messaging-conformance-artifacts/room_fanout_service.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |
| `offline_catchup_worker` | [runtime_offline_catchup_worker.mira](mira/examples/runtime_offline_catchup_worker.mira) | [offline_catchup_worker.mirb3](artifacts/messaging-conformance-artifacts/offline_catchup_worker.mirb3) | `check`, `test`, `emit-binary`, `check-binary`, `test-binary` |

## Public Benchmark Snapshot

| Workload | `MIRA default` | `Go` | `Rust` | Classification |
| --- | ---: | ---: | ---: | --- |
| Messaging transport request path | `62,000 ns` | `419,166 ns` | `433,792 ns` | `ahead` |
| Messaging room fanout | `32,000 ns` | `52,042 ns` | `73,208 ns` | `ahead` |
| Messaging offline replay | `30,000 ns` | `31,250 ns` | `1,097,042 ns` | `near_parity` |

## Public Reading Rule

- treat this pack as workload-scoped proof, not as a universal language claim
- cite the public matrix and proof pack together
- keep distributed, frontend, and off-scope claims out of messaging or analytics promotion

## Verification Notes

- `production_messenger_backend`: portable bytecode tests passed: 5/5 via [production_messenger_backend.mirb3](artifacts/messaging-conformance-artifacts/production_messenger_backend.mirb3)
- `room_fanout_service`: portable bytecode tests passed: 1/1 via [room_fanout_service.mirb3](artifacts/messaging-conformance-artifacts/room_fanout_service.mirb3)
- `offline_catchup_worker`: portable bytecode tests passed: 1/1 via [offline_catchup_worker.mirb3](artifacts/messaging-conformance-artifacts/offline_catchup_worker.mirb3)

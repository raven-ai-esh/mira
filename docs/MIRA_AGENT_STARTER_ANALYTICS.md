# MIRA Agent Starter: Analytics Worker

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Primary anchor:
- [runtime_aggregation_worker_service.mira](mira/examples/runtime_aggregation_worker_service.mira)

## Use This Starter For

- first analytics/statistics worker flow
- first queue/cache/aggregation onboarding path
- first analytics target for a coding agent

## Ask Your Agent

Use a prompt like this:

```text
Build a small MIRA analytics worker starting from runtime_aggregation_worker_service.mira.
Keep canonical SSA/block form, stay inside the promoted 2.6.0 analytics scope,
and change one narrow part of the aggregation flow such as batch size, summary
logic, or queue item shape. After editing, run check, test, emit-binary,
check-binary, and test-binary.
```

## Verification Path

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_aggregation_worker_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_aggregation_worker_service.mira

cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_aggregation_worker_service.mira tmp/mira-native/runtime_aggregation_worker_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_aggregation_worker_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_aggregation_worker_service.mirb3
```

## Graduate Next

Move to the richer production-shaped anchor:

- [runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)

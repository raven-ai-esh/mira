# MIRA Agent Onboarding Commands

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

Copy-paste command packs for the `3.1.0` onboarding release.

## API Starter Commands

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_agent_api_service.mira
cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_agent_api_service.mira
cargo run --manifest-path mirac/Cargo.toml -- test-default mira/examples/runtime_agent_api_service.mira
cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_agent_api_service.mira tmp/mira-native/runtime_agent_api_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_agent_api_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_agent_api_service.mirb3
```

## Messaging Starter Commands

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_direct_message_service.mira
cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_direct_message_service.mira
cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_direct_message_service.mira tmp/mira-native/runtime_direct_message_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_direct_message_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_direct_message_service.mirb3
```

## Analytics Starter Commands

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_aggregation_worker_service.mira
cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_aggregation_worker_service.mira
cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_aggregation_worker_service.mira tmp/mira-native/runtime_aggregation_worker_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_aggregation_worker_service.mirb3
cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_aggregation_worker_service.mirb3
```

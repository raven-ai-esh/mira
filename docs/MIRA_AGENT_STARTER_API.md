# MIRA Agent Starter: Backend API

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Primary anchor:
- [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)

## Use This Starter For

- first maintained API-shaped `MIRA` service
- first coding-agent generation target
- first low-risk verification flow

## Ask Your Agent

Use a prompt like this:

```text
Build a small MIRA API service starting from runtime_agent_api_service.mira.
Keep canonical SSA/block form, target native, and preserve the maintained
service shape. Add one new API status path or small JSON-aware branch without
expanding the documented 2.6.0 scope. After editing, run check, test,
test-default, emit-binary, check-binary, and test-binary.
```

## Verification Path

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_agent_api_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_agent_api_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test-default mira/examples/runtime_agent_api_service.mira

cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_agent_api_service.mira tmp/mira-native/runtime_agent_api_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_agent_api_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_agent_api_service.mirb3
```

## Graduate Next

When this starter feels easy, move to:

- [runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)
- [runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)

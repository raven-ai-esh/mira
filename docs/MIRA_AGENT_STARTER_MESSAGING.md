# MIRA Agent Starter: Messaging Service

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Primary anchor:
- [runtime_direct_message_service.mira](mira/examples/runtime_direct_message_service.mira)

## Use This Starter For

- first message-delivery workflow
- first dedup/ack/retry service-shaped `MIRA` example
- first messaging target for a coding agent

## Ask Your Agent

Use a prompt like this:

```text
Build a small MIRA messaging service starting from runtime_direct_message_service.mira.
Keep the canonical SSA/block shape and stay inside the promoted 2.6.0 messaging
scope. Modify the flow by adding one small delivery rule, conversation id, or
ack/retry branch, then run check, test, emit-binary, check-binary, and
test-binary.
```

## Verification Path

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_direct_message_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_direct_message_service.mira

cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_direct_message_service.mira tmp/mira-native/runtime_direct_message_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_direct_message_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_direct_message_service.mirb3
```

## Graduate Next

Move to the richer production-shaped anchor:

- [runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)

# MIRA Public Quickstart

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Goal: go from zero to one verified service-shaped `MIRA` program quickly.

## Prerequisites

- `rustup` / `cargo`
- `clang`
- this repository checked out locally

Working directory:

```bash
cd <mira-public-repo>
```

## First Program

Use the maintained API-service anchor:

- [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)

## Step 1. Validate The Source

```bash
cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_agent_api_service.mira
```

Expected outcome:

- `{"ok":true,...}`

## Step 2. Run Embedded Tests

```bash
cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_agent_api_service.mira
```

## Step 3. Verify The Promoted Default Backend

```bash
cargo run --manifest-path mirac/Cargo.toml -- test-default mira/examples/runtime_agent_api_service.mira
```

This is the key public check: it exercises the promoted default path instead of
stopping at source validation.

## Step 4. Verify The `MIRB3` Path

```bash
cargo run --manifest-path mirac/Cargo.toml -- emit-binary mira/examples/runtime_agent_api_service.mira tmp/mira-native/runtime_agent_api_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- check-binary tmp/mira-native/runtime_agent_api_service.mirb3

cargo run --manifest-path mirac/Cargo.toml -- test-binary tmp/mira-native/runtime_agent_api_service.mirb3
```

## Optional Step 5. Inspect A Larger Service Anchor

Once the quickstart passes, inspect one promoted production-shaped service:

- [runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)
- [runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)

## What This Quickstart Proves

After these commands, you have already verified:

- canonical source validation
- embedded tests
- promoted default backend execution
- `MIRB3` roundtrip execution

That is enough to judge whether `MIRA` is real for its current public scope.

## Next Steps

- starter packs:
  [MIRA_AGENT_STARTER_API.md](docs/MIRA_AGENT_STARTER_API.md),
  [MIRA_AGENT_STARTER_MESSAGING.md](docs/MIRA_AGENT_STARTER_MESSAGING.md),
  [MIRA_AGENT_STARTER_ANALYTICS.md](docs/MIRA_AGENT_STARTER_ANALYTICS.md)
- first-run demo:
  [MIRA_FIRST_RUN_DEMO.md](docs/MIRA_FIRST_RUN_DEMO.md)
- copy-paste commands:
  [MIRA_AGENT_ONBOARDING_COMMANDS.md](docs/MIRA_AGENT_ONBOARDING_COMMANDS.md)
- prompt kit:
  [MIRA_AGENT_PROMPT_KIT.md](docs/MIRA_AGENT_PROMPT_KIT.md)
- reference corpus:
  [MIRA_PUBLIC_REFERENCE_EXAMPLES.md](docs/MIRA_PUBLIC_REFERENCE_EXAMPLES.md)
- benchmark evidence:
  [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- agent guide:
  [MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md](docs/MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md)

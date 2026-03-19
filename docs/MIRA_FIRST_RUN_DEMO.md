# MIRA First-Run Demo

Date: `2026-03-19`
Goal: convert a fresh reader from public positioning to one verified MIRA service
with minimal operator choice.

## Demo Path

1. Read [MIRA_PUBLIC_POSITIONING.md](docs/MIRA_PUBLIC_POSITIONING.md)
2. Open [MIRA_PUBLIC_QUICKSTART.md](docs/MIRA_PUBLIC_QUICKSTART.md)
3. Copy the API starter prompt from [MIRA_AGENT_PROMPT_KIT.md](docs/MIRA_AGENT_PROMPT_KIT.md)
4. Use [MIRA_AGENT_STARTER_API.md](docs/MIRA_AGENT_STARTER_API.md) as the starter page
5. Run the exact command block below

## Minimal Operator Choice Command Block

```bash
cd <mira-public-repo>

cargo run --manifest-path mirac/Cargo.toml -- check mira/examples/runtime_agent_api_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test mira/examples/runtime_agent_api_service.mira

cargo run --manifest-path mirac/Cargo.toml -- test-default mira/examples/runtime_agent_api_service.mira
```

## Success Condition

The first run is considered successful when:

- the source validates
- embedded tests pass
- the promoted default backend passes

At that point the user or agent has already crossed from “curious” to “running a
real MIRA service.”

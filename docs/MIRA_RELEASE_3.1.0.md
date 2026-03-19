# MIRA 3.1.0

Date: `2026-03-19`
Theme: `Agent Onboarding and First-Use Conversion`

## Scope

`3.1.0` closes the onboarding layer of the community-growth checklist.

It does not widen the technical `MIRA` platform. It packages the existing
`2.6.0` promoted backend scope into starter packs, prompt packs, demo flow, and
copy-paste command sets so a new agent or operator can reach first use quickly.

## What Landed

Onboarding artifacts:

- API starter:
  [MIRA_AGENT_STARTER_API.md](docs/MIRA_AGENT_STARTER_API.md)
- messaging starter:
  [MIRA_AGENT_STARTER_MESSAGING.md](docs/MIRA_AGENT_STARTER_MESSAGING.md)
- analytics starter:
  [MIRA_AGENT_STARTER_ANALYTICS.md](docs/MIRA_AGENT_STARTER_ANALYTICS.md)
- prompt kit:
  [MIRA_AGENT_PROMPT_KIT.md](docs/MIRA_AGENT_PROMPT_KIT.md)
- first-run demo:
  [MIRA_FIRST_RUN_DEMO.md](docs/MIRA_FIRST_RUN_DEMO.md)
- copy-paste command sheet:
  [MIRA_AGENT_ONBOARDING_COMMANDS.md](docs/MIRA_AGENT_ONBOARDING_COMMANDS.md)

## Verification Package

Starter-anchor command chains verified:

- API starter:
  - [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)
- messaging starter:
  - [runtime_direct_message_service.mira](mira/examples/runtime_direct_message_service.mira)
- analytics starter:
  - [runtime_aggregation_worker_service.mira](mira/examples/runtime_aggregation_worker_service.mira)

Each onboarding flow is tied to exact commands rather than descriptive prose
alone.

## Outcome

After `3.1.0`:

- a fresh agent has ready-to-use prompts
- a new operator has low-choice copy-paste commands
- the public trust layer from `3.0.0` now converts into first practical use

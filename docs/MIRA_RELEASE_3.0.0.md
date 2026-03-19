# MIRA 3.0.0

Date: `2026-03-19`
Theme: `Public Positioning and Trust Foundation`

## Scope

`3.0.0` does not extend the technical language/runtime surface.

It closes the first public community-growth release from
[MIRA_COMMUNITY_GROWTH_CHECKLIST.md](docs/MIRA_COMMUNITY_GROWTH_CHECKLIST.md)
by packaging the proven `2.6.0` baseline into a coherent public-facing trust
layer.

## What Landed

Public-facing docs:

- positioning:
  [MIRA_PUBLIC_POSITIONING.md](docs/MIRA_PUBLIC_POSITIONING.md)
- non-goals:
  [MIRA_PUBLIC_NON_GOALS.md](docs/MIRA_PUBLIC_NON_GOALS.md)
- quickstart:
  [MIRA_PUBLIC_QUICKSTART.md](docs/MIRA_PUBLIC_QUICKSTART.md)
- benchmark evidence:
  [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- reference examples:
  [MIRA_PUBLIC_REFERENCE_EXAMPLES.md](docs/MIRA_PUBLIC_REFERENCE_EXAMPLES.md)
- agent guide:
  [MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md](docs/MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md)

Supporting planning assets:

- roadmap:
  [MIRA_COMMUNITY_GROWTH_ROADMAP.md](docs/MIRA_COMMUNITY_GROWTH_ROADMAP.md)
- checklist:
  [MIRA_COMMUNITY_GROWTH_CHECKLIST.md](docs/MIRA_COMMUNITY_GROWTH_CHECKLIST.md)

## Verification Package

Technical anchor checks:

- baseline source check for:
  [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)
- quickstart command package:
  - `check`
  - `test`
  - `test-default`
  - `emit-binary -> check-binary -> test-binary`
- evidence artifact availability:
  - [advanced-generation-contract-2.6.0.json](artifacts/advanced-generation-contract-2.6.0.json)
  - [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)
  - [advanced-backend-release-bundle-2.6.0.json](artifacts/advanced-backend-release-bundle-2.6.0.json)

## Outcome

`MIRA 3.0.0` makes the backend-dominance baseline legible to outsiders.

After this release:

- a new reader can understand what `MIRA` is for
- the main public claims stay narrow and defendable
- a first service-shaped evaluation path is explicit
- benchmark evidence is summarized without hype
- agents have a public target guide instead of only internal context

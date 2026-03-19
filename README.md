# MIRA

`MIRA` is an agent-first, verification-first, backend-first language.

It is designed for cases where the important question is not:

- is this pleasant for humans to hand-write?

but:

- can an agent generate it canonically, verify it predictably, and run it on a
  promoted native backend with evidence-backed performance?

## Why MIRA Exists

Mainstream languages optimize for human flexibility, ecosystem breadth, and
stylistic freedom.

`MIRA` optimizes for a different target:

- canonical generation
- low stylistic entropy
- stable verification rail
- workload-scoped backend evidence
- agent-native delivery discipline

## Current Public Scope

As of `MIRA 2.6.0`, the promoted public scope is:

- advanced messaging backends
- analytics/statistics microservice workloads
- distributed backend coordination paths supported by released proof artifacts

This is a narrow claim on purpose.

`MIRA` is not presented here as a universal replacement for `Python`, `Go`, or
`Rust`.

## Start Here

- positioning:
  [MIRA_PUBLIC_POSITIONING.md](docs/MIRA_PUBLIC_POSITIONING.md)
- docs index:
  [MIRA_PUBLIC_DOCS_INDEX.md](docs/MIRA_PUBLIC_DOCS_INDEX.md)
- quickstart:
  [MIRA_PUBLIC_QUICKSTART.md](docs/MIRA_PUBLIC_QUICKSTART.md)
- agent generation guide:
  [MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md](docs/MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md)

## Public Proof

- benchmark evidence index:
  [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- benchmark matrix:
  [MIRA_PUBLIC_BENCHMARK_MATRIX.md](docs/MIRA_PUBLIC_BENCHMARK_MATRIX.md)
- messaging proof pack:
  [MIRA_PUBLIC_PROOF_PACK_MESSAGING.md](docs/MIRA_PUBLIC_PROOF_PACK_MESSAGING.md)
- analytics proof pack:
  [MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md](docs/MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md)
- distributed proof pack:
  [MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md](docs/MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md)

## First Working Paths

- backend API starter:
  [MIRA_AGENT_STARTER_API.md](docs/MIRA_AGENT_STARTER_API.md)
- messaging starter:
  [MIRA_AGENT_STARTER_MESSAGING.md](docs/MIRA_AGENT_STARTER_MESSAGING.md)
- analytics starter:
  [MIRA_AGENT_STARTER_ANALYTICS.md](docs/MIRA_AGENT_STARTER_ANALYTICS.md)
- first-run demo:
  [MIRA_FIRST_RUN_DEMO.md](docs/MIRA_FIRST_RUN_DEMO.md)

## Canonical Examples

- API service:
  [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)
- direct messaging service:
  [runtime_direct_message_service.mira](mira/examples/runtime_direct_message_service.mira)
- aggregation worker:
  [runtime_aggregation_worker_service.mira](mira/examples/runtime_aggregation_worker_service.mira)
- production messenger backend:
  [runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)
- production analytics platform:
  [runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)

## Public Rule

If you talk about `MIRA` publicly:

- keep the claim scoped
- link proof pages, not just slogans
- link benchmark diagnostics when discussing numbers
- do not extrapolate beyond the released promoted workload classes

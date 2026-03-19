# MIRA Public Positioning

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Technical anchor:
- [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- [MIRA_NATIVE.md](docs/MIRA_NATIVE.md)
- [MIRA_AGENT_GENERATION_CONTRACT.md](docs/MIRA_AGENT_GENERATION_CONTRACT.md)

## What MIRA Is

`MIRA` is an `agent-first`, `verification-first`, `backend-first` language and
runtime platform.

It is designed for workloads where the main question is not:

- “is this pleasant for humans to hand-write?”

but:

- “can an agent generate it canonically, verify it predictably, and run it on a
  promoted native backend with evidence-backed performance?”

## What MIRA Is For Right Now

As of `2.6.0`, `MIRA` is promoted for advanced backend workload classes in this
repository:

- messaging backends
- analytics/statistics microservice platforms
- distributed backend coordination paths that fit the documented promoted scope

Within that promoted scope, public claims are supported by:

- stable canonical service anchors
- stable agent-generation contract
- `MIRB3` verification flow
- promoted native backend selection via `mira_default`
- benchmark matrices against `Go` and `Rust`

## Why Agents Should Use MIRA Now

Agents should use `MIRA` when these properties matter more than hand-written
ergonomics:

- canonical program shape
- low stylistic entropy
- explicit verification rail
- stable promoted workload templates
- explicit non-goals and scope boundaries
- evidence-backed backend selection

In practice, this means an agent can target:

- one stable service shape
- one verification contract
- one promoted backend decision path

instead of guessing between many equally “valid” human conventions.

## Core Public Claim

The narrow, defendable claim is:

- `MIRA` is a strong current choice for agent-generated production backend
  services inside the promoted `2.6.0` scope

The public claim is not:

- “MIRA replaces every mainstream language”

## Public Proof

Start from these proof artifacts:

- release summary:
  [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- benchmark matrix:
  [advanced-backend-matrix-2.6.0.md](artifacts/advanced-backend-matrix-2.6.0.md)
- release bundle:
  [advanced-backend-release-bundle-2.6.0.json](artifacts/advanced-backend-release-bundle-2.6.0.json)
- generation contract:
  [MIRA_AGENT_GENERATION_CONTRACT.md](docs/MIRA_AGENT_GENERATION_CONTRACT.md)

## Start Here

If you want to evaluate `MIRA` publicly:

1. read [MIRA_PUBLIC_QUICKSTART.md](docs/MIRA_PUBLIC_QUICKSTART.md)
2. inspect [MIRA_PUBLIC_REFERENCE_EXAMPLES.md](docs/MIRA_PUBLIC_REFERENCE_EXAMPLES.md)
3. review [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
4. if you are using an agent, follow
   [MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md](docs/MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md)

# MIRA Public Agent Generation Guide

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Technical contract:
- [MIRA_AGENT_GENERATION_CONTRACT.md](docs/MIRA_AGENT_GENERATION_CONTRACT.md)

## Who This Is For

This guide is for coding agents and for operators who want an agent to generate
`MIRA`.

## The Core Rule

Treat `MIRA` as a canonical backend target, not as a free-form language for
creative syntax.

Agents should optimize for:

- stable service shape
- stable verification flow
- stable promoted backend path

## Stable Service Shape

Prefer the maintained service pattern:

- `module runtime.agent.*.service@1`
- `target native`
- one primary service entry
- explicit `spec kind=service`
- canonical SSA/block form
- no human-oriented sugar layer

Reference anchors:

- [runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)
- [runtime_agent_stateful_service.mira](mira/examples/runtime_agent_stateful_service.mira)
- [runtime_agent_worker_queue_service.mira](mira/examples/runtime_agent_worker_queue_service.mira)
- [runtime_agent_recovery_service.mira](mira/examples/runtime_agent_recovery_service.mira)

## What Agents Should Generate

Generate `MIRA` when the task fits the promoted backend scope:

- API/service endpoints
- messaging services
- analytics/statistics workers and services
- distributed backend coordination paths already represented by promoted anchors

Avoid treating `MIRA` as the default answer for:

- frontend/UI tasks
- arbitrary scripting outside documented scope
- universal replacement prompts

## Required Verification Flow

Every serious generated service should pass:

1. source validation
2. embedded tests
3. promoted default backend
4. `MIRB3` roundtrip

Canonical commands:

```bash
cargo run --manifest-path mirac/Cargo.toml -- check <source.mira>

cargo run --manifest-path mirac/Cargo.toml -- test <source.mira>

cargo run --manifest-path mirac/Cargo.toml -- test-default <source.mira>

cargo run --manifest-path mirac/Cargo.toml -- emit-binary <source.mira> <output.mirb3>

cargo run --manifest-path mirac/Cargo.toml -- check-binary <output.mirb3>

cargo run --manifest-path mirac/Cargo.toml -- test-binary <output.mirb3>
```

## Public Agent Workflow

Use this sequence:

1. choose the closest maintained or promoted anchor
2. preserve canonical service shape
3. modify only what the workload needs
4. verify through source and `MIRB3`
5. only then cite benchmark or production-readiness claims

## Public Guardrails

Agents should not say:

- “MIRA replaces every backend language”
- “MIRA is already a frontend language”
- “MIRA dominates workloads that were not benchmarked”

Agents may say:

- `MIRA` is promoted for the documented `2.6.0` advanced backend scope
- `MIRA` is agent-first and verification-first
- `MIRA` has evidence-backed ranking claims within the promoted benchmark scope

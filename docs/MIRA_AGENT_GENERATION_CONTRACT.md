# MIRA Agent Generation Contract

Date: `2026-03-19`
Status: `stable for the 2.6.0 promoted advanced backend scope`

This document defines the stable contract for LLM-generated production backend
services on top of `MIRA 2.6.0`.

## Purpose

`MIRA` is not optimized for human-first ergonomics. The stable contract here is
instead designed for:

- canonical generation by LLMs
- predictable lowering through validator, `MIRB3`, portable execution, and
  promoted native backends
- stable service templates that can be reused without human-oriented sugar

## Stable Service Shape

For the promoted `2.6.0` scope, generated backend services should follow these
rules:

- `module runtime.agent.*.service@1`
- `target native`
- one maintained primary service entry function per file
- service entry returns a scalar status/result
- service entry declares `spec kind=service`
- `constraints` must include:
  - `agent_platform`
  - `maintained`
- code stays in canonical SSA/block form without sugar layers
- workload-specific helper functions may exist in the same file when they are
  part of the maintained canonical anchor

Stable maintained baseline quartet from `2.0.0`:

- API service:
  - [mira/examples/runtime_agent_api_service.mira](mira/examples/runtime_agent_api_service.mira)
- stateful service:
  - [mira/examples/runtime_agent_stateful_service.mira](mira/examples/runtime_agent_stateful_service.mira)
- worker/queue service:
  - [mira/examples/runtime_agent_worker_queue_service.mira](mira/examples/runtime_agent_worker_queue_service.mira)
- recovery-oriented service:
  - [mira/examples/runtime_agent_recovery_service.mira](mira/examples/runtime_agent_recovery_service.mira)

Reusable template anchors:

- API/service template:
  - [mira/examples/runtime_service_api_template.mira](mira/examples/runtime_service_api_template.mira)
- worker template:
  - [mira/examples/runtime_service_worker_template.mira](mira/examples/runtime_service_worker_template.mira)

## Promoted Platform Scope

The stable agent-generation scope now combines these already-proven slices:

- protocol/service runtime breadth from `1.1.0`
- data/storage depth from `1.2.0`
- concurrency/recovery runtime from `1.3.0`
- portable native backend dominance from `1.4.0`
- autonomous verification and operational hardening from `1.5.0`
- realtime transport and session runtime from `2.1.0`
- messaging and delivery core from `2.2.0`
- analytics and dataflow depth from `2.3.0`
- distributed coordination from `2.4.0`
- portable native backend supremacy from `2.5.0`
- operational and benchmark dominance from `2.6.0`

This means generated services may rely on:

- HTTP/session/runtime helpers
- JSON/config/string/bytes helpers
- DB/cache/client/runtime slices already promoted in the release train
- concurrency primitives already proven in the promoted runtime subset
- service lifecycle, observability, checkpoint, degraded-mode, and release
  hardening helpers
- promoted messaging runtime semantics: transport, fanout, replay, catch-up,
  checkpointed delivery
- promoted analytics semantics: ingest, aggregation, worker throughput,
  failure-recovery
- promoted distributed semantics: shard routing, lease transfer, placement,
  coordination

## Maintained Advanced Anchors

Rich production-shaped capability anchors:

- messenger backend:
  - [mira/examples/runtime_production_messenger_backend.mira](mira/examples/runtime_production_messenger_backend.mira)
- analytics microservice platform:
  - [mira/examples/runtime_production_analytics_platform.mira](mira/examples/runtime_production_analytics_platform.mira)

Benchmark-only emitted-friendly dominance anchors:

- messaging benchmark:
  - [mira/examples/runtime_advanced_messaging_benchmark.mira](mira/examples/runtime_advanced_messaging_benchmark.mira)
- analytics benchmark:
  - [mira/examples/runtime_advanced_analytics_benchmark.mira](mira/examples/runtime_advanced_analytics_benchmark.mira)

## Verification Contract

Every maintained generated service in the promoted scope must pass all of:

1. source validation and embedded tests:
   - `mirac check`
   - `mirac test`
2. promoted default path:
   - `mirac test-default`
3. `MIRB3` roundtrip:
   - `emit-binary -> check-binary -> test-binary`
4. promoted native backends:
   - `mirac test-asm-arm64` where runnable
   - `mirac test-asm-x86_64` on promoted target triples
5. release matrix inclusion:
   - representative benchmark evidence

Authoritative release tooling for the stable quartet:

- contract checker:
  - [mirac/tools/agent_generation_contract_check.py](mirac/tools/agent_generation_contract_check.py)
- benchmark matrix:
  - [mirac/tools/agent_platform_benchmark_matrix.py](mirac/tools/agent_platform_benchmark_matrix.py)
- release bundle:
  - [mirac/tools/agent_platform_release_bundle.py](mirac/tools/agent_platform_release_bundle.py)

Authoritative `2.6.0` advanced-scope tooling:

- advanced contract checker:
  - [mirac/tools/advanced_backend_generation_contract_check.py](mirac/tools/advanced_backend_generation_contract_check.py)
- advanced workload matrix:
  - [mirac/tools/advanced_backend_dominance_matrix.py](mirac/tools/advanced_backend_dominance_matrix.py)
- advanced release bundle:
  - [mirac/tools/advanced_backend_release_bundle.py](mirac/tools/advanced_backend_release_bundle.py)
- advanced operational conformance:
  - [mirac/tools/advanced_workload_conformance.py](mirac/tools/advanced_workload_conformance.py)
- advanced regression hardening:
  - [mirac/tools/advanced_workload_resource_regression.py](mirac/tools/advanced_workload_resource_regression.py)

## Contract Rules For New Generated Services

- Prefer canonical existing ops over introducing new sugar.
- Prefer stable maintained templates over bespoke source layouts.
- Do not introduce new service abstractions unless they can be verified across
  source, `MIRB3`, portable execution, and promoted native backends.
- Do not claim production-default backend support for a service that only works
  on the `C -> clang` fallback path.
- If `mira_default` needs a fallback for a benchmark or release artifact,
  record that fallback explicitly in the artifact.

## Non-Goals

This contract does not promise:

- universal ecosystem parity with every `Rust`, `Go`, or `Python` backend stack
- arbitrary new syntax for human convenience
- backend promotion from narrow kernel-only wins

It does promise:

- stable canonical generation for the promoted backend-platform scope
- stable maintained service anchors
- stable release evidence for the promoted scope

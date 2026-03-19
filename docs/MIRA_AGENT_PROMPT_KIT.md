# MIRA Agent Prompt Kit

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

This page contains copy-ready prompts for coding agents.

## Prompt 1. Build A Backend API

```text
Build a small MIRA backend API starting from mira/examples/runtime_agent_api_service.mira.
Keep target native, canonical SSA/block form, and maintained service shape.
Do not introduce syntax sugar or widen the documented 2.6.0 scope.
After changes, run:
- check
- test
- test-default
- emit-binary
- check-binary
- test-binary
Summarize the exact artifact paths and verification results.
```

## Prompt 2. Build A Messaging Service

```text
Build a narrow MIRA messaging service starting from mira/examples/runtime_direct_message_service.mira.
Stay inside the promoted 2.6.0 messaging scope and preserve canonical service shape.
Modify one delivery behavior only, then run:
- check
- test
- emit-binary
- check-binary
- test-binary
Report the commands run and whether the result stays inside public messaging scope.
```

## Prompt 3. Build An Analytics Worker

```text
Build a narrow MIRA analytics worker starting from mira/examples/runtime_aggregation_worker_service.mira.
Stay inside the promoted 2.6.0 analytics scope, keep canonical SSA/block form,
and change one small aggregation behavior only.
Then run:
- check
- test
- emit-binary
- check-binary
- test-binary
Summarize the exact behavior changed and the verification evidence.
```

## Rules For Agents

- prefer existing maintained or promoted anchors over free-form greenfield source
- keep the change narrow on first pass
- verify before making public claims
- do not claim universal language replacement
- do not claim frontend/fullstack support from these backend-only examples

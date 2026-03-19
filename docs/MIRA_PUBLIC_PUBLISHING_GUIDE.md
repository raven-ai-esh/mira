# MIRA Public Publishing Guide

Date: `2026-03-19`
Status: `launch bundle preparation`

This guide explains how to move the current local `MIRA` launch assets into a
real public release.

## Current Constraint

The current workspace:

- is not itself a git repository
- has no attached public remote
- stores launch assets locally under `docs/` and `tmp/mira-release/`

So the right next step is not “write more local markdown”.
The right next step is “export a clean public repo and publish the launch
bundle”.

## Recommended Public Repo Structure

```text
README.md
LICENSE
docs/
  index.md
  positioning.md
  non-goals.md
  quickstart.md
  agent-generation-guide.md
  benchmark-evidence.md
  benchmark-matrix.md
  benchmark-diagnostics.md
  comparison-go.md
  comparison-rust.md
  proof-messaging.md
  proof-analytics.md
  proof-distributed.md
  starter-api.md
  starter-messaging.md
  starter-analytics.md
examples/
artifacts/
  advanced-backend-matrix-2.6.0.json
  advanced-backend-matrix-2.6.0-diagnostics.json
  messaging-hardening-2.6.0.json
  analytics-hardening-2.6.0.json
  distributed-benchmark-2.4.0.json
```

## Recommended Export Order

1. Start from:
   [MIRA_PUBLIC_REPO_README.md](docs/MIRA_PUBLIC_REPO_README.md)
2. Copy the docs linked in:
   [MIRA_PUBLIC_DOCS_INDEX.md](docs/MIRA_PUBLIC_DOCS_INDEX.md)
3. Copy the public proof artifacts referenced by:
   [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
4. Copy starter examples from `mira/examples/` that appear in the public docs.
5. Include the benchmark/proof JSON artifacts from `tmp/mira-release/`.

## What Must Be Public On Day 1

- public repo README
- docs index
- quickstart
- benchmark evidence index
- public benchmark matrix
- at least one messaging proof pack
- at least one analytics proof pack
- one agent-generation guide

## What Can Stay Internal On Day 1

- internal evolution artifacts
- memory files
- workspace-specific automation
- unrelated personal-activity directories

## Verification Before Publishing

Run these locally before copying or publishing:

```bash
python3 mirac/tools/public_benchmark_visibility_refresh.py
python3 mirac/tools/public_launch_bundle_check.py
```

## Public Rule

The public repo should feel like a clean `MIRA` product surface, not like a
dump of the whole personal workspace.

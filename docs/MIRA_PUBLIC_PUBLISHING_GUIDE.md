# MIRA Public Publishing Guide

Date: `2026-03-19`
Status: `public repo published; next step is external announcement cadence`

This guide explains how to move the prepared `MIRA` launch assets into an
ongoing public release process.

## Current State

The current workspace:

- is not itself a git repository
- stores source launch assets locally under `docs/` and `tmp/mira-release/`

The dedicated public repo is now live at:

- [raven-ai-esh/mira](https://github.com/raven-ai-esh/mira)

So the next step is no longer “publish the repo”.
The next step is “run the external announcement and early-adopter loop”.

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
   [MIRA_PUBLIC_REPO_README.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_REPO_README.md)
2. Copy the docs linked in:
   [MIRA_PUBLIC_DOCS_INDEX.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_DOCS_INDEX.md)
3. Copy the public proof artifacts referenced by:
   [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
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

## Verification Before Updating The Public Repo

Run these locally before copying or publishing:

```bash
python3 /Users/sheremetovegor/Documents/Raven/personal-activity/mirac/tools/public_benchmark_visibility_refresh.py
python3 /Users/sheremetovegor/Documents/Raven/personal-activity/mirac/tools/public_launch_bundle_check.py
```

## Public Rule

The public repo should feel like a clean `MIRA` product surface, not like a
dump of the whole personal workspace.

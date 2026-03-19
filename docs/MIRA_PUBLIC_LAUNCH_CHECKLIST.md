# MIRA Public Launch Checklist

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Status: `prepared locally, not yet externally published`

This checklist is the exact execution path from local launch assets to public
release.

## Phase 1. Public Repo Creation

- [ ] Create or choose the dedicated public `MIRA` repository.
- [ ] Copy the launch-facing docs into that repo.
- [ ] Add the public-facing README from
      [MIRA_PUBLIC_REPO_README.md](docs/MIRA_PUBLIC_REPO_README.md)
      as the repo root `README.md`.
- [ ] Ensure license, contribution policy, and issue/discussion settings are
      present.
- [ ] Publish the canonical docs index from
      [MIRA_PUBLIC_DOCS_INDEX.md](docs/MIRA_PUBLIC_DOCS_INDEX.md).

## Phase 2. Proof Visibility

- [ ] Publish benchmark evidence index:
      [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- [ ] Publish benchmark matrix and diagnostics pages.
- [ ] Publish `Go` and `Rust` comparison pages.
- [ ] Publish messaging, analytics, and distributed proof packs.
- [ ] Publish or attach raw evidence artifacts from `tmp/mira-release`.

## Phase 3. First-Use Conversion

- [ ] Publish quickstart and first-run demo.
- [ ] Publish API, messaging, and analytics starter packs.
- [ ] Publish prompt kit and onboarding commands.
- [ ] Verify that a new visitor can follow one working path without private
      context.

## Phase 4. First Announcement Wave

- [ ] Publish short launch post.
- [ ] Publish technical benchmark/proof post.
- [ ] Publish agent-first usage post.
- [ ] Publish one long-form technical explanation.
- [ ] Publish one community call-to-try or call-to-reproduce.

## Phase 5. Post-Launch Hygiene

- [ ] Open an issue/discussion channel for early adopters.
- [ ] Capture first external questions and blockers.
- [ ] Start tracking which links and artifacts get reused externally.
- [ ] Feed external confusion back into docs and onboarding pack.

## Required Public Truths

- [ ] Every claim about `MIRA` is scoped to released workloads.
- [ ] Every benchmark claim links to reproducible evidence.
- [ ] Distributed proof is described as capability plus verification unless a
      stronger matrix is released.
- [ ] No post claims universal replacement of `Python`, `Go`, or `Rust`.

## Exit Condition

Launch is considered actually public only when:

- the repo is reachable externally
- the docs index is published
- the proof pages are reachable
- at least one first-wave public post has gone out

Until then, this remains a prepared launch bundle, not a completed launch.

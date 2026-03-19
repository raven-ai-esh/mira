# MIRA Public Launch Checklist

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Status: `public repo published, announcement wave pending`

This checklist is the exact execution path from local launch assets to public
release.

Public repo:

- [raven-ai-esh/mira](https://github.com/raven-ai-esh/mira)
- [public-launch-v1 release](https://github.com/raven-ai-esh/mira/releases/tag/public-launch-v1)

## Phase 1. Public Repo Creation

- [x] Create or choose the dedicated public `MIRA` repository.
- [x] Copy the launch-facing docs into that repo.
- [x] Add the public-facing README from
      [MIRA_PUBLIC_REPO_README.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_REPO_README.md)
      as the repo root `README.md`.
- [ ] Ensure license, contribution policy, and issue/discussion settings are
      present.
- [x] Publish the canonical docs index from
      [MIRA_PUBLIC_DOCS_INDEX.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_DOCS_INDEX.md).

## Phase 2. Proof Visibility

- [x] Publish benchmark evidence index:
      [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](/Users/sheremetovegor/Documents/Raven/personal-activity/docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- [x] Publish benchmark matrix and diagnostics pages.
- [x] Publish `Go` and `Rust` comparison pages.
- [x] Publish messaging, analytics, and distributed proof packs.
- [x] Publish or attach raw evidence artifacts from `tmp/mira-release`.

## Phase 3. First-Use Conversion

- [x] Publish quickstart and first-run demo.
- [x] Publish API, messaging, and analytics starter packs.
- [x] Publish prompt kit and onboarding commands.
- [ ] Verify that a new visitor can follow one working path without private
      context.

## Phase 4. First Announcement Wave

- [ ] Publish short launch post.
- [ ] Publish technical benchmark/proof post.
- [ ] Publish agent-first usage post.
- [ ] Publish one long-form technical explanation.
- [ ] Publish one community call-to-try or call-to-reproduce.

## Phase 5. Post-Launch Hygiene

- [x] Open an issue/discussion channel for early adopters.
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

Launch is considered externally announced only when:

- the repo is reachable externally
- the docs index is published
- the proof pages are reachable
- at least one first-wave public post has gone out

Until then, this is a live public repo and docs surface, but not yet a fully
announced launch.

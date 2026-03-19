# MIRA Public Launch Checklist

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`
Status: `public repo published, first-wave posts live`

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

- [x] Publish short launch post.
      GitHub: [issue #3](https://github.com/raven-ai-esh/mira/issues/3)
      Moltbook profile: [codexravencore910](https://www.moltbook.com/u/codexravencore910)
      Moltbook post id: `24f1148c-c5e6-4166-9948-6e62ce4931b0`
- [x] Publish technical benchmark/proof post.
      GitHub: [issue #2](https://github.com/raven-ai-esh/mira/issues/2)
- [x] Publish agent-first usage post.
      GitHub: [issue #5](https://github.com/raven-ai-esh/mira/issues/5)
- [x] Publish one long-form technical explanation.
      GitHub: [issue #4](https://github.com/raven-ai-esh/mira/issues/4)
- [x] Publish one community call-to-try or call-to-reproduce.
      GitHub: [issue #1](https://github.com/raven-ai-esh/mira/issues/1)

## Phase 5. Post-Launch Hygiene

- [x] Open an issue/discussion channel for early adopters.
- [ ] Capture first external questions and blockers.
- [ ] Start tracking which links and artifacts get reused externally.
- [ ] Feed external confusion back into docs and onboarding pack.

## Required Public Truths

- [x] Every claim about `MIRA` is scoped to released workloads.
- [x] Every benchmark claim links to reproducible evidence.
- [x] Distributed proof is described as capability plus verification unless a
      stronger matrix is released.
- [x] No post claims universal replacement of `Python`, `Go`, or `Rust`.

## Exit Condition

Launch is considered externally announced only when:

- the repo is reachable externally
- the docs index is published
- the proof pages are reachable
- at least one first-wave public post has gone out

That condition is now met.

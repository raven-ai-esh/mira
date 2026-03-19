# MIRA Public Launch Bundle

Date: `2026-03-19`
Status: `ready for external publication packaging`

This document summarizes the first real public launch bundle for `MIRA`.

## What This Bundle Is

This is the package you can export into a dedicated public `MIRA` repository or
docs site right now.

It includes:

- public repo-facing README
- public docs index
- proof packs
- benchmark pages
- starter and quickstart assets
- launch execution checklist
- first-wave post drafts

## Main Entry Points

- repo README:
  [MIRA_PUBLIC_REPO_README.md](docs/MIRA_PUBLIC_REPO_README.md)
- docs index:
  [MIRA_PUBLIC_DOCS_INDEX.md](docs/MIRA_PUBLIC_DOCS_INDEX.md)
- benchmark evidence index:
  [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- publishing guide:
  [MIRA_PUBLIC_PUBLISHING_GUIDE.md](docs/MIRA_PUBLIC_PUBLISHING_GUIDE.md)
- launch checklist:
  [MIRA_PUBLIC_LAUNCH_CHECKLIST.md](docs/MIRA_PUBLIC_LAUNCH_CHECKLIST.md)
- launch posts:
  [MIRA_PUBLIC_LAUNCH_POSTS.md](docs/MIRA_PUBLIC_LAUNCH_POSTS.md)

## Source Of Truth

This bundle is anchored in:

- `MIRA 2.6.0` public technical baseline:
  [MIRA_RELEASE_2.6.0.md](docs/MIRA_RELEASE_2.6.0.md)
- `3.0.0` positioning foundation:
  [MIRA_RELEASE_3.0.0.md](docs/MIRA_RELEASE_3.0.0.md)
- `3.1.0` onboarding foundation:
  [MIRA_RELEASE_3.1.0.md](docs/MIRA_RELEASE_3.1.0.md)
- `3.2.0` proof visibility foundation:
  [MIRA_RELEASE_3.2.0.md](docs/MIRA_RELEASE_3.2.0.md)

## Verification

Validate the bundle locally with:

```bash
python3 mirac/tools/public_benchmark_visibility_refresh.py
python3 mirac/tools/public_launch_bundle_check.py
```

Manifest:

- [manifest-public-launch-v1.json](artifacts/manifest-public-launch-v1.json)

## Important Constraint

This bundle is prepared locally.

It does not mean external publication has already happened.

Actual public launch starts only after:

- public repo creation
- external docs publication
- first-wave public posts

Those exact external actions are tracked in
[MIRA_PUBLIC_LAUNCH_CHECKLIST.md](docs/MIRA_PUBLIC_LAUNCH_CHECKLIST.md).

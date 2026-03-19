# MIRA 3.2.0

Date: `2026-03-19`
Theme: `Public Proof and Benchmark Visibility`

## Scope

`3.2.0` does not widen the technical `MIRA` platform.

It closes the proof-visibility layer of the community-growth roadmap by turning
existing `2.4.0` and `2.6.0` release artifacts into public-facing proof packs,
comparison pages, diagnostics pages, and a repeatable refresh workflow.

## What Landed

Public proof packs:

- messaging:
  [MIRA_PUBLIC_PROOF_PACK_MESSAGING.md](docs/MIRA_PUBLIC_PROOF_PACK_MESSAGING.md)
- analytics:
  [MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md](docs/MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md)
- distributed:
  [MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md](docs/MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md)

Public benchmark pages:

- matrix:
  [MIRA_PUBLIC_BENCHMARK_MATRIX.md](docs/MIRA_PUBLIC_BENCHMARK_MATRIX.md)
- diagnostics:
  [MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md](docs/MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md)
- `Go` comparison:
  [MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md](docs/MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md)
- `Rust` comparison:
  [MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md](docs/MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md)
- short shareable snippets:
  [MIRA_PUBLIC_BENCHMARK_SNIPPETS.md](docs/MIRA_PUBLIC_BENCHMARK_SNIPPETS.md)

Refresh workflow:

- evidence index:
  [MIRA_PUBLIC_BENCHMARK_EVIDENCE.md](docs/MIRA_PUBLIC_BENCHMARK_EVIDENCE.md)
- refresh script:
  [public_benchmark_visibility_refresh.py](mirac/tools/public_benchmark_visibility_refresh.py)

## Verification Package

Primary source artifacts used by the refresh workflow:

- advanced matrix:
  [advanced-backend-matrix-2.6.0.json](artifacts/advanced-backend-matrix-2.6.0.json)
- advanced diagnostics:
  [advanced-backend-matrix-2.6.0-diagnostics.json](artifacts/advanced-backend-matrix-2.6.0-diagnostics.json)
- messaging hardening:
  [messaging-hardening-2.6.0.json](artifacts/messaging-hardening-2.6.0.json)
- analytics hardening:
  [analytics-hardening-2.6.0.json](artifacts/analytics-hardening-2.6.0.json)
- distributed benchmark:
  [distributed-benchmark-2.4.0.json](artifacts/distributed-benchmark-2.4.0.json)

Verification command:

```bash
python3 mirac/tools/public_benchmark_visibility_refresh.py
```

## Outcome

After `3.2.0`:

- public proof is split into workload-specific packs instead of one generic
  benchmark page
- public comparisons against `Go` and `Rust` are narrow, explicit, and
  reproducible
- distributed proof is visible without overclaiming distributed dominance
- benchmark-publication refresh is repeatable from release artifacts rather than
  hand-maintained prose

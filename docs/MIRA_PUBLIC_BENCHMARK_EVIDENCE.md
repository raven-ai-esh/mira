# MIRA Public Benchmark Evidence

Date: `2026-03-19`
Baseline: `MIRA 2.6.0`

## Scope

This page is the public evidence index for benchmark-backed `MIRA` claims.

It stays intentionally narrow:

- promoted advanced messaging and analytics workloads come from `2.6.0`
- distributed coordination proof stays anchored in `2.4.0`
- no universal language-dominance claim is made here

## Public Evidence Pages

- benchmark matrix:
  [MIRA_PUBLIC_BENCHMARK_MATRIX.md](docs/MIRA_PUBLIC_BENCHMARK_MATRIX.md)
- diagnostics:
  [MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md](docs/MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md)
- `Go` comparison:
  [MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md](docs/MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md)
- `Rust` comparison:
  [MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md](docs/MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md)
- short shareable snippets:
  [MIRA_PUBLIC_BENCHMARK_SNIPPETS.md](docs/MIRA_PUBLIC_BENCHMARK_SNIPPETS.md)

## Public Proof Packs

- messaging:
  [MIRA_PUBLIC_PROOF_PACK_MESSAGING.md](docs/MIRA_PUBLIC_PROOF_PACK_MESSAGING.md)
- analytics:
  [MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md](docs/MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md)
- distributed coordination:
  [MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md](docs/MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md)

## Source Artifacts

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
- release bundle:
  [advanced-backend-release-bundle-2.6.0.json](artifacts/advanced-backend-release-bundle-2.6.0.json)

## Refresh Workflow

Rebuild every public benchmark visibility page from the release artifacts with:

```bash
python3 mirac/tools/public_benchmark_visibility_refresh.py
```

This workflow regenerates the matrix, diagnostics, comparison pages, proof
packs, and short public snippets directly from the release JSON artifacts.

## Public Rule

When citing `MIRA` benchmark results publicly:

- keep the claim scoped
- link the matrix or proof pack
- link the diagnostics or bundle when challenged on provenance
- avoid extrapolating to workloads outside the promoted released scope

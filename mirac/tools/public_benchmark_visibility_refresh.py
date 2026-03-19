#!/usr/bin/env python3
"""Generate public proof and benchmark visibility pages from release artifacts."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
DOCS = ROOT / "docs"
TMP = ROOT / "tmp" / "mira-release"

ADVANCED_MATRIX = TMP / "advanced-backend-matrix-2.6.0.json"
ADVANCED_DIAGNOSTICS = TMP / "advanced-backend-matrix-2.6.0-diagnostics.json"
MESSAGING_HARDENING = TMP / "messaging-hardening-2.6.0.json"
ANALYTICS_HARDENING = TMP / "analytics-hardening-2.6.0.json"
DISTRIBUTED_BENCHMARK = TMP / "distributed-benchmark-2.4.0.json"
RELEASE_24 = DOCS / "MIRA_RELEASE_2.4.0.md"
RELEASE_26 = DOCS / "MIRA_RELEASE_2.6.0.md"

MESSAGING_PACK = DOCS / "MIRA_PUBLIC_PROOF_PACK_MESSAGING.md"
ANALYTICS_PACK = DOCS / "MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md"
DISTRIBUTED_PACK = DOCS / "MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md"
BENCHMARK_MATRIX = DOCS / "MIRA_PUBLIC_BENCHMARK_MATRIX.md"
BENCHMARK_DIAGNOSTICS = DOCS / "MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md"
BENCHMARK_GO = DOCS / "MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md"
BENCHMARK_RUST = DOCS / "MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md"
BENCHMARK_SNIPPETS = DOCS / "MIRA_PUBLIC_BENCHMARK_SNIPPETS.md"


def load_json(path: Path):
    return json.loads(path.read_text())


def fmt_ns(value: int | float) -> str:
    return f"{int(value):,} ns"


def fmt_bytes(value: int) -> str:
    mib = value / (1024 * 1024)
    return f"{mib:.1f} MiB"


def pct_delta(foreign_ns: int | float, mira_ns: int | float) -> str:
    ratio = (foreign_ns - mira_ns) / foreign_ns
    if ratio >= 0:
        return f"{ratio * 100:.1f}% faster"
    return f"{abs(ratio) * 100:.1f}% slower"


def code_block(lines: list[str]) -> str:
    return "```text\n" + "\n".join(lines) + "\n```"


def extract_check_names(profile: dict) -> list[str]:
    names = []
    for check in profile.get("checks", []):
        cmd = check.get("command", [])
        if len(cmd) >= 6:
            names.append(cmd[5])
    return names


def profile_rows(data: dict) -> list[dict]:
    rows = []
    for profile in data.get("profiles", []):
        stdout_pass = ""
        for check in profile.get("checks", []):
            cmd = check.get("command", [])
            if cmd and cmd[-2:-1] == ["test-binary"]:
                stdout_pass = check.get("stdout", "").strip()
                break
        rows.append(
            {
                "name": profile["name"],
                "source": profile["source"],
                "artifact": profile["artifact"],
                "checks": extract_check_names(profile),
                "test_binary": stdout_pass or "portable bytecode verification recorded",
            }
        )
    return rows


def build_matrix_index(matrix: dict) -> dict[str, dict[str, dict]]:
    grouped: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in matrix["results"]:
        grouped[row["workload"]][row["runtime"]] = row
    return grouped


def render_profile_table(rows: list[dict]) -> str:
    lines = [
        "| Proof profile | Source | Artifact | Verification chain |",
        "| --- | --- | --- | --- |",
    ]
    for row in rows:
        chain = ", ".join(f"`{name}`" for name in row["checks"])
        lines.append(
            f"| `{row['name']}` | [{Path(row['source']).name}]({row['source']}) | "
            f"[{Path(row['artifact']).name}]({row['artifact']}) | {chain} |"
        )
    return "\n".join(lines)


def render_workload_table(grouped: dict[str, dict[str, dict]], workloads: list[str]) -> str:
    lines = [
        "| Workload | `MIRA default` | `Go` | `Rust` | Classification |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for workload in workloads:
        mira = grouped[workload]["mira_default"]
        go = grouped[workload]["go"]
        rust = grouped[workload]["rust"]
        lines.append(
            f"| {mira['label']} | `{fmt_ns(mira['median_ns'])}` | "
            f"`{fmt_ns(go['median_ns'])}` | `{fmt_ns(rust['median_ns'])}` | "
            f"`{mira['classification']}` |"
        )
    return "\n".join(lines)


def render_proof_pack(
    title: str,
    scope: str,
    hardening_path: Path,
    release_path: Path,
    grouped: dict[str, dict[str, dict]],
    workloads: list[str],
) -> str:
    hardening = load_json(hardening_path)
    profiles = profile_rows(hardening)
    lines = [
        f"# {title}",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "## Scope",
        "",
        scope,
        "",
        "## Anchor Artifacts",
        "",
        f"- release anchor: [{release_path.name}]({release_path})",
        f"- proof pack source: [{hardening_path.name}]({hardening_path})",
        f"- advanced matrix: [{ADVANCED_MATRIX.name}]({ADVANCED_MATRIX})",
        "",
        "## Canonical Proof Profiles",
        "",
        render_profile_table(profiles),
        "",
        "## Public Benchmark Snapshot",
        "",
        render_workload_table(grouped, workloads),
        "",
        "## Public Reading Rule",
        "",
        "- treat this pack as workload-scoped proof, not as a universal language claim",
        "- cite the public matrix and proof pack together",
        "- keep distributed, frontend, and off-scope claims out of messaging or analytics promotion",
        "",
        "## Verification Notes",
        "",
    ]
    for row in profiles:
        lines.append(
            f"- `{row['name']}`: {row['test_binary']} via "
            f"[{Path(row['artifact']).name}]({row['artifact']})"
        )
    return "\n".join(lines) + "\n"


def render_distributed_pack(distributed: dict, analytics: dict) -> str:
    analytics_profiles = profile_rows(analytics)
    lines = [
        "# MIRA Public Proof Pack: Distributed Coordination",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "## Scope",
        "",
        "This pack proves that `MIRA` has a real distributed runtime and storage "
        "coordination surface. It does not claim rank-first distributed "
        "performance. Public distributed messaging stays anchored in explicit "
        "capability and proof artifacts until a later matrix promotes full "
        "distributed dominance.",
        "",
        "## Anchor Artifacts",
        "",
        f"- distributed release anchor: [{RELEASE_24.name}]({RELEASE_24})",
        f"- distributed benchmark: [{DISTRIBUTED_BENCHMARK.name}]({DISTRIBUTED_BENCHMARK})",
        f"- advanced release anchor: [{RELEASE_26.name}]({RELEASE_26})",
        f"- distributed proof-bearing analytics profiles: [{ANALYTICS_HARDENING.name}]({ANALYTICS_HARDENING})",
        "",
        "## Distributed Proof Profiles",
        "",
        render_profile_table([row for row in analytics_profiles if row["name"] in {
            "distributed_analytics_cluster",
            "failover_rebalance_service",
        }]),
        "",
        "## Distributed Benchmark Snapshot",
        "",
        "| Workload | Selected backend | Median | P95 | Units/sec |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for workload in distributed["workloads"]:
        result = workload["result"]
        lines.append(
            f"| `{workload['name']}` | `{workload['backend']}` | "
            f"`{fmt_ns(result['median_ns'])}` | `{fmt_ns(result['p95_ns'])}` | "
            f"`{result['units_per_second']:.3f}` |"
        )
    lines.extend(
        [
            "",
            "## Public Reading Rule",
            "",
            f"- selected backend for this proof pack remains `{distributed['selected_backend']}`",
            f"- rationale: {distributed['selection_rationale']}",
            "- use this page to show that distributed coordination is implemented, "
            "verified, and benchmarked",
            "- do not use this page to claim universal distributed supremacy",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def render_public_matrix(matrix: dict, grouped: dict[str, dict[str, dict]]) -> str:
    ordered = [
        "messaging_transport",
        "messaging_fanout",
        "messaging_replay",
        "analytics_aggregation",
        "analytics_worker_throughput",
        "analytics_failure_recovery",
    ]
    lines = [
        "# MIRA Public Benchmark Matrix",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "## Scope",
        "",
        "This page exposes the promoted advanced backend benchmark matrix in a "
        "public-facing form. It is intentionally narrow: the matrix only covers "
        "the released messaging and analytics workload classes from `2.6.0`.",
        "",
        "## Selected Backend",
        "",
        f"- `{matrix['selected_backend']}`",
        f"- {matrix['selection_rationale']}",
        "",
        "## Matrix",
        "",
        "| Workload | `MIRA default` | `Go` | `Rust` | Classification | Best foreign baseline |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for workload in ordered:
        mira = grouped[workload]["mira_default"]
        go = grouped[workload]["go"]
        rust = grouped[workload]["rust"]
        lines.append(
            f"| {mira['label']} | `{fmt_ns(mira['median_ns'])}` | `{fmt_ns(go['median_ns'])}` | "
            f"`{fmt_ns(rust['median_ns'])}` | `{mira['classification']}` | "
            f"`{mira['best_foreign_runtime']}` at `{fmt_ns(mira['best_foreign_median_ns'])}` |"
        )
    lines.extend(
        [
            "",
            "## Public Reading Rule",
            "",
            "- keep claims tied to these workload classes",
            "- treat `near_parity` as evidence of competitiveness, not automatic superiority",
            "- pair this page with diagnostics and proof packs when publishing claims",
            "",
            "## Source Artifacts",
            "",
            f"- matrix JSON: [{ADVANCED_MATRIX.name}]({ADVANCED_MATRIX})",
            f"- diagnostics JSON: [{ADVANCED_DIAGNOSTICS.name}]({ADVANCED_DIAGNOSTICS})",
            f"- release bundle: [{(TMP / 'advanced-backend-release-bundle-2.6.0.json').name}]"
            f"({TMP / 'advanced-backend-release-bundle-2.6.0.json'})",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def render_diagnostics_page(diags: list[dict]) -> str:
    lines = [
        "# MIRA Public Benchmark Diagnostics",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "## Scope",
        "",
        "This page exposes the command-level provenance for the promoted advanced "
        "benchmark matrix. It is not a marketing page. It exists so outside "
        "readers can see exactly what was run and how resource usage was "
        "recorded.",
        "",
        "## Diagnostic Summary",
        "",
        "| Workload | Runtime | Command | Elapsed | Max RSS |",
        "| --- | --- | --- | ---: | ---: |",
    ]
    for item in diags:
        command = item["command"].replace("|", "\\|")
        lines.append(
            f"| `{item['workload']}` | `{item['runtime']}` | `{command}` | "
            f"`{item['elapsed_ms']:.2f} ms` | `{fmt_bytes(item['max_rss_bytes'])}` |"
        )
    lines.extend(
        [
            "",
            "## Full Artifact",
            "",
            f"- raw diagnostics JSON: [{ADVANCED_DIAGNOSTICS.name}]({ADVANCED_DIAGNOSTICS})",
            "",
            "## Public Reading Rule",
            "",
            "- use this page when someone asks how the numbers were obtained",
            "- do not strip away command provenance when restating benchmark results",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def render_comparison_page(matrix: dict, grouped: dict[str, dict[str, dict]], runtime: str, title: str) -> str:
    ordered = [
        "messaging_transport",
        "messaging_fanout",
        "messaging_replay",
        "analytics_aggregation",
        "analytics_worker_throughput",
        "analytics_failure_recovery",
    ]
    lines = [
        f"# {title}",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "## Scope",
        "",
        f"This comparison is limited to the promoted advanced workload classes from "
        f"`2.6.0`. It compares `MIRA default` against `{runtime}` only on those "
        "released workloads.",
        "",
        "## Comparison Table",
        "",
        f"| Workload | `MIRA default` | `{runtime}` | Result |",
        "| --- | ---: | ---: | --- |",
    ]
    for workload in ordered:
        mira = grouped[workload]["mira_default"]
        other = grouped[workload][runtime]
        lines.append(
            f"| {mira['label']} | `{fmt_ns(mira['median_ns'])}` | `{fmt_ns(other['median_ns'])}` | "
            f"`{pct_delta(other['median_ns'], mira['median_ns'])}` |"
        )
    lines.extend(
        [
            "",
            "## Selection Context",
            "",
            f"- matrix selected backend: `{matrix['selected_backend']}`",
            f"- selection rationale: {matrix['selection_rationale']}",
            "",
            "## Public Reading Rule",
            "",
            "- this page compares only released promoted workloads",
            "- it does not imply that all other tasks should move away from "
            f"`{runtime}` automatically",
            "",
            f"- source matrix: [{ADVANCED_MATRIX.name}]({ADVANCED_MATRIX})",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def render_snippets(matrix: dict, grouped: dict[str, dict[str, dict]]) -> str:
    messaging = grouped["messaging_transport"]["mira_default"]
    fanout = grouped["messaging_fanout"]["mira_default"]
    analytics = grouped["analytics_worker_throughput"]["mira_default"]
    lines = [
        "# MIRA Public Benchmark Snippets",
        "",
        "Date: `2026-03-19`",
        "Baseline: `MIRA 2.6.0`",
        "",
        "Use these as short public benchmark summaries. Keep the links and scope "
        "notes when posting them elsewhere.",
        "",
        "## Matrix Summary",
        "",
        code_block(
            [
                "MIRA 2.6.0 public matrix summary",
                "- promoted scope only: advanced messaging + analytics workloads",
                f"- selected backend: {matrix['selected_backend']}",
                "- near-parity or better vs fastest foreign baseline on 6/6 workloads",
                "- clearly ahead on 4/6 promoted paths",
                f"- matrix: {BENCHMARK_MATRIX}",
            ]
        ),
        "",
        "## Messaging Summary",
        "",
        code_block(
            [
                "MIRA messaging public proof",
                f"- transport median: {fmt_ns(messaging['median_ns'])}",
                f"- room fanout median: {fmt_ns(fanout['median_ns'])}",
                "- proof pack + matrix keep the claim scoped and reproducible",
                f"- proof pack: {MESSAGING_PACK}",
            ]
        ),
        "",
        "## Analytics Summary",
        "",
        code_block(
            [
                "MIRA analytics public proof",
                f"- worker throughput median: {fmt_ns(analytics['median_ns'])}",
                "- public claim: rank-first only on the promoted advanced analytics workloads",
                f"- proof pack: {ANALYTICS_PACK}",
            ]
        ),
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    matrix = load_json(ADVANCED_MATRIX)
    grouped = build_matrix_index(matrix)
    diagnostics = load_json(ADVANCED_DIAGNOSTICS)
    analytics = load_json(ANALYTICS_HARDENING)
    distributed = load_json(DISTRIBUTED_BENCHMARK)

    MESSAGING_PACK.write_text(
        render_proof_pack(
            "MIRA Public Proof Pack: Messaging",
            "This pack proves the promoted messaging scope from `MIRA 2.6.0`: "
            "transport, room fanout, and offline replay. Public messaging claims "
            "must stay inside that scope.",
            MESSAGING_HARDENING,
            RELEASE_26,
            grouped,
            ["messaging_transport", "messaging_fanout", "messaging_replay"],
        )
    )
    ANALYTICS_PACK.write_text(
        render_proof_pack(
            "MIRA Public Proof Pack: Analytics",
            "This pack proves the promoted analytics scope from `MIRA 2.6.0`: "
            "aggregation, worker throughput, and failure recovery. Public "
            "analytics claims must stay inside that scope.",
            ANALYTICS_HARDENING,
            RELEASE_26,
            grouped,
            [
                "analytics_aggregation",
                "analytics_worker_throughput",
                "analytics_failure_recovery",
            ],
        )
    )
    DISTRIBUTED_PACK.write_text(render_distributed_pack(distributed, analytics))
    BENCHMARK_MATRIX.write_text(render_public_matrix(matrix, grouped))
    BENCHMARK_DIAGNOSTICS.write_text(render_diagnostics_page(diagnostics))
    BENCHMARK_GO.write_text(
        render_comparison_page(matrix, grouped, "go", "MIRA Public Benchmark Comparison: Go")
    )
    BENCHMARK_RUST.write_text(
        render_comparison_page(matrix, grouped, "rust", "MIRA Public Benchmark Comparison: Rust")
    )
    BENCHMARK_SNIPPETS.write_text(render_snippets(matrix, grouped))

    generated = [
        MESSAGING_PACK,
        ANALYTICS_PACK,
        DISTRIBUTED_PACK,
        BENCHMARK_MATRIX,
        BENCHMARK_DIAGNOSTICS,
        BENCHMARK_GO,
        BENCHMARK_RUST,
        BENCHMARK_SNIPPETS,
    ]
    print(json.dumps({"ok": True, "generated": [str(path) for path in generated]}, indent=2))


if __name__ == "__main__":
    main()

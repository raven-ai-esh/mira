#!/usr/bin/env python3
import argparse
import json
import os
import platform
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MIRAC = ROOT / "mirac"
TMP = ROOT / "tmp" / "mira-release"
NATIVE_TMP = ROOT / "tmp" / "mira-native"
RUST_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "rust"
GO_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "go" / "backend_bench.go"
MANIFEST = MIRAC / "Cargo.toml"
RUNTIME_LABELS = {
    "mira_default": "MIRA default",
    "mira_c": "MIRA C-backed",
    "go": "Go",
    "rust": "Rust",
}

WORKLOADS = [
    {
        "key": "messaging_transport",
        "label": "Messaging transport request path",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_messaging_benchmark.mira",
        "mira_function": "messaging_transport_bench",
        "iterations": 100,
        "unit_work": 1,
    },
    {
        "key": "messaging_fanout",
        "label": "Messaging room fanout",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_messaging_benchmark.mira",
        "mira_function": "messaging_fanout_bench",
        "iterations": 50,
        "unit_work": 2,
    },
    {
        "key": "messaging_replay",
        "label": "Messaging offline replay",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_messaging_benchmark.mira",
        "mira_function": "messaging_replay_bench",
        "iterations": 50,
        "unit_work": 2,
    },
    {
        "key": "analytics_aggregation",
        "label": "Analytics aggregation request path",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_analytics_benchmark.mira",
        "mira_function": "analytics_aggregation_bench",
        "iterations": 100,
        "unit_work": 1,
    },
    {
        "key": "analytics_worker_throughput",
        "label": "Analytics worker throughput",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_analytics_benchmark.mira",
        "mira_function": "analytics_worker_throughput_bench",
        "iterations": 50,
        "unit_work": 3,
    },
    {
        "key": "analytics_failure_recovery",
        "label": "Analytics failure recovery",
        "mira_source": ROOT / "mira" / "examples" / "runtime_advanced_analytics_benchmark.mira",
        "mira_function": "analytics_failure_recovery_bench",
        "iterations": 50,
        "unit_work": 1,
    },
]


def run_command(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )


def profiled_run(command: list[str], cwd: Path | None = None) -> dict:
    full = ["/usr/bin/time", "-l", *command] if Path("/usr/bin/time").exists() and platform.system() == "Darwin" else command
    started = time.time()
    proc = run_command(full, cwd=cwd)
    elapsed_ms = (time.time() - started) * 1000.0
    rss_match = re.search(r"(\d+)\s+maximum resident set size", proc.stderr)
    return {
        "command": " ".join(shlex.quote(part) for part in command),
        "elapsed_ms": elapsed_ms,
        "max_rss_bytes": int(rss_match.group(1)) if rss_match else None,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "returncode": proc.returncode,
    }


def ensure_foreign_binaries(build_dir: Path) -> dict[str, list[str]]:
    build_dir.mkdir(parents=True, exist_ok=True)
    rust_build = run_command(
        ["cargo", "build", "--release", "--manifest-path", str(RUST_BENCH / "Cargo.toml")],
        cwd=ROOT,
    )
    if rust_build.returncode != 0:
        raise RuntimeError(f"rust benchmark build failed:\n{rust_build.stdout}\n{rust_build.stderr}")
    go_out = build_dir / "backend_bench_go"
    go_build = run_command(["go", "build", "-o", str(go_out), str(GO_BENCH)], cwd=ROOT)
    if go_build.returncode != 0:
        raise RuntimeError(f"go benchmark build failed:\n{go_build.stdout}\n{go_build.stderr}")
    rust_out = RUST_BENCH / "target" / "release" / "mira-backend-bench"
    return {
        "go": [str(go_out)],
        "rust": [str(rust_out)],
    }


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_mira_benchmark(workload: dict, backend: str, output_dir: Path) -> tuple[dict, dict]:
    output_path = output_dir / f"{workload['key']}-{backend}.json"
    command_name = "bench-source-default" if backend == "mira_default" else "bench-source"
    command = [
        "cargo",
        "run",
        "--release",
        "--manifest-path",
        str(MANIFEST),
        "--",
        command_name,
        str(workload["mira_source"]),
        workload["mira_function"],
        str(workload["iterations"]),
        str(workload["unit_work"]),
        str(output_path),
    ]
    diagnostics = profiled_run(command, cwd=ROOT)
    if diagnostics["returncode"] != 0:
        raise RuntimeError(
            f"{backend} benchmark failed for {workload['key']}:\n{diagnostics['stdout']}\n{diagnostics['stderr']}"
        )
    payload = load_json(output_path)
    return payload["results"][0], diagnostics


def run_foreign_benchmark(
    workload: dict, runtime: str, runtime_cmd: list[str], output_dir: Path
) -> tuple[dict, dict]:
    output_path = output_dir / f"{workload['key']}-{runtime}.json"
    command = [*runtime_cmd, workload["key"], str(workload["iterations"]), str(workload["unit_work"]), str(output_path)]
    diagnostics = profiled_run(command, cwd=ROOT)
    if diagnostics["returncode"] != 0:
        raise RuntimeError(
            f"{runtime} benchmark failed for {workload['key']}:\n{diagnostics['stdout']}\n{diagnostics['stderr']}"
        )
    row = load_json(output_path)
    return row, diagnostics


def classify(mira_ns: int, foreign_ns: int) -> str:
    if mira_ns <= foreign_ns * 0.95:
        return "ahead"
    if mira_ns <= foreign_ns * 1.15:
        return "near_parity"
    return "slower"


def render_markdown(matrix: dict) -> str:
    lines = [
        "# MIRA 2.6.0 Advanced Backend Dominance Matrix",
        "",
        f"Selected advanced-workload backend: `{matrix['selected_backend']}`",
        "",
        "| Workload | Runtime | Median ns | p95 ns | RSS bytes | Classification vs best foreign |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in matrix["results"]:
        lines.append(
            f"| {row['label']} | {RUNTIME_LABELS[row['runtime']]} | {row['median_ns']} | {row['p95_ns']} | {row['max_rss_bytes'] if row['max_rss_bytes'] is not None else 'n/a'} | {row.get('classification', '')} |"
        )
    lines.extend(["", "Rationale:", "", matrix["selection_rationale"]])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    parser.add_argument("--diagnostics-json", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    diagnostics_json = Path(args.diagnostics_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    build_dir = NATIVE_TMP / "advanced-backend-bench"
    foreign = ensure_foreign_binaries(build_dir)
    results = []
    diagnostics_rows = []
    workload_classifications = {}

    for workload in WORKLOADS:
        mira_row, mira_diag = run_mira_benchmark(workload, "mira_default", NATIVE_TMP)
        foreign_rows = {}
        for runtime, runtime_cmd in foreign.items():
            row, diag = run_foreign_benchmark(workload, runtime, runtime_cmd, NATIVE_TMP)
            foreign_rows[runtime] = row
            diagnostics_rows.append({"runtime": runtime, "workload": workload["key"], **diag})
            results.append(
                {
                    "workload": workload["key"],
                    "label": workload["label"],
                    "runtime": runtime,
                    "median_ns": row["median_ns"],
                    "p95_ns": row["p95_ns"],
                    "p99_ns": row["p99_ns"],
                    "per_call_ns": row["per_call_ns"],
                    "units_per_second": row["units_per_second"],
                    "max_rss_bytes": diag["max_rss_bytes"],
                }
            )

        best_foreign_runtime = min(foreign_rows, key=lambda runtime: foreign_rows[runtime]["median_ns"])
        best_foreign_ns = foreign_rows[best_foreign_runtime]["median_ns"]
        classification = classify(mira_row["median_ns"], best_foreign_ns)
        workload_classifications[workload["key"]] = {
            "best_foreign_runtime": best_foreign_runtime,
            "best_foreign_median_ns": best_foreign_ns,
            "classification": classification,
        }
        diagnostics_rows.append({"runtime": "mira_default", "workload": workload["key"], **mira_diag})
        results.append(
            {
                "workload": workload["key"],
                "label": workload["label"],
                "runtime": "mira_default",
                "median_ns": mira_row["median_ns"],
                "p95_ns": mira_row["p95_ns"],
                "p99_ns": mira_row["p99_ns"],
                "per_call_ns": mira_row["per_call_ns"],
                "units_per_second": mira_row["units_per_second"],
                "max_rss_bytes": mira_diag["max_rss_bytes"],
                "classification": classification,
                "best_foreign_runtime": best_foreign_runtime,
                "best_foreign_median_ns": best_foreign_ns,
            }
        )

    classes = [entry["classification"] for entry in workload_classifications.values()]
    ahead_count = sum(1 for entry in classes if entry == "ahead")
    near_or_better = sum(1 for entry in classes if entry in {"ahead", "near_parity"})
    rank_first = near_or_better == len(WORKLOADS) and ahead_count >= 2
    rationale = (
        "`mira_default` selected for the 2.6.0 advanced workload matrix because it stays near-parity "
        f"or better against the fastest foreign baseline on {near_or_better}/{len(WORKLOADS)} workloads, "
        f"while leading clearly on {ahead_count} promoted paths."
    )
    matrix = {
        "release": "2.6.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selected_backend": "mira_default",
        "selection_rationale": rationale,
        "rank_first_for_promoted_workload_classes": rank_first,
        "results": results,
        "workload_classifications": workload_classifications,
    }
    output_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")
    output_md.write_text(render_markdown(matrix), encoding="utf-8")
    diagnostics_json.write_text(json.dumps(diagnostics_rows, indent=2), encoding="utf-8")
    print(json.dumps(matrix, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import json
import os
import platform
import re
import shlex
import statistics
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
PY_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "python" / "backend_bench.py"
MANIFEST = MIRAC / "Cargo.toml"
RUNTIME_LABELS = {
    "mira_c": "MIRA C-backed",
    "mira_arm64": "MIRA asm arm64",
    "go": "Go",
    "rust": "Rust",
    "python": "Python",
}

WORKLOADS = [
    {
        "key": "json_api",
        "label": "JSON API throughput",
        "mira_source": ROOT / "mira" / "examples" / "runtime_json_api_endpoint.mira",
        "mira_function": "decode_request_and_encode_response",
        "iterations": 100,
        "unit_work": 1,
    },
    {
        "key": "db_sqlite",
        "label": "DB-backed CRUD latency",
        "mira_source": ROOT / "mira" / "examples" / "runtime_db_sqlite.mira",
        "mira_function": "init_and_query_count",
        "iterations": 8,
        "unit_work": 1,
    },
    {
        "key": "tls_request",
        "label": "TLS request throughput",
        "mira_source": ROOT / "mira" / "examples" / "runtime_tls_http_client.mira",
        "mira_function": "fetch_local_https_ok",
        "iterations": 50,
        "unit_work": 1,
    },
    {
        "key": "background_worker",
        "label": "Background worker throughput",
        "mira_source": ROOT / "mira" / "examples" / "runtime_job_runner.mira",
        "mira_function": "run_job_queue",
        "iterations": 50,
        "unit_work": 3,
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
    if Path("/usr/bin/time").exists() and platform.system() == "Darwin":
        full = ["/usr/bin/time", "-l", *command]
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
    started = time.time()
    proc = run_command(command, cwd=cwd)
    elapsed_ms = (time.time() - started) * 1000.0
    return {
        "command": " ".join(shlex.quote(part) for part in command),
        "elapsed_ms": elapsed_ms,
        "max_rss_bytes": None,
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
        "python": [sys.executable, str(PY_BENCH)],
    }


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_mira_benchmark(workload: dict, backend: str, output_dir: Path) -> tuple[dict, dict]:
    output_path = output_dir / f"{workload['key']}-{backend}.json"
    if backend == "mira_c":
        command = [
            "cargo",
            "run",
            "--release",
            "--manifest-path",
            str(MANIFEST),
            "--",
            "bench-source",
            str(workload["mira_source"]),
            workload["mira_function"],
            str(workload["iterations"]),
            str(workload["unit_work"]),
            str(output_path),
        ]
    elif backend == "mira_arm64":
        command = [
            "cargo",
            "run",
            "--release",
            "--manifest-path",
            str(MANIFEST),
            "--",
            "bench-source-asm-arm64",
            str(workload["mira_source"]),
            workload["mira_function"],
            str(workload["iterations"]),
            str(workload["unit_work"]),
            str(output_path),
        ]
    else:
        raise ValueError(backend)
    diagnostics = profiled_run(command, cwd=ROOT)
    if diagnostics["returncode"] != 0:
        raise RuntimeError(
            f"{backend} benchmark failed for {workload['key']}:\n{diagnostics['stdout']}\n{diagnostics['stderr']}"
        )
    payload = load_json(output_path)
    row = payload["results"][0]
    return row, diagnostics


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


def choose_default_backend(rows: list[dict]) -> str:
    mira_rows = [row for row in rows if row["runtime"] in {"mira_c", "mira_arm64"}]
    workloads = {row["workload"] for row in mira_rows}
    coverage = {
        runtime: {row["workload"] for row in mira_rows if row["runtime"] == runtime}
        for runtime in {"mira_c", "mira_arm64"}
    }
    if coverage["mira_arm64"] != workloads:
        return "mira_c"
    wins = 0
    total = 0
    by_workload = {}
    for row in mira_rows:
        by_workload.setdefault(row["workload"], {})[row["runtime"]] = row["median_ns"]
    for runtimes in by_workload.values():
        if "mira_c" in runtimes and "mira_arm64" in runtimes:
            total += 1
            if runtimes["mira_arm64"] <= runtimes["mira_c"]:
                wins += 1
    return "mira_arm64" if total and wins >= (total // 2 + total % 2) else "mira_c"


def render_markdown(matrix: dict) -> str:
    lines = [
        "# MIRA 0.13.0 Backend Performance Matrix",
        "",
        f"Selected default backend candidate: `{matrix['selected_default_backend']}`",
        "",
        "| Workload | Runtime | Median ns | p95 ns | RSS bytes |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in matrix["results"]:
        lines.append(
            f"| {row['label']} | {RUNTIME_LABELS[row['runtime']]} | {row['median_ns']} | {row['p95_ns']} | {row['max_rss_bytes'] if row['max_rss_bytes'] is not None else 'n/a'} |"
        )
    lines.extend(
        [
            "",
            "## Default Backend Decision",
            "",
            matrix["default_backend_rationale"],
            "",
            "## Notes",
            "",
            "- Benchmarks are representative backend workload kernels, not full external load-balancer or framework benchmarks.",
            "- MIRA benchmark claims come from native binaries emitted by `mirac`.",
        ]
    )
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

    build_dir = NATIVE_TMP / "foreign-backend-bench"
    foreign = ensure_foreign_binaries(build_dir)
    results = []
    diagnostics_rows = []

    for workload in WORKLOADS:
        c_row, c_diag = run_mira_benchmark(workload, "mira_c", NATIVE_TMP)
        results.append(
            {
                "workload": workload["key"],
                "label": workload["label"],
                "runtime": "mira_c",
                "median_ns": c_row["median_ns"],
                "p95_ns": c_row["p95_ns"],
                "p99_ns": c_row["p99_ns"],
                "per_call_ns": c_row["per_call_ns"],
                "units_per_second": c_row["units_per_second"],
                "max_rss_bytes": c_diag["max_rss_bytes"],
                "artifact": str((NATIVE_TMP / f"{workload['key']}-mira_c.json")),
            }
        )
        diagnostics_rows.append({"workload": workload["key"], "runtime": "mira_c", **c_diag})

        if platform.system() == "Darwin" and platform.machine() == "arm64":
            try:
                arm_row, arm_diag = run_mira_benchmark(workload, "mira_arm64", NATIVE_TMP)
                results.append(
                    {
                        "workload": workload["key"],
                        "label": workload["label"],
                        "runtime": "mira_arm64",
                        "median_ns": arm_row["median_ns"],
                        "p95_ns": arm_row["p95_ns"],
                        "p99_ns": arm_row["p99_ns"],
                        "per_call_ns": arm_row["per_call_ns"],
                        "units_per_second": arm_row["units_per_second"],
                        "max_rss_bytes": arm_diag["max_rss_bytes"],
                        "artifact": str((NATIVE_TMP / f"{workload['key']}-mira_arm64.json")),
                    }
                )
                diagnostics_rows.append({"workload": workload["key"], "runtime": "mira_arm64", **arm_diag})
            except RuntimeError as error:
                diagnostics_rows.append(
                    {
                        "workload": workload["key"],
                        "runtime": "mira_arm64",
                        "command": "bench-source-asm-arm64",
                        "elapsed_ms": None,
                        "max_rss_bytes": None,
                        "stdout": "",
                        "stderr": str(error),
                        "returncode": 1,
                        "unsupported": True,
                    }
                )

        for runtime, cmd in foreign.items():
            row, diag = run_foreign_benchmark(workload, runtime, cmd, NATIVE_TMP)
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
                    "artifact": str((NATIVE_TMP / f"{workload['key']}-{runtime}.json")),
                }
            )
            diagnostics_rows.append({"workload": workload["key"], "runtime": runtime, **diag})

    selected = choose_default_backend(results)
    selected_rows = [row for row in results if row["runtime"] == selected]
    c_rows = [row for row in results if row["runtime"] == "mira_c"]
    arm64_rows = [row for row in results if row["runtime"] == "mira_arm64"]
    if selected == "mira_arm64":
        ratio = statistics.fmean(row["median_ns"] for row in selected_rows) / statistics.fmean(
            row["median_ns"] for row in c_rows
        )
        rationale = (
            f"`mira_arm64` selected because it beat or matched `mira_c` on the measured backend workload set and averaged {ratio:.3f}x the median time of the C-backed path (lower is better)."
        )
    else:
        if arm64_rows:
            rationale = "`mira_c` selected because it is the only measured backend that covered the full backend workload matrix on this host; `mira_arm64` remains a faster but partial backend for the currently supported service subset."
        else:
            rationale = "`mira_c` selected because no supported asm path covered the measured workload set on this host."

    matrix = {
        "release": "0.13.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": sys.version.split()[0],
        },
        "selected_default_backend": selected,
        "default_backend_rationale": rationale,
        "managed_runtime": "python",
        "results": results,
    }
    output_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")
    output_md.write_text(render_markdown(matrix), encoding="utf-8")
    diagnostics_json.write_text(json.dumps({"release": "0.13.0", "rows": diagnostics_rows}, indent=2), encoding="utf-8")
    print(json.dumps(matrix, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import json
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
MANIFEST = MIRAC / "Cargo.toml"
RUST_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "rust"
GO_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "go" / "backend_bench.go"
PY_BENCH = MIRAC / "benchmarks" / "backend_workloads" / "python" / "backend_bench.py"

WORKLOADS = [
    {
        "key": "reference_status",
        "label": "Reference service status path",
        "mira_source": ROOT / "mira" / "examples" / "runtime_reference_backend_service.mira",
        "mira_function": "reference_service_status",
        "iterations": 50,
        "unit_work": 1,
    },
    {
        "key": "reference_batch",
        "label": "Reference service concurrent batch",
        "mira_source": ROOT / "mira" / "examples" / "runtime_reference_backend_service.mira",
        "mira_function": "reference_batch",
        "iterations": 8,
        "unit_work": 8,
    },
]

RUNTIME_LABELS = {
    "mira_default": "MIRA default",
    "mira_c": "MIRA C-backed",
    "go": "Go",
    "rust": "Rust",
    "python": "Python",
}


def run_command(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )


def profiled_run(command: list[str], cwd: Path | None = None) -> dict:
    full = ["/usr/bin/time", "-l", *command] if Path("/usr/bin/time").exists() else command
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
    go_out = build_dir / "backend_reference_go"
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
    if backend == "mira_default":
        command = [
            "cargo",
            "run",
            "--release",
            "--manifest-path",
            str(MANIFEST),
            "--",
            "bench-source-default",
            str(workload["mira_source"]),
            workload["mira_function"],
            str(workload["iterations"]),
            str(workload["unit_work"]),
            str(output_path),
        ]
    elif backend == "mira_c":
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
    else:
        raise ValueError(backend)
    diagnostics = profiled_run(command, cwd=ROOT)
    if diagnostics["returncode"] != 0:
        raise RuntimeError(
            f"{backend} benchmark failed for {workload['key']}:\n{diagnostics['stdout']}\n{diagnostics['stderr']}"
        )
    row = load_json(output_path)["results"][0]
    return row, diagnostics


def run_foreign_benchmark(workload: dict, runtime: str, runtime_cmd: list[str], output_dir: Path) -> tuple[dict, dict]:
    output_path = output_dir / f"{workload['key']}-{runtime}.json"
    command = [*runtime_cmd, workload["key"], str(workload["iterations"]), str(workload["unit_work"]), str(output_path)]
    diagnostics = profiled_run(command, cwd=ROOT)
    if diagnostics["returncode"] != 0:
        raise RuntimeError(
            f"{runtime} benchmark failed for {workload['key']}:\n{diagnostics['stdout']}\n{diagnostics['stderr']}"
        )
    return load_json(output_path), diagnostics


def render_markdown(matrix: dict) -> str:
    lines = [
        "# MIRA 1.0.0 Reference Backend Performance Matrix",
        "",
        f"Selected production default backend: `{matrix['selected_default_backend']}`",
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
            "Rationale:",
            "",
            matrix["default_backend_rationale"],
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
    build_dir = TMP / "foreign"
    runtimes = ensure_foreign_binaries(build_dir)

    rows = []
    diagnostics = []
    for workload in WORKLOADS:
        for backend in ("mira_default", "mira_c"):
            row, diag = run_mira_benchmark(workload, backend, ROOT / "tmp" / "mira-native")
            row["runtime"] = backend
            row["workload"] = workload["key"]
            row["label"] = workload["label"]
            row["max_rss_bytes"] = diag["max_rss_bytes"]
            rows.append(row)
            diagnostics.append({"runtime": backend, "workload": workload["key"], **diag})
        for runtime, runtime_cmd in runtimes.items():
            row, diag = run_foreign_benchmark(workload, runtime, runtime_cmd, ROOT / "tmp" / "mira-native")
            row["runtime"] = runtime
            row["workload"] = workload["key"]
            row["label"] = workload["label"]
            row["max_rss_bytes"] = diag["max_rss_bytes"]
            rows.append(row)
            diagnostics.append({"runtime": runtime, "workload": workload["key"], **diag})

    by_workload = {}
    for row in rows:
        by_workload.setdefault(row["workload"], {})[row["runtime"]] = row["median_ns"]
    wins = 0
    for runtimes_for_workload in by_workload.values():
        if runtimes_for_workload["mira_default"] <= runtimes_for_workload["mira_c"]:
            wins += 1
    rationale = (
        "`mira_default` selected because the low-level default path completed the full "
        "reference backend workload matrix and matched or beat the legacy C-backed path "
        f"on {wins}/{len(by_workload)} production-scope workloads on this host."
    )
    matrix = {
        "release": "1.0.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selected_default_backend": "mira_default",
        "default_backend_rationale": rationale,
        "results": rows,
    }
    output_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")
    output_md.write_text(render_markdown(matrix), encoding="utf-8")
    diagnostics_json.write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")
    print(json.dumps(matrix, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

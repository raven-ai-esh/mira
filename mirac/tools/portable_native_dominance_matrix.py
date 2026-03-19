#!/usr/bin/env python3
import argparse
import json
import re
import shlex
import subprocess
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MIRAC = ROOT / "mirac"
MANIFEST = MIRAC / "Cargo.toml"
NATIVE_TMP = ROOT / "tmp" / "mira-native"

WORKLOADS = [
    {
        "key": "emitted_reference",
        "label": "Portable native reference service",
        "mira_source": ROOT / "mira" / "examples" / "runtime_emitted_reference_service.mira",
        "mira_function": "emitted_reference_request_batch",
        "iterations": 8,
        "unit_work": 8,
    },
    {
        "key": "emitted_stateful",
        "label": "Portable native stateful service",
        "mira_source": ROOT / "mira" / "examples" / "runtime_emitted_stateful_service.mira",
        "mira_function": "emitted_stateful_request_batch",
        "iterations": 8,
        "unit_work": 8,
    },
]

RUNTIME_LABELS = {
    "mira_default": "MIRA default",
    "mira_c": "MIRA C-backed",
    "mira_arm64": "MIRA asm arm64",
    "mira_x86_64": "MIRA asm x86_64",
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


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_mira_benchmark(workload: dict, backend: str) -> tuple[dict, dict]:
    output_path = NATIVE_TMP / f"{workload['key']}-{backend}.json"
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
    elif backend == "mira_x86_64":
        command = [
            "cargo",
            "run",
            "--release",
            "--manifest-path",
            str(MANIFEST),
            "--",
            "bench-source-asm-x86_64",
            str(workload["mira_source"]),
            "x86_64-apple-macos13",
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


def render_markdown(matrix: dict) -> str:
    lines = [
        "# MIRA 1.4.0 Portable Native Dominance Matrix",
        "",
        f"Selected dominant production backend: `{matrix['selected_default_backend']}`",
        "",
        "| Workload | Runtime | Median ns | p95 ns | RSS bytes |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in matrix["results"]:
        lines.append(
            f"| {row['label']} | {RUNTIME_LABELS[row['runtime']]} | {row['median_ns']} | {row['p95_ns']} | {row['max_rss_bytes'] if row['max_rss_bytes'] is not None else 'n/a'} |"
        )
    lines.extend(["", "Rationale:", "", matrix["default_backend_rationale"]])
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

    rows = []
    diagnostics = []
    for workload in WORKLOADS:
        for backend in ("mira_default", "mira_c", "mira_arm64", "mira_x86_64"):
            row, diag = run_mira_benchmark(workload, backend)
            row["runtime"] = backend
            row["workload"] = workload["key"]
            row["label"] = workload["label"]
            row["max_rss_bytes"] = diag["max_rss_bytes"]
            rows.append(row)
            diagnostics.append({"runtime": backend, "workload": workload["key"], **diag})

    by_workload = {}
    for row in rows:
        by_workload.setdefault(row["workload"], {})[row["runtime"]] = row["median_ns"]
    wins_vs_c = 0
    for runtimes in by_workload.values():
        if runtimes["mira_default"] <= runtimes["mira_c"]:
            wins_vs_c += 1
    rationale = (
        "`mira_default` selected because the emitted portable native path stays ahead of the "
        "legacy C-backed path on the promoted emitted-friendly service subset "
        f"for {wins_vs_c}/{len(by_workload)} measured workloads on this host."
    )
    matrix = {
        "release": "1.4.0",
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

#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
TOOLS = ROOT / "mirac" / "tools"
TMP = ROOT / "tmp" / "mira-release"
BENCH_DIR = TMP / "benchmarks-1.5.0"

BENCH_PROFILES = [
    {
        "name": "self_healing_api",
        "source": ROOT / "mira" / "examples" / "runtime_self_healing_api_service.mira",
        "function": "recover_api_status",
        "iterations": 40,
        "unit_work": 1,
    },
    {
        "name": "degraded_mode",
        "source": ROOT / "mira" / "examples" / "runtime_degraded_mode_service.mira",
        "function": "degraded_service_status",
        "iterations": 40,
        "unit_work": 1,
    },
    {
        "name": "recovery_worker",
        "source": ROOT / "mira" / "examples" / "runtime_recovery_worker_service.mira",
        "function": "recover_worker_cursor",
        "iterations": 40,
        "unit_work": 1,
    },
]


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect(command: list[str]) -> dict:
    proc = run(command)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}\n{proc.stdout}\n{proc.stderr}")
    return {"command": command, "stdout": proc.stdout, "stderr": proc.stderr}


def try_run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return run(command)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    BENCH_DIR.mkdir(parents=True, exist_ok=True)

    conformance_json = TMP / "service-conformance-1.5.0.json"
    regression_json = TMP / "resource-regression-1.5.0.json"
    expect([str(TOOLS / "generated_service_conformance.py"), "--output-json", str(conformance_json)])
    expect(
        [
            str(TOOLS / "runtime_resource_leak_regression.py"),
            "--iterations",
            "4",
            "--output-json",
            str(regression_json),
        ]
    )

    benches = []
    for profile in BENCH_PROFILES:
        bench_json = BENCH_DIR / f"{profile['name']}.json"
        default_command = [
            "cargo",
            "run",
            "--release",
            "--manifest-path",
            str(MANIFEST),
            "--",
            "bench-source-default",
            str(profile["source"]),
            profile["function"],
            str(profile["iterations"]),
            str(profile["unit_work"]),
            str(bench_json),
        ]
        proc = try_run(default_command)
        backend = "mira_default"
        fallback_reason = None
        if proc.returncode != 0:
            fallback_reason = (proc.stdout + "\n" + proc.stderr).strip()
            fallback_command = [
                "cargo",
                "run",
                "--release",
                "--manifest-path",
                str(MANIFEST),
                "--",
                "bench-source",
                str(profile["source"]),
                profile["function"],
                str(profile["iterations"]),
                str(profile["unit_work"]),
                str(bench_json),
            ]
            expect(fallback_command)
            backend = "mira_c"
        benches.append(
            {
                "name": profile["name"],
                "source": str(profile["source"]),
                "function": profile["function"],
                "backend": backend,
                "fallback_reason": fallback_reason,
                "result": json.loads(bench_json.read_text(encoding="utf-8"))["results"][0],
                "artifact": str(bench_json),
            }
        )

    payload = {
        "release": "1.5.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "conformance_artifact": str(conformance_json),
        "regression_artifact": str(regression_json),
        "benchmark_artifacts": benches,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_md.write_text(
        "# MIRA 1.5.0 Agent Release Bundle\n\n"
        + f"- Conformance: `{conformance_json}`\n"
        + f"- Resource regression: `{regression_json}`\n"
        + "\n".join(
            [
                f"- Benchmark `{row['name']}` via `{row['backend']}`: `{row['artifact']}` median={row['result']['median_ns']} ns"
                for row in benches
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

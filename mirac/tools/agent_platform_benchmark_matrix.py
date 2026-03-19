#!/usr/bin/env python3
import argparse
import json
import time
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"

CASES = [
    {
        "name": "agent_api_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_api_service.mira",
        "function": "maintained_agent_api_status",
        "iterations": 40,
        "unit_work": 1,
    },
    {
        "name": "agent_stateful_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_stateful_service.mira",
        "function": "maintained_agent_stateful_status",
        "iterations": 40,
        "unit_work": 1,
    },
    {
        "name": "agent_worker_queue_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_worker_queue_service.mira",
        "function": "maintained_agent_worker_queue_status",
        "iterations": 20,
        "unit_work": 3,
    },
    {
        "name": "agent_recovery_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_recovery_service.mira",
        "function": "maintained_agent_recovery_status",
        "iterations": 20,
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


def read_results(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["results"][0]


def classify(default_ns: int, c_ns: int) -> str:
    if default_ns <= c_ns:
        return "default_wins"
    if default_ns <= int(c_ns * 1.15):
        return "near_parity"
    return "c_wins"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    bench_dir = output_json.parent / "benchmarks-2.0.0"
    bench_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for case in CASES:
        default_json = bench_dir / f"{case['name']}-default.json"
        c_json = bench_dir / f"{case['name']}-c.json"
        expect(
            [
                "cargo",
                "run",
                "--release",
                "--manifest-path",
                str(MANIFEST),
                "--",
                "bench-source-default",
                str(case["source"]),
                case["function"],
                str(case["iterations"]),
                str(case["unit_work"]),
                str(default_json),
            ]
        )
        expect(
            [
                "cargo",
                "run",
                "--release",
                "--manifest-path",
                str(MANIFEST),
                "--",
                "bench-source",
                str(case["source"]),
                case["function"],
                str(case["iterations"]),
                str(case["unit_work"]),
                str(c_json),
            ]
        )
        default_result = read_results(default_json)
        c_result = read_results(c_json)
        rows.append(
            {
                "name": case["name"],
                "source": str(case["source"]),
                "function": case["function"],
                "mira_default": default_result,
                "mira_c": c_result,
                "classification": classify(
                    default_result["median_ns"], c_result["median_ns"]
                ),
            }
        )

    dominant = all(
        row["classification"] in ("default_wins", "near_parity") for row in rows
    ) and any(row["classification"] == "default_wins" for row in rows)
    selected = "mira_default" if dominant else "mira_c"
    rationale = (
        "`mira_default` selected because every maintained workload is either ahead of or within near-parity to `mira_c`, and at least one representative maintained workload clearly wins on the dominant path."
        if dominant
        else "`mira_c` selected because the maintained workload set still materially depends on the legacy path."
    )

    payload = {
        "release": "2.0.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selected_default_backend": selected,
        "selection_rationale": rationale,
        "workloads": rows,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_md.write_text(
        "# MIRA 2.0.0 Agent Platform Benchmark Matrix\n\n"
        + f"- Selected default backend: `{selected}`\n"
        + f"- Rationale: {rationale}\n"
        + "\n".join(
            [
                f"- `{row['name']}`: `mira_default={row['mira_default']['median_ns']} ns`, `mira_c={row['mira_c']['median_ns']} ns`, classification=`{row['classification']}`"
                for row in rows
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

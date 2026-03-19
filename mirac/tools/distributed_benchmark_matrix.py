#!/usr/bin/env python3
import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
EXAMPLES = ROOT / "mira" / "examples"

CASES = [
    {
        "name": "shard_messaging_edge",
        "source": EXAMPLES / "runtime_shard_messaging_edge.mira",
        "function": "shard_edge_route",
        "iterations": 30,
        "unit_work": 1,
        "rewrites": {
            "/tmp/mira_2_4_edge.place": "edge.place",
            "/tmp/mira_2_4_edge.lease": "edge.lease",
        },
    },
    {
        "name": "distributed_analytics_cluster",
        "source": EXAMPLES / "runtime_distributed_analytics_cluster.mira",
        "function": "replay_cluster_checkpoint",
        "iterations": 20,
        "unit_work": 2,
        "rewrites": {
            "/tmp/mira_2_4_analytics.place": "analytics.place",
            "/tmp/mira_2_4_analytics.lease": "analytics.lease",
            "/tmp/mira_2_4_analytics.coord": "analytics.coord",
            "/tmp/mira_2_4_analytics.stream": "analytics.stream",
        },
    },
    {
        "name": "failover_rebalance_service",
        "source": EXAMPLES / "runtime_failover_rebalance_service.mira",
        "function": "rebalance_without_duplicate_work",
        "iterations": 20,
        "unit_work": 2,
        "rewrites": {
            "/tmp/mira_2_4_rebalance.place": "rebalance.place",
            "/tmp/mira_2_4_rebalance.lease": "rebalance.lease",
            "/tmp/mira_2_4_rebalance.coord": "rebalance.coord",
            "/tmp/mira_2_4_rebalance.stream": "rebalance.stream",
        },
    },
]


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect(command: list[str]) -> subprocess.CompletedProcess[str]:
    proc = run(command)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(command)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def read_result(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["results"][0]


def rewrite_source(case: dict, temp_dir: Path) -> Path:
    text = case["source"].read_text(encoding="utf-8")
    for original, replacement_name in case["rewrites"].items():
        text = text.replace(original, str(temp_dir / replacement_name))
    rewritten = temp_dir / case["source"].name
    rewritten.write_text(text, encoding="utf-8")
    return rewritten


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)

    bench_dir = output_json.parent / "distributed-bench-2.4.0"
    bench_dir.mkdir(parents=True, exist_ok=True)

    workloads = []
    for case in CASES:
        with tempfile.TemporaryDirectory(prefix=f"{case['name']}_") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            rewritten = rewrite_source(case, temp_dir)
            result_json = bench_dir / f"{case['name']}-mira-c.json"
            expect(
                [
                    "cargo",
                    "run",
                    "--release",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "bench-source",
                    str(rewritten),
                    case["function"],
                    str(case["iterations"]),
                    str(case["unit_work"]),
                    str(result_json),
                ]
            )
            workloads.append(
                {
                    "name": case["name"],
                    "source": str(case["source"]),
                    "function": case["function"],
                    "iterations": case["iterations"],
                    "unit_work": case["unit_work"],
                    "backend": "mira_c",
                    "result": read_result(result_json),
                }
            )

    payload = {
        "release": "2.4.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selected_backend": "mira_c",
        "selection_rationale": (
            "Distributed 2.4.0 benchmark artifact is anchored on `mira_c` while the "
            "promoted emitted/default backend has not yet been extended to the full "
            "distributed coordination surface."
        ),
        "workloads": workloads,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# MIRA 2.4.0 Distributed Benchmark Matrix",
        "",
        "- Selected backend for this release artifact: `mira_c`",
        "- Rationale: Distributed coordination 2.4.0 is measured on the fully supported backend while the emitted/default path has not yet taken this full slice.",
        "",
    ]
    for row in workloads:
        result = row["result"]
        lines.append(
            f"- `{row['name']}`: `median={result['median_ns']} ns`, `p95={result['p95_ns']} ns`, `units_per_second={result['units_per_second']}`"
        )
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

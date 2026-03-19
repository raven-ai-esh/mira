#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"

PROFILES = [
    {
        "name": "json_api_steady",
        "source": ROOT / "mira" / "examples" / "runtime_json_api_endpoint.mira",
        "function": "decode_request_and_encode_response",
        "iterations": 100,
        "unit_work": 1,
        "rounds": 5,
    },
    {
        "name": "db_sqlite_roundtrip",
        "source": ROOT / "mira" / "examples" / "runtime_db_sqlite.mira",
        "function": "init_and_query_count",
        "iterations": 8,
        "unit_work": 1,
        "rounds": 5,
    },
    {
        "name": "tls_request_steady",
        "source": ROOT / "mira" / "examples" / "runtime_tls_http_client.mira",
        "function": "fetch_local_https_ok",
        "iterations": 50,
        "unit_work": 1,
        "rounds": 5,
    },
    {
        "name": "worker_batch",
        "source": ROOT / "mira" / "examples" / "runtime_http_worker_service.mira",
        "function": "serve_request_batch",
        "iterations": 12,
        "unit_work": 8,
        "rounds": 5,
    },
]


def percentile(samples: list[int], pct: int) -> int:
    ordered = sorted(samples)
    if not ordered:
        return 0
    index = (pct * (len(ordered) - 1)) // 100
    return ordered[index]


def run_bench(profile: dict, output_path: Path) -> dict:
    command = [
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
        str(output_path),
    ]
    started = time.time()
    proc = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    elapsed_ms = (time.time() - started) * 1000.0
    if proc.returncode != 0:
        raise RuntimeError(f"load-test bench failed for {profile['name']}:\n{proc.stdout}\n{proc.stderr}")
    data = json.loads(output_path.read_text(encoding="utf-8"))["results"][0]
    return {
        "profile": profile["name"],
        "median_ns": data["median_ns"],
        "p95_ns": data["p95_ns"],
        "p99_ns": data["p99_ns"],
        "per_call_ns": data["per_call_ns"],
        "units_per_second": data["units_per_second"],
        "elapsed_ms": elapsed_ms,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    rounds = []
    temp_dir = output_json.parent / "load-profile-rounds"
    temp_dir.mkdir(parents=True, exist_ok=True)
    for profile in PROFILES:
        profile_rows = []
        for index in range(profile["rounds"]):
            row = run_bench(profile, temp_dir / f"{profile['name']}-{index}.json")
            profile_rows.append(row)
        medians = [row["median_ns"] for row in profile_rows]
        p95s = [row["p95_ns"] for row in profile_rows]
        rounds.append(
            {
                "profile": profile["name"],
                "rounds": profile_rows,
                "summary": {
                    "median_of_medians_ns": percentile(medians, 50),
                    "p95_of_medians_ns": percentile(medians, 95),
                    "p95_of_p95_ns": percentile(p95s, 95),
                    "max_median_ns": max(medians),
                    "min_median_ns": min(medians),
                },
            }
        )
    payload = {
        "release": "0.13.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "profiles": rounds,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"

CASES = [
    {
        "name": "direct_message_service",
        "source": ROOT / "mira" / "examples" / "runtime_direct_message_service.mira",
        "function": "direct_message_service_status",
        "iterations": 40,
        "unit_work": 1,
    },
    {
        "name": "room_fanout_service",
        "source": ROOT / "mira" / "examples" / "runtime_room_fanout_service.mira",
        "function": "room_fanout_service_status",
        "iterations": 40,
        "unit_work": 2,
    },
    {
        "name": "offline_catchup_worker",
        "source": ROOT / "mira" / "examples" / "runtime_offline_catchup_worker.mira",
        "function": "offline_catchup_worker_status",
        "iterations": 40,
        "unit_work": 2,
    },
]


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect(command: list[str]) -> subprocess.CompletedProcess[str]:
    proc = run(command)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(command)}\n{proc.stdout}\n{proc.stderr}"
        )
    return proc


def maybe_run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return run(command)


def read_results(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["results"][0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    bench_dir = output_json.parent / "messaging-bench-2.2.0"
    bench_dir.mkdir(parents=True, exist_ok=True)

    workloads = []
    for case in CASES:
        c_json = bench_dir / f"{case['name']}-mira-c.json"
        default_json = bench_dir / f"{case['name']}-mira-default.json"
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
        c_result = read_results(c_json)

        default_proc = maybe_run(
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
        default_supported = default_proc.returncode == 0 and default_json.exists()
        default_result = read_results(default_json) if default_supported else None
        fallback_reason = None
        if not default_supported:
            fallback_reason = (
                default_proc.stderr.strip().splitlines()[-1]
                if default_proc.stderr.strip()
                else default_proc.stdout.strip().splitlines()[-1]
                if default_proc.stdout.strip()
                else "unknown default-backend failure"
            )

        workloads.append(
            {
                "name": case["name"],
                "source": str(case["source"]),
                "function": case["function"],
                "iterations": case["iterations"],
                "unit_work": case["unit_work"],
                "mira_c": c_result,
                "mira_default_supported": default_supported,
                "mira_default": default_result,
                "fallback_reason": fallback_reason,
            }
        )

    selected_backend = "mira_c"
    rationale = (
        "Messaging 2.2.0 benchmark artifact is anchored on `mira_c` because the "
        "default portable-native backend does not yet lower the full messaging "
        "delivery surface on this release."
    )
    payload = {
        "release": "2.2.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selected_backend": selected_backend,
        "selection_rationale": rationale,
        "workloads": workloads,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# MIRA 2.2.0 Messaging Benchmark Matrix",
        "",
        f"- Selected backend for this release artifact: `{selected_backend}`",
        f"- Rationale: {rationale}",
        "",
    ]
    for row in workloads:
        lines.append(
            f"- `{row['name']}`: `mira_c={row['mira_c']['median_ns']} ns`, "
            f"`mira_default_supported={str(row['mira_default_supported']).lower()}`"
        )
        if row["mira_default_supported"]:
            lines.append(
                f"  default median: `{row['mira_default']['median_ns']} ns`"
            )
        else:
            lines.append(f"  fallback reason: `{row['fallback_reason']}`")
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

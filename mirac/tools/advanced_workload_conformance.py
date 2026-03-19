#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"

PROFILE_SETS = {
    "messaging": [
        {
            "name": "production_messenger_backend",
            "source": ROOT / "mira" / "examples" / "runtime_production_messenger_backend.mira",
            "required_tokens": ["messaging", "production-messenger-backend", "spec kind=service"],
        },
        {
            "name": "room_fanout_service",
            "source": ROOT / "mira" / "examples" / "runtime_room_fanout_service.mira",
            "required_tokens": ["fanout", "msg_fanout", "service_metric_count_dim"],
        },
        {
            "name": "offline_catchup_worker",
            "source": ROOT / "mira" / "examples" / "runtime_offline_catchup_worker.mira",
            "required_tokens": ["replay", "msg_replay_open", "service_metric_count_dim"],
        },
    ],
    "analytics": [
        {
            "name": "production_analytics_platform",
            "source": ROOT / "mira" / "examples" / "runtime_production_analytics_platform.mira",
            "required_tokens": ["analytics", "production-analytics-platform", "spec kind=service"],
        },
        {
            "name": "aggregation_worker_service",
            "source": ROOT / "mira" / "examples" / "runtime_aggregation_worker_service.mira",
            "required_tokens": ["analytics", "aggregation", "batch", "window"],
        },
        {
            "name": "distributed_analytics_cluster",
            "source": ROOT / "mira" / "examples" / "runtime_distributed_analytics_cluster.mira",
            "required_tokens": ["distributed", "stream_replay_open", "coord_store_u32"],
        },
        {
            "name": "failover_rebalance_service",
            "source": ROOT / "mira" / "examples" / "runtime_failover_rebalance_service.mira",
            "required_tokens": ["lease_transfer", "placement_assign", "coord_store_u32"],
        },
    ],
}


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect_ok(command: list[str]) -> dict:
    proc = run(command)
    return {
        "command": command,
        "ok": proc.returncode == 0,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "returncode": proc.returncode,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-set", required=True, choices=sorted(PROFILE_SETS))
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    artifact_dir = output_json.parent / f"{args.profile_set}-conformance-artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    failures: list[str] = []
    for profile in PROFILE_SETS[args.profile_set]:
        source_text = profile["source"].read_text(encoding="utf-8")
        missing = [token for token in profile["required_tokens"] if token not in source_text]
        artifact = artifact_dir / f"{profile['name']}.mirb3"
        checks = [
            expect_ok(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "check", str(profile["source"])]),
            expect_ok(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "test", str(profile["source"])]),
            expect_ok(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "emit-binary", str(profile["source"]), str(artifact)]),
            expect_ok(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "check-binary", str(artifact)]),
            expect_ok(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "test-binary", str(artifact)]),
        ]
        ok = not missing and all(check["ok"] for check in checks)
        if missing:
            failures.append(f"{profile['name']} missing required tokens: {', '.join(missing)}")
        for check in checks:
            if not check["ok"]:
                failures.append(f"{profile['name']} failed command: {' '.join(check['command'])}")
        rows.append(
            {
                "name": profile["name"],
                "source": str(profile["source"]),
                "artifact": str(artifact),
                "missing_tokens": missing,
                "checks": checks,
                "ok": ok,
            }
        )

    payload = {
        "release": "2.6.0",
        "profile_set": args.profile_set,
        "ok": not failures,
        "profiles": rows,
        "failures": failures,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

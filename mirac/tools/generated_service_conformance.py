#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
TMP = ROOT / "tmp" / "mira-release" / "conformance"

PROFILES = [
    {
        "name": "self_healing_api_service",
        "source": ROOT / "mira" / "examples" / "runtime_self_healing_api_service.mira",
        "required_tokens": [
            "service_set_degraded",
            "service_checkpoint_save_u32",
            "service_trace_link",
            "service_metric_count_dim",
        ],
    },
    {
        "name": "degraded_mode_service",
        "source": ROOT / "mira" / "examples" / "runtime_degraded_mode_service.mira",
        "required_tokens": [
            "service_set_degraded",
            "service_failure_count",
            "service_event_total",
            "service_metric_total",
        ],
    },
    {
        "name": "recovery_worker_service",
        "source": ROOT / "mira" / "examples" / "runtime_recovery_worker_service.mira",
        "required_tokens": [
            "service_checkpoint_load_u32",
            "supervisor_should_restart",
            "service_event",
            "service_metric_count_dim",
        ],
    },
]


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
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)

    rows = []
    failures: list[str] = []
    for profile in PROFILES:
        source_text = profile["source"].read_text(encoding="utf-8")
        missing = [token for token in profile["required_tokens"] if token not in source_text]
        artifact = TMP / f"{profile['name']}.mirb3"
        checks = [
            expect_ok(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "check",
                    str(profile["source"]),
                ]
            ),
            expect_ok(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test",
                    str(profile["source"]),
                ]
            ),
            expect_ok(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "emit-binary",
                    str(profile["source"]),
                    str(artifact),
                ]
            ),
            expect_ok(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "check-binary",
                    str(artifact),
                ]
            ),
            expect_ok(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test-binary",
                    str(artifact),
                ]
            ),
        ]
        ok = not missing and all(check["ok"] for check in checks)
        if missing:
            failures.append(f"{profile['name']} missing required tokens: {', '.join(missing)}")
        for check in checks:
            if not check["ok"]:
                failures.append(
                    f"{profile['name']} failed command: {' '.join(check['command'])}"
                )
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

    payload = {"ok": not failures, "profiles": rows, "failures": failures}
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import json
import platform
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
TMP = ROOT / "tmp" / "mira-release"

SERVICES = [
    {
        "name": "agent_api_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_api_service.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "agent_platform",
            "maintained",
        ],
    },
    {
        "name": "agent_stateful_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_stateful_service.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "agent_platform",
            "maintained",
        ],
    },
    {
        "name": "agent_worker_queue_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_worker_queue_service.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "agent_platform",
            "maintained",
        ],
    },
    {
        "name": "agent_recovery_service",
        "source": ROOT / "mira" / "examples" / "runtime_agent_recovery_service.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "agent_platform",
            "maintained",
        ],
    },
]


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect(command: list[str]) -> dict:
    proc = run(command)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}\n{proc.stdout}\n{proc.stderr}")
    return {"command": command, "stdout": proc.stdout, "stderr": proc.stderr}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    artifact_dir = TMP / "agent-contract-2.0.0"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    host = {"system": platform.system(), "machine": platform.machine()}
    checks = []
    for service in SERVICES:
        text = service["source"].read_text(encoding="utf-8")
        missing = [token for token in service["required_tokens"] if token not in text]
        if missing:
            raise RuntimeError(f"{service['source']} missing required tokens: {missing}")
        mirb3 = artifact_dir / f"{service['name']}.mirb3"
        records = []
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "check",
                    str(service["source"]),
                ]
            )
        )
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test",
                    str(service["source"]),
                ]
            )
        )
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test-default",
                    str(service["source"]),
                ]
            )
        )
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "emit-binary",
                    str(service["source"]),
                    str(mirb3),
                ]
            )
        )
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "check-binary",
                    str(mirb3),
                ]
            )
        )
        records.append(
            expect(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test-binary",
                    str(mirb3),
                ]
            )
        )
        if host["system"] == "Darwin" and host["machine"] == "arm64":
            records.append(
                expect(
                    [
                        "cargo",
                        "run",
                        "--manifest-path",
                        str(MANIFEST),
                        "--",
                        "test-asm-arm64",
                        str(service["source"]),
                    ]
                )
            )
        if host["system"] == "Darwin":
            for triple in ("x86_64-apple-macos13", "x86_64-unknown-linux-gnu"):
                records.append(
                    expect(
                        [
                            "cargo",
                            "run",
                            "--manifest-path",
                            str(MANIFEST),
                            "--",
                            "test-asm-x86_64",
                            str(service["source"]),
                            triple,
                        ]
                    )
                )
        checks.append(
            {
                "name": service["name"],
                "source": str(service["source"]),
                "mirb3": str(mirb3),
                "checks": records,
            }
        )

    payload = {
        "release": "2.0.0",
        "kind": "agent_generation_contract",
        "ok": True,
        "host": host,
        "services": checks,
    }
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

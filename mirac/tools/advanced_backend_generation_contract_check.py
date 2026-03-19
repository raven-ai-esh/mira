#!/usr/bin/env python3
import argparse
import json
import platform
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
TMP = ROOT / "tmp" / "mira-release"

ADVANCED_ANCHORS = [
    {
        "name": "production_messenger_backend",
        "source": ROOT / "mira" / "examples" / "runtime_production_messenger_backend.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "messaging",
            "production-messenger-backend",
        ],
    },
    {
        "name": "production_analytics_platform",
        "source": ROOT / "mira" / "examples" / "runtime_production_analytics_platform.mira",
        "required_tokens": [
            "target native",
            "spec kind=service",
            "analytics",
            "production-analytics-platform",
        ],
    },
]

DEFAULT_BASELINE = [
    ROOT / "mira" / "examples" / "runtime_emitted_messaging_service.mira",
    ROOT / "mira" / "examples" / "runtime_emitted_analytics_service.mira",
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
    artifact_dir = TMP / "advanced-contract-2.6.0"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    host = {"system": platform.system(), "machine": platform.machine()}
    anchors = []
    for anchor in ADVANCED_ANCHORS:
        text = anchor["source"].read_text(encoding="utf-8")
        missing = [token for token in anchor["required_tokens"] if token not in text]
        if missing:
            raise RuntimeError(f"{anchor['source']} missing required tokens: {missing}")
        mirb3 = artifact_dir / f"{anchor['name']}.mirb3"
        checks = [
            expect(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "check", str(anchor["source"])]),
            expect(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "test", str(anchor["source"])]),
            expect(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "emit-binary", str(anchor["source"]), str(mirb3)]),
            expect(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "check-binary", str(mirb3)]),
            expect(["cargo", "run", "--manifest-path", str(MANIFEST), "--", "test-binary", str(mirb3)]),
        ]
        anchors.append(
            {
                "name": anchor["name"],
                "source": str(anchor["source"]),
                "mirb3": str(mirb3),
                "checks": checks,
            }
        )

    default_checks = []
    for source in DEFAULT_BASELINE:
        default_checks.append(
            {
                "source": str(source),
                "check": expect(
                    [
                        "cargo",
                        "run",
                        "--manifest-path",
                        str(MANIFEST),
                        "--",
                        "test-default",
                        str(source),
                    ]
                ),
            }
        )

    payload = {
        "release": "2.6.0",
        "kind": "advanced_backend_generation_contract",
        "ok": True,
        "host": host,
        "advanced_anchors": anchors,
        "default_backend_baseline": default_checks,
    }
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

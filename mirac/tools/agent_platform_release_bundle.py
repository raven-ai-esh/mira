#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
TOOLS = ROOT / "mirac" / "tools"
TMP = ROOT / "tmp" / "mira-release"


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def expect(command: list[str]) -> dict:
    proc = run(command)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}\n{proc.stdout}\n{proc.stderr}")
    return {"command": command, "stdout": proc.stdout, "stderr": proc.stderr}


def require(path: Path) -> str:
    if not path.exists():
        raise RuntimeError(f"required artifact missing: {path}")
    return str(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    contract_json = TMP / "agent-generation-contract-2.0.0.json"
    matrix_json = TMP / "agent-platform-matrix-2.0.0.json"
    matrix_md = TMP / "agent-platform-matrix-2.0.0.md"

    expect([str(TOOLS / "agent_generation_contract_check.py"), "--output-json", str(contract_json)])
    expect(
        [
            str(TOOLS / "agent_platform_benchmark_matrix.py"),
            "--output-json",
            str(matrix_json),
            "--output-md",
            str(matrix_md),
        ]
    )

    payload = {
        "release": "2.0.0",
        "agent_generation_contract_artifact": str(contract_json),
        "benchmark_matrix_json": str(matrix_json),
        "benchmark_matrix_md": str(matrix_md),
        "operational_hardening_artifacts": {
            "conformance_json": require(TMP / "service-conformance-1.5.0.json"),
            "resource_regression_json": require(TMP / "resource-regression-1.5.0.json"),
            "release_bundle_json": require(TMP / "agent-release-bundle-1.5.0.json"),
            "release_bundle_md": require(TMP / "agent-release-bundle-1.5.0.md"),
        },
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_md.write_text(
        "# MIRA 2.0.0 Agent Platform Release Bundle\n\n"
        + f"- Agent generation contract: `{contract_json}`\n"
        + f"- Benchmark matrix: `{matrix_json}`\n"
        + f"- Benchmark matrix markdown: `{matrix_md}`\n"
        + f"- Operational hardening conformance: `{TMP / 'service-conformance-1.5.0.json'}`\n"
        + f"- Operational hardening regression: `{TMP / 'resource-regression-1.5.0.json'}`\n"
        + f"- Operational hardening bundle: `{TMP / 'agent-release-bundle-1.5.0.json'}`\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

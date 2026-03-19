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

    contract_json = TMP / "advanced-generation-contract-2.6.0.json"
    matrix_json = TMP / "advanced-backend-matrix-2.6.0.json"
    matrix_md = TMP / "advanced-backend-matrix-2.6.0.md"
    diagnostics_json = TMP / "advanced-backend-matrix-2.6.0-diagnostics.json"
    messaging_conf = TMP / "messaging-hardening-2.6.0.json"
    messaging_reg = TMP / "messaging-hardening-regression-2.6.0.json"
    analytics_conf = TMP / "analytics-hardening-2.6.0.json"
    analytics_reg = TMP / "analytics-hardening-regression-2.6.0.json"

    expect(["python3", str(TOOLS / "advanced_backend_generation_contract_check.py"), "--output-json", str(contract_json)])
    expect(
        [
            "python3",
            str(TOOLS / "advanced_backend_dominance_matrix.py"),
            "--output-json",
            str(matrix_json),
            "--output-md",
            str(matrix_md),
            "--diagnostics-json",
            str(diagnostics_json),
        ]
    )
    expect(["python3", str(TOOLS / "advanced_workload_conformance.py"), "--profile-set", "messaging", "--output-json", str(messaging_conf)])
    expect(["python3", str(TOOLS / "advanced_workload_resource_regression.py"), "--profile-set", "messaging", "--output-json", str(messaging_reg)])
    expect(["python3", str(TOOLS / "advanced_workload_conformance.py"), "--profile-set", "analytics", "--output-json", str(analytics_conf)])
    expect(["python3", str(TOOLS / "advanced_workload_resource_regression.py"), "--profile-set", "analytics", "--output-json", str(analytics_reg)])

    payload = {
        "release": "2.6.0",
        "advanced_generation_contract_artifact": str(contract_json),
        "benchmark_matrix_json": str(matrix_json),
        "benchmark_matrix_md": str(matrix_md),
        "benchmark_diagnostics_json": str(diagnostics_json),
        "operational_hardening_artifacts": {
            "messaging_conformance_json": require(messaging_conf),
            "messaging_regression_json": require(messaging_reg),
            "analytics_conformance_json": require(analytics_conf),
            "analytics_regression_json": require(analytics_reg),
        },
        "portable_native_supremacy_baseline": {
            "matrix_json": require(TMP / "portable-native-supremacy-2.5.0.json"),
            "matrix_md": require(TMP / "portable-native-supremacy-2.5.0.md"),
            "diagnostics_json": require(TMP / "portable-native-supremacy-2.5.0-diagnostics.json"),
        },
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_md.write_text(
        "# MIRA 2.6.0 Advanced Backend Release Bundle\n\n"
        + f"- Advanced generation contract: `{contract_json}`\n"
        + f"- Advanced backend matrix: `{matrix_json}`\n"
        + f"- Advanced backend matrix markdown: `{matrix_md}`\n"
        + f"- Advanced backend diagnostics: `{diagnostics_json}`\n"
        + f"- Messaging conformance: `{messaging_conf}`\n"
        + f"- Messaging regression: `{messaging_reg}`\n"
        + f"- Analytics conformance: `{analytics_conf}`\n"
        + f"- Analytics regression: `{analytics_reg}`\n"
        + f"- Portable-native baseline: `{TMP / 'portable-native-supremacy-2.5.0.json'}`\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

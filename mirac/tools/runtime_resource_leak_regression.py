#!/usr/bin/env python3
import argparse
import json
import resource
import subprocess
import time
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "mirac" / "Cargo.toml"
TMP = ROOT / "tmp" / "mira-release" / "resource-regression"
PROFILES = [
    ROOT / "mira" / "examples" / "runtime_self_healing_api_service.mira",
    ROOT / "mira" / "examples" / "runtime_degraded_mode_service.mira",
    ROOT / "mira" / "examples" / "runtime_recovery_worker_service.mira",
]


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)

    start_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    failures: list[str] = []
    runs = []
    for iteration in range(args.iterations):
        for source in PROFILES:
            artifact = TMP / f"{source.stem}-{iteration}.mirb3"
            for command in [
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test",
                    str(source),
                ],
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "emit-binary",
                    str(source),
                    str(artifact),
                ],
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(MANIFEST),
                    "--",
                    "test-binary",
                    str(artifact),
                ],
            ]:
                started = time.time()
                proc = run(command)
                elapsed_ms = (time.time() - started) * 1000.0
                runs.append(
                    {
                        "source": str(source),
                        "command": command,
                        "elapsed_ms": elapsed_ms,
                        "returncode": proc.returncode,
                    }
                )
                if proc.returncode != 0:
                    failures.append(f"{source.name} failed {' '.join(command)}")
    end_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    payload = {
        "ok": not failures,
        "iterations": args.iterations,
        "runs": runs,
        "child_maxrss_delta_kb": max(0, end_rss - start_rss),
        "failures": failures,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

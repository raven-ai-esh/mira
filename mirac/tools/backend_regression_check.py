#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-json", required=True)
    parser.add_argument("--load-json", required=True)
    parser.add_argument("--thresholds-json", required=True)
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    matrix = json.loads(Path(args.matrix_json).read_text(encoding="utf-8"))
    load = json.loads(Path(args.load_json).read_text(encoding="utf-8"))
    thresholds = json.loads(Path(args.thresholds_json).read_text(encoding="utf-8"))

    failures: list[str] = []
    selected = matrix["selected_default_backend"]
    managed = matrix["managed_runtime"]
    runtimes = {row["runtime"] for row in matrix["results"]}
    for required in ["go", "rust", managed]:
        if required not in runtimes:
            failures.append(f"missing competitor runtime {required}")

    by_workload = {}
    for row in matrix["results"]:
        by_workload.setdefault(row["workload"], {})[row["runtime"]] = row

    for workload, limit in thresholds["default_backend_max_median_ns"].items():
        row = by_workload.get(workload, {}).get(selected)
        if row is None:
            failures.append(f"missing selected backend row for {workload}")
            continue
        if row["median_ns"] > limit:
            failures.append(
                f"{workload} median {row['median_ns']} exceeded threshold {limit} for {selected}"
            )

    for profile in load["profiles"]:
        limit = thresholds["load_test_max_p95_of_medians_ns"].get(profile["profile"])
        if limit is None:
            continue
        observed = profile["summary"]["p95_of_medians_ns"]
        if observed > limit:
            failures.append(
                f"{profile['profile']} p95_of_medians {observed} exceeded threshold {limit}"
            )

    payload = {
        "ok": not failures,
        "selected_default_backend": selected,
        "managed_runtime": managed,
        "failures": failures,
    }
    Path(args.output_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

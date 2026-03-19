#!/usr/bin/env python3
"""Validate that the MIRA public launch bundle is internally complete."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
MANIFEST = ROOT / "tmp" / "mira-release" / "manifest-public-launch-v1.json"


def main() -> int:
    data = json.loads(MANIFEST.read_text())
    missing: list[str] = []

    for bucket in ("artifacts", "evidence"):
        for _, path in data[bucket].items():
            if not Path(path).exists():
                missing.append(path)

    for path in data["must_link"]:
        if not Path(path).exists():
            missing.append(path)

    if missing:
        print(json.dumps({"ok": False, "missing": missing}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "ok": True,
                "manifest": str(MANIFEST),
                "artifacts_checked": len(data["artifacts"]),
                "evidence_checked": len(data["evidence"]),
                "must_link_checked": len(data["must_link"]),
                "publication_status": data["publication_status"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a publish-ready MIRA public repo export from the current workspace."""

from __future__ import annotations

import json
import shutil
from pathlib import Path


ROOT = Path("/Users/sheremetovegor/Documents/Raven/personal-activity")
DOCS = ROOT / "docs"
TMP_RELEASE = ROOT / "tmp" / "mira-release"
EXPORT = ROOT / "tmp" / "mira-public-repo"

DOC_FILES = [
    "MIRA_PUBLIC_REPO_README.md",
    "MIRA_PUBLIC_DOCS_INDEX.md",
    "MIRA_PUBLIC_POSITIONING.md",
    "MIRA_PUBLIC_NON_GOALS.md",
    "MIRA_PUBLIC_QUICKSTART.md",
    "MIRA_PUBLIC_AGENT_GENERATION_GUIDE.md",
    "MIRA_AGENT_PROMPT_KIT.md",
    "MIRA_AGENT_ONBOARDING_COMMANDS.md",
    "MIRA_AGENT_STARTER_API.md",
    "MIRA_AGENT_STARTER_MESSAGING.md",
    "MIRA_AGENT_STARTER_ANALYTICS.md",
    "MIRA_FIRST_RUN_DEMO.md",
    "MIRA_PUBLIC_BENCHMARK_EVIDENCE.md",
    "MIRA_PUBLIC_BENCHMARK_MATRIX.md",
    "MIRA_PUBLIC_BENCHMARK_DIAGNOSTICS.md",
    "MIRA_PUBLIC_BENCHMARK_COMPARISON_GO.md",
    "MIRA_PUBLIC_BENCHMARK_COMPARISON_RUST.md",
    "MIRA_PUBLIC_BENCHMARK_SNIPPETS.md",
    "MIRA_PUBLIC_PROOF_PACK_MESSAGING.md",
    "MIRA_PUBLIC_PROOF_PACK_ANALYTICS.md",
    "MIRA_PUBLIC_PROOF_PACK_DISTRIBUTED.md",
    "MIRA_PUBLIC_LAUNCH_BUNDLE.md",
    "MIRA_PUBLIC_LAUNCH_CHECKLIST.md",
    "MIRA_PUBLIC_PUBLISHING_GUIDE.md",
    "MIRA_PUBLIC_LAUNCH_POSTS.md",
    "MIRA_AGENT_GENERATION_CONTRACT.md",
    "MIRA_RELEASE_2.6.0.md",
    "MIRA_RELEASE_3.0.0.md",
    "MIRA_RELEASE_3.1.0.md",
    "MIRA_RELEASE_3.2.0.md",
]

ARTIFACT_FILES = [
    "advanced-backend-matrix-2.6.0.json",
    "advanced-backend-matrix-2.6.0-diagnostics.json",
    "advanced-backend-release-bundle-2.6.0.json",
    "messaging-hardening-2.6.0.json",
    "analytics-hardening-2.6.0.json",
    "distributed-benchmark-2.4.0.json",
    "manifest-public-launch-v1.json",
]

MIRAC_FILES = ["Cargo.toml", "Cargo.lock"]
MIRAC_DIRS = ["src", "tests", "tools", "schema"]


def replace_paths(text: str) -> str:
    replacements = {
        str(ROOT / "docs") + "/": "docs/",
        str(ROOT / "mira") + "/": "mira/",
        str(ROOT / "mirac") + "/": "mirac/",
        str(ROOT / "tmp" / "mira-release") + "/": "artifacts/",
        str(ROOT / "tmp" / "mira-native") + "/": "tmp/mira-native/",
        str(ROOT): "<mira-public-repo>",
        str(ROOT) + "/": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def copy_doc(filename: str) -> None:
    src = DOCS / filename
    if filename == "MIRA_PUBLIC_REPO_README.md":
        dst = EXPORT / "README.md"
    else:
        dst = EXPORT / "docs" / filename
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(replace_paths(src.read_text()))


def main() -> None:
    if EXPORT.exists():
        shutil.rmtree(EXPORT)

    (EXPORT / "docs").mkdir(parents=True, exist_ok=True)
    (EXPORT / "artifacts").mkdir(parents=True, exist_ok=True)
    (EXPORT / "mira" / "examples").mkdir(parents=True, exist_ok=True)
    (EXPORT / "mirac").mkdir(parents=True, exist_ok=True)

    for filename in DOC_FILES:
        copy_doc(filename)

    for path in (ROOT / "mira" / "examples").glob("*.mira"):
        shutil.copy2(path, EXPORT / "mira" / "examples" / path.name)

    for filename in MIRAC_FILES:
        shutil.copy2(ROOT / "mirac" / filename, EXPORT / "mirac" / filename)

    for dirname in MIRAC_DIRS:
        shutil.copytree(
            ROOT / "mirac" / dirname,
            EXPORT / "mirac" / dirname,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "target", ".git"),
        )

    for filename in ARTIFACT_FILES:
        shutil.copy2(TMP_RELEASE / filename, EXPORT / "artifacts" / filename)

    export_manifest = {
        "ok": True,
        "export_root": str(EXPORT),
        "readme": str(EXPORT / "README.md"),
        "docs_index": str(EXPORT / "docs" / "MIRA_PUBLIC_DOCS_INDEX.md"),
        "artifacts": [str(EXPORT / "artifacts" / filename) for filename in ARTIFACT_FILES],
    }
    (EXPORT / "export-manifest.json").write_text(json.dumps(export_manifest, indent=2))
    print(json.dumps(export_manifest, indent=2))


if __name__ == "__main__":
    main()

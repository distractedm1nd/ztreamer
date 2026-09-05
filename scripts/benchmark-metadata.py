#!/usr/bin/env python3
"""Archive build provenance without confusing a sibling checkout with a dependency."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tomllib


def capture(command, cwd):
    try:
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    except OSError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    lock_path = repo / "Cargo.lock"
    with lock_path.open("rb") as source:
        lock = tomllib.load(source)
    shutil.copyfile(lock_path, args.output / "Cargo.lock")
    diff = capture(["git", "diff", "HEAD", "--binary"], repo)
    (args.output / "working-tree.patch").write_text(diff or "")
    metadata = {
        "schema_version": 1,
        "recorded_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "commit": capture(["git", "rev-parse", "HEAD"], repo),
        "status": capture(["git", "status", "--porcelain"], repo),
        "rustc": capture(["rustc", "-Vv"], repo),
        "cargo": capture(["cargo", "-V"], repo),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "cpu": capture(["sysctl", "-n", "machdep.cpu.brand_string"], repo)
        if platform.system() == "Darwin" else capture(["lscpu"], repo),
        "memory": capture(["sysctl", "-n", "hw.memsize"], repo)
        if platform.system() == "Darwin" else capture(["free", "-b"], repo),
        "git_dependencies": [
            {key: package[key] for key in ("name", "version", "source")}
            for package in lock["package"] if package.get("source", "").startswith("git+")
        ],
        "environment": {key: os.environ[key] for key in (
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR",
            "CARGO_PROFILE_RELEASE_DEBUG", "CACHE_STATE", "SNAPSHOT_ID", "PERF", "PERF_FREQ",
            "FETCH_WORKERS", "SOURCE_SEGMENT_BLOCKS", "MAX_PENDING_BYTES", "MAX_BATCH_BYTES",
        ) if key in os.environ},
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()

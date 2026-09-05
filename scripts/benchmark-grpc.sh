#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo/benchmark-runs/grpc"}
run="$run_root/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$run"
python3 -c 'import tomllib'
cargo build --locked --manifest-path "$repo/Cargo.toml" --release -p ztreamer-service --example grpc-load
python3 "$repo/scripts/benchmark-metadata.py" "$repo" "$run/provenance"
command=(cargo run --locked --manifest-path "$repo/Cargo.toml" --release -p ztreamer-service --example grpc-load -- "$@")
printf '%q ' "${command[@]}" > "$run/command.txt"
printf '\n' >> "$run/command.txt"
set +e
"${command[@]}" > "$run/results.json" 2> "$run/run.log"
status=$?
set -e
echo "$status" > "$run/exit-status.txt"
echo "gRPC load artifacts: $run"
if (( status != 0 )); then
    cat "$run/run.log" >&2
fi
exit "$status"

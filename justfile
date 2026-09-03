default:
    @just --list

build:
    cargo build --release -p ztreamerd

test:
    cargo test --workspace --locked

clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

fmt:
    cargo fmt --all

# Criterion: `just bench`, `just bench parse_block`, `just bench -- --save-baseline base`
bench *args:
    cargo bench --bench parser --bench codec --bench serve {{ args }}

# Historical genesis→tip. Needs a writable Zakura cache and config.
snapshot cache config *args:
    scripts/benchmark-snapshot.sh {{ cache }} {{ config }} {{ args }}

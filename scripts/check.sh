#!/usr/bin/env bash
# The checks a change must pass: formatting, clippy, tests and rustdoc, stopping at the first failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== test";   cargo test --workspace
echo "== doc";    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

#!/usr/bin/env bash
# The checks a change must pass: formatting, clippy, tests, rustdoc and cargo-deny, stopping at the
# first failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== fuzz";   cargo clippy --manifest-path crates/turbofan-cluster/fuzz/Cargo.toml --bins -- -D warnings
echo "== test";   cargo test --workspace
echo "== doc";    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
echo "== deny";   cargo deny check   # licences, advisories, sources (deny.toml); needs cargo-deny

#!/usr/bin/env bash
# The checks a change must pass: formatting, clippy, tests, rustdoc and cargo-deny, stopping at the
# first failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== clippy, all features"; cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "== fuzz";   cargo fmt --manifest-path crates/turbofan/fuzz/Cargo.toml --check
                  cargo clippy --manifest-path crates/turbofan/fuzz/Cargo.toml --bins --locked -- -D warnings
echo "== test";   cargo test --workspace
echo "== test, all features"; cargo test --workspace --all-features
echo "== doc";    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
echo "== deny";   cargo deny check   # licences, advisories, sources (deny.toml); needs cargo-deny

#!/usr/bin/env bash
# The checks a change must pass: formatting, clippy, tests, rustdoc, cargo-deny and cargo-semver-checks,
# stopping at the first failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== clippy, all features"; cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "== fuzz";   cargo fmt --manifest-path crates/turbofan/fuzz/Cargo.toml --check
                  cargo clippy --manifest-path crates/turbofan/fuzz/Cargo.toml --bins --locked -- -D warnings
# Without features only the unit tests: the integration tests and examples run the same with them
# (plus TLS), clippy above builds every target without them, and CI runs them all so.
echo "== test";   cargo test --workspace --lib
echo "== test, all features"; cargo test --workspace --all-features
echo "== doc";    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
echo "== deny";   cargo deny check   # licences, advisories, sources (deny.toml); needs cargo-deny
# The public API against the last published version's: a break fails until the workspace version is
# bumped to allow it. Hidden items (`#[doc(hidden)]`) aren't checked. Needs cargo-semver-checks.
echo "== semver"; cargo semver-checks -p turbofan --features tls,testing
                  cargo semver-checks -p turbofan-sbe

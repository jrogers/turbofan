#!/usr/bin/env bash
# Fuzzes what the cluster reads from the network (crates/turbofan-cluster/fuzz) with cargo-fuzz:
# each target for SECONDS (default 60), or just TARGET. Needs nightly and `cargo install cargo-fuzz`.
#   scripts/fuzz.sh              # every target, a minute each
#   scripts/fuzz.sh 600 client   # the client target, ten minutes
# Targets: `decode` (frames and log records from raw bytes) and `client` (a lone node fed fuzzed
# client steps, checking every message applies once and in order). Every target runs even if one
# fails, and the script fails at the end if any did. A crash is saved under
# crates/turbofan-cluster/fuzz/artifacts/<target>/; replay it with
#   cargo +nightly fuzz run <target> <file>   (from crates/turbofan-cluster)
set -euo pipefail
cd "$(dirname "$0")/../crates/turbofan-cluster"

seconds=${1:-60}
targets=${2:-$(cargo +nightly fuzz list)}
# The toolchain's host, not the one cargo-fuzz was built for (a prebuilt one may be musl, which
# the sanitizers can't link).
host=$(rustc +nightly -vV | sed -n 's/^host: //p')
failed=
for target in $targets; do
    echo "== $target (${seconds}s)"
    cargo +nightly fuzz run --target "$host" "$target" -- -max_total_time="$seconds" || failed="$failed $target"
done
if [ -n "$failed" ]; then
    echo "failed:$failed" >&2
    exit 1
fi

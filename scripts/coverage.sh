#!/usr/bin/env bash
# Coverage of the core by the simulation and the cluster test: which lines and branches of
# crates/turbofan/src no seed reaches. Mutation testing (scripts/mutants.sh) finds what the tests
# run without checking, in what changed; this finds what they never run at all. A figure isn't
# the goal: an uncovered branch is a fault the sim doesn't make, or dead code.
#   scripts/coverage.sh              # 2000 seeds, a shard per CPU
#   SIM_SEEDS=200 scripts/coverage.sh
# The report lands in target/coverage/html/index.html, and a summary by file is printed. Needs
# cargo-llvm-cov and nightly (branch coverage) with llvm-tools. Builds into
# target/llvm-cov-target, apart from the usual builds.
set -euo pipefail
cd "$(dirname "$0")/.."

seeds=${SIM_SEEDS:-2000}
jobs=${COVERAGE_JOBS:-$(getconf _NPROCESSORS_ONLN)}
cov=(cargo +nightly llvm-cov --release --branch -p turbofan)
ignore='turbofan-sbe/|/tests/|/examples/|/out/|/fuzz/'

cargo +nightly llvm-cov clean --workspace
# Built by listing its tests, which runs none.
"${cov[@]}" --no-report --test sim -- --list >/dev/null
sim=$(find target/llvm-cov-target/release -type f -perm -u+x -name 'sim-*' ! -name '*.*' -print0 |
    xargs -0 ls -t | head -1)

# The sim runs its seeds one after another: a process per shard, each writing a profile of its own.
profiles=$PWD/target/llvm-cov-target
per=$(((seeds + jobs - 1) / jobs))
for ((i = 0; i < jobs; i++)); do
    SIM_BASE=$((i * per)) SIM_SEEDS=$per LLVM_PROFILE_FILE="$profiles/turbofan-%p-%8m.profraw" \
        "$sim" >"$profiles/sim-shard$i.log" 2>&1 &
done
wait
# A seed failing stops its shard; a shard of few seeds missing what a run must reach doesn't count.
if grep -h 'simulation failed' "$profiles"/sim-shard*.log; then
    echo "see $profiles/sim-shard*.log" >&2
    exit 1
fi

"${cov[@]}" --no-report --test cluster
"${cov[@]}" report --html --output-dir target/coverage --ignore-filename-regex "$ignore"
"${cov[@]}" report --ignore-filename-regex "$ignore"
echo "report: target/coverage/html/index.html"

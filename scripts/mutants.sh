#!/usr/bin/env bash
# Mutation testing of the Rust that changed since BASE (a commit; the one before HEAD by default),
# uncommitted changes included: cargo-mutants changes only the lines the diff touches, one way at a
# time (a `<` for a `<=`, a body for a default), and runs the tests against each. A mutant the
# tests still pass on is code they run without checking. Needs cargo-mutants.
#   scripts/mutants.sh             # what the last commit changed, and what's uncommitted
#   scripts/mutants.sh origin/main # what this branch changed
# Results land in mutants.out/ (missed.txt: the mutants the tests missed). It exits as cargo-mutants
# does: 0 if every mutant was caught, 2 if some were missed, 3 if some timed out. Some mutants can't
# be caught, being the same as the code: see a missed one's line before writing a test for it.
# Doctests aren't run against mutants: they take long to build, and the other tests cover them.
# With the `tls` feature: without it, the TLS code isn't built, and its every mutant passes.
set -euo pipefail
cd "$(dirname "$0")/.."

base=${1:-HEAD~1}
diff=$(mktemp)
trap 'rm -f "$diff"' EXIT
git diff --no-ext-diff --no-color "$base" -- '*.rs' >"$diff"
if [[ ! -s $diff ]]; then
    echo "no Rust changed since $base"
    exit 0
fi
cargo mutants --in-diff "$diff" --features turbofan/tls --jobs "${MUTANTS_JOBS:-2}" --no-shuffle -- --tests

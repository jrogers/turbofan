#!/usr/bin/env bash
# Shows how the latest nightly runs went and, for one that failed, fetches what it left behind
# and prints the commands that replay it. Needs the GitHub CLI (`gh`), logged in.
#   scripts/nightly.sh           # the latest scheduled Fuzz and Simulate runs
#   scripts/nightly.sh RUN_ID    # one run, scheduled or not
# Artifacts go to target/nightly/RUN_ID/. Replay at the run's commit (printed), since a change to
# the simulation or a fuzz target can move what a seed or an input does.
set -euo pipefail
cd "$(dirname "$0")/.."

# Prints the replay commands for run $1 of workflow $2, after downloading its artifacts.
replay() {
    local run=$1 workflow=$2 dir=target/nightly/$1 sha
    sha=$(gh run view "$run" --json headSha --jq .headSha)
    rm -rf "$dir"
    if ! gh run download "$run" --dir "$dir" 2>/dev/null; then
        echo "  no artifacts: the job died before uploading them (see the run's log)"
        return
    fi
    echo "  artifacts in $dir; replay at ${sha:0:12} (git worktree add .worktrees/replay $sha):"
    case $workflow in
    Fuzz)
        # The artifact holds fuzz/artifacts/: one directory per target, one file per input.
        for input in "$dir"/fuzz-artifacts/*/*; do
            [ -f "$input" ] || continue
            local target
            target=$(basename "$(dirname "$input")")
            echo "    (cd crates/turbofan-cluster && cargo +nightly fuzz run $target $PWD/$input)"
        done
        ;;
    Simulate)
        # The test prints `simulation failed: SIM_SEED=<n>` for the seed it stopped at.
        local log=$dir/simulation-log/sim.log seed
        if [ -f "$log" ]; then
            seed=$(grep -oE 'SIM_SEED=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
            if [ -n "$seed" ]; then
                echo "    SIM_SEED=$seed cargo test -p turbofan-cluster --release --test sim -- --nocapture"
            else
                echo "    no failing seed in $log"
            fi
        fi
        # The soak has no seed: its log says what went wrong, and it runs again as long.
        if [ -f "$dir/soak-log/soak.log" ]; then
            echo "    soak failed: see $dir/soak-log/soak.log, and run it again:"
            echo "    SOAK_SECS=600 cargo test -p turbofan-cluster --release --test soak -- --nocapture"
        fi
        ;;
    esac
}

# Prints run $1's result, and its replay commands if it failed.
report() {
    local run=$1 workflow conclusion created
    read -r workflow conclusion created < <(gh run view "$run" \
        --json workflowName,conclusion,createdAt --jq '"\(.workflowName) \(.conclusion) \(.createdAt)"')
    echo "$workflow run $run ($created): ${conclusion:-in progress}"
    gh run view "$run" --json jobs --jq '.jobs[] | select(.conclusion == "failure") | "  failed: \(.name)"'
    if [ "$conclusion" = failure ]; then
        replay "$run" "$workflow"
    fi
}

if [ $# -gt 0 ]; then
    report "$1"
    exit
fi
for workflow in Fuzz Simulate; do
    run=$(gh run list --workflow "$workflow" --event schedule --limit 1 --json databaseId --jq '.[0].databaseId // empty')
    if [ -n "$run" ]; then report "$run"; else echo "$workflow: no scheduled runs"; fi
done

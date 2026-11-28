#!/usr/bin/env bash
# Model-checks Raft's own rules with TLC (crates/turbofan/tla/Raft.tla): every interleaving of a
# small cluster, where the simulation samples. Needs Java 11 or later; fetches TLC's jar, of a
# pinned version and checksum, into target/tla/ the first time.
#   scripts/tla.sh                  # every model
#   scripts/tla.sh doubt            # one: crates/turbofan/tla/MC<Name>.cfg
#   scripts/tla.sh doubtTwoTrace    # a scripted run, MC<Name>.tla: under a second
# Each takes hours (its .cfg says how long), so check.sh doesn't run them: run them after
# changing the rules in raft/, and change the spec with them. A violation
# prints the trace that leads to it.
set -euo pipefail
cd "$(dirname "$0")/.."
version=1.7.4
sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
jar=target/tla/tla2tools-$version.jar
if [[ ! -f $jar ]]; then
    mkdir -p target/tla
    curl -sSfL -o "$jar.part" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
    if [[ $(shasum -a 256 "$jar.part" | cut -d' ' -f1) != "$sha256" ]]; then
        echo "tla2tools.jar $version: checksum mismatch" >&2
        rm -f "$jar.part"
        exit 1
    fi
    mv "$jar.part" "$jar"
fi
models=("$@")
[[ ${#models[@]} -gt 0 ]] || models=(doubt)
for model in "${models[@]}"; do
    name="$(tr '[:lower:]' '[:upper:]' <<<"${model:0:1}")${model:1}"
    config=crates/turbofan/tla/MC$name.cfg
    [[ -f $config ]] || { echo "no model $config" >&2; exit 1; }
    # A model of its own module (MC<Name>.tla) is a scripted run: one that stops short is a
    # deadlock, and fails. The rest are searches of MCRaft.tla, which may end anywhere.
    module=crates/turbofan/tla/MC$name.tla
    deadlock=()
    [[ -f $module ]] || { module=crates/turbofan/tla/MCRaft.tla; deadlock=(-deadlock); }
    echo "== $model"
    java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto ${deadlock[@]+"${deadlock[@]}"} -cleanup \
        -metadir "target/tla/states-$model" -config "$config" "$module"
done

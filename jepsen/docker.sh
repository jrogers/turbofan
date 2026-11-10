#!/usr/bin/env bash
# The Jepsen tests in Docker (#14), for writing them: a control node and five nodes, from the
# community's jepsen-docker images (Jepsen itself no longer has a Docker setup). Containers share
# the host's kernel, so its clock: no clock faults here, and the real runs are on EC2.
#   jepsen/docker.sh build        # examples/list_append for Linux, into target/jepsen/
#   jepsen/docker.sh images       # jepsen-docker's images, built locally (its published ones are x86 only)
#   jepsen/docker.sh up | down    # the cluster, the repository at /turbofan on the control node
#   jepsen/docker.sh test ARGS    # lein run test ARGS on the control node: --nemesis partition,kill ...
# Results land in jepsen/store/; `docker.sh test` then `lein run serve` there shows them.
set -euo pipefail
cd "$(dirname "$0")/.."

# jepsen-docker as of 2026-09-22: Debian 13, Jepsen on the control node, LazyFS and the clock
# tools on the nodes.
JEPSEN_DOCKER_COMMIT=4b5707813a0a24966082ae4fd2dd223da447e499
JEPSEN_DOCKER=target/jepsen-docker
export TURBOFAN_ROOT=$PWD
compose() {
    docker compose -f "$JEPSEN_DOCKER/jepsen-compose.yaml" -f jepsen/docker-compose.yaml "$@"
}

case ${1:-} in
build)
    # Debian 12's glibc, older than the nodes' 13. A target directory of its own, beside the Mac's.
    docker run --rm -v "$PWD:/src" -w /src -v turbofan-jepsen-cargo:/usr/local/cargo/registry \
        rust:1-bookworm cargo build --release -p turbofan --example list_append --target-dir target/jepsen
    ;;
images)
    if [[ ! -d $JEPSEN_DOCKER ]]; then
        git clone -q https://github.com/nurturenature/jepsen-docker.git "$JEPSEN_DOCKER"
    fi
    git -C "$JEPSEN_DOCKER" fetch -q origin
    git -C "$JEPSEN_DOCKER" checkout -q "$JEPSEN_DOCKER_COMMIT"
    # The control node installs Jepsen from its repository, by default at main, which may need
    # snapshots that aren't published: the release the harness uses instead.
    docker build -t jepsen-control --build-arg REPO_BRANCH=v0.3.15 "$JEPSEN_DOCKER/jepsen-control"
    for image in node setup; do
        docker build -t "jepsen-$image" "$JEPSEN_DOCKER/jepsen-$image"
    done
    ;;
up)
    compose up --detach --wait
    ;;
down)
    compose down
    ;;
test)
    shift
    docker exec -t jepsen-control bash -lc "cd /turbofan/jepsen && lein run test --nodes-file /jepsen/nodes --ssh-private-key /root/.ssh/id_rsa $(printf '%q ' "$@")"
    ;;
*)
    sed -n '2,9p' "$0" >&2
    exit 2
    ;;
esac

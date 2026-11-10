#!/usr/bin/env bash
# The Jepsen tests on AWS (#14): five nodes and a control node of their own, Debian 13 as
# jepsen.os.debian expects, with real clocks, so clock faults too, which Docker can't do.
#   scripts/aws-jepsen.sh         # launch, set up, run the tests, fetch the results, terminate
#   scripts/aws-jepsen.sh clean   # delete what an earlier run left, should one have died
# Needs the aws CLI logged in (aws login, or aws configure sso), and ssh. Builds HEAD as committed.
#
# Runs, each into target/aws-jepsen/<run>/<name>/ (Jepsen's store, `lein run serve` shows it):
#   all            every fault (partitions, packets, kills, pauses, clocks, members), synced, on LazyFS
#   clock          clock faults alone: bumps, strobes, resets
#   crash          every fault but clocks, --no-fsync, not on LazyFS: a process killed loses nothing
#   lazyfs-unsynced  kills on LazyFS, --no-fsync: expected to fail, losing acknowledged appends
#                  when a majority dies together, as the durability docs say
#
# What it costs: six instances while it runs, five c6i.large (~$0.09/hour each) and a c6i.xlarge
# (~$0.17/hour), on demand (check AWS's pricing), and a public IPv4 address each: ~$0.65/hour.
# Everything it makes is tagged turbofan-jepsen=<run> and deleted on any exit; should this
# machine stop first, each instance shuts itself down after MAX_MINUTES, which terminates it,
# and `clean` deletes the rest.
#
#   AWS_PROFILE   the aws CLI's profile, as for any aws command
#   REGION        us-east-1      AZ            a zone that offers the types
#   TYPE          c6i.large      CONTROL_TYPE  c6i.xlarge (the same architecture: it builds the binary)
#   TIME_LIMIT    300 (seconds a test)          MAX_MINUTES   240
#   RUNS          the runs above, or some of them, space-separated
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_REGION=${REGION:-us-east-1}
TYPE=${TYPE:-c6i.large}
CONTROL_TYPE=${CONTROL_TYPE:-c6i.xlarge}
TIME_LIMIT=${TIME_LIMIT:-300}
MAX_MINUTES=${MAX_MINUTES:-240}
RUNS=${RUNS:-all clock crash lazyfs-unsynced}
TAG=turbofan-jepsen
FAULTS=partition,packet,kill,pause,member

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# Each run's lein arguments, and whether Elle should find it valid.
args() {
    case $1 in
        all) echo "--nemesis $FAULTS,clock --fsync --lazyfs" ;;
        clock) echo "--nemesis clock --fsync" ;;
        crash) echo "--nemesis $FAULTS" ;;
        lazyfs-unsynced) echo "--nemesis kill --lazyfs" ;;
        *) return 1 ;;
    esac
}
expected() { [[ $1 == lazyfs-unsynced ]] && echo invalid || echo valid; }

# Everything tagged by any run: instances first, which the rest can't be deleted under.
clean() {
    local filter="Name=tag-key,Values=$TAG"
    local ids
    ids=$(aws ec2 describe-instances --filters "$filter" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text)
    if [[ -n $ids ]]; then
        log "terminating $ids"
        # shellcheck disable=SC2086 # one ID a word
        aws ec2 terminate-instances --instance-ids $ids >/dev/null
        # shellcheck disable=SC2086
        aws ec2 wait instance-terminated --instance-ids $ids
    fi
    for sg in $(aws ec2 describe-security-groups --filters "$filter" --query 'SecurityGroups[].GroupId' --output text); do
        log "deleting security group $sg"
        aws ec2 delete-security-group --group-id "$sg" >/dev/null
    done
    for key in $(aws ec2 describe-key-pairs --filters "$filter" --query 'KeyPairs[].KeyName' --output text); do
        log "deleting key pair $key"
        aws ec2 delete-key-pair --key-name "$key"
    done
}

if [[ ${1:-} == clean ]]; then
    clean
    exit 0
elif [[ $# -gt 0 ]]; then
    sed -n '2,6p' "$0" >&2
    exit 2
fi

for run in $RUNS; do
    args "$run" >/dev/null || { echo "no run $run" >&2; exit 2; }
done
for tool in aws ssh git curl; do
    command -v "$tool" >/dev/null || { echo "needs $tool" >&2; exit 1; }
done
aws sts get-caller-identity >/dev/null || { echo "the aws CLI isn't logged in: aws login (or aws configure sso)" >&2; exit 1; }
if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
    log "warning: uncommitted changes aren't tested; it builds HEAD, $(git rev-parse --short HEAD)"
fi

RUN=$TAG-$(date +%Y%m%d-%H%M%S)
OUT=target/aws-jepsen/$RUN
mkdir -p "$OUT"
KEY=$OUT/key

# From here, whatever happens, what this run made is deleted.
SG='' IDS=''
cleanup() {
    local status=$?
    trap - EXIT INT TERM
    log "cleaning up $RUN"
    if [[ -n $IDS ]]; then
        # shellcheck disable=SC2086
        aws ec2 terminate-instances --instance-ids $IDS >/dev/null || true
        # shellcheck disable=SC2086
        aws ec2 wait instance-terminated --instance-ids $IDS || true
    fi
    [[ -n $SG ]] && { aws ec2 delete-security-group --group-id "$SG" >/dev/null || true; }
    aws ec2 delete-key-pair --key-name "$RUN" >/dev/null 2>&1 || true
    rm -f "$KEY"
    left=$(aws ec2 describe-instances --filters "Name=tag:$TAG,Values=$RUN" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text || echo unknown)
    if [[ -n $left ]]; then
        log "WARNING: instances may still be running ($left): run scripts/aws-jepsen.sh clean"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# A zone that has both types, and the default VPC's subnet there.
AZ=${AZ:-$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=instance-type,Values=$TYPE,$CONTROL_TYPE" \
    --query 'InstanceTypeOfferings[].[Location,InstanceType]' --output text |
    awk '{n[$1]++} n[$1] == 2 {print $1; exit}')}
[[ -n $AZ ]] || { echo "no zone of $AWS_REGION offers both $TYPE and $CONTROL_TYPE" >&2; exit 1; }
SUBNET=$(aws ec2 describe-subnets --filters "Name=availability-zone,Values=$AZ" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
VPC=$(aws ec2 describe-subnets --subnet-ids "$SUBNET" --query 'Subnets[0].VpcId' --output text)
arch() {
    local arch
    arch=$(aws ec2 describe-instance-types --instance-types "$1" \
        --query 'InstanceTypes[0].ProcessorInfo.SupportedArchitectures[0]' --output text)
    [[ $arch == x86_64 ]] && arch=amd64
    echo "$arch"
}
ARCH=$(arch "$TYPE")
[[ $(arch "$CONTROL_TYPE") == "$ARCH" ]] || { echo "$TYPE and $CONTROL_TYPE differ in architecture" >&2; exit 1; }
AMI=resolve:ssm:/aws/service/debian/release/13/latest/$ARCH
MY_IP=$(curl -fsS https://checkip.amazonaws.com)
log "$RUN: 5 × $TYPE and 1 × $CONTROL_TYPE in $AZ, SSH from $MY_IP"

aws ec2 create-key-pair --key-name "$RUN" --key-type ed25519 \
    --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN}]" \
    --query KeyMaterial --output text >"$KEY"
chmod 400 "$KEY"
SG=$(aws ec2 create-security-group --group-name "$RUN" --description "$RUN" --vpc-id "$VPC" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN}]" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MY_IP/32" >/dev/null
# The nodes and the control node reach each other on any port.
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --source-group "$SG" >/dev/null

# A shutdown terminates, and each shuts itself down after MAX_MINUTES.
launch() {
    aws ec2 run-instances --count "$1" --instance-type "$2" --image-id "$AMI" --key-name "$RUN" \
        --subnet-id "$SUBNET" --security-group-ids "$SG" \
        --associate-public-ip-address --instance-initiated-shutdown-behavior terminate \
        --metadata-options HttpTokens=required \
        --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN}]" \
        "ResourceType=volume,Tags=[{Key=$TAG,Value=$RUN}]" \
        --user-data "$(printf '#!/bin/sh\nshutdown -h +%s\n' "$MAX_MINUTES")" \
        --query 'Instances[].InstanceId' --output text
}
NODE_IDS=$(launch 5 "$TYPE")
IDS=$NODE_IDS
CONTROL_ID=$(launch 1 "$CONTROL_TYPE")
IDS="$NODE_IDS $CONTROL_ID"
log "launched $IDS"
# shellcheck disable=SC2086
aws ec2 wait instance-running --instance-ids $IDS
addresses() {
    # shellcheck disable=SC2086
    aws ec2 describe-instances --instance-ids $1 --query "Reservations[].Instances[].$2" --output text
}
read -r -a PRIVATE <<<"$(addresses "$NODE_IDS" PrivateIpAddress)"
read -r -a PUBLIC <<<"$(addresses "$NODE_IDS" PublicIpAddress)"
CONTROL=$(addresses "$CONTROL_ID" PublicIpAddress)

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$OUT/known_hosts"
    -o ConnectTimeout=5 -o ServerAliveInterval=15 -o BatchMode=yes)
on() { local host=$1; shift; "${SSH[@]}" "admin@$host" "$@"; }
for host in "${PUBLIC[@]}" "$CONTROL"; do
    for _ in $(seq 60); do on "$host" true 2>/dev/null && break; sleep 5; done
    on "$host" true
done
log "SSH up"

# Every machine knows the nodes as n1..n5, as the harness names them. The nodes' clocks are left
# to the clock faults: no daemon puts them right. Each waited for by its own PID: a bare `wait`
# succeeds whatever they did.
HOSTS=$(for i in 1 2 3 4 5; do echo "${PRIVATE[i-1]} n$i"; done)
APT='sudo NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive apt-get -qq'
PIDS=()
for i in 1 2 3 4 5; do
    on "${PUBLIC[i-1]}" "set -e
        echo '$HOSTS' | sudo tee -a /etc/hosts >/dev/null
        sudo hostnamectl set-hostname n$i
        sudo systemctl disable --now chrony systemd-timesyncd 2>/dev/null || true
        $APT update && $APT install -y build-essential >/dev/null" &
    PIDS+=($!)
done
git archive --format=tar HEAD | on "$CONTROL" 'mkdir -p src && tar -x -C src'
log "building $(git rev-parse --short HEAD) on the control node"
on "$CONTROL" "set -e
    echo '$HOSTS' | sudo tee -a /etc/hosts >/dev/null
    printf 'n%s\n' 1 2 3 4 5 >nodes
    $APT update && $APT install -y build-essential curl git default-jdk-headless gnuplot-nox graphviz >/dev/null
    curl -fsSL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein -o lein
    sudo install lein /usr/local/bin/lein && lein version >/dev/null
    curl -fsS https://sh.rustup.rs | sh -s -- -y -q --profile minimal >/dev/null
    . ~/.cargo/env && cd src && cargo build -q --release -p turbofan --example list_append
    cd jepsen && lein deps >/dev/null" &
PIDS+=($!)
for pid in "${PIDS[@]}"; do wait "$pid"; done
scp -q -i "$KEY" -o UserKnownHostsFile="$OUT/known_hosts" "$KEY" "admin@$CONTROL:.ssh/id_ed25519"
log "set up"

VERDICTS=()
for run in $RUNS; do
    log "run $run: $(args "$run")"
    # shellcheck disable=SC2046 # its arguments, a word each
    on "$CONTROL" "cd src/jepsen && lein run test --nodes-file ~/nodes --username admin \
        --ssh-private-key ~/.ssh/id_ed25519 --binary /home/admin/src/target/release/examples/list_append \
        --time-limit $TIME_LIMIT $(args "$run") 2>&1" >"$OUT/$run.log" || true
    if grep -q 'Everything looks good' "$OUT/$run.log"; then got=valid
    elif grep -q 'Analysis invalid' "$OUT/$run.log"; then got=invalid
    else got=broke; fi
    verdict="$run: $got, expected $(expected "$run")"
    log "$verdict"
    VERDICTS+=("$verdict")
    # Its store, then cleared for the next.
    mkdir -p "$OUT/$run"
    on "$CONTROL" 'tar -c -C src/jepsen store' | tar -x -C "$OUT/$run"
    on "$CONTROL" 'rm -rf src/jepsen/store'
done

{
    echo "# $RUN"
    echo
    echo "turbofan $(git rev-parse --short HEAD), 5 × $TYPE and 1 × $CONTROL_TYPE in $AZ ($AWS_REGION), $TIME_LIMIT s a test."
    echo
    printf -- '- %s\n' "${VERDICTS[@]}"
} >"$OUT/results.md"
cat "$OUT/results.md"
log "results in $OUT"

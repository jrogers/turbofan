#!/usr/bin/env bash
# The benchmark on AWS (#15): three nodes of examples/echo.rs on machines of their own, and the
# benchmark driving them from a fourth (BENCH_CLUSTER), all in one cluster placement group.
#   scripts/aws-bench.sh         # launch, build, run, fetch the tables, terminate
#   scripts/aws-bench.sh clean   # delete what an earlier run left, should one have died
# Needs the aws CLI logged in (aws login, or aws configure sso), jq, openssl and ssh. Builds HEAD as committed.
#
# Runs, each table into target/aws-bench/<run>/:
#   local          the in-process benchmark on the clients' machine, its files on the local NVMe
#   nvme           remote, the nodes' logs on their local NVMe, with --no-fsync
#   nvme-fsync     the same, synced (the default)
#   nvme-tls, nvme-tls-fsync   both again over TLS
#   ebs-fsync      synced, the logs on the root EBS volume (gp3): network storage
# Nodes and benchmark alike on jemalloc (LD_PRELOAD), as the guide's performance chapter says a
# service on Linux should be: glibc's malloc costs a third to half of the throughput.
#
# What it costs: four instances while it runs, ~$0.40/hour each for c6id.2xlarge on demand
# (check AWS's pricing), and a public IPv4 address each. Everything it makes is tagged
# turbofan-bench=<run> and deleted on any exit; should this machine stop first, each instance
# shuts itself down after MAX_MINUTES, which terminates it, and `clean` deletes the rest.
#
#   AWS_PROFILE  the aws CLI's profile, as for any aws command (AWS_PROFILE=development ...)
#   REGION       us-east-1           TYPE         c6id.2xlarge (needs local NVMe: a `d` type; x86 or Graviton)
#   AZ           a zone that offers TYPE, should one be out of capacity (us-east-1b)
#   BENCH_SECS   10                  MAX_MINUTES  180
#   RUNS         the runs above, or some of them, space-separated
#   PROFILE      1: profile every run with samply, each node and the benchmark, the build with
#                frame pointers and line tables. Each run's profiles go beside its table
#                (<run>-node<i>.json.gz, <run>-bench.json.gz, with their .syms.json), and its
#                cluster's status, which names the leader (<run>-status.txt): `samply load` one,
#                or read it as JSON. The profiler costs a few per cent: its tables aren't the
#                guide's figures.
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_REGION=${REGION:-us-east-1}
TYPE=${TYPE:-c6id.2xlarge}
BENCH_SECS=${BENCH_SECS:-10}
MAX_MINUTES=${MAX_MINUTES:-180}
RUNS=${RUNS:-local nvme nvme-fsync nvme-tls nvme-tls-fsync ebs-fsync}
PROFILE=${PROFILE:-0}
[[ $PROFILE == 0 || $PROFILE == 1 ]] || { echo "PROFILE is 0 or 1, not $PROFILE" >&2; exit 2; }
# The clients' machine's builds: with PROFILE, stacks samply can walk and name. The same for
# every cargo command there, else it rebuilds without.
CARGO_ENV=''
[[ $PROFILE == 1 ]] && CARGO_ENV="RUSTFLAGS='-C force-frame-pointers=yes' CARGO_PROFILE_RELEASE_DEBUG=line-tables-only"
SAMPLY_VERSION=0.13.1
TAG=turbofan-bench
HOME_DIR=/home/ubuntu
ADMIN_TOKEN=bench-admin
PEER_TOKEN=bench-peer

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

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
    for pg in $(aws ec2 describe-placement-groups --filters "$filter" --query 'PlacementGroups[].GroupName' --output text); do
        log "deleting placement group $pg"
        aws ec2 delete-placement-group --group-name "$pg"
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

for tool in aws jq openssl ssh git; do
    command -v "$tool" >/dev/null || { echo "needs $tool" >&2; exit 1; }
done
aws sts get-caller-identity >/dev/null || { echo "the aws CLI isn't logged in: aws login (or aws configure sso)" >&2; exit 1; }
if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
    log "warning: uncommitted changes aren't benchmarked; it builds HEAD, $(git rev-parse --short HEAD)"
fi

RUN=$TAG-$(date +%Y%m%d-%H%M%S)
OUT=target/aws-bench/$RUN
mkdir -p "$OUT"
KEY=$OUT/key
TAGS="ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN}]"

# From here, whatever happens, what this run made is deleted.
SG='' PG='' IDS=''
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
    [[ -n $PG ]] && { aws ec2 delete-placement-group --group-name "$PG" || true; }
    aws ec2 delete-key-pair --key-name "$RUN" >/dev/null 2>&1 || true
    rm -f "$KEY"
    left=$(aws ec2 describe-instances --filters "Name=tag:$TAG,Values=$RUN" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text || echo unknown)
    if [[ -n $left ]]; then
        log "WARNING: instances may still be running ($left): run scripts/aws-bench.sh clean"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# A zone that has the type, and the default VPC's subnet there.
AZ=${AZ:-$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=instance-type,Values=$TYPE" --query 'InstanceTypeOfferings[0].Location' --output text)}
[[ $AZ != None ]] || { echo "$TYPE isn't offered in $AWS_REGION" >&2; exit 1; }
SUBNET=$(aws ec2 describe-subnets --filters "Name=availability-zone,Values=$AZ" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
VPC=$(aws ec2 describe-subnets --subnet-ids "$SUBNET" --query 'Subnets[0].VpcId' --output text)
# Ubuntu for the type's architecture: x86 or Graviton.
ARCH=$(aws ec2 describe-instance-types --instance-types "$TYPE" \
    --query 'InstanceTypes[0].ProcessorInfo.SupportedArchitectures[0]' --output text)
[[ $ARCH == x86_64 ]] && ARCH=amd64
AMI=resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/$ARCH/hvm/ebs-gp3/ami-id
MY_IP=$(curl -fsS https://checkip.amazonaws.com)
log "$RUN: 4 × $TYPE in $AZ, SSH from $MY_IP"

aws ec2 create-key-pair --key-name "$RUN" --key-type ed25519 \
    --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN}]" \
    --query KeyMaterial --output text >"$KEY"
chmod 400 "$KEY"
SG=$(aws ec2 create-security-group --group-name "$RUN" --description "$RUN" --vpc-id "$VPC" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN}]" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MY_IP/32" >/dev/null
# The nodes and the clients reach each other on any port.
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --source-group "$SG" >/dev/null
aws ec2 create-placement-group --group-name "$RUN" --strategy cluster \
    --tag-specifications "ResourceType=placement-group,Tags=[{Key=$TAG,Value=$RUN}]" >/dev/null
PG=$RUN

# One request, as a cluster placement group wants. A shutdown terminates, and each shuts itself
# down after MAX_MINUTES.
IDS=$(aws ec2 run-instances --count 4 --instance-type "$TYPE" --image-id "$AMI" --key-name "$RUN" \
    --subnet-id "$SUBNET" --security-group-ids "$SG" --placement "GroupName=$PG" \
    --associate-public-ip-address --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=40,VolumeType=gp3,DeleteOnTermination=true}' \
    --tag-specifications "$TAGS" "ResourceType=volume,Tags=[{Key=$TAG,Value=$RUN}]" \
    --user-data "$(printf '#!/bin/sh\nshutdown -h +%s\n' "$MAX_MINUTES")" \
    --query 'Instances[].InstanceId' --output text)
log "launched $IDS"
# shellcheck disable=SC2086
aws ec2 wait instance-running --instance-ids $IDS
# shellcheck disable=SC2086
read -r -a PUBLIC <<<"$(aws ec2 describe-instances --instance-ids $IDS \
    --query 'Reservations[].Instances[].PublicIpAddress' --output text)"
# shellcheck disable=SC2086
read -r -a PRIVATE <<<"$(aws ec2 describe-instances --instance-ids $IDS \
    --query 'Reservations[].Instances[].PrivateIpAddress' --output text)"
# The first three the nodes, the fourth the clients'.
CLIENT=${PUBLIC[3]}

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$OUT/known_hosts"
    -o ConnectTimeout=5 -o ServerAliveInterval=15 -o BatchMode=yes)
on() { local host=$1; shift; "${SSH[@]}" "ubuntu@$host" "$@"; }
# Up once cloud-init is done: until then it may restart sshd, and a first connection's success
# doesn't last.
for host in "${PUBLIC[@]}"; do
    for _ in $(seq 60); do on "$host" 'cloud-init status --wait >/dev/null; true' 2>/dev/null && break; sleep 5; done
    on "$host" true
done
log "SSH up"

# The instance store, on /data; the clients' machine gets Rust and the source. Each waited for
# by its own PID: a bare `wait` succeeds whatever they did.
PIDS=()
for host in "${PUBLIC[@]}"; do
    on "$host" 'set -e
        dev=$(lsblk -dno NAME,MODEL | awk "/Instance Storage/ {print \"/dev/\" \$1; exit}")
        [ -n "$dev" ] || { echo "no instance store: a d type is needed" >&2; exit 1; }
        sudo mkfs.ext4 -q -F "$dev" && sudo mkdir -p /data && sudo mount "$dev" /data
        sudo chown ubuntu /data && mkdir -p /home/ubuntu/ebs
        sudo apt-get -qq update && sudo NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive apt-get -qq install -y libjemalloc2 >/dev/null
        test -f /usr/lib/$(uname -m)-linux-gnu/libjemalloc.so.2' &
    PIDS+=($!)
done
for pid in "${PIDS[@]}"; do wait "$pid"; done
git archive --format=tar HEAD | on "$CLIENT" 'mkdir -p src && tar -x -C src'
log "building $(git rev-parse --short HEAD) on the clients' machine"
on "$CLIENT" "set -e
    sudo NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive apt-get -qq install -y build-essential >/dev/null
    curl -fsS https://sh.rustup.rs | sh -s -- -y -q --profile minimal >/dev/null
    . ~/.cargo/env && cd src ${CARGO_ENV:+&& export $CARGO_ENV}
    cargo build -q --release -p turbofan --example echo --features tls
    cargo bench -q --no-run -p turbofan --bench cluster --features tls"
if [[ $PROFILE == 1 ]]; then
    log "building samply $SAMPLY_VERSION"
    on "$CLIENT" ". ~/.cargo/env && cargo install -q --locked samply@$SAMPLY_VERSION"
    # Every process sampled, the kernel's stacks named, without root.
    for host in "${PUBLIC[@]}"; do
        on "$host" 'sudo sysctl -q kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0'
    done
fi
# The binaries to the nodes, from the clients' machine, by the nodes' private addresses.
scp -q -i "$KEY" -o UserKnownHostsFile="$OUT/known_hosts" "$KEY" "ubuntu@$CLIENT:.ssh/id_ed25519"
for ip in "${PRIVATE[@]:0:3}"; do
    on "$CLIENT" "scp -q -o StrictHostKeyChecking=accept-new src/target/release/examples/echo ubuntu@$ip:echo"
    if [[ $PROFILE == 1 ]]; then
        on "$CLIENT" "scp -q .cargo/bin/samply ubuntu@$ip:samply"
    fi
done

# TLS: one certificate for every node, naming their private addresses, signed by a CA of this run.
certify() {
    local dir=$1; shift
    local sans
    sans=$(printf 'IP:%s,' "$@")
    mkdir -p "$dir"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj /CN=bench-ca \
        -keyout "$dir/ca.key" -out "$dir/ca.pem" 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=bench-node \
        -keyout "$dir/node.key" -out "$dir/node.csr" 2>/dev/null
    openssl x509 -req -in "$dir/node.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca.key" -days 2 -out "$dir/node.pem" \
        -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "${sans%,}") 2>/dev/null
}
certify "$OUT/certs" "${PRIVATE[@]:0:3}"
for host in "${PUBLIC[@]}"; do
    tar --no-xattrs -c -C "$OUT" certs/ca.pem certs/node.pem certs/node.key | on "$host" 'tar -x'
done

list() { local port=$1 i out=''; for i in 1 2 3; do out+="$i=${PRIVATE[i-1]}:$port,"; done; echo "${out%,}"; }
FLAGS="--peers $(list 7001) --clients $(list 7101) --admin $(list 7201)"
TLS_FLAGS='--tls-cert certs/node.pem --tls-key certs/node.key --tls-peer-ca certs/ca.pem'
CLIENTS=$(list 7101 | sed 's/[0-9]=//g')
# Expanded on each machine: its own architecture's path.
JEMALLOC='LD_PRELOAD=/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so.2'

# The benchmark's command on the clients' machine, `envs` before it: with PROFILE, its binary run
# under samply, the profile to `profile` there.
bench() {
    local envs=$1 profile=$2
    if [[ $PROFILE == 1 ]]; then
        # cargo runs a benchmark in its package's directory: so does this.
        echo ". ~/.cargo/env && cd src/crates/turbofan && export $CARGO_ENV && $envs ~/.cargo/bin/samply record \
            --save-only --unstable-presymbolicate -o $profile -- \
            \$(ls -t ../../target/release/deps/cluster-* | grep -v '\.d\$' | head -1) --bench"
    else
        echo ". ~/.cargo/env && cd src && $envs cargo bench -q -p turbofan --bench cluster --features tls"
    fi
}

# A profile samply wrote on `host`, `file` there without its extension, to `out` here, and gone
# from there.
fetch_profile() {
    local host=$1 file=$2 out=$3
    on "$host" "cat $file.json.gz" >"$out.json.gz"
    on "$host" "cat $file.json.syms.json" >"$out.json.syms.json"
    on "$host" "rm -f $file.json.gz $file.json.syms.json"
}

# One remote run: the nodes started with `extra` flags and their logs under `data`, a leader
# waited for, the benchmark run against them, and the nodes stopped and their data removed.
remote() {
    local name=$1 data=$2 extra=$3 fsync=$4 tls=$5 i status_tls='' bench_tls=''
    for i in 1 2 3; do
        on "${PUBLIC[i-1]}" "TURBOFAN_ADMIN_TOKEN=$ADMIN_TOKEN TURBOFAN_PEER_TOKEN=$PEER_TOKEN $JEMALLOC \
            nohup ./echo serve --id $i --data $data/n$i $FLAGS $extra >node.log 2>&1 </dev/null &"
    done
    if [[ $tls == 1 ]]; then
        status_tls='--tls-ca certs/ca.pem'
        bench_tls="BENCH_TLS_CA=$HOME_DIR/certs/ca.pem"
    fi
    if ! on "$CLIENT" "for _ in \$(seq 100); do
            TURBOFAN_ADMIN_TOKEN=$ADMIN_TOKEN src/target/release/examples/echo status --admin ${PRIVATE[0]}:7201 \
                $status_tls 2>/dev/null | grep -q '^leader [0-9]' && exit 0
            sleep 0.2
        done; exit 1"; then
        for i in 1 2 3; do echo "node $i:" >&2; on "${PUBLIC[i-1]}" 'tail -20 node.log' >&2; done
        echo "$name: no leader elected" >&2
        return 1
    fi
    for i in 1 2 3; do
        on "${PUBLIC[i-1]}" 'grep -q libjemalloc /proc/$(pgrep -x echo)/maps' ||
            { echo "$name: node $i isn't on jemalloc" >&2; return 1; }
    done
    if [[ $PROFILE == 1 ]]; then
        for i in 1 2 3; do
            on "${PUBLIC[i-1]}" 'nohup ./samply record --save-only --unstable-presymbolicate -o profile.json.gz \
                -p "$(pgrep -x echo)" >samply.log 2>&1 </dev/null &'
        done
        # Which node's profile is the leader's.
        on "$CLIENT" "TURBOFAN_ADMIN_TOKEN=$ADMIN_TOKEN src/target/release/examples/echo status \
            --admin ${PRIVATE[0]}:7201 $status_tls" >"$OUT/$name-status.txt"
    fi
    log "run $name"
    on "$CLIENT" "$(bench "BENCH_SECS=$BENCH_SECS BENCH_CLUSTER=$CLIENTS BENCH_FSYNC=$fsync $bench_tls $JEMALLOC" \
        "$HOME_DIR/bench.json.gz")" | tee "$OUT/$name.md"
    for i in 1 2 3; do
        # By name: `-f 'echo serve'` matches this command's own shell too, and kills it.
        on "${PUBLIC[i-1]}" "pkill -TERM -x echo || true
            while pgrep -x echo >/dev/null; do sleep 0.2; done
            rm -rf $data/n$i"
        if [[ $PROFILE == 1 ]]; then
            # samply stops once what it samples does; told to, should it not have.
            on "${PUBLIC[i-1]}" 'pkill -INT -x samply || true
                while pgrep -x samply >/dev/null; do sleep 0.2; done'
            fetch_profile "${PUBLIC[i-1]}" profile "$OUT/$name-node$i"
        fi
    done
    if [[ $PROFILE == 1 ]]; then
        fetch_profile "$CLIENT" bench "$OUT/$name-bench"
    fi
}

for run in $RUNS; do
    case $run in
        local)
            log "run local"
            on "$CLIENT" "mkdir -p /data/tmp && $(bench "TMPDIR=/data/tmp BENCH_SECS=$BENCH_SECS $JEMALLOC" \
                "$HOME_DIR/bench.json.gz")" | tee "$OUT/local.md"
            if [[ $PROFILE == 1 ]]; then
                fetch_profile "$CLIENT" bench "$OUT/local-bench"
            fi
            ;;
        nvme) remote nvme /data --no-fsync 0 0 ;;
        nvme-fsync) remote nvme-fsync /data '' 1 0 ;;
        nvme-tls) remote nvme-tls /data "$TLS_FLAGS --no-fsync" 0 1 ;;
        nvme-tls-fsync) remote nvme-tls-fsync /data "$TLS_FLAGS" 1 1 ;;
        ebs-fsync) remote ebs-fsync "$HOME_DIR/ebs" '' 1 0 ;;
        *) echo "no run $run" >&2; exit 2 ;;
    esac
done

# What it ran on, for the guide.
{
    echo "# $RUN"
    echo
    echo "turbofan $(git rev-parse --short HEAD), $TYPE in $AZ ($AWS_REGION), $BENCH_SECS s a run, on jemalloc."
    if [[ $PROFILE == 1 ]]; then
        echo
        echo "Profiled (PROFILE=1), with frame pointers: a few per cent slower, not figures for the guide."
    fi
    on "$CLIENT" 'echo; lscpu | grep "Model name"; uname -sr; . ~/.cargo/env && rustc --version
        lsblk -dno NAME,MODEL,SIZE | grep -v loop'
    for run in $RUNS; do
        echo
        echo "## $run"
        echo
        cat "$OUT/$run.md"
    done
} >"$OUT/results.md"
log "results in $OUT/results.md"

//! A service's command line, so its binary is one call to [`run`]:
//!
//! ```text
//! <binary> serve --id 1 --data ./n1 \
//!     --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
//!     --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
//! <binary> snapshot --admin 127.0.0.1:7201
//! ```
//!
//! To add a fourth node, start it with `--join` and its own addresses among the lists (the
//! others' are where it finds the cluster), then ask the cluster to add it:
//!
//! ```text
//! <binary> serve --id 4 --data ./n4 --join --peers 1=...,4=127.0.0.1:7004 --clients ... --admin ...
//! <binary> add-member --admin 127.0.0.1:7201 --member 4=127.0.0.1:7004,127.0.0.1:7104,127.0.0.1:7204
//! <binary> remove-member --admin 127.0.0.1:7201 --id 4
//! ```
//!
//! `add-member --learner` adds it as a learner: it takes the log but doesn't vote, so the
//! cluster's commits don't wait on it, as for a standby in another region. Nor does anything
//! else wait for it to catch up: `--wait <entries>` returns once it's within that many of the
//! leader's log, and `status --admin <addr>` shows how far behind each member is. `set-role --id <n>
//! --voter` makes a caught-up learner a voter in place, and `--learner` the reverse, without
//! shipping it the log again. A cluster can start
//! with learners too: `serve --learners 3,4`, the same on every node it starts with.
//!
//! Before stopping the leader, `transfer-leadership --admin <addr> [--to <n>]` hands over to a
//! caught-up member, so a rolling restart costs a quick election rather than an election
//! timeout. `shutdown --admin <addr>` snapshots every member at the same index and stops them, so
//! a restart loads the snapshot rather than replaying the log.
//!
//! `TURBOFAN_ADMIN_TOKEN`, if set, is the admin token: `serve` refuses operators without it, and
//! the other commands send it. `TURBOFAN_PEER_TOKEN`, if set, is the peer token: `serve` sends it
//! to peers, and refuses peers without it. They're read from the environment, not flags, to keep
//! them out of the process list.
//!
//! With the cluster lost, `promote --admin <addr>` makes the learner at that admin address the
//! one member of a new cluster, of a new epoch, from its log as it is: fence the old cluster first.
//! Its nodes can't talk to the new one, which grows with `add-member`.
//!
//! A binary built on [`run`] uses [`NodeConfig::new`](crate::NodeConfig::new)'s defaults, and one on
//! [`run_with`] its own; then `serve --election-timeout <ms> --heartbeat <ms>` sets the timing,
//! which depends on the network between the voters.
//!
//! A snapshot is a backup: `snapshot --admin <addr>` takes one, which the node writes to
//! `<data>/snapshots/`, whole and checked, and the newest there is a file to copy off the node.
//! `serve --keep-snapshots <n>` keeps the newest `n` (1 by default). With the cluster lost,
//! `restore --from <file>`, with `serve`'s flags, makes a data directory of it for this node,
//! then `serve` runs it as the one member of a new cluster, of a new epoch, which grows with
//! `add-member`. What was applied after the snapshot is lost.
//!
//! The log isn't synced to disk: the other nodes' copies are the redundancy, and a crashed
//! process loses nothing, but a majority losing power together may lose committed entries.
//! `serve --fsync` syncs it once a batch, before acknowledging it.
//!
//! TLS is optional. `serve --tls-cert <pem> --tls-key <pem> --tls-peer-ca <pem> --tls-admin-ca
//! <pem>` puts it on all three listeners and to peers (see [`crate::tls`]); the operators'
//! commands then take `--tls-ca <pem> --tls-cert <pem> --tls-key <pem>`, a certificate the admin
//! CA signed. Only in a binary built with the `tls` feature; without it, the flags are refused.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use crate::driver::{self, serve};

use crate::node::{self, Service};
use crate::raft::{self, Member, NodeId};
use crate::remote::{Admin, AdminError, ClusterStatus};
use crate::tls::{ClientTls, NodeTls};

/// A new member may take a while to catch up.
const MEMBERS_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The admin token's environment variable.
const ADMIN_TOKEN: &str = "TURBOFAN_ADMIN_TOKEN";
/// The peer token's environment variable.
const PEER_TOKEN: &str = "TURBOFAN_PEER_TOKEN";
/// The operators' commands.
const COMMANDS: [&str; 9] = [
    "snapshot",
    "add-member",
    "remove-member",
    "set-role",
    "activate",
    "shutdown",
    "transfer-leadership",
    "promote",
    "status",
];
/// How often `add-member --wait` asks how far behind the new member is.
const WAIT_POLL: Duration = Duration::from_secs(2);

const USAGE: &str = "usage:
  serve --id <n> --data <dir> --peers <n=addr,...> --clients <n=addr,...> --admin <n=addr,...>
        [--join | --learners <n,...>] [--keep-snapshots <n>] [--fsync]
        [--election-timeout <ms>] [--heartbeat <ms>]
        [--tls-cert <pem> --tls-key <pem> --tls-peer-ca <pem> --tls-admin-ca <pem>]
  restore --from <snapshot file>, then serve's flags: a new cluster of one, this node
  snapshot --admin <addr>
  add-member --admin <addr> --member <n>=<peer addr>,<client addr>,<admin addr> [--learner]
        [--wait <entries>]
  status --admin <addr>
  remove-member --admin <addr> --id <n>
  set-role --admin <addr> --id <n> (--voter | --learner)
  transfer-leadership --admin <addr> [--to <n>]
  shutdown --admin <addr>
  promote --admin <learner's admin addr>
  activate --admin <addr> [--protocol <n>] [--service <n>]
an operator's command, over TLS: [--tls-ca <pem> --tls-cert <pem> --tls-key <pem>]";

/// Runs the command line for a node of `service`, which serves cluster `service_id`, with
/// [`NodeConfig::new`](crate::NodeConfig::new)'s defaults: [`run_with`] to change them.
#[must_use]
pub fn run<S: Service + 'static>(service_id: u32, service: S) -> ExitCode {
    run_with(service_id, service, |_| {})
}

/// As [`run`], with `configure` changing a node's configuration before it serves: the settings
/// that are the service's to choose, such as [`NodeConfig::session_timeout`] or
/// [`NodeConfig::egress_ring_entries`]. `serve`'s own flags (`--election-timeout`, `--heartbeat`,
/// `--keep-snapshots`, `--fsync`) apply after it, for what an operator chooses for a deployment.
///
/// [`NodeConfig::session_timeout`]: crate::NodeConfig::session_timeout
/// [`NodeConfig::egress_ring_entries`]: crate::NodeConfig::egress_ring_entries
#[must_use]
pub fn run_with<S: Service + 'static>(
    service_id: u32,
    service: S,
    configure: impl FnOnce(&mut driver::Config),
) -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve_config(&args[1..], service_id).and_then(|mut config| {
            configure(&mut config);
            tune(&args[1..], &mut config)?;
            (config.admin_token, config.peer_token) = (token(ADMIN_TOKEN)?, token(PEER_TOKEN)?);
            if config.admin_token.is_none() && config.tls.is_none() {
                eprintln!("warning: {ADMIN_TOKEN} isn't set, so anyone who can reach the admin listener can run it");
            }
            if config.peer_token.is_none() && config.tls.is_none() {
                eprintln!(
                    "warning: {PEER_TOKEN} isn't set, so anyone who can reach the peer listener can act as a node"
                );
            }
            runtime()?.block_on(serve(config, service)).map_err(|e| format!("node stopped: {e}"))
        }),
        Some("restore") => restore(&args[1..], service_id),
        Some(command) if COMMANDS.contains(&command) => operate(command, &args[1..], service_id),
        _ => Err(USAGE.to_owned()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// `restore`: a data directory for `serve`, with the same flags, from a snapshot file.
fn restore(args: &[String], service_id: u32) -> Result<(), String> {
    let config = serve_config(args, service_id)?;
    let (index, epoch) =
        driver::restore(flag(args, "--from")?.as_ref(), &config).map_err(|e| format!("restore: {e}"))?;
    println!(
        "restored the snapshot of index {index} into {}: serve it with the same flags, as a new cluster \
         of epoch {epoch}, then add-member the others",
        config.data_dir.display()
    );
    Ok(())
}

/// An operator's `command`, one of [`COMMANDS`], through the admin listener `--admin` names.
fn operate(command: &str, args: &[String], service_id: u32) -> Result<(), String> {
    let mut admin = Admin::new(flag(args, "--admin")?, service_id).with_token(token(ADMIN_TOKEN)?.unwrap_or_default());
    if let Some([ca, cert, key]) = tls_flags(args, ["--tls-ca", "--tls-cert", "--tls-key"])? {
        let tls = ClientTls::load(ca.as_ref(), Some((cert.as_ref(), key.as_ref())));
        admin = admin.with_tls(tls.map_err(|e| format!("TLS: {e}"))?);
    }
    let failed = |e: AdminError| format!("{command}: {e}");
    match command {
        "snapshot" => {
            let index = runtime()?.block_on(admin.request_snapshot()).map_err(failed)?;
            println!("snapshot taken at index {index}");
        }
        "add-member" => {
            let member = Member { learner: args.iter().any(|a| a == "--learner"), ..member(&flag(args, "--member")?)? };
            let admin = admin.with_timeout(MEMBERS_REQUEST_TIMEOUT);
            let wait = match flag(args, "--wait") {
                Ok(n) => Some(n.parse::<u64>().map_err(|_| "--wait: a number of entries".to_owned())?),
                Err(_) => None,
            };
            let runtime = runtime()?;
            let index = runtime.block_on(admin.add_member(&member)).map_err(failed)?;
            let what = if member.learner { "learner" } else { "member" };
            println!("{what} {} added at index {index}", member.id);
            if let Some(within) = wait {
                runtime.block_on(wait_caught_up(&admin, member.id, within)).map_err(failed)?;
            }
        }
        "remove-member" => {
            let id: NodeId = flag(args, "--id")?.parse().map_err(|_| "--id: a number from 1".to_owned())?;
            let admin = admin.with_timeout(MEMBERS_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(admin.remove_member(id)).map_err(failed)?;
            println!("member {id} removed at index {index}: stop it");
        }
        "set-role" => {
            let id: NodeId = flag(args, "--id")?.parse().map_err(|_| "--id: a number from 1".to_owned())?;
            let learner = learner_flag(args)?;
            let admin = admin.with_timeout(MEMBERS_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(admin.set_role(id, learner)).map_err(failed)?;
            println!("member {id} is a {} from index {index}", if learner { "learner" } else { "voter" });
        }
        "activate" => {
            let version = |name| match flag(args, name) {
                Ok(v) => v.parse().map_err(|_| format!("{name}: a number")),
                Err(_) => Ok(0),
            };
            let (protocol, service) = (version("--protocol")?, version("--service")?);
            let index = runtime()?.block_on(admin.activate(protocol, service)).map_err(failed)?;
            println!("protocol {protocol} and service version {service} active from index {index}");
        }
        "shutdown" => {
            let index = runtime()?.block_on(admin.shutdown()).map_err(failed)?;
            println!("snapshot taken at index {index}: the members are stopping");
        }
        "promote" => {
            let index = runtime()?.block_on(admin.promote()).map_err(failed)?;
            println!(
                "promoted: the one member of a new cluster, its epoch logged at index {index}; add members with add-member"
            );
        }
        "transfer-leadership" => {
            let to = match flag(args, "--to") {
                Ok(to) => Some(to.parse().map_err(|_| "--to: a number from 1".to_owned())?),
                Err(_) => None,
            };
            let leader = runtime()?.block_on(admin.transfer_leadership(to)).map_err(failed)?;
            println!("node {leader} leads");
        }
        "status" => print_status(&runtime()?.block_on(admin.status()).map_err(failed)?),
        _ => unreachable!("one of COMMANDS"),
    }
    Ok(())
}

/// `set-role`'s `--learner` (true) or `--voter` (false): one of them.
fn learner_flag(args: &[String]) -> Result<bool, String> {
    match (args.iter().any(|a| a == "--voter"), args.iter().any(|a| a == "--learner")) {
        (true, false) => Ok(false),
        (false, true) => Ok(true),
        _ => Err(format!("set-role: --voter or --learner\n{USAGE}")),
    }
}

fn print_status(status: &ClusterStatus) {
    println!("leader {}, last index {}, commit index {}", status.leader, status.last_index, status.commit_index);
    for m in &status.members {
        let (role, lag) = (if m.learner { "learner" } else { "voter" }, status.lag(m.id).unwrap_or(0));
        println!("member {} {role}: matched {}, {lag} behind", m.id, m.matched);
    }
}

/// Waits for member `id` to be within `within` entries of the leader's log, saying how far behind
/// it is meanwhile.
async fn wait_caught_up(admin: &Admin, id: NodeId, within: u64) -> Result<(), AdminError> {
    loop {
        // Committed, it's a member: absent, it was removed since.
        let Some(lag) = admin.status().await?.lag(id) else { return Err(AdminError::MembersRefused) };
        if lag <= within {
            println!("member {id} is {lag} entries behind: caught up");
            return Ok(());
        }
        println!("member {id} is {lag} entries behind");
        tokio::time::sleep(WAIT_POLL).await;
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())
}

fn serve_config(args: &[String], service_id: u32) -> Result<driver::Config, String> {
    let id: NodeId = flag(args, "--id")?.parse().map_err(|_| "--id: a number from 1".to_owned())?;
    let peers = members(&flag(args, "--peers")?)?;
    let clients = members(&flag(args, "--clients")?)?;
    let admins = members(&flag(args, "--admin")?)?;
    let ids = |list: &[(NodeId, SocketAddr)]| list.iter().map(|(m, _)| *m).collect::<Vec<_>>();
    if ids(&clients) != ids(&peers) || ids(&admins) != ids(&peers) || !ids(&peers).contains(&id) {
        return Err("--peers, --clients and --admin must list the same members, --id among them".into());
    }
    let own = |list: &[(NodeId, SocketAddr)]| list.iter().find(|(m, _)| *m == id).map(|&(_, a)| a).expect("checked");
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).expect("small");
    let learners = learners(args, &ids(&peers))?;
    // Joining, the lists are where to find the cluster; otherwise they're its first members.
    let members = if args.iter().any(|a| a == "--join") {
        if !learners.is_empty() {
            return Err("--learners starts a cluster: a node joining is added with add-member".into());
        }
        Vec::new()
    } else {
        let addr = |list: &[(NodeId, SocketAddr)], i: usize| list[i].1.to_string();
        (0..peers.len())
            .map(|i| Member {
                id: peers[i].0,
                peer: addr(&peers, i),
                client: addr(&clients, i),
                admin: addr(&admins, i),
                learner: learners.contains(&peers[i].0),
            })
            .collect()
    };
    let mut node = node::Config::new(id, members);
    node.raft.seed ^= nanos(std::time::UNIX_EPOCH.elapsed().unwrap_or_default());
    Ok(driver::Config {
        node,
        service_id,
        data_dir: flag(args, "--data")?.into(),
        log_fsync: driver::Fsync::Never,
        client_listen: own(&clients),
        admin_listen: own(&admins),
        peer_addrs: peers,
        admin_token: None,
        peer_token: None,
        tls: tls_flags(args, ["--tls-cert", "--tls-key", "--tls-peer-ca", "--tls-admin-ca"])?.map(
            |[cert, key, peer_ca, admin_ca]| NodeTls {
                cert: cert.into(),
                key: key.into(),
                peer_ca: peer_ca.into(),
                admin_ca: admin_ca.into(),
            },
        ),
    })
}

/// `serve`'s flags for what an operator tunes, over `config`, which [`run_with`]'s closure may
/// have changed; then checks the timing makes sense, which Raft would otherwise panic at.
fn tune(args: &[String], config: &mut driver::Config) -> Result<(), String> {
    let given = |name: &str| args.iter().any(|a| a == name);
    let number = |name: &str| -> Result<u64, String> {
        flag(args, name)?.parse().ok().filter(|&n| n > 0).ok_or_else(|| format!("{name}: a number from 1"))
    };
    let node = &mut config.node;
    if given("--election-timeout") {
        node.raft.election_timeout = Duration::from_millis(number("--election-timeout")?);
    }
    if given("--heartbeat") {
        node.raft.heartbeat = Duration::from_millis(number("--heartbeat")?);
    }
    if given("--keep-snapshots") {
        node.snapshots_kept = usize::try_from(number("--keep-snapshots")?).map_err(|_| "--keep-snapshots: too many")?;
    }
    if given("--fsync") {
        config.log_fsync = driver::Fsync::EachBatch;
    }
    let (timeout, heartbeat) = (node.raft.election_timeout, node.raft.heartbeat);
    if heartbeat * 2 >= timeout || timeout >= raft::MAX_TIMEOUT {
        return Err(format!(
            "an election timeout of {timeout:?} and heartbeat of {heartbeat:?}: the heartbeat must be under half the \
             timeout, and the timeout under a year"
        ));
    }
    Ok(())
}

/// The values of the TLS flags `names`: all of them, or none for no TLS.
fn tls_flags<const N: usize>(args: &[String], names: [&str; N]) -> Result<Option<[String; N]>, String> {
    // A flag without its value is an error, not absent: else alone it would mean no TLS.
    for name in names.iter().filter(|&&name| args.iter().any(|a| a == name)) {
        flag(args, name)?;
    }
    let given = names.map(|name| flag(args, name).ok());
    if given.iter().all(Option::is_none) {
        return Ok(None);
    }
    if given.iter().any(Option::is_none) {
        return Err(format!("TLS takes all of {} or none", names.join(", ")));
    }
    Ok(Some(given.map(|value| value.expect("checked"))))
}

/// The token in the environment variable `name`, if it's set and not empty.
fn token(name: &str) -> Result<Option<Vec<u8>>, String> {
    match std::env::var(name) {
        Ok(token) if token.len() > driver::MAX_TOKEN_BYTES => {
            Err(format!("{name}: at most {} bytes", driver::MAX_TOKEN_BYTES))
        }
        Ok(token) => Ok(Some(token.into_bytes()).filter(|t| !t.is_empty())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name}: not UTF-8")),
    }
}

/// The value after `name`.
fn flag(args: &[String], name: &str) -> Result<String, String> {
    let at = args.iter().position(|a| a == name).ok_or_else(|| format!("missing {name}\n{USAGE}"))?;
    args.get(at + 1).cloned().ok_or_else(|| format!("{name} needs a value"))
}

/// `4=<peer addr>,<client addr>,<admin addr>`.
fn member(text: &str) -> Result<Member, String> {
    let bad = || format!("{text}: not <id>=<peer addr>,<client addr>,<admin addr>");
    let (id, addrs) = text.split_once('=').ok_or_else(bad)?;
    let id: NodeId = id.parse().map_err(|_| bad())?;
    let addrs: Vec<SocketAddr> = addrs.split(',').map(str::parse).collect::<Result<_, _>>().map_err(|_| bad())?;
    let [peer, client, admin] = addrs[..] else { return Err(bad()) };
    Ok(Member { id, peer: peer.to_string(), client: client.to_string(), admin: admin.to_string(), learner: false })
}

/// `--learners 3,4`, if given: some of `members`, not all.
fn learners(args: &[String], members: &[NodeId]) -> Result<Vec<NodeId>, String> {
    if !args.iter().any(|a| a == "--learners") {
        return Ok(Vec::new());
    }
    let bad = || "--learners: IDs among the members, not all of them".to_owned();
    let learners: Vec<NodeId> =
        flag(args, "--learners")?.split(',').map(str::parse).collect::<Result<_, _>>().map_err(|_| bad())?;
    if learners.iter().any(|l| !members.contains(l)) || members.iter().all(|m| learners.contains(m)) {
        return Err(bad());
    }
    Ok(learners)
}

/// `1=host:port,2=host:port,...`, sorted by ID.
fn members(list: &str) -> Result<Vec<(NodeId, SocketAddr)>, String> {
    let mut members = Vec::new();
    for member in list.split(',') {
        let (id, addr) = member.split_once('=').ok_or_else(|| format!("{member}: not <id>=<addr>"))?;
        let id: NodeId = id.parse().map_err(|_| format!("{member}: bad ID"))?;
        let addr: SocketAddr = addr.parse().map_err(|_| format!("{member}: bad address"))?;
        members.push((id, addr));
    }
    members.sort_unstable_by_key(|&(id, _)| id);
    if members.windows(2).any(|w| w[0].0 == w[1].0) || members.len() > raft::MAX_MEMBERS {
        return Err(format!("{list}: IDs must be unique, at most {}", raft::MAX_MEMBERS));
    }
    Ok(members)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{learners, serve_config, tls_flags, tune};
    use crate::driver::Fsync;
    use crate::raft::NodeId;

    /// The operator's flags override what the service set, and timing that would make Raft panic
    /// is refused.
    #[test]
    fn serve_flags_tune_over_the_service_and_are_checked() {
        let args = |extra: &[&str]| {
            let base = ["--id", "1", "--data", "d", "--peers", "1=127.0.0.1:1", "--clients", "1=127.0.0.1:2"];
            base.iter().chain(&["--admin", "1=127.0.0.1:3"]).chain(extra).map(|&s| s.to_owned()).collect::<Vec<_>>()
        };
        let tuned = |extra: &[&str]| {
            let args = args(extra);
            let mut config = serve_config(&args, 7).unwrap();
            // As a service's `run_with` might.
            config.node.raft.election_timeout = Duration::from_secs(2);
            tune(&args, &mut config).map(|()| config)
        };
        let config = tuned(&[]).unwrap();
        assert_eq!((config.node.raft.election_timeout, config.node.snapshots_kept), (Duration::from_secs(2), 1));
        assert_eq!(config.log_fsync, Fsync::Never);
        let config = tuned(&["--election-timeout", "900", "--heartbeat", "100", "--keep-snapshots", "3", "--fsync"]);
        let config = config.unwrap();
        assert_eq!(config.node.raft.election_timeout, Duration::from_millis(900));
        assert_eq!((config.node.raft.heartbeat, config.node.snapshots_kept), (Duration::from_millis(100), 3));
        assert_eq!(config.log_fsync, Fsync::EachBatch);
        assert!(tuned(&["--heartbeat", "1000"]).is_err(), "a heartbeat not under half the timeout");
        assert!(tuned(&["--election-timeout", "0"]).is_err());
        assert!(tuned(&["--keep-snapshots", "x"]).is_err());
    }

    /// All the TLS flags or none; one without its value is refused, never read as no TLS.
    #[test]
    fn tls_flags_are_all_or_none() {
        let args = |a: &[&str]| a.iter().map(|&s| s.to_owned()).collect::<Vec<_>>();
        let names = ["--a", "--b"];
        assert_eq!(tls_flags(&args(&["--x", "1"]), names), Ok(None));
        assert_eq!(tls_flags(&args(&["--a", "1", "--b", "2"]), names), Ok(Some(["1".into(), "2".into()])));
        assert!(tls_flags(&args(&["--a", "1"]), names).is_err());
        assert!(tls_flags(&args(&["--a"]), names).is_err());
    }

    /// Learners are some of the members, never all: a cluster needs a voter.
    #[test]
    fn learners_are_some_of_the_members() {
        let args = |a: &[&str]| a.iter().map(|&s| s.to_owned()).collect::<Vec<_>>();
        let members = [1, 2, 3].map(NodeId);
        assert_eq!(learners(&args(&["--x"]), &members), Ok(vec![]));
        assert_eq!(learners(&args(&["--learners", "3,2"]), &members), Ok(vec![NodeId(3), NodeId(2)]));
        assert!(learners(&args(&["--learners", "1,2,3"]), &members).is_err(), "no voter");
        assert!(learners(&args(&["--learners", "4"]), &members).is_err(), "not a member");
        assert!(learners(&args(&["--learners", "x"]), &members).is_err());
        assert!(learners(&args(&["--learners"]), &members).is_err(), "no value");
    }
}

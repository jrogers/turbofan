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
//! An address is `host:port`, the host an IP address or a name: a node looks a name up each time
//! it dials it, so a node can move, as a Kubernetes pod does, and keep its name (a StatefulSet's
//! `oms-0.oms:7001`). With a name among its own addresses, `serve --listen <ip>` says which
//! interface its listeners bind, on their ports (`--listen 0.0.0.0` for all of them); with IP
//! addresses, it binds those, unless `--listen` says otherwise.
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
//! `serve` stops on SIGTERM (as Kubernetes or systemd stops a process) or Ctrl-C, a leader first
//! handing leadership over, as `transfer-leadership` does: see [`driver::serve_until`].
//!
//! `capture --admin <addr>` has that node copy its latest snapshot and its log from there to its
//! commit index into `<data>/captures/` (see [`crate::capture`]): copy it off the node, then
//! delete it there. `replay --capture <dir> [--to <n>]` replays one into this binary's service,
//! printing every effect it makes, a line each: run it with two versions and diff them.
//!
//! The log is synced to disk once a batch, before it's acknowledged. `serve --no-fsync` doesn't:
//! the other nodes' copies are then the redundancy, and a crashed process loses nothing, but a
//! majority of the voters losing power together may lose committed entries: see the guide's
//! Running a cluster.
//!
//! TLS is optional. `serve --tls-cert <pem> --tls-key <pem> --tls-peer-ca <pem> --tls-admin-ca
//! <pem>` puts it on all three listeners and to peers (see [`crate::tls`]), and without
//! `--tls-peer-ca`, on the admin listener only; the operators' commands then take `--tls-ca <pem>
//! --tls-cert <pem> --tls-key <pem>`, a certificate the admin CA signed. Without `--tls-admin-ca`,
//! operators present none (`--tls-ca <pem>` alone), and the admin token says who they are. Only in
//! a binary built with the `tls` feature; without it, the flags are refused. `--tls-plain accept`
//! takes plain TCP beside TLS on the peer and client listeners, and `--tls-plain dial` dials
//! peers over it too, to turn TLS on or off over a running cluster, node by node
//! ([`NodeTls::plain`]).

use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use crate::driver::{self, serve_until};

use crate::node::{self, Effect, Service};
use crate::raft::{self, Member, NodeId};
use crate::remote::{Admin, AdminError, Captured, ClusterStatus};
use crate::tls::{ClientTls, NodeTls, Plain};

/// A new member may take a while to catch up.
const MEMBERS_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The admin token's environment variable.
const ADMIN_TOKEN: &str = "TURBOFAN_ADMIN_TOKEN";
/// The peer token's environment variable.
const PEER_TOKEN: &str = "TURBOFAN_PEER_TOKEN";
/// The operators' commands.
const COMMANDS: [&str; 10] = [
    "snapshot",
    "add-member",
    "remove-member",
    "set-role",
    "activate",
    "shutdown",
    "transfer-leadership",
    "promote",
    "status",
    "capture",
];
/// How often `add-member --wait` asks how far behind the new member is.
const WAIT_POLL: Duration = Duration::from_secs(2);

const USAGE: &str = "usage:
  serve --id <n> --data <dir> --peers <n=addr,...> --clients <n=addr,...> --admin <n=addr,...>
        [--listen <ip>] [--join | --learners <n,...>] [--keep-snapshots <n>] [--no-fsync]
        (an address is <host>:<port>; with a host name for this node's, --listen is the
        interface its listeners bind, on those ports: 0.0.0.0, say)
        (with --no-fsync, the log isn't synced before acknowledging: a majority of the voters
        losing power together may lose committed entries)
        [--election-timeout <ms>] [--heartbeat <ms>]
        [--tls-cert <pem> --tls-key <pem> [--tls-admin-ca <pem>] [--tls-peer-ca <pem> [--tls-peer-ids]]
         [--tls-reload <secs>] [--tls-plain accept|dial]] (the files reread every 60 seconds,
         or --tls-reload's; --tls-plain takes plain TCP beside TLS, and with dial, dials peers
         over it, to turn TLS on or off node by node)
        (without --tls-peer-ca, TLS on the admin listener only; without --tls-admin-ca,
        operators present no certificate, and the admin token says who they are)
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
  capture --admin <addr>: that node's snapshot and log, in a directory of its data directory
  replay --capture <dir> [--to <n>]: a capture into this service, printing what it does
an operator's command, over TLS: [--tls-ca <pem> [--tls-cert <pem> --tls-key <pem>]]";

/// Runs the command line for a node of `service`, which serves cluster `service_id`, with
/// [`NodeConfig::new`](crate::NodeConfig::new)'s defaults: [`run_with`] to change them.
#[must_use]
pub fn run<S: Service + 'static>(service_id: u32, service: S) -> ExitCode {
    run_with(service_id, service, |_| {})
}

/// As [`run`], with `configure` changing a node's configuration before it serves: the settings
/// that are the service's to choose, such as [`NodeConfig::session_timeout`] or
/// [`NodeConfig::egress_ring_entries`]. `serve`'s own flags (`--election-timeout`, `--heartbeat`,
/// `--keep-snapshots`, `--no-fsync`) apply after it, for what an operator chooses for a deployment.
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
            if config.admin_token.is_none() && config.tls.as_ref().is_none_or(|t| t.admin_ca.is_none()) {
                eprintln!("warning: {ADMIN_TOKEN} isn't set, so anyone who can reach the admin listener can run it");
            }
            if config.peer_token.is_none() && config.tls.as_ref().is_none_or(|t| t.peer_ca.is_none()) {
                eprintln!(
                    "warning: {PEER_TOKEN} isn't set, so anyone who can reach the peer listener can act as a node"
                );
            }
            let runtime = runtime()?;
            let stop = runtime.block_on(async { terminated() }).map_err(|e| format!("signals: {e}"))?;
            runtime.block_on(serve_until(config, service, stop)).map_err(|e| format!("node stopped: {e}"))
        }),
        Some("restore") => restore(&args[1..], service_id),
        Some("replay") => replay(&args[1..], service),
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

/// `replay`: a capture into `service`, printing each effect it makes, a line each, to compare
/// with another version's: `<index> send <session> <payload in hex>`, and `schedule`, `cancel`,
/// `close` and `propose` likewise.
fn replay<S: Service>(args: &[String], service: S) -> Result<(), String> {
    let to = parsed(args, "--to")?;
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let mut failed = Ok(());
    let mut last = None;
    let each = |index, effects: &[Effect]| {
        last = Some(index);
        for effect in effects {
            let line = match effect {
                Effect::Send(session, payload) => format!("{index} send {} {}", session.0, hex(payload)),
                Effect::Schedule { id, at } => format!("{index} schedule {id} {at}"),
                Effect::Cancel(id) => format!("{index} cancel {id}"),
                Effect::Close(session) => format!("{index} close {}", session.0),
                Effect::Propose(payload) => format!("{index} propose {}", hex(payload)),
            };
            if failed.is_ok() {
                failed = writeln!(out, "{line}");
            }
        }
    };
    crate::capture::replay(flag(args, "--capture")?.as_ref(), service, to, each).map_err(|e| format!("replay: {e}"))?;
    failed.and_then(|()| out.flush()).map_err(|e| format!("replay: {e}"))?;
    match last {
        Some(last) => eprintln!("replayed to entry {last}"),
        None => eprintln!("no entries to replay after the snapshot"),
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
    let identity = tls_flags(args, ["--tls-cert", "--tls-key"])?;
    match optional_flag(args, "--tls-ca")? {
        Some(ca) => {
            let identity = identity.as_ref().map(|[cert, key]| (cert.as_ref(), key.as_ref()));
            let tls = ClientTls::load(ca.as_ref(), identity).map_err(|e| format!("TLS: {e}"))?;
            admin = admin.with_tls(tls);
        }
        None if identity.is_some() => return Err("--tls-cert and --tls-key take --tls-ca".into()),
        None => {}
    }
    let failed = |e: AdminError| format!("{command}: {e}");
    match command {
        "snapshot" => {
            let index = runtime()?.block_on(admin.request_snapshot()).map_err(failed)?;
            println!("snapshot taken at index {index}");
        }
        "add-member" => {
            let member = member_to_add(args)?;
            let admin = admin.with_timeout(MEMBERS_REQUEST_TIMEOUT);
            let wait: Option<u64> = parsed(args, "--wait")?;
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
            let (protocol, service) = (parsed(args, "--protocol")?, parsed(args, "--service")?);
            let (protocol, service, index) =
                runtime()?.block_on(activate(&admin, protocol, service)).map_err(failed)?;
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
            let leader = runtime()?.block_on(admin.transfer_leadership(parsed(args, "--to")?)).map_err(failed)?;
            println!("node {leader} leads");
        }
        "status" => print_status(&runtime()?.block_on(admin.status()).map_err(failed)?),
        "capture" => {
            let Captured { snapshot_index, to, dir, .. } = runtime()?.block_on(admin.capture()).map_err(failed)?;
            println!("captured the snapshot of {snapshot_index} and the log to {to} in {dir}: copy it off the node");
        }
        _ => unreachable!("one of COMMANDS"),
    }
    Ok(())
}

/// `activate`: a version left out is the one active, so that naming the other doesn't step it
/// back. Returns the versions, and the index they're active from.
async fn activate(admin: &Admin, protocol: Option<u32>, service: Option<u32>) -> Result<(u32, u32, u64), AdminError> {
    let (protocol, service) = match (protocol, service) {
        (Some(protocol), Some(service)) => (protocol, service),
        _ => {
            let status = admin.status().await?;
            (protocol.or(status.protocol).unwrap_or(0), service.or(status.service_version).unwrap_or(0))
        }
    };
    Ok((protocol, service, admin.activate(protocol, service).await?))
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
    if let (Some(protocol), Some(service)) = (status.protocol, status.service_version) {
        println!("protocol {protocol}, service version {service}");
    }
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

/// Completes on SIGTERM, as Kubernetes or systemd stops a process, or Ctrl-C. Listening from the
/// start, so neither kills the process outright later.
fn terminated() -> std::io::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("stopping: handing leadership over if this node leads");
    })
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())
}

fn serve_config(args: &[String], service_id: u32) -> Result<driver::Config, String> {
    let id: NodeId = flag(args, "--id")?.parse().map_err(|_| "--id: a number from 1".to_owned())?;
    let peers = members(&flag(args, "--peers")?)?;
    let clients = members(&flag(args, "--clients")?)?;
    let admins = members(&flag(args, "--admin")?)?;
    let ids = |list: &[(NodeId, String)]| list.iter().map(|(m, _)| *m).collect::<Vec<_>>();
    if ids(&clients) != ids(&peers) || ids(&admins) != ids(&peers) || !ids(&peers).contains(&id) {
        return Err("--peers, --clients and --admin must list the same members, --id among them".into());
    }
    let interface = optional_flag(args, "--listen")?;
    let interface: Option<IpAddr> =
        interface.map(|ip| ip.parse().map_err(|_| format!("--listen {ip}: an IP address"))).transpose()?;
    let own = |list: &[(NodeId, String)]| -> Result<SocketAddr, String> {
        let addr = &list.iter().find(|(m, _)| *m == id).expect("checked").1;
        match interface {
            Some(ip) => Ok(SocketAddr::new(ip, port(addr).expect("checked"))),
            None => addr.parse().map_err(|_| format!("{addr}: a host name, so --listen <ip> to listen on")),
        }
    };
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).expect("small");
    let learners = learners(args, &ids(&peers))?;
    // Joining, the lists are where to find the cluster; otherwise they're its first members.
    let members = if args.iter().any(|a| a == "--join") {
        if !learners.is_empty() {
            return Err("--learners starts a cluster: a node joining is added with add-member".into());
        }
        Vec::new()
    } else {
        let addr = |list: &[(NodeId, String)], i: usize| list[i].1.clone();
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
        log_fsync: driver::Fsync::default(),
        client_listen: own(&clients)?,
        admin_listen: own(&admins)?,
        peer_listen: Some(own(&peers)?),
        peer_addrs: peers,
        admin_token: None,
        peer_token: None,
        tls: node_tls(args)?,
    })
}

/// `serve`'s TLS: none, or a certificate and key, with an admin CA for operators' certificates and
/// a peer CA for TLS beyond the admin listener.
fn node_tls(args: &[String]) -> Result<Option<NodeTls>, String> {
    let (peer_ca, admin_ca) = (optional_flag(args, "--tls-peer-ca")?, optional_flag(args, "--tls-admin-ca")?);
    let peer_ids = args.iter().any(|a| a == "--tls-peer-ids");
    let plain = optional_flag(args, "--tls-plain")?;
    let plain_parsed = match plain.as_deref() {
        None => Plain::Refused,
        Some("accept") => Plain::Accepted,
        Some("dial") => Plain::Dialled,
        Some(_) => return Err("--tls-plain: accept or dial".into()),
    };
    let reload = optional_flag(args, "--tls-reload")?;
    let reload_every = match &reload {
        Some(secs) => secs.parse().ok().filter(|&n| n > 0).map(Duration::from_secs),
        None => Some(crate::tls::RELOAD_EVERY),
    };
    let reload_every = reload_every.ok_or("--tls-reload: a number of seconds from 1")?;
    match tls_flags(args, ["--tls-cert", "--tls-key"])? {
        Some(_) if peer_ids && peer_ca.is_none() => Err("--tls-peer-ids takes --tls-peer-ca".into()),
        Some(_) if plain.is_some() && (peer_ca.is_none() || peer_ids) => {
            Err("--tls-plain takes --tls-peer-ca, and not --tls-peer-ids".into())
        }
        Some([cert, key]) => Ok(Some(NodeTls {
            cert: cert.into(),
            key: key.into(),
            peer_ca: peer_ca.map(Into::into),
            admin_ca: admin_ca.map(Into::into),
            peer_ids,
            reload_every,
            plain: plain_parsed,
        })),
        None if peer_ca.is_some() || admin_ca.is_some() || peer_ids || plain.is_some() || reload.is_some() => Err(
            "--tls-peer-ca, --tls-admin-ca, --tls-peer-ids, --tls-plain and --tls-reload take --tls-cert and --tls-key"
                .into(),
        ),
        None => Ok(None),
    }
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
    // `--fsync`, the default since 0.12, is still taken, for command lines written before it.
    match (given("--fsync"), given("--no-fsync")) {
        (true, true) => return Err("--fsync or --no-fsync, not both".into()),
        (true, false) => config.log_fsync = driver::Fsync::EachBatch,
        (false, true) => config.log_fsync = driver::Fsync::Never,
        (false, false) => {}
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
    let mut given = names.map(|_| None);
    for (value, name) in given.iter_mut().zip(names) {
        *value = optional_flag(args, name)?;
    }
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
    optional_flag(args, name)?.ok_or_else(|| format!("missing {name}\n{USAGE}"))
}

/// The value after `name`, if it's given. Given without one is an error, never taken for
/// absent: a trailing `--wait` would otherwise not wait.
/// A flag's value, if it's given, as a number.
fn parsed<T: std::str::FromStr>(args: &[String], name: &str) -> Result<Option<T>, String> {
    optional_flag(args, name)?.map(|v| v.parse().map_err(|_| format!("{name}: a number"))).transpose()
}

fn optional_flag(args: &[String], name: &str) -> Result<Option<String>, String> {
    let Some(at) = args.iter().position(|a| a == name) else { return Ok(None) };
    args.get(at + 1).cloned().map(Some).ok_or_else(|| format!("{name} needs a value"))
}

/// `4=<peer addr>,<client addr>,<admin addr>`.
fn member(text: &str) -> Result<Member, String> {
    let bad = || format!("{text}: not <id>=<peer addr>,<client addr>,<admin addr>");
    let (id, addrs) = text.split_once('=').ok_or_else(bad)?;
    let id: NodeId = id.parse().map_err(|_| bad())?;
    let addrs: Vec<String> = addrs.split(',').map(address).collect::<Result<_, _>>()?;
    let [peer, client, admin] = <[String; 3]>::try_from(addrs).map_err(|_| bad())?;
    Ok(Member { id, peer, client, admin, learner: false })
}

/// `add-member`'s member: `--member`, a learner with `--learner`.
fn member_to_add(args: &[String]) -> Result<Member, String> {
    Ok(Member { learner: args.iter().any(|a| a == "--learner"), ..member(&flag(args, "--member")?)? })
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

/// `host:port`, the host an IP address or a name, as it goes on the wire.
fn address(addr: &str) -> Result<String, String> {
    let host = addr.rsplit_once(':').map_or("", |(host, _)| host);
    if host.is_empty() || port(addr).is_none() || addr.len() > usize::from(u8::MAX) {
        return Err(format!("{addr}: not <host>:<port>, at most {} bytes", u8::MAX));
    }
    Ok(addr.to_owned())
}

/// `host:port`'s port.
fn port(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')?.1.parse().ok()
}

/// `1=host:port,2=host:port,...`, sorted by ID.
fn members(list: &str) -> Result<Vec<(NodeId, String)>, String> {
    let mut members = Vec::new();
    for member in list.split(',') {
        let (id, addr) = member.split_once('=').ok_or_else(|| format!("{member}: not <id>=<addr>"))?;
        let id: NodeId = id.parse().map_err(|_| format!("{member}: bad ID"))?;
        members.push((id, address(addr)?));
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

    use super::{Plain, learners, member_to_add, node_tls, optional_flag, serve_config, tls_flags, tune};
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
        assert_eq!(config.log_fsync, Fsync::EachBatch);
        let config = tuned(&["--election-timeout", "900", "--heartbeat", "100", "--keep-snapshots", "3", "--no-fsync"]);
        let config = config.unwrap();
        assert_eq!(config.node.raft.election_timeout, Duration::from_millis(900));
        assert_eq!((config.node.raft.heartbeat, config.node.snapshots_kept), (Duration::from_millis(100), 3));
        assert_eq!(config.log_fsync, Fsync::Never);
        assert_eq!(tuned(&["--fsync"]).unwrap().log_fsync, Fsync::EachBatch, "as before it was the default");
        assert!(tuned(&["--fsync", "--no-fsync"]).is_err());
        assert!(tuned(&["--heartbeat", "1000"]).is_err(), "a heartbeat not under half the timeout");
        assert!(tuned(&["--election-timeout", "0"]).is_err());
        assert!(tuned(&["--keep-snapshots", "x"]).is_err());
    }

    /// Addresses may be host names, dialled as given; with one for this node, `--listen` says
    /// where to bind, and without it the node is refused rather than bound somewhere it wasn't told.
    #[test]
    fn host_names_are_dialled_and_listen_binds() {
        let args = |peer: &str, extra: &[&str]| {
            let peers = format!("1={peer}:7001,2=node-2.svc:7001");
            let base =
                ["--id", "1", "--data", "d", "--peers", &peers, "--clients", "1=node-1.svc:7101,2=node-2.svc:7101"];
            let admin = ["--admin", "1=10.0.0.1:7201,2=node-2.svc:7201"];
            base.iter().chain(&admin).chain(extra).map(|&s| s.to_owned()).collect::<Vec<_>>()
        };
        let config = serve_config(&args("node-1.svc", &["--listen", "0.0.0.0"]), 7).unwrap();
        assert_eq!(config.peer_addrs[1], (NodeId(2), "node-2.svc:7001".to_owned()));
        assert_eq!(config.node.raft.members[1].client, "node-2.svc:7101");
        let listening = [config.peer_listen.unwrap(), config.client_listen, config.admin_listen].map(|a| a.to_string());
        assert_eq!(listening, ["0.0.0.0:7001", "0.0.0.0:7101", "0.0.0.0:7201"]);
        assert!(serve_config(&args("node-1.svc", &[]), 7).unwrap_err().contains("--listen"));
        assert!(serve_config(&args("node-1.svc", &["--listen", "node-1.svc"]), 7).is_err(), "--listen is an IP");
        assert!(serve_config(&args("", &["--listen", "0.0.0.0"]), 7).is_err(), "no host");
        assert!(serve_config(&args(&"a".repeat(256), &["--listen", "0.0.0.0"]), 7).is_err(), "too long for the wire");
        let added = ["--member", "4=node-4.svc:7001,node-4.svc:7101,node-4.svc:7201"].map(str::to_owned);
        assert_eq!(member_to_add(&added).unwrap().admin, "node-4.svc:7201");
    }

    /// `add-member` adds a voter, or with `--learner`, a learner.
    #[test]
    fn add_member_adds_a_learner_with_learner() {
        let args = |extra: &[&str]| {
            let base = ["--admin", "127.0.0.1:3", "--member", "4=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3"];
            base.iter().chain(extra).map(|&s| s.to_owned()).collect::<Vec<_>>()
        };
        let voter = member_to_add(&args(&[])).unwrap();
        assert_eq!((voter.id, voter.peer.as_str(), voter.learner), (NodeId(4), "127.0.0.1:1", false));
        assert!(member_to_add(&args(&["--learner"])).unwrap().learner);
        assert!(member_to_add(&args(&[])[..2]).is_err(), "no --member");
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

    /// Without `--tls-peer-ca`, TLS on the admin listener only; with it alone, refused.
    #[test]
    fn tls_without_a_peer_ca_is_the_admin_listeners_alone() {
        let args = |a: &[&str]| a.iter().map(|&s| s.to_owned()).collect::<Vec<_>>();
        let tls = ["--tls-cert", "c", "--tls-key", "k", "--tls-admin-ca", "a"];
        let admin_only = node_tls(&args(&tls)).unwrap().unwrap();
        assert_eq!((admin_only.peer_ca, admin_only.admin_ca), (None, Some("a".into())));
        let no_admin_ca = node_tls(&args(&tls[..4])).unwrap().unwrap();
        assert_eq!((no_admin_ca.peer_ca, no_admin_ca.admin_ca), (None, None));
        assert!(node_tls(&args(&["--tls-admin-ca", "a"])).is_err());
        let ids = node_tls(&args(&[&tls[..], &["--tls-peer-ca", "p", "--tls-peer-ids"]].concat())).unwrap().unwrap();
        assert!(ids.peer_ids);
        assert!(node_tls(&args(&[&tls[..], &["--tls-peer-ids"]].concat())).is_err(), "without a peer CA");
        assert_eq!(ids.reload_every, Duration::from_secs(60));
        let reload = node_tls(&args(&[&tls[..], &["--tls-reload", "5"]].concat())).unwrap().unwrap();
        assert_eq!(reload.reload_every, Duration::from_secs(5));
        assert!(node_tls(&args(&[&tls[..], &["--tls-reload", "0"]].concat())).is_err());
        assert!(node_tls(&args(&["--tls-reload", "5"])).is_err(), "without a certificate");
        let all = node_tls(&args(&[&tls[..], &["--tls-peer-ca", "p"]].concat())).unwrap().unwrap();
        assert_eq!((all.peer_ca, all.peer_ids), (Some("p".into()), false));
        assert!(node_tls(&args(&["--tls-peer-ca", "p"])).is_err());
        assert!(node_tls(&args(&[])).unwrap().is_none());
    }

    /// `--tls-plain accept` takes plain TCP beside TLS, and `dial` dials peers over it too; only
    /// with a peer CA and a certificate, not with peer IDs, and nothing else.
    #[test]
    fn tls_plain_is_accept_or_dial() {
        let args = |a: &[&str]| a.iter().map(|&s| s.to_owned()).collect::<Vec<_>>();
        let tls = ["--tls-cert", "c", "--tls-key", "k", "--tls-peer-ca", "p"];
        let with = |more: &[&str]| node_tls(&args(&[&tls[..], more].concat()));
        assert_eq!(with(&[]).unwrap().unwrap().plain, Plain::Refused);
        assert_eq!(with(&["--tls-plain", "accept"]).unwrap().unwrap().plain, Plain::Accepted);
        assert_eq!(with(&["--tls-plain", "dial"]).unwrap().unwrap().plain, Plain::Dialled);
        assert!(with(&["--tls-plain", "yes"]).is_err());
        assert!(with(&["--tls-plain", "accept", "--tls-peer-ids"]).is_err(), "with peer IDs");
        assert!(node_tls(&args(&[&tls[..4], &["--tls-plain", "accept"]].concat())).is_err(), "without a peer CA");
        assert!(node_tls(&args(&["--tls-plain", "accept"])).is_err(), "without a certificate");
    }

    /// A flag given without its value is an error, not taken for absent.
    #[test]
    fn a_flag_without_its_value_is_refused() {
        let args = |a: &[&str]| a.iter().map(|&s| s.to_owned()).collect::<Vec<_>>();
        assert_eq!(optional_flag(&args(&["--x", "1"]), "--wait"), Ok(None));
        assert_eq!(optional_flag(&args(&["--wait", "5"]), "--wait"), Ok(Some("5".into())));
        assert!(optional_flag(&args(&["--wait"]), "--wait").is_err());
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

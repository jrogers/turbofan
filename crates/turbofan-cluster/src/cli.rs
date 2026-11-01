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
use crate::remote::{activate, add_member, remove_member, request_snapshot, shutdown, transfer_leadership};
use crate::tls::{ClientTls, NodeTls};

const ELECTION_TIMEOUT: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(50);
/// Applied log records kept in memory for followers that fall behind.
const LOG_WINDOW_BYTES: usize = 64 << 20;
const SESSION_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_EVERY_ENTRIES: u64 = 100_000;
const EGRESS_RING_ENTRIES: usize = 8192;
const SNAPSHOT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A new member may take a while to catch up.
const MEMBERS_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The admin token's environment variable.
const ADMIN_TOKEN: &str = "TURBOFAN_ADMIN_TOKEN";
/// The peer token's environment variable.
const PEER_TOKEN: &str = "TURBOFAN_PEER_TOKEN";
/// The operators' commands.
const COMMANDS: [&str; 6] = ["snapshot", "add-member", "remove-member", "activate", "shutdown", "transfer-leadership"];

const USAGE: &str = "usage:
  serve --id <n> --data <dir> --peers <n=addr,...> --clients <n=addr,...> --admin <n=addr,...> [--join]
        [--tls-cert <pem> --tls-key <pem> --tls-peer-ca <pem> --tls-admin-ca <pem>]
  snapshot --admin <addr>
  add-member --admin <addr> --member <n>=<peer addr>,<client addr>,<admin addr>
  remove-member --admin <addr> --id <n>
  transfer-leadership --admin <addr> [--to <n>]
  shutdown --admin <addr>
  activate --admin <addr> [--protocol <n>] [--service <n>]
an operator's command, over TLS: [--tls-ca <pem> --tls-cert <pem> --tls-key <pem>]";

/// Runs the command line for a node of `service`, which serves cluster `service_id`.
#[must_use]
pub fn run<S: Service + Send + 'static>(service_id: u32, service: S) -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve_config(&args[1..], service_id).and_then(|mut config| {
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

/// An operator's `command`, one of [`COMMANDS`], through the admin listener `--admin` names.
fn operate(command: &str, args: &[String], service_id: u32) -> Result<(), String> {
    let admin = flag(args, "--admin")?;
    let token = &token(ADMIN_TOKEN)?.unwrap_or_default();
    let tls = match tls_flags(args, ["--tls-ca", "--tls-cert", "--tls-key"])? {
        Some([ca, cert, key]) => {
            let tls = ClientTls::load(ca.as_ref(), Some((cert.as_ref(), key.as_ref())));
            Some(tls.map_err(|e| format!("TLS: {e}"))?)
        }
        None => None,
    };
    let tls = tls.as_ref();
    let failed = |e: std::io::Error| format!("{command}: {e}");
    match command {
        "snapshot" => {
            let request = request_snapshot(&admin, service_id, token, tls, SNAPSHOT_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(request).map_err(failed)?;
            println!("snapshot taken at index {index}");
        }
        "add-member" => {
            let member = member(&flag(args, "--member")?)?;
            let request = add_member(&admin, service_id, token, tls, &member, MEMBERS_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(request).map_err(failed)?;
            println!("member {} added at index {index}", member.id);
        }
        "remove-member" => {
            let id: NodeId = flag(args, "--id")?.parse().map_err(|_| "--id: a number from 1".to_owned())?;
            let request = remove_member(&admin, service_id, token, tls, id, MEMBERS_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(request).map_err(failed)?;
            println!("member {id} removed at index {index}: stop it");
        }
        "activate" => {
            let version = |name| match flag(args, name) {
                Ok(v) => v.parse().map_err(|_| format!("{name}: a number")),
                Err(_) => Ok(0),
            };
            let (protocol, service) = (version("--protocol")?, version("--service")?);
            let request = activate(&admin, service_id, token, tls, protocol, service, SNAPSHOT_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(request).map_err(failed)?;
            println!("protocol {protocol} and service version {service} active from index {index}");
        }
        "shutdown" => {
            let request = shutdown(&admin, service_id, token, tls, SNAPSHOT_REQUEST_TIMEOUT);
            let index = runtime()?.block_on(request).map_err(failed)?;
            println!("snapshot taken at index {index}: the members are stopping");
        }
        "transfer-leadership" => {
            let to = match flag(args, "--to") {
                Ok(to) => to.parse().map_err(|_| "--to: a number from 1".to_owned())?,
                Err(_) => 0,
            };
            let request = transfer_leadership(&admin, service_id, token, tls, to, SNAPSHOT_REQUEST_TIMEOUT);
            let leader = runtime()?.block_on(request).map_err(failed)?;
            println!("node {leader} leads");
        }
        _ => unreachable!("one of COMMANDS"),
    }
    Ok(())
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
    // Joining, the lists are where to find the cluster; otherwise they're its first members.
    let members = if args.iter().any(|a| a == "--join") {
        Vec::new()
    } else {
        let addr = |list: &[(NodeId, SocketAddr)], i: usize| list[i].1.to_string();
        (0..peers.len())
            .map(|i| Member {
                id: peers[i].0,
                peer: addr(&peers, i),
                client: addr(&clients, i),
                admin: addr(&admins, i),
            })
            .collect()
    };
    Ok(driver::Config {
        node: node::Config {
            raft: raft::Config {
                id,
                members,
                election_timeout_ns: nanos(ELECTION_TIMEOUT),
                heartbeat_ns: nanos(HEARTBEAT),
                seed: u64::from(id) ^ nanos(std::time::UNIX_EPOCH.elapsed().unwrap_or_default()),
                log_window_bytes: LOG_WINDOW_BYTES,
            },
            session_timeout_ns: nanos(SESSION_TIMEOUT),
            snapshot_every_entries: SNAPSHOT_EVERY_ENTRIES,
            egress_ring_entries: EGRESS_RING_ENTRIES,
            protocol: node::PROTOCOL,
        },
        service_id,
        data_dir: flag(args, "--data")?.into(),
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
    Ok(Member { id, peer: peer.to_string(), client: client.to_string(), admin: admin.to_string() })
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
    use super::tls_flags;

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
}

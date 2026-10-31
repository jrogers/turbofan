//! A service's command line, so its binary is one call to [`run`]:
//!
//! ```text
//! <binary> serve --id 1 --data ./n1 \
//!     --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
//!     --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
//! <binary> snapshot --admin 127.0.0.1:7201
//! ```

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use crate::driver::{self, serve};
use crate::node::{self, Service};
use crate::raft::{self, NodeId};
use crate::remote::request_snapshot;

const ELECTION_TIMEOUT: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(50);
const SESSION_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_EVERY_ENTRIES: u64 = 100_000;
const EGRESS_RING_ENTRIES: usize = 8192;
const SNAPSHOT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

const USAGE: &str = "usage:
  serve --id <n> --data <dir> --peers <n=addr,...> --clients <n=addr,...> --admin <n=addr,...>
  snapshot --admin <addr>";

/// Runs the command line for a node of `service`, which serves cluster `service_id`.
#[must_use]
pub fn run<S: Service + Send + 'static>(service_id: u32, service: S) -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve_config(&args[1..], service_id)
            .and_then(|config| runtime()?.block_on(serve(config, service)).map_err(|e| format!("node stopped: {e}"))),
        Some("snapshot") => flag(&args[1..], "--admin").and_then(|admin| {
            let index = runtime()?
                .block_on(request_snapshot(&admin, service_id, SNAPSHOT_REQUEST_TIMEOUT))
                .map_err(|e| format!("snapshot: {e}"))?;
            println!("snapshot taken at index {index}");
            Ok(())
        }),
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
    let strings = |list: &[(NodeId, SocketAddr)]| list.iter().map(|&(m, a)| (m, a.to_string())).collect();
    Ok(driver::Config {
        node: node::Config {
            raft: raft::Config {
                id,
                members: ids(&peers),
                election_timeout_ns: nanos(ELECTION_TIMEOUT),
                heartbeat_ns: nanos(HEARTBEAT),
                seed: u64::from(id) ^ nanos(std::time::UNIX_EPOCH.elapsed().unwrap_or_default()),
            },
            client_addrs: strings(&clients),
            admin_addrs: strings(&admins),
            session_timeout_ns: nanos(SESSION_TIMEOUT),
            snapshot_every_entries: SNAPSHOT_EVERY_ENTRIES,
            egress_ring_entries: EGRESS_RING_ENTRIES,
        },
        service_id,
        data_dir: flag(args, "--data")?.into(),
        client_listen: own(&clients),
        admin_listen: own(&admins),
        peer_addrs: peers,
    })
}

/// The value after `name`.
fn flag(args: &[String], name: &str) -> Result<String, String> {
    let at = args.iter().position(|a| a == name).ok_or_else(|| format!("missing {name}\n{USAGE}"))?;
    args.get(at + 1).cloned().ok_or_else(|| format!("{name} needs a value"))
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

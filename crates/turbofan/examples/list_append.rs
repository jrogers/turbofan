//! Jepsen's list-append workload as a service (#14): lists of numbers by key, each transaction
//! one message, so one entry of the log, applied whole. A transaction of reads alone goes as a
//! query, never logged. Elle checks a history of them for strict serializability, which a single
//! linearizable log should give.
//!
//! Its nodes run as any service's ([`turbofan::run`]): `serve`, `--no-fsync`, TLS, `add-member`
//! and the rest. `gateway` runs a gateway beside a node, for Jepsen's clients: they speak a line
//! of text to it, and it holds a session with the cluster for each of their connections, resent
//! through failovers as any client's is.
//!
//! ```text
//! cargo build --release -p turbofan --example list_append
//! target/release/examples/list_append serve --id 1 --data ./n1 --peers ... --clients ... --admin ...
//! target/release/examples/list_append gateway --listen 0.0.0.0:7301 \
//!     --clients 10.0.0.1:7101,10.0.0.2:7101,10.0.0.3:7101 [--timeout-ms 5000]
//! ```
//!
//! A request is a line: a transaction's operations, separated by `;`, each `a <key> <value>` to
//! append or `r <key>` to read, keys and values unsigned 64-bit integers: `a 1 5;r 1`. The answer
//! is a line too:
//!
//! - `ok [[:append 1 5] [:r 1 [3 5]]]`: applied, and what it read, as Jepsen writes it (EDN).
//! - `fail <why>`: certainly not applied, and never will be.
//! - `info <why>`: maybe applied, maybe not, maybe later. The gateway closes the connection after
//!   one, as the session may still deliver its response: a new connection has a new session.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, Read};
use std::process::ExitCode;

use turbofan::codec::MAX_PAYLOAD_BYTES;
use turbofan::{Ctx, Service, SessionId, SnapshotWriter};

/// This cluster's `Hello.service_id`.
const SERVICE_ID: u32 = 10;
/// The most values a list holds: a read of a full one (at most 21 bytes a value) is well within
/// an answer. Jepsen appends a few hundred to a key at most before moving to the next.
const MAX_LIST_VALUES: usize = 1024;
/// The most keys kept. Jepsen's histories use some thousands.
const MAX_KEYS: usize = 1 << 16;
/// The most operations a transaction has. Jepsen's have a handful.
const MAX_TRANSACTION_OPS: usize = 64;

/// One operation of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Append { key: u64, value: u64 },
    Read { key: u64 },
}

impl Op {
    fn key(self) -> u64 {
        match self {
            Self::Append { key, .. } | Self::Read { key } => key,
        }
    }
}

/// A transaction, from its text: `a 1 5;r 1`. Untrusted, from any client: refused, never
/// panicked on.
fn parse(text: &[u8]) -> Result<Vec<Op>, &'static str> {
    let text = std::str::from_utf8(text).map_err(|_| "not-utf8")?;
    let mut ops = Vec::new();
    for op in text.split(';') {
        if ops.len() == MAX_TRANSACTION_OPS {
            return Err("too-many-ops");
        }
        let number = |word: Option<&str>| word.and_then(|w| w.parse::<u64>().ok()).ok_or("malformed");
        let mut words = op.split_ascii_whitespace();
        let op = match words.next() {
            Some("a") => Op::Append { key: number(words.next())?, value: number(words.next())? },
            Some("r") => Op::Read { key: number(words.next())? },
            _ => return Err("malformed"),
        };
        if words.next().is_some() {
            return Err("malformed");
        }
        ops.push(op);
    }
    Ok(ops)
}

fn read_only(ops: &[Op]) -> bool {
    ops.iter().all(|op| matches!(op, Op::Read { .. }))
}

#[derive(Debug, Default)]
struct ListAppend {
    lists: BTreeMap<u64, Vec<u64>>,
}

impl ListAppend {
    /// What `ops` read, as their answer, and the lists they leave: or why they can't be applied,
    /// with nothing changed.
    fn transact(&self, ops: &[Op]) -> Result<(String, BTreeMap<u64, Vec<u64>>), &'static str> {
        let mut touched: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        let mut answer = String::from("ok [");
        for (i, &op) in ops.iter().enumerate() {
            let key = op.key();
            let list = touched.entry(key).or_insert_with(|| self.lists.get(&key).cloned().unwrap_or_default());
            if i > 0 {
                answer.push(' ');
            }
            match op {
                Op::Append { value, .. } => {
                    if list.len() == MAX_LIST_VALUES {
                        return Err("list-full");
                    }
                    list.push(value);
                    write!(answer, "[:append {key} {value}]").expect("a String takes any write");
                }
                Op::Read { .. } => {
                    let values: Vec<String> = list.iter().map(u64::to_string).collect();
                    write!(answer, "[:r {key} [{}]]", values.join(" ")).expect("a String takes any write");
                }
            }
        }
        answer.push(']');
        let new_keys = touched.iter().filter(|(key, list)| !list.is_empty() && !self.lists.contains_key(key)).count();
        if self.lists.len() + new_keys > MAX_KEYS {
            return Err("too-many-keys");
        }
        if answer.len() > MAX_PAYLOAD_BYTES {
            return Err("answer-too-long");
        }
        Ok((answer, touched))
    }
}

impl Service for ListAppend {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let answer = match parse(payload).and_then(|ops| self.transact(&ops)) {
            Ok((answer, touched)) => {
                // A read of a key never appended to leaves it absent, as it was.
                self.lists.extend(touched.into_iter().filter(|(_, list)| !list.is_empty()));
                answer
            }
            Err(why) => format!("fail {why}"),
        };
        ctx.send(session, answer).expect("an answer is checked to be within MAX_PAYLOAD_BYTES");
    }

    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        let answer = match parse(query) {
            Ok(ops) if !read_only(&ops) => "fail not-read-only".to_owned(),
            Ok(ops) => self.transact(&ops).map_or_else(|why| format!("fail {why}"), |(answer, _)| answer),
            Err(why) => format!("fail {why}"),
        };
        Some(answer.into_bytes())
    }

    /// Each list as its key, its length and its values, little-endian `u64`s, in key order.
    fn snapshot(&self) -> SnapshotWriter {
        let lists = self.lists.clone();
        Box::new(move |out| {
            for (key, list) in &lists {
                out.write_all(&key.to_le_bytes())?;
                out.write_all(&u64::try_from(list.len()).expect("a list is at most MAX_LIST_VALUES").to_le_bytes())?;
                for value in list {
                    out.write_all(&value.to_le_bytes())?;
                }
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let invalid = |why: &str| io::Error::new(io::ErrorKind::InvalidData, format!("not a snapshot: {why}"));
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let (words, rest) = bytes.as_chunks::<8>();
        if !rest.is_empty() {
            return Err(invalid("not whole words"));
        }
        let mut words = words.iter().map(|w| u64::from_le_bytes(*w));
        let mut lists = BTreeMap::new();
        while let Some(key) = words.next() {
            let len = words.next().and_then(|n| usize::try_from(n).ok()).ok_or_else(|| invalid("no length"))?;
            if len == 0 || len > MAX_LIST_VALUES {
                return Err(invalid("a list's length"));
            }
            let list: Vec<u64> = words.by_ref().take(len).collect();
            if list.len() < len {
                return Err(invalid("a list cut short"));
            }
            if lists.len() == MAX_KEYS || lists.insert(key, list).is_some() {
                return Err(invalid("keys repeated, or too many"));
            }
        }
        self.lists = lists;
        Ok(())
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("gateway") {
        return gateway::main(&args[1..]);
    }
    turbofan::run(SERVICE_ID, ListAppend::default())
}

/// The gateway: Jepsen's clients' lines in, each connection a session of its own.
mod gateway {
    use std::process::ExitCode;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Semaphore;
    use turbofan::ClusterClient;
    use turbofan::client::{Event, QueryError};
    use turbofan::codec::MAX_PAYLOAD_BYTES;

    use super::{SERVICE_ID, parse, read_only};

    /// The most connections served at once: Jepsen runs some tens of clients.
    const MAX_CONNECTIONS: usize = 256;
    /// How long a request waits for its answer by default: Jepsen's clients wait a little longer.
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
    /// As the client guide's examples.
    const KEEPALIVE: Duration = Duration::from_millis(500);

    const USAGE: &str = "usage: list_append gateway --listen <addr> --clients <addr>,<addr>,... [--timeout-ms <ms>]";

    pub fn main(args: &[String]) -> ExitCode {
        let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1));
        let (Some(listen), Some(clients)) = (flag("--listen"), flag("--clients")) else {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        };
        let timeout = match flag("--timeout-ms").map(|ms| ms.parse()) {
            None => DEFAULT_TIMEOUT,
            Some(Ok(ms)) => Duration::from_millis(ms),
            Some(Err(_)) => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        };
        let clients: Vec<String> = clients.split(',').map(str::to_owned).collect();
        let runtime = tokio::runtime::Runtime::new().expect("a runtime starts");
        let result = runtime.block_on(async {
            let listener = TcpListener::bind(listen).await?;
            serve(listener, clients, timeout).await
        });
        match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("gateway: {e}");
                ExitCode::FAILURE
            }
        }
    }

    /// Serves `listener`'s connections, each a session with the cluster at `clients`.
    pub async fn serve(listener: TcpListener, clients: Vec<String>, timeout: Duration) -> std::io::Result<()> {
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        loop {
            let permit = permits.clone().acquire_owned().await.expect("the semaphore is never closed");
            let (stream, _) = listener.accept().await?;
            let clients = clients.clone();
            tokio::spawn(async move {
                if let Err(e) = connection(stream, &clients, timeout).await {
                    eprintln!("gateway: a connection: {e}");
                }
                drop(permit);
            });
        }
    }

    /// One client's requests, in turn, until it goes or an answer is `info`.
    async fn connection(stream: TcpStream, clients: &[String], timeout: Duration) -> std::io::Result<()> {
        stream.set_nodelay(true)?;
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut session = ClusterClient::connect(clients, SERVICE_ID, Vec::new(), KEEPALIVE)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut line = Vec::new();
        loop {
            line.clear();
            let limit = u64::try_from(MAX_PAYLOAD_BYTES + 1).expect("a payload's size fits");
            if (&mut read).take(limit).read_until(b'\n', &mut line).await? == 0 {
                break;
            }
            let Some(request) = line.strip_suffix(b"\n") else {
                write.write_all(b"fail too-long\n").await?;
                break;
            };
            let request = request.strip_suffix(b"\r").unwrap_or(request);
            let answer = request_answer(&mut session, request, timeout).await;
            write.write_all(&answer).await?;
            write.write_all(b"\n").await?;
            if answer.starts_with(b"info") {
                break;
            }
        }
        session.close().await;
        Ok(())
    }

    /// The answer to one request: as the service gave it, or `fail` or `info` from here.
    async fn request_answer(session: &mut ClusterClient, request: &[u8], timeout: Duration) -> Vec<u8> {
        let ops = match parse(request) {
            Ok(ops) => ops,
            Err(why) => return format!("fail {why}").into_bytes(),
        };
        if read_only(&ops) {
            // Never logged: a query that isn't answered had no effect.
            return match tokio::time::timeout(timeout, session.query(request.to_vec())).await {
                Ok(Ok(answer)) => answer,
                Ok(Err(QueryError::Closed)) => b"info session-closed".to_vec(),
                Ok(Err(e)) => format!("fail {e}").into_bytes(),
                Err(_) => b"fail timeout".to_vec(),
            };
        }
        if let Err(e) = session.send(request.to_vec()).await {
            return format!("fail {e}").into_bytes();
        }
        match tokio::time::timeout(timeout, response(session)).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(why)) => format!("info {why}").into_bytes(),
            Err(_) => b"info timeout".to_vec(),
        }
    }

    /// The next response: there's one for each message, in order.
    async fn response(session: &mut ClusterClient) -> Result<Vec<u8>, &'static str> {
        loop {
            match session.recv().await {
                Some(Event::Message(answer)) => return Ok(answer),
                Some(Event::Lost { .. }) => return Err("response-lost"),
                Some(Event::Closed { .. }) | None => return Err("session-closed"),
                Some(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener as StdListener};
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use turbofan::driver::{self, serve_until};
    use turbofan::raft::{Member, NodeId};
    use turbofan::{Effect, node};

    use super::*;

    fn answer(service: &mut ListAppend, txn: &str) -> String {
        let mut ctx = Ctx::new(0);
        service.on_message(&mut ctx, SessionId(1), txn.as_bytes());
        let effects: [Effect; 1] = ctx.take_effects().try_into().expect("one answer a message");
        let [Effect::Send(to, answer)] = effects else { panic!("{effects:?}") };
        assert_eq!(to, SessionId(1));
        String::from_utf8(answer).unwrap()
    }

    #[test]
    fn a_transaction_reads_its_own_appends_and_is_kept_whole() {
        let mut service = ListAppend::default();
        assert_eq!(answer(&mut service, "r 1"), "ok [[:r 1 []]]");
        assert!(service.lists.is_empty(), "a read keeps no key");
        assert_eq!(
            answer(&mut service, "a 1 3;a 1 5;r 1;a 2 7"),
            "ok [[:append 1 3] [:append 1 5] [:r 1 [3 5]] [:append 2 7]]"
        );
        assert_eq!(answer(&mut service, "r 2;r 1"), "ok [[:r 2 [7]] [:r 1 [3 5]]]");
        assert_eq!(service.on_query(SessionId(1), b"r 1"), Some(b"ok [[:r 1 [3 5]]]".to_vec()));
        assert_eq!(service.on_query(SessionId(1), b"a 1 9"), Some(b"fail not-read-only".to_vec()));
    }

    #[test]
    fn a_refused_transaction_changes_nothing() {
        let mut service = ListAppend::default();
        let fill: Vec<String> = (0..MAX_LIST_VALUES).map(|v| format!("a 1 {v}")).collect();
        for chunk in fill.chunks(MAX_TRANSACTION_OPS) {
            assert!(answer(&mut service, &chunk.join(";")).starts_with("ok"));
        }
        assert_eq!(answer(&mut service, "a 2 1;a 1 0"), "fail list-full");
        assert!(!service.lists.contains_key(&2), "the append before the refused one is undone");
        // Every read of a full list, together, is too long an answer.
        let reads = vec!["r 1"; MAX_TRANSACTION_OPS].join(";");
        assert_eq!(answer(&mut service, &format!("a 3 1;{reads}")[..]).as_str(), "fail too-many-ops");
        assert_eq!(answer(&mut service, &format!("a 3 1;{}", reads.split_at(4).1)), "fail answer-too-long");
        assert!(!service.lists.contains_key(&3));
        for bad in ["", "a 1", "r 1 2", "x 1", "a -1 2", "r 1;", "a 1 99999999999999999999"] {
            assert_eq!(answer(&mut service, bad), "fail malformed", "{bad:?}");
        }
        assert_eq!(answer(&mut service, "r \u{ff}"), "fail malformed");
    }

    #[test]
    fn a_snapshot_restores_the_same_lists_and_a_damaged_one_is_refused() {
        let mut service = ListAppend::default();
        answer(&mut service, "a 1 3;a 1 5;a 9 1");
        let mut bytes = Vec::new();
        service.snapshot()(&mut bytes).unwrap();
        let mut restored = ListAppend::default();
        restored.restore(&mut bytes.as_slice()).unwrap();
        assert_eq!(restored.lists, service.lists);
        for cut in [1, 8, 16, bytes.len() - 8] {
            assert!(ListAppend::default().restore(&mut &bytes[..cut]).is_err(), "cut at {cut}");
        }
    }

    /// A node of one, and a gateway in front of it, over TCP.
    #[tokio::test]
    async fn the_gateway_answers_lines_from_the_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let addrs: Vec<SocketAddr> =
            (0..3).map(|_| StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()).collect();
        let member = Member::new(NodeId(1), addrs[0].to_string(), addrs[1].to_string(), addrs[2].to_string());
        let config = driver::Config::new(
            node::Config::new(NodeId(1), vec![member]),
            SERVICE_ID,
            dir.path().join("1"),
            vec![(NodeId(1), addrs[0])],
            addrs[1],
            addrs[2],
        );
        tokio::spawn(serve_until(config, ListAppend::default(), std::future::pending()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = listener.local_addr().unwrap();
        tokio::spawn(gateway::serve(listener, vec![addrs[1].to_string()], Duration::from_secs(10)));

        let stream = TcpStream::connect(gateway).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        for (request, expected) in [
            ("a 1 3;a 1 5", "ok [[:append 1 3] [:append 1 5]]"),
            ("r 1", "ok [[:r 1 [3 5]]]"),
            ("a 1 7;r 1;r 2", "ok [[:append 1 7] [:r 1 [3 5 7]] [:r 2 []]]"),
            ("r 1;r 1", "ok [[:r 1 [3 5 7]] [:r 1 [3 5 7]]]"),
            ("a 1", "fail malformed"),
        ] {
            write.write_all(format!("{request}\n").as_bytes()).await.unwrap();
            let line = tokio::time::timeout(Duration::from_secs(20), lines.next_line()).await.unwrap().unwrap();
            assert_eq!(line.as_deref(), Some(expected), "{request}");
        }
    }
}

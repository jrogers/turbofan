//! A proxy in front of each node's peer and client listeners, to fault the links over real TCP:
//! cut (open connections dropped, new ones refused), stalled (what they carry held, so TCP backs
//! up behind it), or slowed. A peer connection carries what the node that dialled it sends, and
//! nothing back, so a link is that node and the one dialled. A client's carries its requests one
//! way and their responses the other: links from [`CLIENTS`] to the node, and back. The proxy
//! knows who dialled from the hello, the connection's first frame: over plain TCP only, as over
//! TLS the hello can't be read.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use turbofan_cluster::codec;
use turbofan_cluster::raft::NodeId;
use turbofan_cluster::wire;

/// A link: the node that dialled, and the node dialled; or [`CLIENTS`] and a node, either way.
pub type Link = (NodeId, NodeId);

/// The clients, in a [`Link`]: no node is 0.
pub const CLIENTS: NodeId = NodeId(0);

/// How the links are faulted, now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Faults {
    pub cut: BTreeSet<Link>,
    pub stalled: BTreeSet<Link>,
    /// How long each chunk a link carries waits before it's passed on.
    pub delay: BTreeMap<Link, Duration>,
}

/// The proxy: one listener for each node, forwarding to its peer listener.
pub struct Proxy {
    faults: watch::Sender<Faults>,
    listeners: Vec<JoinHandle<()>>,
}

impl Proxy {
    /// Listens at each of `routes`' addresses for node `to`, forwarding to its listener at the
    /// target address: its peer or client listener. In a tokio runtime.
    pub fn start(routes: Vec<(NodeId, SocketAddr, SocketAddr)>) -> Self {
        let (faults, _) = watch::channel(Faults::default());
        let mut listeners = Vec::new();
        for (to, listen, target) in routes {
            let listener = std::net::TcpListener::bind(listen).expect("the proxy's port is free");
            listener.set_nonblocking(true).expect("a listener's mode");
            let listener = TcpListener::from_std(listener).expect("in a runtime");
            let faults = faults.subscribe();
            listeners.push(tokio::spawn(async move {
                while let Ok((inbound, _)) = listener.accept().await {
                    tokio::spawn(relay(inbound, to, target, faults.clone()));
                }
            }));
        }
        Self { faults, listeners }
    }

    /// Changes the faults with `change`.
    pub fn fault(&self, change: impl FnOnce(&mut Faults)) {
        self.faults.send_modify(change);
    }

    /// Cuts every link to and from `id`, of `ids`.
    pub fn isolate(&self, id: NodeId, ids: &[NodeId]) {
        self.fault(|f| {
            for &other in ids.iter().filter(|&&o| o != id) {
                f.cut.insert((id, other));
                f.cut.insert((other, id));
            }
        });
    }

    /// No faults.
    pub fn heal(&self) {
        self.fault(|f| *f = Faults::default());
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        for listener in &self.listeners {
            listener.abort();
        }
    }
}

/// Forwards a connection to node `to`, at `target`, as the faults say: a client's both ways.
async fn relay(mut inbound: TcpStream, to: NodeId, target: SocketAddr, faults: watch::Receiver<Faults>) {
    let Ok(hello) = read_frame(&mut inbound).await else { return };
    let Some(from) = dialler(&hello) else { return };
    if faults.borrow().cut.contains(&(from, to)) {
        return;
    }
    let Ok(mut outbound) = TcpStream::connect(target).await else { return };
    if outbound.write_all(&hello).await.is_err() {
        return;
    }
    if from != CLIENTS {
        pump(inbound, outbound, (from, to), faults).await;
        return;
    }
    let ((in_read, in_write), (out_read, out_write)) = (inbound.into_split(), outbound.into_split());
    // Either way ending ends both: the other's half is dropped with it.
    tokio::select! {
        () = pump(in_read, out_write, (CLIENTS, to), faults.clone()) => {}
        () = pump(out_read, in_write, (to, CLIENTS), faults) => {}
    }
}

/// Carries what `inbound` reads to `outbound`, over `link`, as the faults say.
async fn pump(
    mut inbound: impl AsyncReadExt + Unpin,
    mut outbound: impl AsyncWriteExt + Unpin,
    link: Link,
    mut faults: watch::Receiver<Faults>,
) {
    let mut chunk = vec![0; 64 * 1024];
    loop {
        let read = tokio::select! {
            read = inbound.read(&mut chunk) => read,
            // A fault changed: a cut drops the connection at once, not at its next chunk.
            changed = faults.changed() => {
                if changed.is_err() || faults.borrow().cut.contains(&link) {
                    return;
                }
                continue;
            }
        };
        let Ok(n @ 1..) = read else { return };
        // Held while stalled; dropped if cut meanwhile.
        loop {
            let (cut, stalled, delay) = {
                let f = faults.borrow_and_update();
                (f.cut.contains(&link), f.stalled.contains(&link), f.delay.get(&link).copied())
            };
            if cut {
                return;
            }
            if !stalled {
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                break;
            }
            if faults.changed().await.is_err() {
                return;
            }
        }
        if outbound.write_all(&chunk[..n]).await.is_err() {
            return;
        }
    }
}

/// The node a peer's hello says it is, or [`CLIENTS`] for a client's.
fn dialler(hello: &[u8]) -> Option<NodeId> {
    let Ok(Some((wire::Decoded::Hello(h), _))) = codec::decode_frame(hello) else { return None };
    match h.role() {
        wire::Role::Client => Some(CLIENTS),
        wire::Role::Peer => (h.node_id() != 0).then(|| NodeId(h.node_id())),
        _ => None,
    }
}

/// One frame, its length and all.
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let len = stream.read_u32_le().await?;
    let len = usize::try_from(len).expect("u32 fits usize");
    if len > codec::MAX_FRAME_BYTES {
        return Err(std::io::Error::other("a frame too long for a hello"));
    }
    let mut frame = vec![0; 4 + len];
    frame[..4].copy_from_slice(&u32::try_from(len).expect("checked").to_le_bytes());
    stream.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

//! A proxy in front of each node's peer and client listeners, to fault the links over real TCP:
//! cut (open connections dropped, new ones refused), stalled (what they carry held, so TCP backs
//! up behind it), or slowed. A peer connection carries what the node that dialled it sends, and
//! nothing back but TLS's own, so a link is that node and the one dialled; or [`PEERS`] and the node dialled,
//! every peer's link to it at once. The proxy knows which node dialled from its hello, the
//! connection's first frame: over plain TCP only, as over TLS the hello can't be read, and then
//! only links from [`PEERS`] can be faulted. A client's connection carries its requests one way
//! and their responses the other: links from [`CLIENTS`] to the node, and back.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use turbofan::codec;
use turbofan::raft::NodeId;
use turbofan::wire;

/// A link: the node that dialled, and the node dialled; or [`CLIENTS`] and a node, either way.
pub type Link = (NodeId, NodeId);

/// The clients, in a [`Link`]: no node is 0.
pub const CLIENTS: NodeId = NodeId(0);

/// Every node, in a [`Link`] to one: none is 255.
pub const PEERS: NodeId = NodeId(u8::MAX);

/// What a node's listener behind the proxy takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listener {
    Peer,
    Client,
}

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
    /// Listens at each of `routes`' addresses for node `to`, forwarding to its listener of that
    /// kind at the target address; reading peers' hellos unless the nodes speak `tls`. In a tokio
    /// runtime.
    pub fn start(routes: Vec<(NodeId, Listener, SocketAddr, SocketAddr)>, tls: bool) -> Self {
        let (faults, _) = watch::channel(Faults::default());
        let mut listeners = Vec::new();
        for (to, kind, listen, target) in routes {
            let listener = std::net::TcpListener::bind(listen).expect("the proxy's port is free");
            listener.set_nonblocking(true).expect("a listener's mode");
            let listener = TcpListener::from_std(listener).expect("in a runtime");
            let faults = faults.subscribe();
            listeners.push(tokio::spawn(async move {
                while let Ok((inbound, _)) = listener.accept().await {
                    tokio::spawn(relay(inbound, to, kind, tls, target, faults.clone()));
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

/// Forwards a connection to node `to`'s listener of `kind`, at `target`, both ways, as the faults
/// say: a client's both ways, a peer's the way it sends (what comes back is TLS's own).
async fn relay(
    mut inbound: TcpStream,
    to: NodeId,
    kind: Listener,
    tls: bool,
    target: SocketAddr,
    faults: watch::Receiver<Faults>,
) {
    let mut hello = Vec::new();
    let from = match kind {
        Listener::Client => CLIENTS,
        Listener::Peer if tls => PEERS,
        Listener::Peer => {
            let Ok(frame) = read_frame(&mut inbound).await else { return };
            let Some(from) = dialler(&frame) else { return };
            hello = frame;
            from
        }
    };
    let links = [(from, to), (PEERS, to)];
    if links.iter().any(|link| faults.borrow().cut.contains(link)) {
        return;
    }
    let Ok(mut outbound) = TcpStream::connect(target).await else { return };
    if outbound.write_all(&hello).await.is_err() {
        return;
    }
    let (there, back): (&[Link], &[Link]) = match kind {
        Listener::Peer => (&links, &[]),
        Listener::Client => (&[(CLIENTS, to)], &[(to, CLIENTS)]),
    };
    let ((in_read, in_write), (out_read, out_write)) = (inbound.into_split(), outbound.into_split());
    // Either way ending ends both: the other's half is dropped with it.
    tokio::select! {
        () = pump(in_read, out_write, there, faults.clone()) => {}
        () = pump(out_read, in_write, back, faults) => {}
    }
}

/// Carries what `inbound` reads to `outbound`, as the faults on any of `links` say.
async fn pump(
    mut inbound: impl AsyncReadExt + Unpin,
    mut outbound: impl AsyncWriteExt + Unpin,
    links: &[Link],
    mut faults: watch::Receiver<Faults>,
) {
    let mut chunk = vec![0; 64 * 1024];
    loop {
        let read = tokio::select! {
            read = inbound.read(&mut chunk) => read,
            // A fault changed: a cut drops the connection at once, not at its next chunk.
            changed = faults.changed() => {
                if changed.is_err() || links.iter().any(|link| faults.borrow().cut.contains(link)) {
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
                let delay = links.iter().find_map(|link| f.delay.get(link).copied());
                (links.iter().any(|l| f.cut.contains(l)), links.iter().any(|l| f.stalled.contains(l)), delay)
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

/// The node a peer's hello says it is.
fn dialler(hello: &[u8]) -> Option<NodeId> {
    let Ok(Some((wire::Decoded::Hello(h), _))) = codec::decode_frame(hello) else { return None };
    (h.role() == wire::Role::Peer && h.node_id() != 0).then(|| NodeId(h.node_id()))
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

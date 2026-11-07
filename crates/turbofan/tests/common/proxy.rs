//! A proxy in front of each node's peer and client listeners, to fault the links over real TCP:
//! cut (open connections dropped, new ones refused), stalled (what they carry held, so TCP backs
//! up behind it), or slowed. A peer connection carries what the node that dialled it sends, and
//! nothing back but TLS's own, so a link is that node and the one dialled. The proxy knows which
//! node dialled from its hello, the connection's first frame. Over TLS it ends the TLS the node
//! dialled with to read it, and dials the node behind it over TLS again, with the nodes' own
//! certificate: every node has the one, so to the node dialled, the proxy is the node that dialled.
//! A client's connection carries its requests one way and their responses the other: links from
//! [`CLIENTS`] to the node, and back. Clients' connections are passed on as they are, TLS and all.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use turbofan::codec;
use turbofan::raft::NodeId;
use turbofan::tls::NodeTls;
use turbofan::wire;

/// A link: the node that dialled, and the node dialled; or [`CLIENTS`] and a node, either way.
pub type Link = (NodeId, NodeId);

/// The clients, in a [`Link`]: no node is 0.
pub const CLIENTS: NodeId = NodeId(0);

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

/// The proxy: one listener for each node's peer and client listeners, forwarding to it.
pub struct Proxy {
    faults: watch::Sender<Faults>,
    listeners: Vec<JoinHandle<()>>,
}

impl Proxy {
    /// Listens at each of `routes`' addresses for node `to`, forwarding to its listener of that
    /// kind at the target address, ending and making again the TLS to peers if the nodes have
    /// `tls`. In a tokio runtime.
    pub fn start(routes: Vec<(NodeId, Listener, SocketAddr, SocketAddr)>, tls: Option<&NodeTls>) -> Self {
        let tls = tls.map(|tls| Arc::new(Tls::load(tls)));
        let (faults, _) = watch::channel(Faults::default());
        let mut listeners = Vec::new();
        for (to, kind, listen, target) in routes {
            let listener = std::net::TcpListener::bind(listen).expect("the proxy's port is free");
            listener.set_nonblocking(true).expect("a listener's mode");
            let listener = TcpListener::from_std(listener).expect("in a runtime");
            let (faults, tls) = (faults.subscribe(), tls.clone());
            listeners.push(tokio::spawn(async move {
                while let Ok((inbound, _)) = listener.accept().await {
                    tokio::spawn(relay(inbound, to, kind, tls.clone(), target, faults.clone()));
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
    inbound: TcpStream,
    to: NodeId,
    kind: Listener,
    tls: Option<Arc<Tls>>,
    target: SocketAddr,
    faults: watch::Receiver<Faults>,
) {
    match tls {
        Some(tls) if kind == Listener::Peer => {
            let Ok(inbound) = tls.accept(inbound).await else { return };
            forward(inbound, to, kind, tls.connect(target), faults).await;
        }
        _ => forward(inbound, to, kind, TcpStream::connect(target), faults).await,
    }
}

/// [`relay`]'s forwarding, once the TLS a peer dialled with, if any, is ended: `outbound`
/// connects to the node behind the proxy, once the dialler is known and its link isn't cut.
async fn forward<I, O>(
    mut inbound: I,
    to: NodeId,
    kind: Listener,
    outbound: impl Future<Output = io::Result<O>>,
    faults: watch::Receiver<Faults>,
) where
    I: AsyncRead + AsyncWrite + Unpin,
    O: AsyncRead + AsyncWrite + Unpin,
{
    let mut hello = Vec::new();
    let from = match kind {
        Listener::Client => CLIENTS,
        Listener::Peer => {
            let Ok(frame) = read_frame(&mut inbound).await else { return };
            let Some(from) = dialler(&frame) else { return };
            hello = frame;
            from
        }
    };
    if faults.borrow().cut.contains(&(from, to)) {
        return;
    }
    let Ok(mut outbound) = outbound.await else { return };
    if outbound.write_all(&hello).await.is_err() {
        return;
    }
    let (there, back): (&[Link], &[Link]) = match kind {
        Listener::Peer => (&[(from, to)], &[]),
        Listener::Client => (&[(CLIENTS, to)], &[(to, CLIENTS)]),
    };
    let ((in_read, in_write), (out_read, out_write)) = (tokio::io::split(inbound), tokio::io::split(outbound));
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
async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let len = stream.read_u32_le().await?;
    let len = usize::try_from(len).expect("u32 fits usize");
    if len > codec::MAX_FRAME_BYTES {
        return Err(io::Error::other("a frame too long for a hello"));
    }
    let mut frame = vec![0; 4 + len];
    frame[..4].copy_from_slice(&u32::try_from(len).expect("checked").to_le_bytes());
    stream.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

/// What the proxy ends a peer's TLS with, and makes it again with: the nodes' own certificate,
/// and their CA.
#[cfg(feature = "tls")]
struct Tls {
    acceptor: tokio_rustls::TlsAcceptor,
    connector: tokio_rustls::TlsConnector,
}

#[cfg(feature = "tls")]
impl Tls {
    fn load(tls: &NodeTls) -> Self {
        use tokio_rustls::rustls::pki_types::pem::PemObject;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use tokio_rustls::rustls::server::WebPkiClientVerifier;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig, crypto};
        let provider = Arc::new(crypto::ring::default_provider());
        let certs = |path| CertificateDer::pem_file_iter(path).unwrap().map(Result::unwrap).collect::<Vec<_>>();
        let key = || PrivateKeyDer::from_pem_file(&tls.key).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(certs(tls.peer_ca.as_ref().expect("a peer CA")));
        let roots = Arc::new(roots);
        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone()).build().unwrap();
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs(&tls.cert), key())
            .unwrap();
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs(&tls.cert), key())
            .unwrap();
        Self { acceptor: Arc::new(server).into(), connector: Arc::new(client).into() }
    }

    async fn accept(&self, stream: TcpStream) -> io::Result<impl AsyncRead + AsyncWrite + Unpin + use<>> {
        self.acceptor.accept(stream).await
    }

    async fn connect(&self, target: SocketAddr) -> io::Result<impl AsyncRead + AsyncWrite + Unpin + use<>> {
        let stream = TcpStream::connect(target).await?;
        self.connector.connect(target.ip().into(), stream).await
    }
}

/// Without the feature, no node has TLS, so the proxy has none to end.
#[cfg(not(feature = "tls"))]
enum Tls {}

#[cfg(not(feature = "tls"))]
impl Tls {
    fn load(_: &NodeTls) -> Self {
        unreachable!("a node's TLS is refused without the feature")
    }

    async fn accept(&self, _: TcpStream) -> io::Result<TcpStream> {
        match *self {}
    }

    async fn connect(&self, _: SocketAddr) -> io::Result<TcpStream> {
        match *self {}
    }
}

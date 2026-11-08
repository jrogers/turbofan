//! TLS, optional, on all three listeners or the admin listener alone, from PEM files: rustls,
//! TLS 1.3 only.
//!
//! A node presents its certificate on each listener, and to the peers it dials. Peers check each
//! other's against the peer CA, both ways; operators present one the admin CA signed; clients
//! only check the node's, and say who they are by their credentials, as without TLS. Without an
//! admin CA, operators are as clients: they present no certificate, and the admin token says who
//! they are. A
//! certificate is checked against the address dialled, so a node's must name (as a subject
//! alternative name) every host it's reached at: its peer, client and admin addresses'.
//!
//! A CA file is a bundle: every certificate in it, root or intermediate, is trusted, and a
//! certificate that chains to any of them is taken. A certificate file holds the chain, its own
//! certificate first, then any intermediates, which are sent with it.
//!
//! A peer certificate stands for the cluster, not for one node: any node with one may say it's
//! any member, as with the peer token. Unless [`NodeTls::peer_ids`]: then each node's certificate
//! names its node ID, as the DNS name `node-<id>.turbofan` beside its hosts', and a peer is taken
//! only as the node its certificate names, both ways: a node accepting a connection checks the
//! node ID the hello says, and one dialling, the member it dials.
//!
//! Without a peer CA ([`NodeTls::admin_only`]), only operators connect over TLS: peers and clients
//! stay on plain TCP, so a cluster can put TLS on its admin listeners alone, one node at a time.
//!
//! A node with TLS to peers and one without can't talk, so a running cluster turns it on or off
//! with [`NodeTls::plain`], node by node, a restart each: first every node to [`Plain::Dialled`],
//! taking plain TCP beside TLS and dialling peers over plain TCP, as those not yet restarted do;
//! then every node to [`Plain::Accepted`], dialling over TLS, which every node now takes; then
//! every node to [`Plain::Refused`], TLS only. Turning it off is the same backwards: every node to
//! `Accepted`, then `Dialled`, then without TLS. A listener tells the two apart by how a
//! connection begins, a TLS handshake or a hello. Clients move to TLS once every node takes it,
//! and before the last step: `turbofan_plain_connections` counts the connections still plain.
//! Only on the peer and client listeners: the admin listener's TLS is each node's own already.
//!
//! A node rereads its files every [`NodeTls::reload_every`], and when they've changed, loads them
//! again, for the connections made after: a certificate or CA is replaced by rewriting its file.
//! Files that don't load, half written, say, are left until they do, the node keeping what it
//! had, and counted in `turbofan_tls_reloads` (by `result`, `ok` or `failed`). What it has TLS on
//! is as it started: a CA added or taken away is a restart.
//!
//! Only with the `tls` feature. Without it the API is the same, but loading any TLS
//! configuration ([`ClientTls::load`], or `serve` with a [`NodeTls`]) is an
//! [`io::ErrorKind::Unsupported`] error, so there is never TLS to run and every connection is
//! plain TCP.

use std::fmt;
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, BufReader, BufWriter, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::raft::NodeId;

#[cfg(feature = "tls")]
use rustls_impl as imp;
#[cfg(not(feature = "tls"))]
use without as imp;

/// A node's TLS: its own certificate, and the CAs it checks others' against. PEM files.
///
/// Non-exhaustive, so that a new setting isn't a breaking change: build it with
/// [`NodeTls::new`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeTls {
    /// The node's certificate chain, its own first. It's a client's too, to the peers it dials,
    /// so if it has an extended key usage, that must allow both `serverAuth` and `clientAuth`:
    /// with `serverAuth` alone, as a CA issues for a server, every peer refuses the node.
    pub cert: PathBuf,
    /// The certificate's private key.
    pub key: PathBuf,
    /// The CA that signed every node's certificate. With none, TLS is on the admin listener only:
    /// peers and clients are plain TCP.
    pub peer_ca: Option<PathBuf>,
    /// The CA that signed operators' certificates. With none, operators present none: the
    /// connection is encrypted and the node's certificate checked, but only the admin token says
    /// who an operator is.
    pub admin_ca: Option<PathBuf>,
    /// Whether every peer's certificate must name its node ID, as the DNS name
    /// `node-<id>.turbofan`: a peer is taken only as the node its certificate names. Off, a peer
    /// certificate stands for any member. Only with a peer CA. Turn it on once every node's
    /// certificate names its ID; a wildcard name, `*.turbofan`, names every node.
    pub peer_ids: bool,
    /// How often the node rereads the files, to load them again if they've changed: 60 seconds
    /// by default. Not zero.
    pub reload_every: Duration,
    /// Whether plain TCP is taken beside TLS on the peer and client listeners, and peers dialled
    /// over it, while TLS is turned on or off over a running cluster: see [the module](self).
    /// Only with a peer CA, and without [`NodeTls::peer_ids`], since a peer on plain TCP has no
    /// certificate to name it.
    pub plain: Plain,
}

/// Plain TCP beside TLS, for [`NodeTls::plain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Plain {
    /// TLS only: plain TCP refused. The default.
    #[default]
    Refused,
    /// Plain TCP taken beside TLS, on the peer and client listeners; peers dialled over TLS.
    Accepted,
    /// Plain TCP taken beside TLS, on the peer and client listeners, and peers dialled over plain
    /// TCP.
    Dialled,
}

/// [`NodeTls::reload_every`]'s default.
pub(crate) const RELOAD_EVERY: Duration = Duration::from_secs(60);

impl NodeTls {
    /// The node's certificate `cert` and its `key`, the `peer_ca` that signed every node's, and
    /// the `admin_ca` that signed operators'.
    #[must_use]
    pub fn new(
        cert: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
        peer_ca: impl Into<PathBuf>,
        admin_ca: impl Into<PathBuf>,
    ) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            peer_ca: Some(peer_ca.into()),
            admin_ca: Some(admin_ca.into()),
            peer_ids: false,
            reload_every: RELOAD_EVERY,
            plain: Plain::Refused,
        }
    }

    /// TLS on the admin listener only: the node's certificate `cert` and its `key`, and the
    /// `admin_ca` that signed operators'. Peers and clients are plain TCP.
    #[must_use]
    pub fn admin_only(cert: impl Into<PathBuf>, key: impl Into<PathBuf>, admin_ca: impl Into<PathBuf>) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            peer_ca: None,
            admin_ca: Some(admin_ca.into()),
            peer_ids: false,
            reload_every: RELOAD_EVERY,
            plain: Plain::Refused,
        }
    }
}

/// A client's or operator's TLS: the CA the nodes' certificates are checked against, and for an
/// operator, its own certificate and key, which the admin CA signed. PEM files.
#[derive(Clone)]
pub struct ClientTls(imp::Connector);

impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientTls")
    }
}

impl ClientTls {
    /// Reads the CA that signed the nodes' certificates, and for an operator, its own certificate
    /// chain and key.
    ///
    /// # Errors
    ///
    /// If a file can't be read, or isn't PEM of the right kind; or, without the `tls` feature,
    /// always.
    pub fn load(ca: &Path, identity: Option<(&Path, &Path)>) -> io::Result<Self> {
        imp::connector(ca, identity).map(Self)
    }
}

/// What a listener accepts TLS with.
pub(crate) type Acceptor = imp::Acceptor;

/// What TLS runs over TCP as.
pub(crate) trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stream for T {}

/// What a connection runs over: TCP, buffered both ways as rustls buffers TLS (else every frame
/// written is a system call, and every one read two), or TLS over TCP. Whatever writes to one
/// flushes it, as TLS needs too.
pub(crate) enum Conn {
    Plain(BufReader<BufWriter<TcpStream>>),
    #[cfg_attr(not(feature = "tls"), expect(dead_code, reason = "without the feature there is never TLS to run"))]
    Tls(Box<dyn Stream>),
}

/// The half of a [`Conn`] read from, and the half written to, apart. The one read from is
/// buffered here, for a reader to see what's arrived whole ([`BufReader::buffer`]).
pub(crate) type ReadHalf = BufReader<Box<dyn AsyncRead + Unpin + Send>>;
pub(crate) type WriteHalf = Box<dyn AsyncWrite + Unpin + Send>;

impl Conn {
    /// Split to be read and written at once, by tasks of their own. Plain TCP splits in two
    /// sockets' worth of halves, each with a buffer of its own: [`tokio::io::split`] would put a
    /// lock between them, taken on every read and write, which the reader and writer of a busy
    /// connection wait on. TLS can't, one session's state being both ways', so it keeps the lock,
    /// with a buffer either side of it: else every frame written is a TLS record of its own and a
    /// system call, made holding the lock the reader waits on.
    pub(crate) fn split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Self::Plain(stream) => {
                // Read past the hello, what the other end sent next: read first, before the socket.
                let read_ahead = stream.buffer().to_vec();
                let stream = stream.into_inner();
                assert!(stream.buffer().is_empty(), "a connection is split flushed");
                let (read, write) = stream.into_inner().into_split();
                (BufReader::new(Box::new(Cursor::new(read_ahead).chain(read))), Box::new(BufWriter::new(write)))
            }
            Self::Tls(stream) => {
                let (read, write) = tokio::io::split(stream);
                (BufReader::new(Box::new(read)), Box::new(BufWriter::new(write)))
            }
        }
    }
}

impl AsyncRead for Conn {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// A node's TLS, loaded: what each listener accepts with, and what it dials peers with. Without
/// a peer CA, only the admin listener's.
#[derive(Clone)]
pub(crate) struct Loaded {
    pub(crate) peer: Option<Acceptor>,
    pub(crate) client: Option<Acceptor>,
    pub(crate) admin: Acceptor,
    pub(crate) dial: Option<ClientTls>,
    /// [`NodeTls::peer_ids`], with a peer CA.
    pub(crate) peer_ids: bool,
    /// Whether the peer and client listeners take plain TCP beside TLS ([`NodeTls::plain`]).
    pub(crate) plain: bool,
}

impl Loaded {
    pub(crate) fn load(tls: &NodeTls) -> io::Result<Self> {
        if tls.reload_every.is_zero() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "TLS: reload_every is zero"));
        }
        if tls.plain != Plain::Refused && (tls.peer_ca.is_none() || tls.peer_ids) {
            let refused = "TLS: plain TCP beside TLS needs a peer CA, and not peer_ids";
            return Err(io::Error::new(io::ErrorKind::InvalidInput, refused));
        }
        let (cluster, admin) = imp::acceptors(tls)?;
        let dial = match tls.plain {
            Plain::Dialled => None,
            _ => tls.peer_ca.as_deref().map(|ca| ClientTls::load(ca, Some((&tls.cert, &tls.key)))).transpose()?,
        };
        let (peer, client) = cluster.unzip();
        let peer_ids = tls.peer_ids && tls.peer_ca.is_some();
        Ok(Self { peer, client, admin, dial, peer_ids, plain: tls.plain != Plain::Refused })
    }
}

/// A node's TLS as it stands: its files' latest that loaded, or none for plain TCP.
pub(crate) type Current = watch::Receiver<Option<Loaded>>;

/// A node's TLS, if it has any, loaded; and what rereads it, to run as long as the node does.
pub(crate) fn start(tls: Option<NodeTls>) -> io::Result<(Current, impl Future<Output = ()> + Send + 'static)> {
    let (current, receiver) = watch::channel(tls.as_ref().map(Loaded::load).transpose()?);
    let reloading = async move {
        if let Some(tls) = tls {
            reload(tls, current).await;
        }
    };
    Ok((receiver, reloading))
}

/// Rereads `tls`'s files every [`NodeTls::reload_every`], and when they've changed and load, puts
/// them in `current`.
async fn reload(tls: NodeTls, current: watch::Sender<Option<Loaded>>) {
    // Small files, a minute apart: read in place rather than on a blocking thread.
    let read = |tls: &NodeTls| -> Vec<Option<Vec<u8>>> {
        let paths = [Some(&tls.cert), Some(&tls.key), tls.peer_ca.as_ref(), tls.admin_ca.as_ref()];
        paths.into_iter().flatten().map(|path| std::fs::read(path).ok()).collect()
    };
    let mut loaded = read(&tls);
    loop {
        tokio::time::sleep(tls.reload_every).await;
        let files = read(&tls);
        if files == loaded {
            continue;
        }
        let result = match Loaded::load(&tls) {
            Ok(new) => {
                current.send_replace(Some(new));
                loaded = files;
                "ok"
            }
            Err(_) => "failed",
        };
        metrics::counter!("turbofan_tls_reloads", "result" => result).increment(1);
    }
}

/// A TCP connection to `addr` (`host:port`), over TLS if there's `tls`, checking the
/// certificate names `host`, and node `node` if there's one: else it's
/// [`io::ErrorKind::PermissionDenied`].
pub(crate) async fn connect(addr: &str, tls: Option<&ClientTls>, node: Option<NodeId>) -> io::Result<Conn> {
    let stream = TcpStream::connect(addr).await?;
    tune(&stream);
    match tls {
        Some(tls) => imp::connect(&tls.0, addr, stream, node).await,
        None => Ok(plain(stream)),
    }
}

/// The certificate a connection's other end presented, if any.
pub(crate) type Cert = imp::Cert;

/// Whether `cert` names node `id`.
pub(crate) fn names(cert: Option<&Cert>, id: NodeId) -> bool {
    cert.is_some_and(|cert| imp::names(cert, id))
}

/// How long a connection's other end may leave what's sent it unacknowledged, or keepalive probes
/// unanswered, before the connection is dropped: one that vanished without closing (its host gone,
/// a firewall dropping its packets) is otherwise kept for the kernel's quarter of an hour. A peer's
/// dialler only writes, so it would write into the void, and not dial again, all that time.
#[cfg_attr(not(any(target_os = "linux", target_os = "android")), expect(dead_code, reason = "Linux's alone"))]
const DEAD_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a connection is idle before keepalive probes start, and the time between them: probes
/// find a dead one that has nothing unacknowledged, such as a peer's, read from and never written.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// Sets a new connection's options: no Nagle delay, and dropped once its other end is dead
/// ([`DEAD_CONNECTION_TIMEOUT`]). Best effort: a connection without them still works.
fn tune(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let socket = socket2::SockRef::from(stream);
    let probes = socket2::TcpKeepalive::new().with_time(KEEPALIVE_IDLE).with_interval(KEEPALIVE_INTERVAL);
    let _ = socket.set_tcp_keepalive(&probes);
    // Linux's alone: elsewhere what's unacknowledged is given up on in the kernel's own time.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = socket.set_tcp_user_timeout(Some(DEAD_CONNECTION_TIMEOUT));
}

fn plain(stream: TcpStream) -> Conn {
    Conn::Plain(BufReader::new(BufWriter::new(stream)))
}

/// An accepted TCP connection, over TLS if there's `tls` (with plain TCP taken beside it, by
/// the listener `plain_beside` names, only if the connection begins with a TLS handshake), and
/// the certificate the other end presented, if it did. A plain one taken beside TLS is counted,
/// by listener, in `turbofan_plain_connections`.
pub(crate) async fn accept(
    stream: TcpStream,
    tls: Option<&Acceptor>,
    plain_beside: Option<&'static str>,
) -> io::Result<(Conn, Option<Cert>)> {
    tune(&stream);
    match (tls, plain_beside) {
        (Some(tls), None) => imp::accept(tls, stream).await,
        (Some(tls), Some(listener)) => {
            if begins_tls(&stream).await? {
                return imp::accept(tls, stream).await;
            }
            metrics::counter!("turbofan_plain_connections", "listener" => listener).increment(1);
            Ok((plain(stream), None))
        }
        (None, _) => Ok((plain(stream), None)),
    }
}

/// The first bytes of a TLS connection: a handshake record (22), of version 3.1 to 3.3, which a
/// `ClientHello`'s record says it is.
const TLS_RECORD: (u8, u8, std::ops::RangeInclusive<u8>) = (22, 3, 1..=3);

/// A plain connection begins with its hello's length, little-endian: a TLS record's first three
/// bytes only if it's at least 0x01_0316 bytes long. A hello is a few fixed fields and its
/// token, which a kilobyte covers with room for fields to come.
const _: () = assert!(crate::driver::MAX_TOKEN_BYTES + 1024 < 0x01_0316);

/// Whether `stream` begins with a TLS handshake ([`TLS_RECORD`]), peeked, not read.
async fn begins_tls(stream: &TcpStream) -> io::Result<bool> {
    let mut start = [0; 3];
    loop {
        let n = stream.peek(&mut start).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let (record, major, minor) = &TLS_RECORD;
        if start[0] != *record || (n >= 2 && start[1] != *major) {
            return Ok(false);
        }
        if n == 3 {
            return Ok(minor.contains(&start[2]));
        }
        // ponytail: polls for the rest, the first segment having been under three bytes, which no
        // TLS stack or hello sends; the hello's timeout bounds it. A buffered replay if one ever does.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// TLS, by rustls on ring.
#[cfg(feature = "tls")]
mod rustls_impl {
    use std::fmt;
    use std::io;
    use std::path::Path;
    use std::sync::Arc;

    use tokio::net::TcpStream;
    use tokio_rustls::rustls::client::verify_server_name;
    use tokio_rustls::rustls::crypto::{CryptoProvider, ring};
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use tokio_rustls::rustls::server::{ParsedCertificate, WebPkiClientVerifier};
    use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
    pub(crate) use tokio_rustls::{TlsAcceptor as Acceptor, TlsConnector as Connector};

    use super::{Conn, NodeId, NodeTls};

    pub(crate) type Cert = CertificateDer<'static>;

    pub(super) fn connector(ca: &Path, identity: Option<(&Path, &Path)>) -> io::Result<Connector> {
        let builder = ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_root_certificates(roots(ca)?);
        let config = match identity {
            Some((cert, key)) => builder.with_client_auth_cert(certs(cert)?, private_key(key)?).map_err(invalid)?,
            None => builder.with_no_client_auth(),
        };
        Ok(Connector::from(Arc::new(config)))
    }

    /// The peer and client listeners' acceptors, if there's a peer CA, and the admin listener's.
    pub(super) fn acceptors(tls: &NodeTls) -> io::Result<(Option<(Acceptor, Acceptor)>, Acceptor)> {
        let (chain, key) = (certs(&tls.cert)?, private_key(&tls.key)?);
        let server = |ca: Option<&Path>| -> io::Result<Acceptor> {
            let builder = ServerConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(invalid)?;
            let builder = match ca {
                Some(ca) => builder.with_client_cert_verifier(
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots(ca)?), provider())
                        .build()
                        .map_err(invalid)?,
                ),
                None => builder.with_no_client_auth(),
            };
            let config = builder.with_single_cert(chain.clone(), key.clone_key()).map_err(invalid)?;
            Ok(Acceptor::from(Arc::new(config)))
        };
        let cluster = match &tls.peer_ca {
            Some(peer_ca) => Some((server(Some(peer_ca))?, server(None)?)),
            None => None,
        };
        Ok((cluster, server(tls.admin_ca.as_deref())?))
    }

    pub(super) async fn connect(
        tls: &Connector,
        addr: &str,
        stream: TcpStream,
        node: Option<NodeId>,
    ) -> io::Result<Conn> {
        let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let name = ServerName::try_from(host.to_owned()).map_err(invalid)?;
        let stream = tls.connect(name, stream).await?;
        if let Some(node) = node {
            let cert = stream.get_ref().1.peer_certificates().and_then(<[Cert]>::first);
            if !cert.is_some_and(|cert| names(cert, node)) {
                let denied = format!("{addr}: its certificate doesn't name node {node}");
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, denied));
            }
        }
        Ok(Conn::Tls(Box::new(stream)))
    }

    pub(super) async fn accept(tls: &Acceptor, stream: TcpStream) -> io::Result<(Conn, Option<Cert>)> {
        let stream = tls.accept(stream).await?;
        let cert = stream.get_ref().1.peer_certificates().and_then(<[Cert]>::first).cloned();
        Ok((Conn::Tls(Box::new(stream)), cert))
    }

    /// Whether `cert`, already verified, names node `id`.
    pub(super) fn names(cert: &Cert, id: NodeId) -> bool {
        let Ok(name) = ServerName::try_from(format!("node-{id}.turbofan")) else { return false };
        ParsedCertificate::try_from(cert).is_ok_and(|cert| verify_server_name(&cert, &name).is_ok())
    }

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(ring::default_provider())
    }

    fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, e)
    }

    fn pem_error(path: &Path, e: impl fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{}: {e}", path.display()))
    }

    fn certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
        let certs = CertificateDer::pem_file_iter(path).map_err(|e| pem_error(path, e))?;
        let certs = certs.collect::<Result<Vec<_>, _>>().map_err(|e| pem_error(path, e))?;
        if certs.is_empty() {
            return Err(pem_error(path, "no certificate"));
        }
        Ok(certs)
    }

    fn private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
        PrivateKeyDer::from_pem_file(path).map_err(|e| pem_error(path, e))
    }

    fn roots(path: &Path) -> io::Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        for cert in certs(path)? {
            roots.add(cert).map_err(invalid)?;
        }
        Ok(roots)
    }
}

/// No TLS: nothing loads, so there's never a connector or an acceptor to use.
#[cfg(not(feature = "tls"))]
mod without {
    use std::io;
    use std::path::Path;

    use tokio::net::TcpStream;

    use super::{Conn, NodeId, NodeTls};

    /// Uninhabited: without the feature, no TLS value can exist.
    #[derive(Clone)]
    pub(crate) enum Never {}
    pub(crate) type Connector = Never;
    pub(crate) type Acceptor = Never;
    pub(crate) type Cert = Never;

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "TLS: built without turbofan's `tls` feature")
    }

    pub(super) fn connector(_ca: &Path, _identity: Option<(&Path, &Path)>) -> io::Result<Connector> {
        Err(unsupported())
    }

    pub(super) fn acceptors(_tls: &NodeTls) -> io::Result<(Option<(Acceptor, Acceptor)>, Acceptor)> {
        Err(unsupported())
    }

    #[allow(clippy::unused_async, reason = "the same signature as with the feature")]
    pub(super) async fn connect(
        tls: &Connector,
        _addr: &str,
        _stream: TcpStream,
        _node: Option<NodeId>,
    ) -> io::Result<Conn> {
        match *tls {}
    }

    #[allow(clippy::unused_async, reason = "the same signature as with the feature")]
    pub(super) async fn accept(tls: &Acceptor, _stream: TcpStream) -> io::Result<(Conn, Option<Never>)> {
        match *tls {}
    }

    pub(super) fn names(cert: &Cert, _id: NodeId) -> bool {
        match *cert {}
    }

    #[cfg(test)]
    mod tests {
        use std::path::{Path, PathBuf};

        use super::super::{ClientTls, Loaded, NodeTls, Plain};

        /// Asked for TLS it can't give, a build without it says so, rather than going plain.
        #[test]
        fn tls_configuration_is_refused() {
            let error = ClientTls::load(Path::new("ca.pem"), None).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
            let file = PathBuf::from;
            let node = NodeTls {
                cert: file("c"),
                key: file("k"),
                peer_ca: Some(file("p")),
                admin_ca: Some(file("a")),
                peer_ids: false,
                reload_every: std::time::Duration::from_secs(1),
                plain: Plain::Refused,
            };
            assert_eq!(Loaded::load(&node).err().map(|e| e.kind()), Some(std::io::ErrorKind::Unsupported));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::{Loaded, NodeTls, Plain, accept, begins_tls, connect};
    use crate::driver::{read_frame, write_frames};

    /// A listener taking plain TCP beside TLS tells them apart by the first three bytes: a TLS
    /// handshake's record, of versions 3.1 to 3.3, from a hello's length, even one whose first
    /// byte is a handshake's (278 bytes) or first two (790 bytes), and from a handshake's first
    /// byte and third with another's second; a connection closed before sending anything is
    /// neither.
    #[tokio::test]
    async fn a_tls_handshake_is_told_from_a_hello() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hello = |len: u32| len.to_le_bytes().to_vec();
        let cases = [
            (vec![22, 3, 1, 0, 200], Some(true)),
            (vec![22, 3, 3, 0, 200], Some(true)),
            (hello(278), Some(false)),
            (hello(790), Some(false)),
            (hello(40), Some(false)),
            (vec![22, 4, 1, 0], Some(false)),
            (vec![], None),
        ];
        for (start, tls) in cases {
            let dialled = tokio::spawn(async move {
                let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                stream.write_all(&start).await.unwrap();
                stream
            });
            let (accepted, _) = listener.accept().await.unwrap();
            let stream = dialled.await.unwrap();
            if tls.is_none() {
                drop(stream);
            }
            assert_eq!(begins_tls(&accepted).await.ok(), tls);
        }
    }

    /// Plain TCP beside TLS needs a peer CA, else peers and clients are plain already, and can't
    /// be with peer IDs, since a plain peer has no certificate to name it.
    #[test]
    fn plain_beside_tls_without_a_peer_ca_or_with_peer_ids_is_refused() {
        let mut admin_only = NodeTls::admin_only("c", "k", "a");
        admin_only.plain = Plain::Accepted;
        let mut ids = NodeTls::new("c", "k", "p", "a");
        (ids.peer_ids, ids.plain) = (true, Plain::Dialled);
        // Refused for that, before the files, which aren't there, are read.
        for tls in [admin_only, ids] {
            let error = Loaded::load(&tls).err().expect("refused");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("plain TCP beside TLS"), "{error}");
        }
    }

    /// Split plain TCP, its halves go on from where the whole left off: what was read ahead past the
    /// hello, in the buffer, is read first, then the socket's; and each half works while the other
    /// waits, the reader blocked as the writer writes.
    #[tokio::test]
    async fn a_split_connection_loses_nothing_read_ahead() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let dialled = tokio::spawn(async move { connect(&addr, None, None).await.unwrap() });
        let (mut accepted, _) = accept(listener.accept().await.unwrap().0, None, None).await.unwrap();
        let mut dialled = dialled.await.unwrap();
        let frame = |byte: u8| [&3_u32.to_le_bytes()[..], &[byte; 3]].concat();
        // The hello and the frame after it in one write, read into one buffer.
        write_frames(&mut dialled, [&frame(1)[..], &frame(2)[..]]).await.unwrap();
        assert_eq!(read_frame(&mut accepted).await.unwrap(), frame(1), "the hello");

        let (mut reader, mut writer) = accepted.split();
        // Lost, it would never come: waited for a while, not for ever.
        let read_ahead = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut reader)).await;
        assert_eq!(read_ahead.expect("read ahead before the split").unwrap(), frame(2));
        let waiting = tokio::spawn(async move { (read_frame(&mut reader).await.unwrap(), reader) });
        write_frames(&mut writer, [&frame(3)[..]]).await.unwrap();
        assert_eq!(read_frame(&mut dialled).await.unwrap(), frame(3), "written while the reader waits");
        dialled.write_all(&frame(4)).await.unwrap();
        dialled.flush().await.unwrap();
        let (read, _reader) = waiting.await.unwrap();
        assert_eq!(read, frame(4), "from the socket, after the buffer");
    }

    /// A connection shut down is closed at the other end, which reads its end, though this end
    /// is still held.
    #[tokio::test]
    async fn a_connection_shut_down_is_closed_at_the_other_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let dialled = tokio::spawn(async move { connect(&addr, None, None).await.unwrap() });
        let (mut accepted, _) = accept(listener.accept().await.unwrap().0, None, None).await.unwrap();
        let mut dialled = dialled.await.unwrap();
        dialled.shutdown().await.unwrap();
        let read = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut accepted)).await;
        let error = read.expect("closed, not left open").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        drop(dialled);
    }

    /// A connection, dialled or accepted, probes its other end when idle, and on Linux gives up on
    /// what goes unacknowledged in [`super::DEAD_CONNECTION_TIMEOUT`], not the kernel's own time.
    #[tokio::test]
    async fn a_connection_finds_its_other_end_dead() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (dialled, accepted) = tokio::join!(super::connect(&addr, None, None), listener.accept());
        let super::Conn::Plain(dialled) = dialled.unwrap() else { panic!("plain TCP") };
        let (accepted, _) = super::accept(accepted.unwrap().0, None, None).await.unwrap();
        let super::Conn::Plain(accepted) = accepted else { panic!("plain TCP") };
        for stream in [dialled.get_ref().get_ref(), accepted.get_ref().get_ref()] {
            let socket = socket2::SockRef::from(stream);
            assert!(socket.keepalive().unwrap());
            #[cfg(target_os = "linux")]
            assert_eq!(socket.tcp_user_timeout().unwrap(), Some(super::DEAD_CONNECTION_TIMEOUT));
        }
    }
}

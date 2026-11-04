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
//! Only with the `tls` feature. Without it the API is the same, but loading any TLS
//! configuration ([`ClientTls::load`], or `serve` with a [`NodeTls`]) is an
//! [`io::ErrorKind::Unsupported`] error, so there is never TLS to run and every connection is
//! plain TCP.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream;

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
}

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
        }
    }

    /// TLS on the admin listener only: the node's certificate `cert` and its `key`, and the
    /// `admin_ca` that signed operators'. Peers and clients are plain TCP.
    #[must_use]
    pub fn admin_only(cert: impl Into<PathBuf>, key: impl Into<PathBuf>, admin_ca: impl Into<PathBuf>) -> Self {
        Self { cert: cert.into(), key: key.into(), peer_ca: None, admin_ca: Some(admin_ca.into()), peer_ids: false }
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

/// What a connection runs over: TCP, or TLS over TCP.
pub(crate) trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stream for T {}
pub(crate) type Conn = Box<dyn Stream>;

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
}

impl Loaded {
    pub(crate) fn load(tls: &NodeTls) -> io::Result<Self> {
        let (cluster, admin) = imp::acceptors(tls)?;
        let dial = tls.peer_ca.as_deref().map(|ca| ClientTls::load(ca, Some((&tls.cert, &tls.key)))).transpose()?;
        let (peer, client) = cluster.unzip();
        Ok(Self { peer, client, admin, dial, peer_ids: tls.peer_ids && tls.peer_ca.is_some() })
    }
}

/// A TCP connection to `addr` (`host:port`), over TLS if there's `tls`, checking the
/// certificate names `host`, and node `node` if there's one: else it's
/// [`io::ErrorKind::PermissionDenied`].
pub(crate) async fn connect(addr: &str, tls: Option<&ClientTls>, node: Option<NodeId>) -> io::Result<Conn> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
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

/// Plain TCP, buffered both ways as rustls buffers TLS: else every frame written is a system
/// call, and every one read two. Whatever writes to a [`Conn`] flushes it, as TLS needs too.
fn plain(stream: TcpStream) -> Conn {
    Box::new(BufStream::new(stream))
}

/// An accepted TCP connection, over TLS if there's `tls`, and the certificate the other end
/// presented, if it did.
pub(crate) async fn accept(stream: TcpStream, tls: Option<&Acceptor>) -> io::Result<(Conn, Option<Cert>)> {
    let _ = stream.set_nodelay(true);
    match tls {
        Some(tls) => imp::accept(tls, stream).await,
        None => Ok((plain(stream), None)),
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
        Ok(Box::new(stream))
    }

    pub(super) async fn accept(tls: &Acceptor, stream: TcpStream) -> io::Result<(Conn, Option<Cert>)> {
        let stream = tls.accept(stream).await?;
        let cert = stream.get_ref().1.peer_certificates().and_then(<[Cert]>::first).cloned();
        Ok((Box::new(stream), cert))
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
        io::Error::new(io::ErrorKind::Unsupported, "TLS: built without turbofan-cluster's `tls` feature")
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

        use super::super::{ClientTls, Loaded, NodeTls};

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
            };
            assert_eq!(Loaded::load(&node).err().map(|e| e.kind()), Some(std::io::ErrorKind::Unsupported));
        }
    }
}

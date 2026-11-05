//! A listener-backed node served over a network its caller supplies.
//!
//! `tokeirad` accepts connections on a TCP listener and dials its peers and
//! the placement controller over TCP. [`NodeTransport`] replaces all three for
//! a caller that carries the cluster's traffic over its own network, such as
//! the turmoil simulator in `tools/simulation/turmoil`. The service graph, the
//! routing and the runtime are the ones `tokeirad` builds; only the
//! connections underneath differ.
//!
//! Incoming connections arrive as one end of an in-memory pipe
//! ([`DuplexStream`]). The caller copies bytes between its own stream type and
//! the other end, so this crate needs to know nothing about that network.

use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use tokeira_edge::PeerConnector;
use tokeira_runtime::ControllerConnection;
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    net::TcpStream,
    sync::mpsc,
};
use tonic::transport::server::{Connected, TcpConnectInfo};

/// The connections of a node whose caller supplies its network.
///
/// Passed to [`crate::TokeiradHandle::start_in_memory_with_transport`].
pub struct NodeTransport {
    /// Each connection another host opens to this node, as one end of an
    /// in-memory pipe. The node stops accepting when the sender is dropped.
    pub incoming: mpsc::Receiver<DuplexStream>,
    /// Opens channels to peer nodes, for the owner-to-owner calls a
    /// controller-managed cluster makes.
    pub peers: PeerConnector,
    /// The node's connection to the placement controller, shared by its
    /// membership stream and its routing subscription. Present exactly when
    /// the node's configuration names a controller endpoint.
    pub controller: Option<ControllerConnection>,
}

impl fmt::Debug for NodeTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeTransport")
            .field("incoming", &self.incoming)
            .field("has_controller", &self.controller.is_some())
            .finish_non_exhaustive()
    }
}

/// One connection the node's gRPC server accepts.
///
/// tonic serves a single connection type, so production's TCP sockets and a
/// supplied network's pipes share this enum. Production keeps its socket's
/// address in the request extensions, which the HTTP API reads for the
/// caller's address; a pipe has none to report.
pub(crate) enum ServedConnection {
    Tcp(TcpStream),
    Pipe(DuplexStream),
}

impl Connected for ServedConnection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Self::Tcp(stream) => stream.connect_info(),
            Self::Pipe(_) => TcpConnectInfo {
                local_addr: None,
                remote_addr: None,
            },
        }
    }
}

impl AsyncRead for ServedConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Pipe(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServedConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Pipe(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    // hyper writes vectored when the connection says it can, and a TCP socket
    // can; forwarding both keeps production's writes as they were.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Pipe(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(stream) => stream.is_write_vectored(),
            Self::Pipe(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Self::Pipe(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Pipe(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

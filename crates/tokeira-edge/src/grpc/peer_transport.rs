//! Reusable channels and hop-local metadata for workflow owner RPCs.
//!
//! Channels multiplex calls without retaining request credentials. The bounded
//! cache retires idle endpoints when placement changes accumulate over time.
//! How a channel reaches its peer is a [`PeerConnector`]: production dials
//! over TCP, and a harness that carries the cluster's traffic over its own
//! network supplies its own.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use http::HeaderMap;
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};

/// Opens a lazy channel to a peer node's configured [`Endpoint`].
///
/// Production uses [`Endpoint::connect_lazy`], a TCP dial. A harness that
/// carries traffic over its own network, such as the turmoil simulator in
/// `tools/simulation/turmoil`, passes one built on
/// [`Endpoint::connect_with_connector_lazy`]. Either way the channel must stay
/// lazy, because the cache creates it while holding its lock.
pub type PeerConnector = Arc<dyn Fn(&Endpoint) -> Channel + Send + Sync>;

pub(crate) struct PeerChannels {
    entries: Mutex<HashMap<String, (Channel, Instant)>>,
    connect: PeerConnector,
}

impl Default for PeerChannels {
    fn default() -> Self {
        Self::with_connector(Arc::new(Endpoint::connect_lazy))
    }
}

impl fmt::Debug for PeerChannels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerChannels")
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

impl PeerChannels {
    pub(crate) fn with_connector(connect: PeerConnector) -> Self {
        Self {
            entries: Mutex::default(),
            connect,
        }
    }

    pub(crate) fn get(&self, authority: &str) -> Result<Channel, Status> {
        let mut entries = self.entries.lock().expect("peer channel cache poisoned");
        if let Some((channel, used)) = entries.get_mut(authority) {
            *used = Instant::now();
            return Ok(channel.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{authority}"))
            .map_err(|_| Status::unavailable("workflow-task owner endpoint is invalid"))?
            .connect_timeout(Duration::from_secs(5));
        let channel = (self.connect)(&endpoint);
        if entries.len() >= 256 {
            let oldest = entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(authority.to_owned(), (channel.clone(), Instant::now()));
        Ok(channel)
    }
}

pub(crate) fn start_metadata(mut headers: HeaderMap) -> tonic::metadata::MetadataMap {
    // Authenticators may consume custom headers. Retain those application
    // headers, but never copy gRPC/HTTP transport negotiation to another hop.
    let remove: Vec<_> = headers
        .keys()
        .filter(|name| {
            name.as_str().starts_with("grpc-")
                || matches!(
                    name.as_str(),
                    "host"
                        | "content-length"
                        | "content-type"
                        | "connection"
                        | "te"
                        | "transfer-encoding"
                        | "trailer"
                        | "upgrade"
                        | "keep-alive"
                )
        })
        .cloned()
        .collect();
    for name in remove {
        headers.remove(name);
    }
    tonic::metadata::MetadataMap::from_headers(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptors::{AllowAllAuthenticator, Authenticator};

    #[test]
    fn authentication_allowlist_does_not_copy_poll_framing() {
        let mut headers = HeaderMap::new();
        for name in [
            "authorization",
            "authorization-extras",
            "grpc-encoding",
            "grpc-accept-encoding",
            "grpc-timeout",
            "x-unrelated",
        ] {
            headers.insert(name, "synthetic".parse().unwrap());
        }
        let selected = AllowAllAuthenticator.forwarding_headers(&headers);
        assert_eq!(selected.len(), 2);
        let metadata = start_metadata(headers);
        assert!(!metadata.contains_key("grpc-encoding"));
        assert!(!metadata.contains_key("grpc-accept-encoding"));
        assert!(!metadata.contains_key("grpc-timeout"));
        assert!(metadata.contains_key("authorization"));
    }
}

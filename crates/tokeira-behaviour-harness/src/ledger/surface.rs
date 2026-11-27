//! Descriptor enumeration joined to the canonical feature catalog, never a local RPC list.

use prost::Message;
use prost_types::FileDescriptorSet;
use tokeira_compatibility::{
    FEATURE_MATRIX, FeatureEntry, NEWER_VENDORED_WIRE_RPCS, verify_feature_catalog,
};

use super::{LedgerError, invalid};

pub(super) struct Rpc {
    pub key: String,
    pub newer: bool,
    pub feature: &'static FeatureEntry,
}

pub(super) fn rpcs() -> Result<Vec<Rpc>, LedgerError> {
    let descriptors = FileDescriptorSet::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .map_err(|error| invalid(format!("public descriptors: {error}")))?;
    let mut keys = Vec::new();
    for file in descriptors.file {
        for service in &file.service {
            // The descriptor also contains imported server-internal services.
            // The public contract is these two vendored API services; enumerate
            // every method within them, including methods absent from the matrix.
            if !matches!(
                (file.package(), service.name()),
                ("temporal.api.workflowservice.v1", "WorkflowService")
                    | ("temporal.api.operatorservice.v1", "OperatorService")
            ) {
                continue;
            }
            for method in &service.method {
                keys.push((
                    format!("/{}.{}/{}", file.package(), service.name(), method.name()),
                    format!("{}.{}", service.name(), method.name()),
                ));
            }
        }
    }
    let short: Vec<_> = keys.iter().map(|(_, short)| short.as_str()).collect();
    verify_feature_catalog(FEATURE_MATRIX, &short, NEWER_VENDORED_WIRE_RPCS)
        .map_err(|error| invalid(error.to_string()))?;
    keys.sort();
    Ok(keys
        .into_iter()
        .map(|(key, short)| Rpc {
            key,
            newer: NEWER_VENDORED_WIRE_RPCS.contains(&short.as_str()),
            feature: FEATURE_MATRIX
                .iter()
                .find(|feature| feature.rpcs.contains(&short.as_str()))
                .expect("verified unique ownership"),
        })
        .collect())
}

use http::{HeaderMap, HeaderName, HeaderValue};
use tonic::metadata::{KeyAndValueRef, MetadataMap};

/// Convert tonic metadata into an HTTP header map for the existing edge pipeline.
pub fn metadata_to_header_map(metadata: &MetadataMap) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(metadata.len());

    for entry in metadata.iter() {
        let (name, value) = match entry {
            KeyAndValueRef::Ascii(name, value) => (name.as_str(), value.as_encoded_bytes()),
            KeyAndValueRef::Binary(name, value) => (name.as_str(), value.as_encoded_bytes()),
        };

        let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value) else {
            continue;
        };

        headers.append(name, value);
    }

    headers
}

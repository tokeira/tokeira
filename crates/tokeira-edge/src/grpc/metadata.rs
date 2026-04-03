use http::{HeaderMap, HeaderName, HeaderValue};
use tonic::metadata::{KeyAndValueRef, MetadataMap};

/// Convert tonic metadata into an HTTP header map for the existing edge pipeline.
pub fn metadata_to_header_map(metadata: &MetadataMap) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(metadata.len());

    for entry in metadata.iter() {
        let (name, value) = match entry {
            KeyAndValueRef::Ascii(name, value) => {
                (name.as_str(), value.as_encoded_bytes())
            }
            KeyAndValueRef::Binary(name, value) => {
                (name.as_str(), value.as_encoded_bytes())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_request_id_and_authorization() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-request-id", "req-123".parse().unwrap());
        metadata.insert("authorization", "Bearer token".parse().unwrap());

        let headers = metadata_to_header_map(&metadata);
        assert_eq!(headers.get("x-request-id").unwrap(), "req-123");
        assert_eq!(headers.get("authorization").unwrap(), "Bearer token");
    }
}

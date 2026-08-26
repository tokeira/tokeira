//! Canonical physical encoding for binary DSQL identities.
//!
//! Aurora DSQL stores `BYTEA` values but does not support indexing them. Tokeira
//! therefore represents binary identities that participate in primary or
//! secondary keys as lowercase hexadecimal `TEXT`. Two hexadecimal characters
//! encode each byte, so the mapping is stable and lossless. CHASM paths do not
//! use this expansion: their canonical representation is already UTF-8 text.

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Encode arbitrary bytes as canonical lowercase hexadecimal text.
pub(super) fn encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::encode;

    proptest! {
        #[test]
        fn hexadecimal_key_preserves_byte_order(
            left in proptest::collection::vec(any::<u8>(), 0..128),
            right in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            prop_assert_eq!(encode(&left).cmp(&encode(&right)), left.cmp(&right));
        }

        #[test]
        fn hexadecimal_key_preserves_prefixes(
            prefix in proptest::collection::vec(any::<u8>(), 0..128),
            suffix in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let mut joined = prefix.clone();
            joined.extend_from_slice(&suffix);
            prop_assert!(encode(&joined).starts_with(&encode(&prefix)));
        }
    }

    #[test]
    fn hexadecimal_key_has_a_stable_known_encoding() {
        assert_eq!(encode(&[0x00, 0x0f, 0x10, 0xab, 0xff]), "000f10abff");
    }
}

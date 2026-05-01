//! DSQL-friendly spread-key helpers.
//!
//! Aurora DSQL distributes rows by primary key. These helpers derive stable
//! UUIDv8 keys from logical identifiers so hot logical prefixes do not become
//! hot physical key ranges.

use uuid::Uuid;

const DOMAIN_TAG: &[u8] = b"tokeira/dsql-key/v1\0";

/// Derive a deterministic UUIDv8 from length-prefixed byte parts.
#[must_use]
pub fn dsql_spread_uuid(parts: &[&[u8]]) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_TAG);
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }

    let hash = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::dsql_spread_uuid;

    fn hamming_distance(a: uuid::Uuid, b: uuid::Uuid) -> u32 {
        a.as_bytes()
            .iter()
            .zip(b.as_bytes())
            .map(|(left, right)| (left ^ right).count_ones())
            .sum()
    }

    #[test]
    fn empty_and_single_empty_part_are_distinct_uuidv8_values() {
        let empty = dsql_spread_uuid(&[]);
        let single_empty = dsql_spread_uuid(&[b""]);

        assert_ne!(empty, single_empty);
        assert_eq!(empty.get_version_num(), 8);
        assert_eq!(single_empty.get_version_num(), 8);
    }

    proptest! {
        #[test]
        fn deterministic(parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16)) {
            let refs = parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
            prop_assert_eq!(dsql_spread_uuid(&refs), dsql_spread_uuid(&refs));
        }

        #[test]
        fn length_prefix_prevents_boundary_collisions(bytes in prop::collection::vec(any::<u8>(), 2..128), split in 1usize..127) {
            let split = split.min(bytes.len() - 1);
            let one_part = dsql_spread_uuid(&[bytes.as_slice()]);
            let two_parts = dsql_spread_uuid(&[&bytes[..split], &bytes[split..]]);
            prop_assert_ne!(one_part, two_parts);
        }

        #[test]
        fn uuidv8_format_invariant(parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16)) {
            let refs = parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let uuid = dsql_spread_uuid(&refs);
            prop_assert_eq!(uuid.get_version_num(), 8);
            prop_assert_eq!(uuid.as_bytes()[8] & 0xc0, 0x80);
        }

        #[test]
        fn single_bit_flip_has_avalanche_behavior(mut bytes in prop::collection::vec(any::<u8>(), 1..128), bit in 0usize..1024) {
            let bit = bit % (bytes.len() * 8);
            let original = dsql_spread_uuid(&[bytes.as_slice()]);
            bytes[bit / 8] ^= 1 << (bit % 8);
            let changed = dsql_spread_uuid(&[bytes.as_slice()]);
            let distance = hamming_distance(original, changed);
            prop_assert!((30..=98).contains(&distance), "distance={distance}");
        }
    }
}

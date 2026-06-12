//! The node path encoder.
//!
//! Every node in an Execution's tree is keyed by an **encoded path** whose byte
//! sort order makes any subtree, collection, or ancestor chain a single prefix
//! range scan. `$` introduces a child field and `#` introduces a collection
//! (map) child; the separators sort **below** every byte that can begin a
//! path-segment character, so the half-open range `[encode(path), subtree_end)`
//! returns exactly the subtree rooted at `path` and nothing else
//! (Requirement 4.2, 4.3).
//!
//! This is a tokeira-owned implementation that reproduces the `v1.31.0`
//! sort/separator **contract** (`chasm/path_encoder.go:25-75 @ v1.31.0`) without
//! porting Temporal code (Requirement 4.5, `AGENTS §8`). The contract has three
//! load-bearing parts, all verified against that source:
//!
//! 1. **Two adjacent separators.** `#` (`0x23`) for a collection child, `$`
//!    (`0x24`) for a field child. They are adjacent in value so that, after
//!    escaping, the field children and collection children of a node are grouped
//!    contiguously.
//! 2. **Escape `\`, `$`, `#`, and every code point `< '#'`.** Concretely the
//!    escape predicate is `c == '\\' || c == '$' || c <= '#'` (the `c <= '#'`
//!    clause subsumes `#` itself and every control/punctuation code point below
//!    it). Escaping is a single `\` (`0x5C`) prefix. The consequence is the whole
//!    point of the scheme: the smallest byte that can begin a *raw* segment
//!    character is `%` (`0x25`) — every smaller code point is escaped and an
//!    escape begins with `\` (`0x5C`), which is itself larger than the
//!    separators. So **both separators sort strictly below any segment-content
//!    byte**, which is what makes a parent's encoding a true prefix of all its
//!    descendants and of nothing else.
//! 3. **The subtree upper bound is `encode(path)` + `%`.** Because `%` (`0x25`)
//!    is the minimal value strictly greater than both separators, a sibling whose
//!    *string* extends the parent's encoding (e.g. node `foo` vs. sibling `foox`)
//!    begins, at the divergence point, with a byte `>= '%'` and so sorts at or
//!    after `encode(parent) + '%'`, while every descendant begins with a
//!    separator `< '%'` and so sorts before it. This is why a single byte
//!    suffices to close the range; see [`subtree_range_end`].
//!
//! ## Round-trip and the first segment
//!
//! [`encode`] writes no separator before the first segment (a direct child of the
//! root is always a field child; the root is never a collection), so [`decode`]
//! assigns [`SegmentKind::Field`] to the first segment. A well-formed path
//! therefore has `path[0].kind == SegmentKind::Field`, and under that
//! precondition `decode(encode(path)) == path` and `encode(decode(bytes)) ==
//! bytes` hold losslessly. The kind of the first segment is otherwise ignored by
//! [`encode`]; construct root children with [`PathSegment::field`].

use serde::{Deserialize, Serialize};

use crate::ChasmError;

/// Separator that introduces a **field** child node (`$`, `0x24`).
///
/// Written before every non-first [`SegmentKind::Field`] segment. Larger than
/// [`COLLECTION_SEPARATOR`] by one, so a node's field children and collection
/// children form two adjacent contiguous groups under byte sort.
pub const NAME_SEPARATOR: u8 = b'$';

/// Separator that introduces a **collection (map)** child node (`#`, `0x23`).
///
/// Written before every non-first [`SegmentKind::Collection`] segment. It is the
/// smallest separator, so collection children sort immediately before field
/// children of the same parent.
pub const COLLECTION_SEPARATOR: u8 = b'#';

/// The escape prefix (`\`, `0x5C`) written before any segment character that
/// would otherwise collide with a separator or undermine the sort order.
pub const ESCAPE_CHAR: u8 = b'\\';

/// The byte appended to an encoded path to form the exclusive upper bound of its
/// subtree range scan (`%`, `0x25`).
///
/// It is the minimal value strictly greater than both separators, which is
/// exactly what makes `[encode(path), encode(path) + SUBTREE_RANGE_END_SUFFIX)`
/// capture the node and all its descendants while excluding siblings whose
/// encoding string-extends the node's (`path_encoder.go:25-44 @ v1.31.0`).
const SUBTREE_RANGE_END_SUFFIX: u8 = b'%';

/// Whether a path segment is reached through a field separator or a collection
/// separator — i.e. which of `$`/`#` introduces it.
///
/// This is the field-vs-collection distinction the encoding carries in the
/// separator preceding a segment. The root's direct children are always
/// [`SegmentKind::Field`] (the root is never a collection), so the first segment
/// of any path is `Field`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SegmentKind {
    /// A field child, introduced by [`NAME_SEPARATOR`] (`$`).
    Field,
    /// A collection (map) child, introduced by [`COLLECTION_SEPARATOR`] (`#`).
    Collection,
}

/// One component of a node path: a non-empty name plus the kind of separator that
/// introduces it.
///
/// A node's full path is a `&[PathSegment]`; the root is the empty slice. The
/// `kind` records whether the segment is a field child or a collection child,
/// which the encoder maps to `$`/`#` (for every segment after the first) and the
/// decoder recovers from the separator it splits on. `name` MUST be non-empty —
/// [`encode`] rejects an empty name, mirroring the `v1.31.0` contract
/// (`path_encoder.go:60 @ v1.31.0`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathSegment {
    /// The segment name (a field name or a map key). Must be non-empty.
    pub name: String,
    /// Whether this segment is a field child (`$`) or a collection child (`#`).
    pub kind: SegmentKind,
}

impl PathSegment {
    /// Construct a field-child segment (introduced by `$`).
    pub fn field(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SegmentKind::Field,
        }
    }

    /// Construct a collection-child segment (introduced by `#`).
    pub fn collection(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SegmentKind::Collection,
        }
    }
}

/// True for any code point that must be escaped to preserve the sort contract.
///
/// Escapes the escape char itself (`\`), the field separator (`$`), and — via the
/// `c <= '#'` clause — the collection separator and every code point below it.
/// After escaping, the smallest byte that can begin a raw segment character is
/// `%` (`0x25`), strictly above both separators (`path_encoder.go:71-75 @
/// v1.31.0`).
#[inline]
fn must_escape(c: char) -> bool {
    c == ESCAPE_CHAR as char || c == NAME_SEPARATOR as char || c <= COLLECTION_SEPARATOR as char
}

/// The node path encoder: the canonical handle for turning a node path into its
/// prefix-range-scannable byte key and back.
///
/// This is a zero-sized, stateless handle — every method is a pure function of
/// its inputs — that mirrors the shape of the `v1.31.0` `NodePathEncoder`
/// interface and its `defaultPathEncoder` implementor
/// (`path_encoder.go:9-13,25-75 @ v1.31.0`). It is the entry point the node tree
/// and the storage node table use so their range scans share one contract; the
/// module-level free functions ([`encode`], [`decode`], [`subtree_range_end`])
/// are the underlying implementation and remain available for call sites that do
/// not hold a handle.
///
/// # Range-scan property (Property 8)
///
/// The encoder's sole reason to exist is this guarantee, which the design names
/// **Property 8 (Node range-scan correctness)** and the property test
/// `prop_path_encoder_order` verifies (Requirement 4.3, 4.4): for any path `p`,
/// the half-open byte range `[encode(p), subtree_range_end(encode(p)))` contains
/// exactly the node at `p` and every descendant of `p` — no sibling, no
/// unrelated node. A `#`-introduced prefix likewise bounds exactly a collection's
/// immediate children. This holds because, after escaping, both separators sort
/// strictly below every byte that can begin a segment-content character (see the
/// module doc), so a parent's encoding is a true byte-prefix of all its
/// descendants' encodings and of nothing else. Subtree loads, collection loads,
/// and ancestor walks therefore each reduce to a single prefix range scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct PathEncoder;

impl PathEncoder {
    /// Construct the (stateless) path encoder.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Encode a node path into its prefix-range-scannable byte key.
    ///
    /// Delegates to the module-level [`encode`]; see it for the full contract and
    /// error behaviour.
    ///
    /// # Errors
    ///
    /// Returns [`ChasmError::Internal`] if any segment name is empty
    /// (`path_encoder.go:60 @ v1.31.0`).
    pub fn encode(&self, path: &[PathSegment]) -> Result<Vec<u8>, ChasmError> {
        encode(path)
    }

    /// Decode a byte key back into its node path.
    ///
    /// Delegates to the module-level [`decode`]; see it for the full contract and
    /// error behaviour.
    ///
    /// # Errors
    ///
    /// - [`ChasmError::Validation`] if `encoded` is not valid UTF-8.
    /// - [`ChasmError::Internal`] if the key ends with a dangling escape character
    ///   (`path_encoder.go:108-110 @ v1.31.0`).
    pub fn decode(&self, encoded: &[u8]) -> Result<Vec<PathSegment>, ChasmError> {
        decode(encoded)
    }

    /// Compute the exclusive upper bound of the subtree range scan for an encoded
    /// path.
    ///
    /// Delegates to the module-level [`subtree_range_end`]; the returned bound
    /// closes the half-open range that realizes Property 8 (see the type doc).
    #[must_use]
    pub fn subtree_range_end(&self, encoded_path: &[u8]) -> Vec<u8> {
        subtree_range_end(encoded_path)
    }
}

/// Encode a node path into its prefix-range-scannable byte key.
///
/// Writes each segment's name with escaping, separated by `$` (field child) or
/// `#` (collection child) according to the segment's [`SegmentKind`]. No
/// separator precedes the first segment. The root path (`&[]`) encodes to the
/// empty key.
///
/// # Errors
///
/// Returns [`ChasmError::Internal`] if any segment name is empty, matching the
/// `v1.31.0` contract which rejects empty node names rather than emitting an
/// ambiguous key (`path_encoder.go:60 @ v1.31.0`).
pub fn encode(path: &[PathSegment]) -> Result<Vec<u8>, ChasmError> {
    let mut out = Vec::new();
    for (i, segment) in path.iter().enumerate() {
        if segment.name.is_empty() {
            return Err(ChasmError::Internal(format!(
                "path contains empty node name at index {i}"
            )));
        }

        // No separator precedes the first segment: the root is never a
        // collection, so its children are bare field names (path_encoder.go:55-66
        // @ v1.31.0). For later segments the separator carries the field-vs-
        // collection distinction the decoder recovers.
        if i > 0 {
            match segment.kind {
                SegmentKind::Field => out.push(NAME_SEPARATOR),
                SegmentKind::Collection => out.push(COLLECTION_SEPARATOR),
            }
        }

        let mut buf = [0u8; 4];
        for c in segment.name.chars() {
            if must_escape(c) {
                out.push(ESCAPE_CHAR);
            }
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        }
    }
    Ok(out)
}

/// Decode a byte key back into its node path.
///
/// Inverse of [`encode`]: splits on unescaped `$`/`#` separators, recovering each
/// segment's [`SegmentKind`] from the separator that introduced it (the first
/// segment is always [`SegmentKind::Field`], as it has no leading separator). The
/// empty key decodes to the root path (`&[]`).
///
/// # Errors
///
/// - [`ChasmError::Validation`] if `encoded` is not valid UTF-8.
/// - [`ChasmError::Internal`] if the key ends with a dangling escape character,
///   which cannot be produced by [`encode`] and indicates a corrupt key
///   (`path_encoder.go:108-110 @ v1.31.0`).
pub fn decode(encoded: &[u8]) -> Result<Vec<PathSegment>, ChasmError> {
    let encoded = std::str::from_utf8(encoded)
        .map_err(|e| ChasmError::Validation(format!("encoded path is not valid UTF-8: {e}")))?;

    if encoded.is_empty() {
        return Ok(Vec::new());
    }

    let mut path = Vec::new();
    let mut buf = String::new();
    // The kind of the segment currently being accumulated. The first segment has
    // no leading separator and is always a field child.
    let mut pending_kind = SegmentKind::Field;
    let mut escaped = false;

    for c in encoded.chars() {
        if escaped {
            buf.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '$' | '#' => {
                path.push(PathSegment {
                    name: std::mem::take(&mut buf),
                    kind: pending_kind,
                });
                // The separator just consumed introduces the *next* segment.
                pending_kind = if c == '#' {
                    SegmentKind::Collection
                } else {
                    SegmentKind::Field
                };
            }
            _ => buf.push(c),
        }
    }

    if escaped {
        return Err(ChasmError::Internal(
            "encoded path ends with a dangling escape character".to_string(),
        ));
    }

    path.push(PathSegment {
        name: buf,
        kind: pending_kind,
    });
    Ok(path)
}

/// Compute the exclusive upper bound of the subtree range scan for an encoded
/// path.
///
/// A range query `encoded_path >= encode(path) AND encoded_path < subtree_range_end(encode(path))`
/// returns exactly the node at `path` and every descendant, and no sibling. The
/// bound is `encode(path)` with `%` (`0x25`) appended: `%` is the minimal byte
/// strictly above both separators, so descendants (which continue with a
/// separator `< '%'`) fall inside the range while siblings whose encoding
/// string-extends the node's (continuing with a content byte `>= '%'`) fall
/// outside it (`path_encoder.go:38-44 @ v1.31.0`; Requirement 4.3, 4.4).
pub fn subtree_range_end(encoded_path: &[u8]) -> Vec<u8> {
    let mut end = encoded_path.to_vec();
    end.push(SUBTREE_RANGE_END_SUFFIX);
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str) -> PathSegment {
        PathSegment::field(name)
    }

    fn collection(name: &str) -> PathSegment {
        PathSegment::collection(name)
    }

    #[test]
    fn encodes_known_shapes() {
        assert_eq!(encode(&[]).unwrap(), b"");
        assert_eq!(encode(&[field("input")]).unwrap(), b"input");
        assert_eq!(
            encode(&[field("a"), field("b"), field("c")]).unwrap(),
            b"a$b$c"
        );
        // A map entry: "attempts" is a field of the root, "0001" a collection
        // child introduced by '#'.
        assert_eq!(
            encode(&[field("attempts"), collection("0001")]).unwrap(),
            b"attempts#0001"
        );
    }

    #[test]
    fn round_trips_through_decode() {
        let cases = vec![
            vec![],
            vec![field("input")],
            vec![field("state")],
            vec![field("a"), field("b"), field("c")],
            vec![field("attempts"), collection("0001")],
            vec![field("attempts"), collection("0001"), field("details")],
        ];
        for path in cases {
            let encoded = encode(&path).unwrap();
            assert_eq!(decode(&encoded).unwrap(), path, "round-trip for {path:?}");
            // Bytes -> path -> bytes is also lossless.
            assert_eq!(encode(&decode(&encoded).unwrap()).unwrap(), encoded);
        }
    }

    #[test]
    fn escapes_separators_and_low_code_points() {
        // '$', '#', '\\', and a control char all escape; the escaped byte is
        // preceded by a single backslash and the value round-trips exactly.
        for name in ["a$b", "a#b", "a\\b", "a\tb", "a b", "a\"b", "a!b"] {
            let path = vec![field(name)];
            let encoded = encode(&path).unwrap();
            assert_eq!(
                decode(&encoded).unwrap(),
                path,
                "escape round-trip {name:?}"
            );
        }

        // Spot-check the exact escaped form for a '$' in a name.
        assert_eq!(encode(&[field("a$b")]).unwrap(), b"a\\$b");
        // A '#' is escaped via the `c <= '#'` clause.
        assert_eq!(encode(&[field("a#b")]).unwrap(), b"a\\#b");
    }

    #[test]
    fn child_sorts_within_parent_subtree_range() {
        // The encoder's central guarantee: a child's key lies in
        // [parent, subtree_range_end(parent)) while a sibling whose name
        // string-extends the parent's lies outside it.
        let parent = encode(&[field("foo")]).unwrap();
        let end = subtree_range_end(&parent);

        let field_child = encode(&[field("foo"), field("bar")]).unwrap();
        let collection_child = encode(&[field("foo"), collection("0001")]).unwrap();
        let deep_child = encode(&[field("foo"), field("bar"), field("baz")]).unwrap();

        for child in [&field_child, &collection_child, &deep_child] {
            assert!(child.as_slice() >= parent.as_slice(), "{child:?} >= parent");
            assert!(child.as_slice() < end.as_slice(), "{child:?} < range end");
        }

        // The node itself is the inclusive lower bound.
        assert!(parent.as_slice() >= parent.as_slice());
        assert!(parent.as_slice() < end.as_slice());

        // A sibling whose encoding string-extends "foo" must fall outside.
        let sibling = encode(&[field("foox")]).unwrap();
        assert!(sibling.as_slice() >= end.as_slice(), "sibling excluded");
    }

    #[test]
    fn collection_children_group_below_field_children() {
        // Collection separator '#' (0x23) < field separator '$' (0x24), so a
        // node's collection children all sort before its field children — both
        // contiguous, both inside the parent's subtree range.
        let parent = encode(&[field("node")]).unwrap();
        let end = subtree_range_end(&parent);

        let collection_child = encode(&[field("node"), collection("k")]).unwrap();
        let field_child = encode(&[field("node"), field("f")]).unwrap();

        assert!(collection_child.as_slice() < field_child.as_slice());
        assert!(collection_child.as_slice() >= parent.as_slice());
        assert!(field_child.as_slice() < end.as_slice());
    }

    #[test]
    fn empty_segment_name_is_rejected() {
        let err = encode(&[field("")]).unwrap_err();
        assert!(matches!(err, ChasmError::Internal(_)));
    }

    #[test]
    fn dangling_escape_is_rejected() {
        // A lone trailing backslash cannot be produced by `encode`; decode
        // rejects it rather than silently dropping it.
        let err = decode(b"abc\\").unwrap_err();
        assert!(matches!(err, ChasmError::Internal(_)));
    }

    #[test]
    fn encoder_handle_matches_free_functions() {
        // The `PathEncoder` handle is a thin delegation over the module
        // functions; assert they agree so downstream call sites can pick either.
        let enc = PathEncoder::new();
        let path = vec![field("attempts"), collection("0001"), field("details")];
        let bytes = enc.encode(&path).unwrap();
        assert_eq!(bytes, encode(&path).unwrap());
        assert_eq!(enc.decode(&bytes).unwrap(), path);
        assert_eq!(enc.subtree_range_end(&bytes), subtree_range_end(&bytes));
    }
}

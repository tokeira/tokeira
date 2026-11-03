//! Values too large for one DSQL column, stored in parts (`large-value-parts`).
//!
//! DSQL refuses any value over 1 MiB, and a row that holds two values of 1 MiB. A
//! value of at most [`PART_BYTES`] is stored whole in its own column, as every
//! value was before parts existed. A larger value is cut into parts of
//! `PART_BYTES`, the last one shorter: its first part stays in its own column, its
//! other parts, numbered from 1, go to a parts table, and a count column beside the
//! value says how many parts there are. A row written before parts existed has no
//! count, and its column holds the whole value.
//!
//! The part size leaves room in a row for two first parts, as a history batch's
//! events and principals each have one, with the row's other columns: two parts
//! come to 1,966,080 bytes, and a row of 2,000,000 bytes of values commits.
//!
//! Writers write a value's row and all its parts in one transaction. Readers join
//! a split value's parts in the transaction that reads its row, so a reader never
//! sees a count without its parts, nor the parts of two versions of a value.
//! [`join`] checks only that the parts fit together; their consistency rests on
//! those transactions.

/// The most bytes one part holds, in a value's own column or in a parts table:
/// 960 KiB, under DSQL's 1 MiB value limit with room for two parts in one row.
pub const PART_BYTES: usize = 983_040;

// A part fits DSQL's value limit. That a row of two parts commits is DSQL's to
// say: a live test writes one.
const _: () = assert!(PART_BYTES <= crate::write_budget::DSQL_MAX_VALUE_BYTES);

/// A value as it is stored.
#[derive(Debug, PartialEq, Eq)]
pub struct Split<'a> {
    /// What the value's own column holds: the whole value, or its first part.
    pub first: &'a [u8],
    /// The parts after the first, in order: `rest[0]` is part 1. Empty for a whole
    /// value.
    pub rest: Vec<&'a [u8]>,
    /// What the value's count column holds: `None` for a whole value, otherwise
    /// the number of parts, two or more.
    pub count: Option<i32>,
}

impl<'a> Split<'a> {
    /// The parts after the first, each with its part number.
    pub fn numbered(&self) -> impl Iterator<Item = (i32, &'a [u8])> {
        (1..).zip(self.rest.iter().copied())
    }

    /// The number of parts, a whole value counting as one.
    pub fn parts(&self) -> i32 {
        self.count.unwrap_or(1)
    }
}

/// The number of parts [`split`] cuts a value of `len` bytes into, a whole value
/// counting as one: the rows the value takes, its own and one per other part.
pub fn parts_of(len: usize) -> usize {
    len.div_ceil(PART_BYTES).max(1)
}

/// Cut `value` as it is stored: whole when it is at most [`PART_BYTES`], otherwise
/// into parts of `PART_BYTES` and a shorter last part.
pub fn split(value: &[u8]) -> Split<'_> {
    if value.len() <= PART_BYTES {
        return Split {
            first: value,
            rest: Vec::new(),
            count: None,
        };
    }
    let mut parts = value.chunks(PART_BYTES);
    let first = parts
        .next()
        .expect("a value over the part size has a first part");
    let rest = parts.collect::<Vec<_>>();
    let count = i32::try_from(rest.len() + 1).expect("a stored value's part count fits an i32");
    Split {
        first,
        rest,
        count: Some(count),
    }
}

/// A split value whose parts don't fit together.
///
/// Every writer writes a value's row and all its parts in one transaction, so this
/// means a corrupt value; the value is not returned.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{value} is stored in parts that don't fit together: {defect}")]
pub struct ValuePartsError {
    /// The value, by its column and its row, such as
    /// `workflow_hot.state_data of run <run key>`.
    pub value: String,
    /// What is wrong with the parts.
    pub defect: String,
}

/// Rebuild a value from what its own column holds, its count and its other parts,
/// each with its part number, in part-number order.
///
/// Refuses the parts unless they are what [`split`] gives for some value: no count
/// and no other parts, or a count of two or more, parts numbered from 1 to one less
/// than the count, each part but the last holding exactly [`PART_BYTES`], and the
/// last between 1 and `PART_BYTES`. `value` names the value in the error.
pub fn join(
    value: impl FnOnce() -> String,
    first: Vec<u8>,
    count: Option<i32>,
    rest: Vec<(i32, Vec<u8>)>,
) -> Result<Vec<u8>, ValuePartsError> {
    let refuse = |defect: String| ValuePartsError {
        value: value(),
        defect,
    };
    let Some(count) = count else {
        if !rest.is_empty() {
            return Err(refuse(format!(
                "it has no count, but {} other parts",
                rest.len()
            )));
        }
        return Ok(first);
    };
    if count < 2 {
        return Err(refuse(format!("its count is {count}")));
    }
    let expected = usize::try_from(count - 1).expect("a count of two or more is positive");
    if rest.len() != expected {
        return Err(refuse(format!(
            "its count is {count}, but it has {} parts after the first",
            rest.len()
        )));
    }
    if first.len() != PART_BYTES {
        return Err(refuse(format!(
            "its first part holds {} bytes, not {PART_BYTES}",
            first.len()
        )));
    }
    let mut joined = first;
    joined.reserve(expected * PART_BYTES);
    for (index, (part_no, data)) in rest.into_iter().enumerate() {
        let wanted = i32::try_from(index + 1).expect("a part number fits an i32");
        if part_no != wanted {
            return Err(refuse(format!(
                "its part {wanted} is missing, and part {part_no} is in its place"
            )));
        }
        let last = index + 1 == expected;
        let fits = if last {
            (1..=PART_BYTES).contains(&data.len())
        } else {
            data.len() == PART_BYTES
        };
        if !fits {
            return Err(refuse(format!(
                "its part {part_no} of {count} holds {} bytes",
                data.len()
            )));
        }
        joined.extend_from_slice(&data);
    }
    Ok(joined)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// `len` bytes of a pattern that differs from part to part, so parts that
    /// change places change the value.
    fn value(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        let mut bytes = Vec::with_capacity(len + 8);
        while bytes.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.extend_from_slice(&state.to_le_bytes());
        }
        bytes.truncate(len);
        bytes
    }

    /// A value's first part, count and other parts, owned, as a reader fetches
    /// them.
    type Stored = (Vec<u8>, Option<i32>, Vec<(i32, Vec<u8>)>);

    fn stored(value: &[u8]) -> Stored {
        let split = split(value);
        (
            split.first.to_vec(),
            split.count,
            split
                .numbered()
                .map(|(part_no, data)| (part_no, data.to_vec()))
                .collect(),
        )
    }

    fn joined((first, count, rest): Stored) -> Result<Vec<u8>, ValuePartsError> {
        join(|| "the test value".to_owned(), first, count, rest)
    }

    /// Lengths around every multiple of the part size up to 9 MiB, and any length
    /// up to 3 MiB.
    fn lengths() -> impl Strategy<Value = usize> {
        prop_oneof![
            (0usize..=9, -2isize..=2)
                .prop_map(|(parts, offset)| { (parts * PART_BYTES).saturating_add_signed(offset) }),
            0usize..=3 * 1024 * 1024,
        ]
    }

    #[test]
    fn a_value_up_to_the_part_size_is_stored_whole() {
        for len in [0, 1, PART_BYTES - 1, PART_BYTES] {
            let value = value(len, 7);
            let split = split(&value);
            assert_eq!(split.first, value.as_slice());
            assert!(split.rest.is_empty());
            assert_eq!(split.count, None);
            assert_eq!(split.parts(), 1);
        }
    }

    #[test]
    fn a_value_one_byte_over_takes_a_second_part_of_one_byte() {
        let value = value(PART_BYTES + 1, 11);
        let split = split(&value);
        assert_eq!(split.count, Some(2));
        assert_eq!(split.first.len(), PART_BYTES);
        assert_eq!(split.rest, vec![&value[PART_BYTES..]]);
        assert_eq!(
            split
                .numbered()
                .map(|(part_no, _)| part_no)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn a_join_names_the_value_it_refuses() {
        let error = join(
            || "workflow_hot.state_data of run r".to_owned(),
            vec![0; 3],
            Some(1),
            Vec::new(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "workflow_hot.state_data of run r is stored in parts that don't fit together: its \
             count is 1"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // Feature: large-value-parts, Property 1: Splitting and joining a value gives
        // the value back
        #[test]
        fn splitting_and_joining_gives_the_value_back(len in lengths(), seed in any::<u64>()) {
            let value = value(len, seed);
            let split = split(&value);
            if len <= PART_BYTES {
                prop_assert_eq!(split.count, None);
                prop_assert!(split.rest.is_empty());
            } else {
                let count = split.count.expect("a value over the part size has a count");
                prop_assert!(count >= 2);
                prop_assert_eq!(usize::try_from(count).unwrap(), len.div_ceil(PART_BYTES));
                prop_assert_eq!(split.first.len(), PART_BYTES);
                let (last, middle) = split.rest.split_last().expect("a second part");
                prop_assert!(middle.iter().all(|part| part.len() == PART_BYTES));
                prop_assert!((1..=PART_BYTES).contains(&last.len()));
            }
            prop_assert_eq!(usize::try_from(split.parts()).unwrap(), parts_of(len));
            prop_assert_eq!(joined(stored(&value)).expect("joins"), value);
        }

        // Feature: large-value-parts, Property 2: A join refuses parts that don't fit
        // together
        #[test]
        fn a_join_refuses_parts_that_dont_fit_together(
            parts in 2usize..=9,
            tail in 1usize..=PART_BYTES,
            change in 0usize..9,
            pick in any::<usize>(),
            seed in any::<u64>(),
        ) {
            let value = value((parts - 1) * PART_BYTES + tail, seed);
            let (first, count, mut rest) = stored(&value);
            let index = pick % rest.len();
            let changed = match change {
                // One removed.
                0 => { rest.remove(index); (first, count, rest) }
                // One added, after the last.
                1 => {
                    let next = i32::try_from(rest.len() + 2).unwrap();
                    rest.push((next, vec![1]));
                    (first, count, rest)
                }
                // One numbered out of place.
                2 => { rest[index].0 += i32::try_from(rest.len()).unwrap(); (first, count, rest) }
                // One shortened.
                3 => {
                    let last = index + 1 == rest.len();
                    let part = &mut rest[index].1;
                    // A last part one byte shorter still fits; take it to nothing.
                    if last { part.clear(); } else { part.pop(); }
                    (first, count, rest)
                }
                // One lengthened.
                4 => {
                    let last = index + 1 == rest.len();
                    let part = &mut rest[index].1;
                    let grow = if last { PART_BYTES + 1 - part.len() } else { 1 };
                    part.extend(std::iter::repeat_n(0, grow));
                    (first, count, rest)
                }
                // The first part shortened.
                5 => { let mut first = first; first.pop(); (first, count, rest) }
                // A count one too many, or one too few.
                6 => (first, count.map(|count| count + 1), rest),
                7 => (first, count.map(|count| count - 1), rest),
                // No count beside other parts.
                _ => (first, None, rest),
            };
            prop_assert!(joined(changed).is_err());
        }
    }
}

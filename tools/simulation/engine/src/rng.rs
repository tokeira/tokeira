//! Deterministic pseudo-random number generator for reproducible simulation.
//!
//! This is the `XorShift64` generator `tools/placement-sim` uses inline, lifted
//! into the harness verbatim so every simulator in the family draws randomness
//! the same way and a given seed reproduces an identical run. All model
//! randomness MUST flow through this type — reading any other entropy source
//! (thread RNG, wall clock) would break the determinism contract that lets a
//! failing seed be replayed exactly.

/// A deterministic `xorshift64` PRNG seeded from a single `u64`.
///
/// The algorithm and constants match `placement-sim` so behaviour is identical
/// across the simulator family. It is intentionally tiny and non-cryptographic:
/// the only property that matters here is reproducibility, not statistical or
/// security strength.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Create a generator from `seed`. A zero seed would make `xorshift64`
    /// degenerate (it stays at zero forever), so it is forced to 1 — matching
    /// `placement-sim`'s `seed.max(1)`.
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    /// Advance the generator and return the next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Return a value in `[start, end_exclusive)`.
    ///
    /// Panics if `start >= end_exclusive`: an empty or inverted range is a
    /// caller bug, not a runtime condition to absorb (same contract as
    /// `placement-sim`'s `range`).
    pub fn range(&mut self, start: u64, end_exclusive: u64) -> u64 {
        assert!(
            start < end_exclusive,
            "range requires start < end_exclusive, got {start}..{end_exclusive}"
        );
        start + (self.next_u64() % (end_exclusive - start))
    }

    /// Return `true` with the given `percent` probability in `[0, 100]`.
    ///
    /// A convenience over `range` for the common "fire this fault X% of the
    /// time" pattern. `percent >= 100` is always true; `percent == 0` is always
    /// false.
    pub fn bool_with_percent(&mut self, percent: u64) -> bool {
        if percent == 0 {
            return false;
        }
        if percent >= 100 {
            return true;
        }
        self.range(0, 100) < percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_yields_identical_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1_000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        // Not a strong statistical claim — just that the streams are not
        // trivially identical, which would indicate the seed is ignored.
        let a_first: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let b_first: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_ne!(a_first, b_first);
    }

    #[test]
    fn zero_seed_is_not_degenerate() {
        let mut r = Rng::new(0);
        // A zero state would return 0 forever; forcing to 1 avoids that.
        assert_ne!(r.next_u64(), 0);
    }

    #[test]
    fn range_is_bounded_and_inclusive_of_start() {
        let mut r = Rng::new(7);
        let mut saw_start = false;
        for _ in 0..10_000 {
            let v = r.range(5, 8);
            assert!((5..8).contains(&v));
            saw_start |= v == 5;
        }
        assert!(saw_start, "expected the low bound to be reachable");
    }

    #[test]
    #[should_panic(expected = "start < end_exclusive")]
    fn range_rejects_empty_interval() {
        let mut r = Rng::new(1);
        r.range(5, 5);
    }

    #[test]
    fn bool_with_percent_edges() {
        let mut r = Rng::new(99);
        assert!(!r.bool_with_percent(0));
        assert!(r.bool_with_percent(100));
        assert!(r.bool_with_percent(150));
    }
}

use std::{fmt, str::FromStr};

const PREFIX: &str = "Server-Compat-Bump: ";

/// Exact three-component release version accepted by the bump protocol.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CompatibilityVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl CompatibilityVersion {
    /// Construct one protocol version from its numeric components.
    pub(crate) const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for CompatibilityVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for CompatibilityVersion {
    type Err = BumpTrailerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut components = value.split('.');
        let major = parse_component(components.next())?;
        let minor = parse_component(components.next())?;
        let patch = parse_component(components.next())?;
        if components.next().is_some() {
            return Err(BumpTrailerError);
        }
        Ok(Self::new(major, minor, patch))
    }
}

fn parse_component(value: Option<&str>) -> Result<u64, BumpTrailerError> {
    let value = value
        .filter(|value| !value.is_empty())
        .ok_or(BumpTrailerError)?;
    if value.len() > 1 && value.starts_with('0') {
        return Err(BumpTrailerError);
    }
    value.parse().map_err(|_| BumpTrailerError)
}

/// One sufficient reason for advancing the Temporal compatibility claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BumpTrigger {
    /// Upstream added behaviour Tokeira already implements or exposes experimentally.
    ExistingCoverage,
    /// A matrix row became implemented and unblocked the claim.
    NewlyImplemented,
    /// Calendar drift permits an advance across already-classified gaps.
    CalendarDrift,
}

impl BumpTrigger {
    /// Return the stable digit used in commit trailers and audit records.
    pub(crate) const fn digit(self) -> char {
        match self {
            Self::ExistingCoverage => '1',
            Self::NewlyImplemented => '2',
            Self::CalendarDrift => '3',
        }
    }

    fn from_digit(value: &str) -> Result<Self, BumpTrailerError> {
        match value {
            "1" => Ok(Self::ExistingCoverage),
            "2" => Ok(Self::NewlyImplemented),
            "3" => Ok(Self::CalendarDrift),
            _ => Err(BumpTrailerError),
        }
    }
}

/// Machine-checkable claim transition carried by a bump commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BumpTrailer {
    /// Compatibility claim in the commit's parent.
    pub(crate) old: CompatibilityVersion,
    /// Compatibility claim in the bump commit.
    pub(crate) new: CompatibilityVersion,
    /// Protocol trigger that justifies the transition.
    pub(crate) trigger: BumpTrigger,
}

impl fmt::Display for BumpTrailer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PREFIX}{} -> {}, trigger: {}",
            self.old,
            self.new,
            self.trigger.digit()
        )
    }
}

impl FromStr for BumpTrailer {
    type Err = BumpTrailerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let payload = value.strip_prefix(PREFIX).ok_or(BumpTrailerError)?;
        let (versions, trigger) = payload.split_once(", trigger: ").ok_or(BumpTrailerError)?;
        let (old, new) = versions.split_once(" -> ").ok_or(BumpTrailerError)?;
        // Prerelease/build identifiers are valid SemVer but outside the release
        // protocol's exact X.Y.Z grammar; accepting them would widen the audit format.
        let old = old.parse()?;
        let new = new.parse()?;
        Ok(Self {
            old,
            new,
            trigger: BumpTrigger::from_digit(trigger)?,
        })
    }
}

/// A value did not match the release protocol's exact bump-trailer grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("expected `Server-Compat-Bump: X.Y.Z -> X.Y.Z, trigger: [123]`")]
pub struct BumpTrailerError;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        // Feature: release-process, Property 2: trailer rendering and parsing agree.
        #[test]
        fn trailer_round_trips(
            old in (0_u64..100, 0_u64..100, 0_u64..100),
            new in (0_u64..100, 0_u64..100, 0_u64..100),
            trigger in 1_u8..=3,
        ) {
            let trigger = match trigger {
                1 => BumpTrigger::ExistingCoverage,
                2 => BumpTrigger::NewlyImplemented,
                _ => BumpTrigger::CalendarDrift,
            };
            let trailer = BumpTrailer {
                old: CompatibilityVersion::new(old.0, old.1, old.2),
                new: CompatibilityVersion::new(new.0, new.1, new.2),
                trigger,
            };
            prop_assert_eq!(trailer.to_string().parse::<BumpTrailer>(), Ok(trailer));
        }
    }

    #[test]
    fn rejects_non_protocol_semver() {
        assert!(
            "Server-Compat-Bump: 1.2.3-dev -> 2.0.0, trigger: 1"
                .parse::<BumpTrailer>()
                .is_err()
        );
        assert!(
            "Server-Compat-Bump: 1.2.3 -> 2.0.0+meta, trigger: 1"
                .parse::<BumpTrailer>()
                .is_err()
        );
    }
}

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::BuildError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Arch {
    Arm64,
    Amd64,
}

impl Arch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Arm64 => "arm64",
            Self::Amd64 => "amd64",
        }
    }

    pub(crate) fn rust_target(self) -> &'static str {
        match self {
            Self::Arm64 => "aarch64-unknown-linux-gnu",
            Self::Amd64 => "x86_64-unknown-linux-gnu",
        }
    }

    pub fn platform(self) -> &'static str {
        match self {
            Self::Arm64 => "linux/arm64",
            Self::Amd64 => "linux/amd64",
        }
    }
}

impl FromStr for Arch {
    type Err = BuildError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "arm64" => Ok(Self::Arm64),
            "amd64" => Ok(Self::Amd64),
            _ => Err(BuildError::UnsupportedArch {
                supplied: s.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn arch_targets_use_gnu_glibc_triples() {
        assert_eq!(Arch::Arm64.rust_target(), "aarch64-unknown-linux-gnu");
        assert_eq!(Arch::Amd64.rust_target(), "x86_64-unknown-linux-gnu");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn arch_parsing_rejects_unknown_values(s in ".*") {
            match s.as_str() {
                "arm64" => {
                    let arch = Arch::from_str(&s).expect("arm64 must parse");
                    prop_assert_eq!(arch, Arch::Arm64);
                    prop_assert_eq!(arch.as_str(), s);
                }
                "amd64" => {
                    let arch = Arch::from_str(&s).expect("amd64 must parse");
                    prop_assert_eq!(arch, Arch::Amd64);
                    prop_assert_eq!(arch.as_str(), s);
                }
                _ => {
                    let err = Arch::from_str(&s).expect_err("unknown arch must fail");
                    match err {
                        BuildError::UnsupportedArch { supplied } => {
                            prop_assert_eq!(supplied, s);
                        }
                        other => {
                            prop_assert!(false, "unexpected error: {other:?}");
                        }
                    }
                }
            }
        }
    }
}

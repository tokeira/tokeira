//! Narrow secret-pattern scanner for SSM Run Command text.

use std::sync::LazyLock;

use regex::Regex;

#[derive(Debug, Clone)]
pub struct SecretMatch {
    pub pattern: &'static str,
    pub start: usize,
    pub end: usize,
}

static SECRET_PATTERNS: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    [
        ("gh auth with-token", r"gh\s+auth\s+login\s+--with-token"),
        ("GITHUB_TOKEN env", r"\bGITHUB_TOKEN\s*="),
        ("bearer header", r#"-H\s+["']?Authorization:\s*Bearer\s"#),
        ("AWS secret key env", r"\bAWS_SECRET_ACCESS_KEY\s*="),
        ("AWS session token env", r"\bAWS_SESSION_TOKEN\s*="),
        ("NPM auth token", r"\b_authToken\s*="),
        (
            "git credential helper pipe",
            r"git\s+credential-store.*store",
        ),
        (
            "inline private key marker",
            r"-----BEGIN (OPENSSH|RSA|EC) PRIVATE KEY-----",
        ),
    ]
    .into_iter()
    .filter_map(|(name, pattern)| {
        let regex = Regex::new(pattern).ok()?;
        Some((name, regex))
    })
    .collect()
});

pub fn scan(command: &[String]) -> Option<SecretMatch> {
    let joined = command.join(" ");
    SECRET_PATTERNS.iter().find_map(|(pattern, regex)| {
        let found = regex.find(&joined)?;
        Some(SecretMatch {
            pattern: *pattern,
            start: found.start(),
            end: found.end(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{SECRET_PATTERNS, scan};

    #[test]
    fn patterns_have_positive_and_negative_examples() {
        let cases = [
            (
                "gh auth with-token",
                vec!["gh", "auth", "login", "--with-token"],
                vec!["gh", "auth", "status"],
            ),
            (
                "GITHUB_TOKEN env",
                vec!["GITHUB_TOKEN=ghp_example", "cargo", "test"],
                vec!["echo", "GITHUB_USER=octo"],
            ),
            (
                "bearer header",
                vec!["curl", "-H", "Authorization: Bearer abc"],
                vec!["curl", "-H", "Authorization: Basic abc"],
            ),
            (
                "AWS secret key env",
                vec!["AWS_SECRET_ACCESS_KEY=secret"],
                vec!["AWS_ACCESS_KEY_ID=key"],
            ),
            (
                "AWS session token env",
                vec!["AWS_SESSION_TOKEN=session"],
                vec!["AWS_PROFILE=dev"],
            ),
            (
                "NPM auth token",
                vec!["npm", "config", "set", "_authToken=secret"],
                vec!["npm", "config", "get", "registry"],
            ),
            (
                "git credential helper pipe",
                vec!["git", "credential-store", "store"],
                vec!["git", "credential", "fill"],
            ),
            (
                "inline private key marker",
                vec!["echo", "-----BEGIN OPENSSH PRIVATE KEY-----"],
                vec!["echo", "public key only"],
            ),
        ];

        let expected_len = cases.len();
        for (pattern, positive, negative) in cases {
            let positive = positive
                .into_iter()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            let negative = negative
                .into_iter()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            let found = scan(&positive);
            assert_eq!(found.as_ref().map(|found| found.pattern), Some(pattern));
            assert!(scan(&negative).is_none(), "negative case matched {pattern}");
        }
        assert_eq!(SECRET_PATTERNS.len(), expected_len);
    }

    #[test]
    fn match_reports_span() {
        let command = vec![
            "echo".to_string(),
            "prefix".to_string(),
            "GITHUB_TOKEN=secret".to_string(),
            "suffix".to_string(),
        ];
        let found = scan(&command).expect("secret should match");
        assert!(found.end > found.start);
    }
}

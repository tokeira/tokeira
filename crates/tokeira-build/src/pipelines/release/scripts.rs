//! The shell that runs inside release containers, and its offline proof.
//!
//! Every script here is a constant string executed as `sh -c <script> -- <args>`.
//! Data reaches a script only through environment variables and `"$@"`, so no crate
//! name, version, path, or note text is ever interpolated into a command line. The
//! Rust side (`dagger.rs`) owns every decision: a script observes, prepares, or moves
//! bytes, and reports one tab-separated line per fact for the parsers below.
//!
//! The scripts are deliberately runnable outside Dagger. The test module executes
//! them with stub `curl`/`cargo`/`jq` binaries on `PATH` and against a local bare Git
//! remote, which is how the publish state machine, the resume admission, and the
//! all-or-nothing push are proven without a registry, a token, or an engine.

use std::collections::BTreeMap;

use super::{ObservedGitRef, PackageOutcome, PackageResult, ReleaseError, RemoteGitObservation};

/// Environment variable naming the scratch directory scripts write into.
///
/// Production leaves it unset and uses `/tmp` inside the container; tests point it at
/// a private directory so concurrent test processes never share scratch files.
#[cfg(test)]
pub(crate) const SCRATCH_ENV: &str = "RELEASE_SCRATCH";

/// Environment variable naming where the tagged source is extracted.
///
/// Production leaves it unset and uses `/release-source`; tests point it at a
/// private directory.
#[cfg(test)]
pub(crate) const SOURCE_DIR_ENV: &str = "RELEASE_SOURCE_DIR";

/// Space-separated observation delays, in seconds, generated from the Rust schedule.
pub(crate) const OBSERVATION_DELAYS_ENV: &str = "OBSERVATION_DELAYS";

/// Seconds the script waits after a successful upload before the next one.
pub(crate) const SUCCESS_COOLDOWN_ENV: &str = "SUCCESS_COOLDOWN";

const TRAILER_FUNCTIONS: &str = r#"scratch=${RELEASE_SCRATCH:-/tmp}
digest_of() {
  value=$(printf '%s\n' "$1" | sed -n 's/^Release-Plan-Digest: sha256:\([0-9a-f]\{64\}\)$/\1/p' | tail -n 1)
  printf '%s' "${value:-absent}"
}
"#;

/// Observe the remote release branch and tag together, without mutating anything.
///
/// Prints one `REFS` line: branch tip, tag object, peeled tag commit, the Plan digest
/// carried by the branch tip's message, by the tag annotation, and by the tagged
/// commit's message, whether the branch contains the tagged commit, and the push URL
/// `origin` resolves to inside this container. Needs the operator's SSH agent for
/// `ls-remote`/`fetch`; runs nothing but Git.
pub(crate) fn release_observe_script() -> String {
    format!(
        r#"set -eu
{TRAILER_FUNCTIONS}
push_url=$(git remote get-url --push origin)
branch=$(git ls-remote --heads origin "refs/heads/$RELEASE_BRANCH" | awk '{{print $1}}')
tag_object=$(git ls-remote --tags origin "refs/tags/$RELEASE_TAG" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {{print $1}}')
tag_commit=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{{}}" | awk '{{print $1}}')
if [ -n "$tag_object" ] && [ -z "$tag_commit" ]; then tag_commit=$tag_object; fi
branch_digest=absent
tag_digest=absent
commit_digest=absent
parent=absent
contains=0
if [ -n "$branch" ]; then
  git fetch --quiet --no-tags --force origin "refs/heads/$RELEASE_BRANCH:refs/tokeira-release/branch"
  branch_digest=$(digest_of "$(git log -1 --format=%B refs/tokeira-release/branch)")
fi
if [ -n "$tag_object" ]; then
  git fetch --quiet --no-tags --force origin "refs/tags/$RELEASE_TAG:refs/tokeira-release/tag"
  if [ "$(git cat-file -t refs/tokeira-release/tag)" = tag ]; then
    tag_digest=$(digest_of "$(git for-each-ref --format='%(contents)' refs/tokeira-release/tag)")
  fi
  commit_digest=$(digest_of "$(git log -1 --format=%B "$tag_commit")")
  parent=$(git rev-parse --verify --quiet "$tag_commit^" || printf 'absent')
  if [ -n "$branch" ] && git merge-base --is-ancestor "$tag_commit" refs/tokeira-release/branch; then contains=1; fi
fi
printf 'REFS\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${{branch:-absent}}" "${{tag_object:-absent}}" "${{tag_commit:-absent}}" "$branch_digest" "$tag_digest" "$commit_digest" "$contains" "${{parent:-absent}}" "$push_url"
"#
    )
}

/// Turn the prepared workspace into the Release Commit and annotated Release Tag.
///
/// Local Git only: nothing leaves the container. The tagged tree is extracted into the
/// release-source directory so the Hermetic Tag Build reads exactly what the tag holds.
pub(crate) const RELEASE_PREPARE_FRESH_SCRIPT: &str = r#"set -eu
source_dir=${RELEASE_SOURCE_DIR:-/release-source}
mkdir -p "$source_dir"
git add -A
git commit --quiet -m "release: prepare $RELEASE_TAG" -m "Release-Plan-Digest: sha256:$PLAN_DIGEST"
git tag -a "$RELEASE_TAG" -m "$RELEASE_TAG" -m "Release-Plan-Digest: sha256:$PLAN_DIGEST"
git archive "$RELEASE_TAG" | tar -x -C "$source_dir"
printf 'COMMIT\t%s\n' "$(git rev-parse HEAD)"
"#;

/// Extract the already-published tag's tree for a resumed train.
///
/// The observation step fetched `refs/tokeira-release/tag`, so this needs no network.
pub(crate) const RELEASE_PREPARE_RESUME_SCRIPT: &str = r#"set -eu
source_dir=${RELEASE_SOURCE_DIR:-/release-source}
mkdir -p "$source_dir"
git archive refs/tokeira-release/tag | tar -x -C "$source_dir"
printf 'COMMIT\t%s\n' "$(git rev-parse 'refs/tokeira-release/tag^{}')"
"#;

/// Prove that preparation touched only release-owned paths and consumed every fragment.
pub(crate) const VALIDATE_PREPARED_SOURCE_SCRIPT: &str = r#"set -eu
scratch=${RELEASE_SCRATCH:-/tmp}
git status --porcelain --untracked-files=normal | cut -c4- | sort -u >"$scratch/release-actual-paths"
while IFS= read -r path; do
  [ -n "$path" ] || continue
  if ! grep -Fx "$path" "$scratch/release-allowed-paths" >/dev/null; then
    echo "release preparation changed unowned path $path" >&2
    exit 2
  fi
done <"$scratch/release-actual-paths"
while IFS= read -r fragment; do
  [ -n "$fragment" ] || continue
  if [ -e "$fragment" ]; then
    echo "release preparation did not consume fragment $fragment" >&2
    exit 2
  fi
done <"$scratch/release-fragments"
test -f ".changes/$RELEASE_VERSION.md"
if git diff --quiet -- CHANGELOG.md; then
  echo 'release preparation did not merge CHANGELOG.md' >&2
  exit 2
fi
"#;

/// Print the SHA-256 of every packaged crate archive.
pub(crate) const HERMETIC_CHECKSUM_SCRIPT: &str = "set -eu\nsha256sum target/package/*.crate\n";

/// Publish each absent package in order, then prove three-way parity for every package.
///
/// Facts per package: `PACKAGE` on parity, `PENDING` when an upload stayed invisible
/// through the observation window, `FAILED` on a conclusive refusal (with the last
/// lines of Cargo's stderr, token-shaped strings redacted), `MISMATCH` when public
/// bytes differ from the hermetic build, and `DIAG` when Cargo failed without
/// conclusive evidence and the registry is being observed instead. Skip-existing is
/// structural: an upload happens only inside the `404` loop.
pub(crate) const REGISTRY_PUBLISH_SCRIPT: &str = r#"set -eu
scratch=${RELEASE_SCRATCH:-/tmp}
last_success=0
retry_ready=0
observe() {
  curl --silent --show-error --location --output "$scratch/crate.json" --write-out '%{http_code}' "$1"
}
evidence() {
  tail -n 5 "$scratch/publish.err" | tr '\n\t' '  ' | sed -E 's/cio[A-Za-z0-9_-]{20,}/[redacted]/g'
}
for spec in "$@"; do
  name=${spec%%=*}
  version=${spec#*=}
  archive="target/package/$name-$version.crate"
  api="https://crates.io/api/v1/crates/$name/$version"
  code=$(observe "$api")
  outcome=existing
  if [ "$code" = 404 ]; then
    while [ "$code" = 404 ]; do
      now=$(date +%s)
      ready=$((last_success + SUCCESS_COOLDOWN))
      if [ "$retry_ready" -gt "$ready" ]; then ready=$retry_ready; fi
      if [ "$now" -lt "$ready" ]; then sleep $((ready - now)); fi
      # A longer registry deadline may outlive the cooldown. Re-observe before
      # issuing another irreversible upload request in case the prior response
      # was ambiguous but eventually became visible.
      code=$(observe "$api")
      if [ "$code" = 200 ]; then break; fi
      if [ "$code" != 404 ]; then
        printf 'FAILED\t%s\t%s\tregistry observation returned HTTP %s\n' "$name" "$version" "$code"
        exit 6
      fi
      set +e
      cargo publish --locked --package "$name" >"$scratch/publish.out" 2>"$scratch/publish.err"
      publish_status=$?
      publish_started=$(date +%s)
      set -e
      if [ "$publish_status" -ne 0 ]; then
        if grep -Eqi '403 Forbidden|not an owner|not allowed to upload|failed to verify package|failed to prepare local package|already uploaded' "$scratch/publish.err"; then
          printf 'FAILED\t%s\t%s\t%s\n' "$name" "$version" "$(evidence)"
          exit 6
        fi
        retry_seconds=$(sed -n -E 's/.*[Rr]etry-?[Aa]fter[^0-9]*([0-9]+).*/\1/p' "$scratch/publish.err" | tail -n 1)
        if [ -n "$retry_seconds" ]; then
          retry_ready=$((publish_started + retry_seconds))
          continue
        fi
        # Not conclusively rejected: the registry may have accepted the bytes before
        # Cargo gave up, so visibility decides, not the process status.
        printf 'DIAG\t%s\t%s\t%s\n' "$name" "$version" "$(evidence)"
      fi
      for delay in $OBSERVATION_DELAYS; do
        sleep "$delay"
        code=$(observe "$api")
        if [ "$code" = 200 ]; then break; fi
      done
      if [ "$code" != 200 ]; then
        printf 'PENDING\t%s\t%s\n' "$name" "$version"
        exit 6
      fi
      last_success=$publish_started
      outcome=published
    done
  elif [ "$code" != 200 ]; then
    printf 'FAILED\t%s\t%s\tregistry observation returned HTTP %s\n' "$name" "$version" "$code"
    exit 6
  fi
  registry=$(jq -er '.version.checksum' "$scratch/crate.json")
  curl --fail --silent --show-error --location --output "$scratch/download.crate" "https://crates.io/api/v1/crates/$name/$version/download"
  local_sha=$(sha256sum "$archive" | cut -d' ' -f1)
  downloaded_sha=$(sha256sum "$scratch/download.crate" | cut -d' ' -f1)
  if [ "$local_sha" != "$downloaded_sha" ] || [ "$downloaded_sha" != "$registry" ]; then
    printf 'MISMATCH\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry"
    exit 7
  fi
  printf 'PACKAGE\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$outcome" "$local_sha" "$downloaded_sha" "$registry" "https://crates.io/api/v1/crates/$name/$version/readme"
done
"#;

/// Prove three-way parity for every package of an already-published train.
pub(crate) const REGISTRY_VERIFY_SCRIPT: &str = r#"set -eu
scratch=${RELEASE_SCRATCH:-/tmp}
for spec in "$@"; do
  name=${spec%%=*}
  version=${spec#*=}
  archive="target/package/$name-$version.crate"
  api="https://crates.io/api/v1/crates/$name/$version"
  code=$(curl --silent --show-error --location --output "$scratch/crate.json" --write-out '%{http_code}' "$api")
  if [ "$code" = 404 ]; then
    printf 'PENDING\t%s\t%s\n' "$name" "$version"
    exit 6
  fi
  if [ "$code" != 200 ]; then
    printf 'FAILED\t%s\t%s\tregistry observation returned HTTP %s\n' "$name" "$version" "$code"
    exit 6
  fi
  registry=$(jq -er '.version.checksum' "$scratch/crate.json")
  curl --fail --silent --show-error --location --output "$scratch/download.crate" "https://crates.io/api/v1/crates/$name/$version/download"
  local_sha=$(sha256sum "$archive" | cut -d' ' -f1)
  downloaded_sha=$(sha256sum "$scratch/download.crate" | cut -d' ' -f1)
  if [ "$local_sha" != "$downloaded_sha" ] || [ "$downloaded_sha" != "$registry" ]; then
    printf 'MISMATCH\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry"
    exit 7
  fi
  printf 'PACKAGE\t%s\t%s\texisting\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry" "https://crates.io/api/v1/crates/$name/$version/readme"
done
"#;

/// One parsed `REFS` line: what the remote holds for the release branch and tag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RefObservation {
    /// Branch tip and peeled tag, each with the Plan digest its own message carries.
    pub(crate) refs: RemoteGitObservation,
    /// Plan digest in the tagged commit's message, when a tag exists.
    pub(crate) tag_commit_digest: Option<String>,
    /// Whether the branch tip contains the tagged commit.
    pub(crate) branch_contains_tag: bool,
    /// The tagged commit's parent: the source base the train was cut from.
    pub(crate) tag_parent: Option<String>,
    /// The push URL `origin` resolves to inside the container.
    pub(crate) push_url: String,
}

/// Parse the `REFS` line produced by [`release_observe_script`].
pub(crate) fn parse_refs_line(output: &str) -> Result<RefObservation, ReleaseError> {
    let fields = output
        .lines()
        .find(|line| line.starts_with("REFS\t"))
        .map(|line| line.split('\t').collect::<Vec<_>>())
        .ok_or_else(|| executor_error("release ref observation omitted its REFS line"))?;
    if fields.len() != 10 {
        return Err(executor_error(&format!(
            "release ref observation was malformed: {}",
            fields.join(" ")
        )));
    }
    let present = |value: &str| (value != "absent").then(|| value.to_owned());
    let branch = present(fields[1]).map(|tip| ObservedGitRef {
        object_id: tip.clone(),
        commit: tip,
        plan_digest: fields[4].to_owned(),
    });
    let tag = present(fields[2]).map(|object| ObservedGitRef {
        object_id: object,
        commit: fields[3].to_owned(),
        plan_digest: fields[5].to_owned(),
    });
    Ok(RefObservation {
        refs: RemoteGitObservation { branch, tag },
        tag_commit_digest: present(fields[6]),
        branch_contains_tag: fields[7] == "1",
        tag_parent: present(fields[8]),
        push_url: fields[9].to_owned(),
    })
}

/// Parse the `COMMIT` line produced by the preparation scripts.
pub(crate) fn parse_commit_line(output: &str) -> Result<String, ReleaseError> {
    output
        .lines()
        .find_map(|line| line.strip_prefix("COMMIT\t"))
        .filter(|commit| commit.len() == 40 && commit.chars().all(|c| c.is_ascii_hexdigit()))
        .map(str::to_owned)
        .ok_or_else(|| executor_error("release preparation omitted the Release Commit"))
}

/// Parse `sha256sum` output into archive file name to digest.
pub(crate) fn parse_checksum_lines(output: &str) -> BTreeMap<String, String> {
    output
        .lines()
        .filter_map(|line| line.split_once("  "))
        .map(|(digest, path)| {
            let name = path.rsplit('/').next().unwrap_or(path).to_owned();
            (name, digest.to_owned())
        })
        .collect()
}

/// Why a registry script stopped before every package reached parity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RegistryStop {
    /// The upload stayed invisible through the bounded observation window.
    Pending { package: String, version: String },
    /// The registry conclusively refused the package.
    Failed {
        package: String,
        version: String,
        reason: String,
    },
    /// Public bytes differ from the hermetic build.
    Mismatch {
        package: String,
        version: String,
        hermetic: String,
        downloaded: String,
        registry: String,
    },
}

impl RegistryStop {
    /// The typed condition the stop maps to.
    pub(crate) fn into_error(self) -> ReleaseError {
        match self {
            Self::Pending { package, version } => {
                ReleaseError::RegistryPending { package, version }
            }
            Self::Failed {
                package,
                version,
                reason,
            } => ReleaseError::RegistryPublish {
                package,
                version,
                reason,
            },
            Self::Mismatch {
                package,
                version,
                hermetic,
                downloaded,
                registry,
            } => ReleaseError::ArtifactMismatch {
                package,
                version,
                hermetic,
                downloaded,
                registry,
            },
        }
    }
}

/// Everything a registry script reported: verified packages, diagnostics, and the stop.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegistryOutput {
    /// Packages that reached parity, in publication order.
    pub(crate) packages: Vec<PackageResult>,
    /// Sanitized Cargo evidence for uploads whose status was not conclusive.
    pub(crate) diagnostics: Vec<String>,
    /// The condition that stopped the script, if any.
    pub(crate) stop: Option<RegistryStop>,
}

/// Parse the fact lines of [`REGISTRY_PUBLISH_SCRIPT`] or [`REGISTRY_VERIFY_SCRIPT`].
pub(crate) fn parse_registry_output(output: &str) -> Result<RegistryOutput, ReleaseError> {
    let mut parsed = RegistryOutput::default();
    for line in output.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            [
                "PACKAGE",
                name,
                version,
                outcome,
                hermetic,
                downloaded,
                registry,
                readme,
            ] => {
                let outcome = match *outcome {
                    "published" => PackageOutcome::Published,
                    "existing" => PackageOutcome::ExistingVerified,
                    other => {
                        return Err(executor_error(&format!("unknown package outcome {other}")));
                    }
                };
                parsed.packages.push(PackageResult {
                    name: (*name).to_owned(),
                    version: (*version).to_owned(),
                    outcome,
                    hermetic_sha256: Some((*hermetic).to_owned()),
                    downloaded_sha256: Some((*downloaded).to_owned()),
                    registry_sha256: Some((*registry).to_owned()),
                    readme_url: Some((*readme).to_owned()),
                });
            }
            ["DIAG", name, version, evidence] => {
                parsed
                    .diagnostics
                    .push(format!("{name} {version}: {evidence}"));
            }
            ["PENDING", name, version] => {
                parsed.stop = Some(RegistryStop::Pending {
                    package: (*name).to_owned(),
                    version: (*version).to_owned(),
                });
            }
            ["FAILED", name, version, reason] => {
                parsed.stop = Some(RegistryStop::Failed {
                    package: (*name).to_owned(),
                    version: (*version).to_owned(),
                    reason: (*reason).to_owned(),
                });
            }
            ["MISMATCH", name, version, hermetic, downloaded, registry] => {
                parsed.stop = Some(RegistryStop::Mismatch {
                    package: (*name).to_owned(),
                    version: (*version).to_owned(),
                    hermetic: (*hermetic).to_owned(),
                    downloaded: (*downloaded).to_owned(),
                    registry: (*registry).to_owned(),
                });
            }
            [kind, ..]
                if matches!(
                    *kind,
                    "PACKAGE" | "DIAG" | "PENDING" | "FAILED" | "MISMATCH"
                ) =>
            {
                return Err(executor_error(&format!(
                    "malformed registry evidence: {line}"
                )));
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn executor_error(reason: &str) -> ReleaseError {
    ReleaseError::Executor {
        reason: reason.to_owned(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
        process::{Command, Output},
    };

    use super::*;
    use crate::pipelines::release::{
        ReleaseAdmission, admit_release_refs, atomic_git_push_arguments,
        registry_observation_delays, sha256_hex, verify_published_refs, verify_resume_refs,
    };

    const CURL_STUB: &str = r#"#!/bin/sh
set -eu
out=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    --output) out=$2; shift 2 ;;
    --write-out) shift 2 ;;
    --*) shift ;;
    *) url=$1; shift ;;
  esac
done
case "$url" in
  */download) cp "$STUB_HOME/download.crate" "$out" ;;
  *)
    calls=$(cat "$STUB_HOME/api.calls" 2>/dev/null || printf '0')
    calls=$((calls + 1))
    printf '%s' "$calls" >"$STUB_HOME/api.calls"
    code=$(sed -n "${calls}p" "$STUB_HOME/api.responses")
    if [ -z "$code" ]; then code=$(tail -n 1 "$STUB_HOME/api.responses"); fi
    cp "$STUB_HOME/crate.json" "$out"
    printf '%s' "$code"
    ;;
esac
"#;

    const CARGO_STUB: &str = r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$STUB_HOME/cargo.calls"
if [ -f "$STUB_HOME/cargo.stderr" ]; then cat "$STUB_HOME/cargo.stderr" >&2; fi
status=$(cat "$STUB_HOME/cargo.status" 2>/dev/null || printf '0')
exit "$status"
"#;

    const JQ_STUB: &str = r#"#!/bin/sh
set -eu
file=""
for argument in "$@"; do file=$argument; done
sed -n 's/.*"checksum":"\([0-9a-f]*\)".*/\1/p' "$file"
"#;

    const SHA256SUM_STUB: &str = r#"#!/bin/sh
set -eu
if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$@"; else openssl dgst -sha256 -r "$@"; fi
"#;

    const SLEEP_STUB: &str = "#!/bin/sh\nexit 0\n";

    const DATE_STUB: &str = r#"#!/bin/sh
set -eu
now=$(cat "$STUB_HOME/clock" 2>/dev/null || printf '1000')
now=$((now + 1))
printf '%s' "$now" >"$STUB_HOME/clock"
printf '%s\n' "$now"
"#;

    /// A private PATH of stub tools, a scratch directory, and a fake `target/package`.
    struct RegistrySandbox {
        _root: tempfile::TempDir,
        stubs: PathBuf,
        scratch: PathBuf,
        work: PathBuf,
    }

    impl RegistrySandbox {
        fn new(archive_bytes: &[u8], download_bytes: &[u8], responses: &str) -> Self {
            let root = tempfile::tempdir().expect("sandbox root");
            let stubs = root.path().join("stubs");
            let scratch = root.path().join("scratch");
            let work = root.path().join("work");
            fs::create_dir_all(&stubs).expect("stub dir");
            fs::create_dir_all(&scratch).expect("scratch dir");
            fs::create_dir_all(work.join("target/package")).expect("package dir");
            for (name, body) in [
                ("curl", CURL_STUB),
                ("cargo", CARGO_STUB),
                ("jq", JQ_STUB),
                ("sha256sum", SHA256SUM_STUB),
                ("sleep", SLEEP_STUB),
                ("date", DATE_STUB),
            ] {
                let path = stubs.join(name);
                fs::write(&path, body).expect("stub body");
                fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("stub mode");
            }
            fs::write(work.join("target/package/a-1.0.0.crate"), archive_bytes).expect("archive");
            fs::write(stubs.join("download.crate"), download_bytes).expect("download");
            fs::write(stubs.join("api.responses"), responses).expect("responses");
            fs::write(
                stubs.join("crate.json"),
                format!(
                    "{{\"version\":{{\"checksum\":\"{}\"}}}}",
                    sha256_hex(download_bytes)
                ),
            )
            .expect("crate json");
            Self {
                _root: root,
                stubs,
                scratch,
                work,
            }
        }

        fn cargo_fails_with(&self, stderr: &str) {
            fs::write(self.stubs.join("cargo.status"), "101").expect("status");
            fs::write(self.stubs.join("cargo.stderr"), stderr).expect("stderr");
        }

        fn run(&self, script: &str, delays: &str) -> Output {
            let path = format!(
                "{}:{}",
                self.stubs.display(),
                std::env::var("PATH").unwrap_or_default()
            );
            Command::new("sh")
                .arg("-c")
                .arg(script)
                .arg("--")
                .arg("a=1.0.0")
                .current_dir(&self.work)
                .env("PATH", path)
                .env("STUB_HOME", &self.stubs)
                .env(SCRATCH_ENV, &self.scratch)
                .env(OBSERVATION_DELAYS_ENV, delays)
                .env(SUCCESS_COOLDOWN_ENV, "0")
                .output()
                .expect("run script")
        }

        fn cargo_calls(&self) -> String {
            fs::read_to_string(self.stubs.join("cargo.calls")).unwrap_or_default()
        }
    }

    fn stdout(output: &Output) -> String {
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn delay_list() -> String {
        registry_observation_delays()
            .iter()
            .map(|_| "0")
            .collect::<Vec<_>>()
            .join(" ")
    }

    // Feature: release-engineering, Property 8: publish execution is idempotent
    #[test]
    fn publish_script_verifies_existing_packages_without_uploading() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "200\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, &delay_list());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        assert_eq!(parsed.stop, None);
        assert_eq!(parsed.packages.len(), 1);
        assert_eq!(parsed.packages[0].outcome, PackageOutcome::ExistingVerified);
        assert_eq!(
            parsed.packages[0].hermetic_sha256.as_deref(),
            Some(sha256_hex(b"crate bytes").as_str())
        );
        assert!(
            sandbox.cargo_calls().is_empty(),
            "no upload for a public version"
        );
    }

    // Feature: release-engineering, Property 8: publish execution is idempotent
    #[test]
    fn publish_script_uploads_absent_packages_once_then_verifies() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "404\n404\n200\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, &delay_list());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        assert_eq!(parsed.packages[0].outcome, PackageOutcome::Published);
        assert_eq!(sandbox.cargo_calls(), "publish --locked --package a\n");
    }

    // Feature: release-engineering, Property 9: publish pacing respects both clocks
    #[test]
    fn publish_script_reports_pending_when_the_registry_stays_ambiguous() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "404\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, "0 0 0");
        assert_eq!(output.status.code(), Some(6));
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        assert_eq!(
            parsed.stop,
            Some(RegistryStop::Pending {
                package: "a".to_owned(),
                version: "1.0.0".to_owned()
            })
        );
        assert_eq!(
            sandbox.cargo_calls().lines().count(),
            1,
            "one upload, then observation only"
        );
    }

    #[test]
    fn publish_script_reports_conclusive_rejections_with_evidence() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "404\n404\n");
        sandbox.cargo_fails_with("error: failed to publish\n403 Forbidden: token cio0123456789abcdefghijklmnop rejected\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, "0");
        assert_eq!(output.status.code(), Some(6));
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        match parsed.stop {
            Some(RegistryStop::Failed { reason, .. }) => {
                assert!(reason.contains("403 Forbidden"), "{reason}");
                assert!(
                    reason.contains("[redacted]"),
                    "token-shaped strings are redacted: {reason}"
                );
                assert!(!reason.contains("cio0123456789"), "{reason}");
            }
            other => panic!("expected a conclusive failure, observed {other:?}"),
        }
    }

    #[test]
    fn publish_script_observes_after_inconclusive_cargo_failures() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "404\n404\n200\n");
        sandbox.cargo_fails_with("error: timed out waiting for the index\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, "0 0");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        assert_eq!(parsed.packages[0].outcome, PackageOutcome::Published);
        assert_eq!(parsed.diagnostics.len(), 1);
        assert!(parsed.diagnostics[0].contains("timed out"));
    }

    // Feature: release-engineering, Property 11: Artifact Parity is three-way equality
    #[test]
    fn publish_script_refuses_public_bytes_that_differ() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"other bytes", "200\n");
        let output = sandbox.run(REGISTRY_PUBLISH_SCRIPT, &delay_list());
        assert_eq!(output.status.code(), Some(7));
        let parsed = parse_registry_output(&stdout(&output)).expect("parseable");
        assert!(matches!(parsed.stop, Some(RegistryStop::Mismatch { .. })));
    }

    #[test]
    fn verify_script_reports_pending_for_absent_versions() {
        let sandbox = RegistrySandbox::new(b"crate bytes", b"crate bytes", "404\n");
        let output = sandbox.run(REGISTRY_VERIFY_SCRIPT, &delay_list());
        assert_eq!(output.status.code(), Some(6));
        assert!(matches!(
            parse_registry_output(&stdout(&output))
                .expect("parseable")
                .stop,
            Some(RegistryStop::Pending { .. })
        ));
        assert!(sandbox.cargo_calls().is_empty(), "verify never uploads");
    }

    /// A bare remote plus a working clone standing in for the container workspace.
    struct GitSandbox {
        _root: tempfile::TempDir,
        remote: PathBuf,
        work: PathBuf,
        scratch: PathBuf,
        source: PathBuf,
        base: String,
    }

    const PLAN_DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn git(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args([
                "-c",
                "user.name=Release Test",
                "-c",
                "user.email=release@test.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    impl GitSandbox {
        fn new() -> Self {
            let root = tempfile::tempdir().expect("git sandbox");
            let remote = root.path().join("origin.git");
            let work = root.path().join("work");
            let scratch = root.path().join("scratch");
            let source = root.path().join("release-source");
            fs::create_dir_all(&scratch).expect("scratch");
            git(
                root.path(),
                &[
                    "init",
                    "--quiet",
                    "--bare",
                    "--initial-branch=main",
                    "origin.git",
                ],
            );
            git(root.path(), &["clone", "--quiet", "origin.git", "work"]);
            fs::write(work.join("Cargo.toml"), "[workspace]\n").expect("manifest");
            git(&work, &["add", "-A"]);
            git(&work, &["commit", "--quiet", "-m", "base"]);
            git(
                &work,
                &["push", "--quiet", "origin", "HEAD:refs/heads/main"],
            );
            let base = git(&work, &["rev-parse", "HEAD"]);
            Self {
                _root: root,
                remote,
                work,
                scratch,
                source,
                base,
            }
        }

        fn run(&self, script: &str, extra: &[String]) -> Output {
            Command::new("sh")
                .arg("-c")
                .arg(script)
                .arg("--")
                .args(extra)
                .current_dir(&self.work)
                .env("RELEASE_TAG", "v0.2.0")
                .env("RELEASE_BRANCH", "main")
                .env("PLAN_DIGEST", PLAN_DIGEST)
                .env(SCRATCH_ENV, &self.scratch)
                .env(SOURCE_DIR_ENV, &self.source)
                .env("GIT_AUTHOR_NAME", "Release Test")
                .env("GIT_AUTHOR_EMAIL", "release@test.invalid")
                .env("GIT_COMMITTER_NAME", "Release Test")
                .env("GIT_COMMITTER_EMAIL", "release@test.invalid")
                .output()
                .expect("run script")
        }

        fn observe(&self) -> RefObservation {
            let output = self.run(&release_observe_script(), &[]);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            parse_refs_line(&stdout(&output)).expect("REFS line")
        }

        fn push(&self) -> Output {
            let mut arguments = vec!["git".to_owned()];
            arguments.extend(atomic_git_push_arguments("origin", "main", "v0.2.0"));
            Command::new(&arguments[0])
                .args(&arguments[1..])
                .current_dir(&self.work)
                .output()
                .expect("run git push")
        }

        fn remote_ref(&self, name: &str) -> Option<String> {
            let line = git(&self.remote, &["show-ref", "--hash", name]);
            (!line.is_empty()).then_some(line)
        }
    }

    // Feature: release-engineering, Property 13: partial-train state classification and resume
    #[test]
    fn fresh_preparation_and_atomic_push_publish_both_refs_together() {
        let sandbox = GitSandbox::new();
        let before = sandbox.observe();
        assert_eq!(before.refs.tag, None);
        assert_eq!(
            before
                .refs
                .branch
                .as_ref()
                .map(|branch| branch.commit.as_str()),
            Some(sandbox.base.as_str())
        );
        assert!(before.push_url.ends_with("origin.git"));

        fs::write(sandbox.work.join("Cargo.toml"), "[workspace]\n# prepared\n").expect("rewrite");
        let prepared = sandbox.run(RELEASE_PREPARE_FRESH_SCRIPT, &[]);
        assert!(
            prepared.status.success(),
            "{}",
            String::from_utf8_lossy(&prepared.stderr)
        );
        let commit = parse_commit_line(&stdout(&prepared)).expect("COMMIT line");
        assert!(
            sandbox.source.join("Cargo.toml").is_file(),
            "tagged tree extracted"
        );
        assert_eq!(
            sandbox.remote_ref("refs/heads/main"),
            Some(sandbox.base.clone())
        );

        let pushed = sandbox.push();
        assert!(
            pushed.status.success(),
            "{}",
            String::from_utf8_lossy(&pushed.stderr)
        );
        let after = sandbox.observe();
        let tag = verify_resume_refs("v0.2.0", &commit, PLAN_DIGEST, &after.refs)
            .expect("both refs identify the train");
        assert_eq!(tag.commit, commit);
        assert_eq!(after.tag_commit_digest.as_deref(), Some(PLAN_DIGEST));
        assert_eq!(after.tag_parent.as_deref(), Some(sandbox.base.as_str()));
        assert!(after.branch_contains_tag);
    }

    // Feature: release-engineering, Property 13: partial-train state classification and resume
    #[test]
    fn resume_admission_recognizes_this_train_and_refuses_another() {
        let sandbox = GitSandbox::new();
        fs::write(sandbox.work.join("Cargo.toml"), "[workspace]\n# prepared\n").expect("rewrite");
        let prepared = sandbox.run(RELEASE_PREPARE_FRESH_SCRIPT, &[]);
        assert!(prepared.status.success());
        assert!(sandbox.push().status.success());
        let observation = sandbox.observe();

        let base = sandbox.base.clone();
        let this_train = admit_release_refs(&base, "v0.2.0", PLAN_DIGEST, &observation.refs)
            .expect("published refs admit a resume");
        assert!(matches!(this_train, ReleaseAdmission::Resume { .. }));

        let other_digest = "f".repeat(64);
        let foreign = admit_release_refs(&base, "v0.2.0", &other_digest, &observation.refs);
        assert!(
            matches!(foreign, Err(ReleaseError::GitRefConflict { .. })),
            "{foreign:?}"
        );

        let published = verify_published_refs(
            "v0.2.0",
            &observation.refs,
            observation.tag_commit_digest.as_deref(),
            observation.branch_contains_tag,
            Some(PLAN_DIGEST),
        )
        .expect("verify accepts the published train");
        assert_eq!(
            published.commit,
            observation
                .refs
                .tag
                .as_ref()
                .map(|t| t.commit.clone())
                .expect("tag")
        );
    }

    // Feature: release-engineering, Property 13: partial-train state classification and resume
    #[test]
    fn atomic_push_leaves_both_refs_untouched_when_the_tag_is_taken() {
        let sandbox = GitSandbox::new();
        // A foreign tag already occupies the name on the remote.
        git(&sandbox.work, &["tag", "-a", "v0.2.0", "-m", "foreign"]);
        git(
            &sandbox.work,
            &["push", "--quiet", "origin", "refs/tags/v0.2.0"],
        );
        git(&sandbox.work, &["tag", "-d", "v0.2.0"]);

        fs::write(sandbox.work.join("Cargo.toml"), "[workspace]\n# prepared\n").expect("rewrite");
        let prepared = sandbox.run(RELEASE_PREPARE_FRESH_SCRIPT, &[]);
        assert!(
            prepared.status.success(),
            "{}",
            String::from_utf8_lossy(&prepared.stderr)
        );

        let pushed = sandbox.push();
        assert!(
            !pushed.status.success(),
            "the taken tag rejects the transaction"
        );
        assert_eq!(
            sandbox.remote_ref("refs/heads/main"),
            Some(sandbox.base.clone()),
            "the branch update is rolled back with the tag"
        );
        let observation = sandbox.observe();
        let admission = admit_release_refs(&sandbox.base, "v0.2.0", PLAN_DIGEST, &observation.refs);
        assert!(
            matches!(admission, Err(ReleaseError::TagConflict { .. })),
            "{admission:?}"
        );
    }

    #[test]
    fn verify_accepts_a_branch_that_moved_past_the_release() {
        let sandbox = GitSandbox::new();
        fs::write(sandbox.work.join("Cargo.toml"), "[workspace]\n# prepared\n").expect("rewrite");
        assert!(
            sandbox
                .run(RELEASE_PREPARE_FRESH_SCRIPT, &[])
                .status
                .success()
        );
        assert!(sandbox.push().status.success());
        fs::write(sandbox.work.join("README.md"), "later\n").expect("later change");
        git(&sandbox.work, &["add", "-A"]);
        git(&sandbox.work, &["commit", "--quiet", "-m", "later"]);
        git(
            &sandbox.work,
            &["push", "--quiet", "origin", "HEAD:refs/heads/main"],
        );

        let observation = sandbox.observe();
        assert!(observation.branch_contains_tag);
        let published = verify_published_refs(
            "v0.2.0",
            &observation.refs,
            observation.tag_commit_digest.as_deref(),
            observation.branch_contains_tag,
            None,
        )
        .expect("a branch that contains the release still verifies");
        assert!(published.published);
        let resume = admit_release_refs(&sandbox.base, "v0.2.0", PLAN_DIGEST, &observation.refs);
        assert!(
            matches!(resume, Err(ReleaseError::GitRefConflict { .. })),
            "resume keeps the exact-tip rule: {resume:?}"
        );
    }

    #[test]
    fn parsers_reject_malformed_evidence() {
        assert!(parse_refs_line("REFS\tonly\tfour\tfields\n").is_err());
        assert!(parse_commit_line("COMMIT\tnot-a-sha\n").is_err());
        assert!(parse_registry_output("PACKAGE\ta\t1.0.0\n").is_err());
        let checksums = parse_checksum_lines("abc  target/package/a-1.0.0.crate\n");
        assert_eq!(
            checksums.get("a-1.0.0.crate").map(String::as_str),
            Some("abc")
        );
    }
}

//! Cloud-init renderer for remote workstation instances.

use sha2::{Digest, Sha256};

use crate::engine::WorkstationProfile;

pub const BOOTSTRAP_SCHEMA: &str = "v1";

pub const GITHUB_SSH_HOST_KEYS: &[&str] = &[
    "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
    "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=",
    "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=",
];

#[derive(Debug, Clone)]
pub struct BootstrapContext {
    pub workstation_id: String,
    pub bootstrap_fingerprint: String,
    pub profile: WorkstationProfile,
    pub cache_volume_id: String,
    pub repo_volume_id: String,
    pub rust_toolchain_toml: String,
}

pub fn fingerprint(profile: &WorkstationProfile, rust_toolchain_toml: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(BOOTSTRAP_SCHEMA.as_bytes());
    hasher.update(profile.name.as_bytes());
    hasher.update(profile.instance_type.as_bytes());
    hasher.update(profile.repo_url.as_bytes());
    hasher.update(rust_toolchain_toml.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn render(context: &BootstrapContext) -> String {
    [
        header(context),
        filesystem_phase(context),
        toolchain_phase(context),
        environment_phase(),
        repo_dir_phase(),
        agentd_phase(),
        idle_phase(context),
        fingerprint_phase(context),
    ]
    .join("\n\n")
}

fn header(context: &BootstrapContext) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
WORKSTATION_ID={workstation_id:?}
SHELL_USER="$(id -un 1000 2>/dev/null || echo ubuntu)"
SHELL_HOME="$(getent passwd "$SHELL_USER" | cut -d: -f6)"
mkdir -p /etc/tokeira /var/lib/tokeira
"#,
        workstation_id = context.workstation_id
    )
}

fn filesystem_phase(context: &BootstrapContext) -> String {
    format!(
        r#"# idempotency: existing ext4 filesystems and mounts are reused; NVMe is always recreatable.
mkdir -p /work
nvme_by_id="$(find /dev/disk/by-id -maxdepth 1 -name 'nvme-Amazon_EC2_NVMe_Instance_Storage*' -print -quit 2>/dev/null || true)"
nvme_device=""
if [ -n "$nvme_by_id" ]; then
  nvme_device="$(readlink -f "$nvme_by_id")"
fi
if [ -n "$nvme_device" ]; then
  if ! mountpoint -q /work; then
    mkfs.ext4 -F "$nvme_device" || true
    mount "$nvme_device" /work || true
  fi
fi

# Create subdirectories AFTER the NVMe mount so they exist on the NVMe filesystem,
# not hidden under the mount point on the root volume.
mkdir -p /work/cache /work/repo /work/sccache /work/target

cache_id={cache_id:?}
repo_id={repo_id:?}
cache_dev="/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${{cache_id//-/}}"
repo_dev="/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${{repo_id//-/}}"
for dev in "$cache_dev" "$repo_dev"; do
  for _ in $(seq 1 120); do
    [ -e "$dev" ] && break
    sleep 2
  done
done

if [ -e "$cache_dev" ]; then
  blkid "$cache_dev" >/dev/null 2>&1 || mkfs.ext4 -F "$cache_dev"
  mountpoint -q /work/cache || mount "$cache_dev" /work/cache
fi
if [ -e "$repo_dev" ]; then
  blkid "$repo_dev" >/dev/null 2>&1 || mkfs.ext4 -F "$repo_dev"
  mountpoint -q /work/repo || mount "$repo_dev" /work/repo
fi

mkdir -p /work/cache/cargo /work/cache/rustup "$SHELL_HOME/.cargo" "$SHELL_HOME/.rustup" "$SHELL_HOME/.cache/sccache"
chown -R "$SHELL_USER:$SHELL_USER" /work/cache /work/repo /work/sccache /work/target
mountpoint -q "$SHELL_HOME/.cargo" || mount --bind /work/cache/cargo "$SHELL_HOME/.cargo" || true
mountpoint -q "$SHELL_HOME/.rustup" || mount --bind /work/cache/rustup "$SHELL_HOME/.rustup" || true
"#,
        cache_id = context.cache_volume_id,
        repo_id = context.repo_volume_id
    )
}

fn toolchain_phase(_context: &BootstrapContext) -> String {
    format!(
        r#"# idempotency: apt and rustup installs are safe to repeat.
apt-get update
apt-get install -y build-essential curl git jq lld mold unzip ripgrep fd-find gh pkg-config libssl-dev cmake

# Install protoc from GitHub releases (not available via apt on Ubuntu 24.04 ARM64)
if ! command -v protoc >/dev/null 2>&1; then
  curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-aarch_64.zip -o /tmp/protoc.zip
  unzip -o /tmp/protoc.zip -d /usr/local bin/protoc
  unzip -o /tmp/protoc.zip -d /usr/local 'include/*'
  chmod +x /usr/local/bin/protoc
  rm -f /tmp/protoc.zip
fi

if ! command -v rustup >/dev/null 2>&1; then
  su "$SHELL_USER" -c 'curl https://sh.rustup.rs -sSf | sh -s -- -y'
fi
su "$SHELL_USER" -c '$HOME/.cargo/bin/rustup toolchain install stable nightly --component rustfmt --component clippy'

# Install cargo tools without RUSTC_WRAPPER to avoid circular sccache dependency
su "$SHELL_USER" -c 'RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/cargo-tools-build $HOME/.cargo/bin/cargo install cargo-nextest cargo-deny sccache --locked' || true
rm -rf /tmp/cargo-tools-build

mkdir -p "$SHELL_HOME/.ssh"
cat > "$SHELL_HOME/.ssh/known_hosts" <<'KNOWN_HOSTS'
{known_hosts}
KNOWN_HOSTS
cat > "$SHELL_HOME/.ssh/config" <<'SSH_CONFIG'
Host github.com github.com-tokeira-*
  StrictHostKeyChecking yes
  UserKnownHostsFile ~/.ssh/known_hosts
SSH_CONFIG
chown -R "$SHELL_USER:$SHELL_USER" "$SHELL_HOME/.ssh"
chmod 700 "$SHELL_HOME/.ssh"
chmod 600 "$SHELL_HOME/.ssh/config"
"#,
        known_hosts = GITHUB_SSH_HOST_KEYS.join("\n")
    )
}

fn environment_phase() -> String {
    r#"# idempotency: profile.d file is rewritten from the current renderer every run.
cat > /etc/profile.d/tokeira-workstation.sh <<'PROFILE'
export CARGO_TARGET_DIR=/work/target
export RUSTC_WRAPPER=sccache
export SCCACHE_DIR=/work/sccache
export CARGO_INCREMENTAL=0
export PATH="$HOME/.cargo/bin:$PATH"
PROFILE
chmod 0644 /etc/profile.d/tokeira-workstation.sh
"#
    .to_string()
}

fn repo_dir_phase() -> String {
    r#"# Prepare the repo directory structure. Clone is NOT done here — the operator
# runs `tkr workstation github-key add` then clones manually or via remote-exec.
mkdir -p /work/repo
chown "$SHELL_USER:$SHELL_USER" /work/repo
ln -sfn /work/repo/tokeira /work/tokeira
chown -h "$SHELL_USER:$SHELL_USER" /work/tokeira
"#
    .to_string()
}

fn agentd_phase() -> String {
    r#"# idempotency: tmpfiles recreates the runtime directory after reboot.
mkdir -p /run/tokeira-agentd
chown "$SHELL_USER:$SHELL_USER" /run/tokeira-agentd
chmod 0750 /run/tokeira-agentd
cat > /etc/tmpfiles.d/tokeira-agentd.conf <<'TMPFILES'
d /run/tokeira-agentd 0750 ubuntu ubuntu -
TMPFILES
"#
    .to_string()
}

fn idle_phase(context: &BootstrapContext) -> String {
    format!(
        r#"# idempotency: service units are overwritten then enabled.
cat > /etc/tokeira/idle-config.env <<'IDLE_CONFIG'
idle_shutdown_enabled={enabled}
idle_shutdown_minutes={minutes}
idle_load_threshold=0.5
IDLE_CONFIG
cat > /usr/local/bin/tokeira-workstation-idle-check <<'IDLE_SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
source /etc/tokeira/idle-config.env
if [[ "${{idle_shutdown_enabled:-true}}" != "true" ]]; then exit 0; fi
defer_file=/var/lib/tokeira/idle-defer.timestamp
if [[ -f "$defer_file" ]] && (( $(date +%s) < $(cat "$defer_file") )); then exit 0; fi
load_1min=$(cut -d' ' -f1 /proc/loadavg)
if awk "BEGIN {{ exit !($load_1min >= ${{idle_load_threshold:-0.5}}) }}"; then
  echo 0 > /var/lib/tokeira/idle-counter
  exit 0
fi
if pgrep -f "amazon-ssm-agent.*session" >/dev/null; then
  echo 0 > /var/lib/tokeira/idle-counter
  exit 0
fi
counter=$(cat /var/lib/tokeira/idle-counter 2>/dev/null || echo 0)
counter=$((counter + 1))
echo "$counter" > /var/lib/tokeira/idle-counter
firings_required=$(( idle_shutdown_minutes / 5 ))
if (( firings_required > 0 && counter >= firings_required )); then
  /sbin/shutdown -h +1 "Tokeira workstation idle-shutdown"
fi
IDLE_SCRIPT
chmod 0755 /usr/local/bin/tokeira-workstation-idle-check
cat > /etc/systemd/system/tokeira-workstation-idle.service <<'IDLE_SERVICE'
[Unit]
Description=Tokeira workstation idle shutdown check

[Service]
Type=oneshot
ExecStart=/usr/local/bin/tokeira-workstation-idle-check
IDLE_SERVICE
cat > /etc/systemd/system/tokeira-workstation-idle.timer <<'IDLE_TIMER'
[Unit]
Description=Tokeira workstation idle shutdown timer

[Timer]
OnBootSec=10min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
IDLE_TIMER
systemctl daemon-reload
systemctl enable --now tokeira-workstation-idle.timer
"#,
        enabled = context.profile.idle_shutdown_enabled,
        minutes = context.profile.idle_shutdown_minutes
    )
}

fn fingerprint_phase(context: &BootstrapContext) -> String {
    format!(
        r#"# idempotency: fingerprint file always reflects this renderer invocation.
printf '%s\n' {fingerprint:?} > /etc/tokeira/workstation-fingerprint
"#,
        fingerprint = context.bootstrap_fingerprint
    )
}

#[cfg(test)]
mod tests {
    use super::{GITHUB_SSH_HOST_KEYS, fingerprint};
    use crate::engine::WorkstationProfile;

    #[test]
    fn fingerprint_is_deterministic_and_input_sensitive() {
        let profile = WorkstationProfile::c8gd_rust();
        let first = fingerprint(&profile, "[toolchain]\nchannel = \"1.95\"\n");
        let second = fingerprint(&profile, "[toolchain]\nchannel = \"1.95\"\n");
        assert_eq!(first, second);

        let changed = fingerprint(&profile, "[toolchain]\nchannel = \"nightly\"\n");
        assert_ne!(first, changed);
    }

    #[test]
    fn github_host_keys_have_expected_algorithms() {
        let mut algorithms = GITHUB_SSH_HOST_KEYS
            .iter()
            .filter_map(|line| line.split_whitespace().nth(1))
            .collect::<Vec<_>>();
        algorithms.sort();
        assert_eq!(
            algorithms,
            vec!["ecdsa-sha2-nistp256", "ssh-ed25519", "ssh-rsa"]
        );
        for line in GITHUB_SSH_HOST_KEYS {
            let parts = line.split_whitespace().collect::<Vec<_>>();
            assert_eq!(parts.first().copied(), Some("github.com"));
            assert_eq!(parts.len(), 3);
            assert!(!parts[2].is_empty());
        }
    }
}

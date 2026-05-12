//! Workstation lifecycle management via direct AWS SDK calls.

use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_ec2::{
    Client as Ec2Client,
    client::Waiters as _,
    types::{
        BlockDeviceMapping, EbsBlockDevice, Filter, IamInstanceProfileSpecification,
        InstanceNetworkInterfaceSpecification, InstanceType, ResourceType, Tag, TagSpecification,
        VolumeType,
    },
};
use aws_sdk_iam::Client as IamClient;
use aws_sdk_ssm::Client as SsmClient;
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    process::Command,
    time::sleep,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::bootstrap::{self, BootstrapContext};

const WORKSTATION_TAG_KEY: &str = "tokeira-workstation";
const WORKSTATION_TAG_VALUE: &str = "true";
const WORKSTATION_ID_TAG_KEY: &str = "workstation-id";
const WORKSTATION_OWNED_EIP_TAG_KEY: &str = "tokeira-workstation-owned-eip";
const BOOTSTRAP_FINGERPRINT_TAG_KEY: &str = "bootstrap-fingerprint";
const DEFAULT_REGION: &str = "eu-west-2";
const DEFAULT_INSTANCE_TYPE: &str = "c8gd.8xlarge";
const DEFAULT_PROFILE: &str = "c8gd-rust";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkstationProfile {
    pub name: String,
    pub instance_type: String,
    pub ami_family: AmiFamily,
    pub region: String,
    pub root_volume_gib: u32,
    pub cache_volume_gib: u32,
    pub repo_volume_gib: u32,
    pub idle_shutdown_minutes: u32,
    pub idle_shutdown_enabled: bool,
    pub repo_url: String,
    pub git_user_name: Option<String>,
    pub git_user_email: Option<String>,
    pub subnet_id: Option<String>,
}

impl WorkstationProfile {
    pub fn c8gd_rust() -> Self {
        Self {
            name: DEFAULT_PROFILE.to_string(),
            instance_type: DEFAULT_INSTANCE_TYPE.to_string(),
            ami_family: AmiFamily::Ubuntu2404,
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| DEFAULT_REGION.to_string()),
            root_volume_gib: 20,
            cache_volume_gib: 30,
            repo_volume_gib: 40,
            idle_shutdown_minutes: 30,
            idle_shutdown_enabled: true,
            repo_url: default_repo_url(),
            git_user_name: git_config_value("user.name"),
            git_user_email: git_config_value("user.email"),
            subnet_id: None,
        }
    }

    pub fn by_name(name: &str) -> Option<Self> {
        match name {
            DEFAULT_PROFILE => Some(Self::c8gd_rust()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AmiFamily {
    Ubuntu2404,
    AmazonLinux2023,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkstationHandle {
    pub workstation_id: String,
    pub instance_id: String,
    pub cache_volume_id: String,
    pub repo_volume_id: String,
    pub root_volume_id: String,
    pub security_group_id: String,
    pub iam_role_name: String,
    pub instance_profile_name: String,
    pub region: String,
    pub subnet_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpOutcome {
    Created {
        handle: WorkstationHandle,
        bootstrap_fingerprint: String,
        repo_clone_warning: Option<String>,
    },
    Resumed {
        handle: WorkstationHandle,
        bootstrap_drift: BootstrapDrift,
        repo_clone_warning: Option<String>,
    },
    AlreadyRunning {
        handle: WorkstationHandle,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BootstrapDrift {
    UpToDate,
    Drift {
        local_fingerprint: String,
        remote_fingerprint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkstationStatus {
    pub workstation_id: String,
    pub instance_id: String,
    pub state: String,
    pub instance_type: String,
    pub region: String,
    pub cache_volume_id: Option<String>,
    pub repo_volume_id: Option<String>,
    pub security_group_id: Option<String>,
    pub bootstrap_fingerprint: Option<String>,
    pub hourly_cost_usd: Option<f64>,
    pub cumulative_uptime_hours: f64,
    pub cache_volume_gib: Option<u32>,
    pub repo_volume_gib: Option<u32>,
    pub cache_volume_state: Option<String>,
    pub repo_volume_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkstationSummary {
    pub workstation_id: String,
    pub instance_id: String,
    pub state: String,
    pub instance_type: String,
    pub region: String,
    pub hourly_cost_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubRepo {
    pub owner: String,
    pub name: String,
}

/// Result of pre-flight discovery. Contains the values the IaC module needs
/// to provision resources in the correct subnet/AZ.
#[derive(Debug, Clone)]
pub struct PreflightResult {
    pub subnet_id: String,
    pub vpc_id: String,
    pub availability_zone: String,
    pub ami_id: String,
}

impl GithubRepo {
    pub fn parse(value: &str) -> Result<Self, WorkstationError> {
        if value.contains("://") {
            return Err(WorkstationError::InvalidGithubRepo(value.to_string()));
        }
        let Some((owner, name)) = value.split_once('/') else {
            return Err(WorkstationError::InvalidGithubRepo(value.to_string()));
        };
        if owner.is_empty() || name.is_empty() || name.contains('/') {
            return Err(WorkstationError::InvalidGithubRepo(value.to_string()));
        }
        Ok(Self {
            owner: owner.to_string(),
            name: name.trim_end_matches(".git").to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkstationError {
    #[error("aws ec2 error: {0}")]
    Ec2(String),
    #[error("aws ssm error: {0}")]
    Ssm(String),
    #[error("aws iam error: {0}")]
    Iam(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("workstation {0} not found")]
    NotFound(String),
    #[error("multiple workstations match: {0:?}")]
    AmbiguousMatch(Vec<String>),
    #[error("workstation {workstation_id} is in unexpected state {state}")]
    UnexpectedState {
        workstation_id: String,
        state: String,
    },
    #[error(
        "session-manager-plugin is not installed; install it from AWS Session Manager documentation"
    )]
    SessionManagerPluginMissing,
    #[error("github CLI is missing")]
    GhCliMissing,
    #[error("github CLI is not authenticated")]
    GhCliUnauthenticated,
    #[error("invalid GitHub repository {0}; expected owner/name")]
    InvalidGithubRepo(String),
    #[error("github api error: {0}")]
    GithubApi(String),
    #[error("command looks like it contains a secret")]
    SecretInCommand,
    #[error("profile {0} is unknown")]
    UnknownProfile(String),
    #[error("no public subnet was found; pass --subnet-id explicitly")]
    NoPublicSubnet,
    #[error("command failed: {0}")]
    Command(String),
}

#[derive(Debug, Clone)]
pub struct Workstation {
    ec2: Ec2Client,
    ssm: SsmClient,
    iam: IamClient,
    region: String,
}

#[derive(Debug, Clone)]
struct ResolvedSubnet {
    subnet_id: String,
    vpc_id: String,
    availability_zone: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DeployKeyRegistryEntry {
    repo: String,
    key_id: String,
    removed_at: Option<String>,
}

#[derive(Debug, Clone)]
struct VolumeStatus {
    size_gib: Option<u32>,
    state: Option<String>,
}

/// Extract a useful error message from an AWS SDK error. Pulls out the service
/// error code and message for a human-readable diagnostic, falling back to
/// Display for non-service errors (network, timeout, etc).
fn ec2_err(err: impl std::fmt::Display + std::fmt::Debug) -> WorkstationError {
    let debug = format!("{err:?}");
    // Extract code and message from the Debug output
    let code = extract_between(&debug, "code: Some(\"", "\")");
    let message = extract_between(&debug, "message: Some(\"", "\")");
    match (code, message) {
        (Some(code), Some(msg)) => WorkstationError::Ec2(format!("{code}: {msg}")),
        _ => WorkstationError::Ec2(format!("{err}")),
    }
}

fn ssm_err(err: impl std::fmt::Display + std::fmt::Debug) -> WorkstationError {
    let debug = format!("{err:?}");
    let code = extract_between(&debug, "code: Some(\"", "\")");
    let message = extract_between(&debug, "message: Some(\"", "\")");
    match (code, message) {
        (Some(code), Some(msg)) if code == "InvalidInstanceId" => WorkstationError::Ssm(format!(
            "{code}: {msg}; the instance may still be booting — wait 30-60s for the SSM agent to register"
        )),
        (Some(code), Some(msg)) => WorkstationError::Ssm(format!("{code}: {msg}")),
        _ => WorkstationError::Ssm(format!("{err:?}")),
    }
}

fn iam_err(err: impl std::fmt::Display + std::fmt::Debug) -> WorkstationError {
    let debug = format!("{err:?}");
    let code = extract_between(&debug, "code: Some(\"", "\")");
    let message = extract_between(&debug, "message: Some(\"", "\")");
    match (code, message) {
        (Some(code), Some(msg)) => WorkstationError::Iam(format!("{code}: {msg}")),
        _ => WorkstationError::Iam(format!("{err}")),
    }
}

fn extract_between<'a>(haystack: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let start_idx = haystack.find(start)? + start.len();
    let rest = &haystack[start_idx..];
    let end_idx = rest.find(end)?;
    Some(&rest[..end_idx])
}

impl Workstation {
    pub async fn new(region: impl Into<String>) -> Result<Self, WorkstationError> {
        let region = region.into();
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;
        Ok(Self {
            ec2: Ec2Client::new(&config),
            ssm: SsmClient::new(&config),
            iam: IamClient::new(&config),
            region,
        })
    }

    pub async fn up(
        &self,
        profile: &WorkstationProfile,
        workstation_override: Option<&str>,
    ) -> Result<UpOutcome, WorkstationError> {
        let matches = self.discover(workstation_override).await?;
        match matches.as_slice() {
            [] => self.create(profile, workstation_override).await,
            [handle] => self.up_existing(profile, handle).await,
            many => Err(WorkstationError::AmbiguousMatch(
                many.iter().map(|h| h.workstation_id.clone()).collect(),
            )),
        }
    }

    pub async fn stop(&self, workstation_id: &str) -> Result<(), WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        warn!(
            workstation_id,
            "stopping workstation; /work/target and /work/sccache will be erased"
        );
        self.ec2
            .stop_instances()
            .instance_ids(&handle.instance_id)
            .send()
            .await
            .map_err(ec2_err)?;
        self.wait_for_instance_state(&handle.instance_id, "stopped")
            .await?;
        self.release_owned_eip(workstation_id).await?;
        append_uptime_event(workstation_id, "stop")?;
        write_state_with_status(&handle, "Stopped")?;
        Ok(())
    }

    pub async fn destroy(&self, workstation_id: &str) -> Result<(), WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        self.cleanup_deploy_keys(workstation_id).await?;
        let instance_id = handle.instance_id.clone();
        self.ec2
            .terminate_instances()
            .instance_ids(&instance_id)
            .send()
            .await
            .map_err(ec2_err)?;
        self.wait_for_instance_state(&instance_id, "terminated")
            .await?;
        self.release_owned_eip(workstation_id).await?;

        for volume_id in [&handle.cache_volume_id, &handle.repo_volume_id] {
            if !volume_id.is_empty() {
                if let Err(err) = self.ec2.delete_volume().volume_id(volume_id).send().await {
                    warn!(volume_id, error = %err, "failed to delete workstation volume");
                }
            }
        }

        if !handle.security_group_id.is_empty()
            && let Err(err) = self
                .ec2
                .delete_security_group()
                .group_id(handle.security_group_id)
                .send()
                .await
        {
            warn!(error = %err, "failed to delete workstation security group");
        }

        if !handle.instance_profile_name.is_empty() {
            let _ = self
                .iam
                .remove_role_from_instance_profile()
                .instance_profile_name(&handle.instance_profile_name)
                .role_name(&handle.iam_role_name)
                .send()
                .await;
            let _ = self
                .iam
                .delete_instance_profile()
                .instance_profile_name(handle.instance_profile_name)
                .send()
                .await;
        }
        if !handle.iam_role_name.is_empty() {
            let _ = self
                .iam
                .detach_role_policy()
                .role_name(&handle.iam_role_name)
                .policy_arn("arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
                .send()
                .await;
            let _ = self
                .iam
                .delete_role()
                .role_name(handle.iam_role_name)
                .send()
                .await;
        }
        remove_state_dir(workstation_id)?;
        Ok(())
    }

    pub async fn status(
        &self,
        workstation_id: &str,
    ) -> Result<WorkstationStatus, WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let instance = self
            .describe_instance_by_id(&handle.instance_id)
            .await?
            .ok_or_else(|| WorkstationError::NotFound(workstation_id.to_string()))?;
        let instance_type = instance
            .instance_type()
            .map(|value| value.as_str().to_string())
            .unwrap_or_default();
        let cache_volume = self.volume_status(handle.cache_volume_id.as_str()).await?;
        let repo_volume = self.volume_status(handle.repo_volume_id.as_str()).await?;
        Ok(WorkstationStatus {
            workstation_id: workstation_id.to_string(),
            instance_id: handle.instance_id.clone(),
            state: instance_state(&instance),
            hourly_cost_usd: hourly_rate(&self.region, &instance_type),
            instance_type,
            region: self.region.clone(),
            cache_volume_id: non_empty(handle.cache_volume_id.clone()),
            repo_volume_id: non_empty(handle.repo_volume_id.clone()),
            security_group_id: non_empty(handle.security_group_id.clone()),
            bootstrap_fingerprint: tag_value(instance.tags(), BOOTSTRAP_FINGERPRINT_TAG_KEY),
            cumulative_uptime_hours: read_cumulative_uptime_hours(workstation_id),
            cache_volume_gib: cache_volume.as_ref().and_then(|volume| volume.size_gib),
            repo_volume_gib: repo_volume.as_ref().and_then(|volume| volume.size_gib),
            cache_volume_state: cache_volume.and_then(|volume| volume.state),
            repo_volume_state: repo_volume.and_then(|volume| volume.state),
        })
    }

    pub async fn list(&self) -> Result<Vec<WorkstationSummary>, WorkstationError> {
        let mut summaries = Vec::new();
        for handle in self.discover(None).await? {
            if let Some(instance) = self.describe_instance_by_id(&handle.instance_id).await? {
                let instance_type = instance
                    .instance_type()
                    .map(|value| value.as_str().to_string())
                    .unwrap_or_default();
                summaries.push(WorkstationSummary {
                    hourly_cost_usd: hourly_rate(&self.region, &instance_type),
                    workstation_id: handle.workstation_id,
                    instance_id: handle.instance_id,
                    state: instance_state(&instance),
                    instance_type,
                    region: self.region.clone(),
                });
            }
        }
        Ok(summaries)
    }

    pub async fn remote_exec(
        &self,
        workstation_id: &str,
        cwd: &str,
        command: &[String],
        mut stdout: impl AsyncWrite + Send + Unpin,
        mut stderr: impl AsyncWrite + Send + Unpin,
    ) -> Result<i32, WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let shell = shell_command(cwd, command);
        let response = self
            .ssm
            .send_command()
            .document_name("AWS-RunShellScript")
            .instance_ids(handle.instance_id.clone())
            .parameters("commands", vec![shell])
            .send()
            .await
            .map_err(ssm_err)?;
        let command_id = response
            .command()
            .and_then(|command| command.command_id())
            .ok_or_else(|| {
                WorkstationError::Ssm("SSM SendCommand did not return command id".to_string())
            })?
            .to_string();

        let mut seen_stdout = 0;
        let mut seen_stderr = 0;
        loop {
            let invocation = tokio::select! {
                result = self
                    .ssm
                    .get_command_invocation()
                    .command_id(&command_id)
                    .instance_id(&handle.instance_id)
                    .send() => {
                        match result {
                            Ok(inv) => inv,
                            Err(e) => {
                                let debug = format!("{e:?}");
                                if debug.contains("InvocationDoesNotExist") {
                                    sleep(Duration::from_secs(2)).await;
                                    continue;
                                }
                                return Err(ssm_err(e));
                            }
                        }
                    }
                signal = tokio::signal::ctrl_c() => {
                    if let Err(err) = signal {
                        return Err(WorkstationError::Io(err.to_string()));
                    }
                    if let Err(err) = self.ssm.cancel_command().command_id(&command_id).send().await {
                        warn!(command_id, error = %err, "failed to cancel SSM command after SIGINT");
                    }
                    return Err(WorkstationError::Command(
                        "remote command cancelled by SIGINT".to_string(),
                    ));
                }
            };

            if let Some(output) = invocation.standard_output_content() {
                write_delta(&mut stdout, output, &mut seen_stdout).await?;
            }
            if let Some(output) = invocation.standard_error_content() {
                write_delta(&mut stderr, output, &mut seen_stderr).await?;
            }

            let status = invocation
                .status()
                .map(|status| status.as_str())
                .unwrap_or_default();
            if matches!(status, "Success" | "Failed" | "Cancelled" | "TimedOut") {
                return Ok(invocation.response_code());
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn bootstrap(
        &self,
        workstation_id: &str,
        profile: &WorkstationProfile,
    ) -> Result<BootstrapDrift, WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let toolchain = read_rust_toolchain_toml();
        let local = bootstrap::fingerprint(profile, &toolchain);
        let remote = self
            .remote_command_text(
                &handle.instance_id,
                "cat /etc/tokeira/workstation-fingerprint 2>/dev/null || echo MISSING",
            )
            .await?;
        if remote.trim() == local {
            return Ok(BootstrapDrift::UpToDate);
        }
        let script = bootstrap::render(&BootstrapContext {
            workstation_id: workstation_id.to_string(),
            bootstrap_fingerprint: local.clone(),
            profile: profile.clone(),
            cache_volume_id: handle.cache_volume_id,
            repo_volume_id: handle.repo_volume_id,
            rust_toolchain_toml: toolchain,
        });
        self.remote_command_text(&handle.instance_id, &script)
            .await?;
        Ok(BootstrapDrift::Drift {
            local_fingerprint: local,
            remote_fingerprint: remote.trim().to_string(),
        })
    }

    pub async fn idle_defer(
        &self,
        workstation_id: &str,
        until: DateTime<Utc>,
    ) -> Result<(), WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let command = format!(
            "mkdir -p /var/lib/tokeira && printf '%s\\n' {} > /var/lib/tokeira/idle-defer.timestamp",
            until.timestamp()
        );
        self.remote_command_text(&handle.instance_id, &command)
            .await?;
        Ok(())
    }

    pub fn ensure_session_manager_plugin() -> Result<(), WorkstationError> {
        which::which("session-manager-plugin")
            .map(|_| ())
            .map_err(|_| WorkstationError::SessionManagerPluginMissing)
    }

    pub async fn start_interactive_session(
        &self,
        workstation_id: &str,
    ) -> Result<i32, WorkstationError> {
        Self::ensure_session_manager_plugin()?;
        let handle = self.resolve_handle(workstation_id).await?;
        let status = Command::new("aws")
            .args([
                "ssm",
                "start-session",
                "--target",
                &handle.instance_id,
                "--region",
                &self.region,
            ])
            .status()
            .await
            .map_err(|err| WorkstationError::Io(err.to_string()))?;
        Ok(status.code().unwrap_or(1))
    }

    /// Pre-flight discovery: resolves subnet, VPC, AZ, and AMI for the
    /// workstation profile. Called before IaC provisioning to gather the
    /// values that the module needs.
    pub async fn preflight(
        &self,
        profile: &WorkstationProfile,
    ) -> Result<PreflightResult, WorkstationError> {
        let subnet = self.resolve_subnet(profile.subnet_id.as_deref()).await?;
        let ami_id = self.resolve_ami(profile.ami_family).await?;
        Ok(PreflightResult {
            subnet_id: subnet.subnet_id,
            vpc_id: subnet.vpc_id,
            availability_zone: subnet.availability_zone,
            ami_id,
        })
    }

    /// Run a shell command on the workstation and return its stdout.
    /// Resolves the workstation ID to an instance ID from persisted state.
    pub async fn remote_command_text_raw(
        &self,
        workstation_id: &str,
        command: &str,
    ) -> Result<String, WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        self.remote_command_text(&handle.instance_id, command).await
    }

    pub async fn github_key_generate(
        &self,
        workstation_id: &str,
    ) -> Result<String, WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let key_path = format!("/home/tokeira/.ssh/tokeira-workstation-{workstation_id}");
        let command = format!(
            r#"mkdir -p /home/tokeira/.ssh && rm -f {key_path} {key_path}.pub && ssh-keygen -t ed25519 -N '' -C 'tokeira-workstation-{workstation_id}' -f {key_path} >/dev/null 2>&1 && chown tokeira:tokeira {key_path} {key_path}.pub && cat {key_path}.pub"#
        );
        self.remote_command_text(&handle.instance_id, &command)
            .await
    }

    pub async fn github_key_add(
        &self,
        workstation_id: &str,
        _repo: &GithubRepo,
        _read_only: bool,
    ) -> Result<String, WorkstationError> {
        self.github_key_generate(workstation_id).await
    }

    pub async fn github_key_configure(
        &self,
        workstation_id: &str,
        repo: &GithubRepo,
    ) -> Result<(), WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let host_alias = format!("github.com-tokeira-{workstation_id}");
        let key_path = format!("/home/tokeira/.ssh/tokeira-workstation-{workstation_id}");

        // Add host alias to SSH config so git uses the deploy key
        let ssh_config_cmd = format!(
            r#"grep -q 'Host {host_alias}' /home/tokeira/.ssh/config 2>/dev/null || cat >> /home/tokeira/.ssh/config <<'EOF'

Host {host_alias}
  HostName github.com
  IdentityFile {key_path}
  IdentitiesOnly yes
  User git
EOF
chown tokeira:tokeira /home/tokeira/.ssh/config"#
        );
        self.remote_command_text(&handle.instance_id, &ssh_config_cmd)
            .await?;

        // Rewrite git remote to use the host alias (only if repo already cloned)
        let git_cmd = format!(
            "if [ -d /work/repo/tokeira/.git ]; then su tokeira -c 'git -C /work/repo/tokeira remote set-url origin git@{host_alias}:{owner}/{name}.git'; fi",
            owner = repo.owner,
            name = repo.name
        );
        self.remote_command_text(&handle.instance_id, &git_cmd)
            .await?;
        Ok(())
    }

    pub async fn github_key_remove(
        &self,
        workstation_id: &str,
        repo: &GithubRepo,
    ) -> Result<(), WorkstationError> {
        let handle = self.resolve_handle(workstation_id).await?;
        let key_path = format!("/home/tokeira/.ssh/tokeira-workstation-{workstation_id}");
        let command = format!(
            "rm -f {key_path} {key_path}.pub; su tokeira -c 'git -C /work/repo/tokeira remote set-url origin https://github.com/{owner}/{name}.git' || true",
            owner = repo.owner,
            name = repo.name
        );
        self.remote_command_text(&handle.instance_id, &command)
            .await?;
        Ok(())
    }

    async fn up_existing(
        &self,
        profile: &WorkstationProfile,
        handle: &WorkstationHandle,
    ) -> Result<UpOutcome, WorkstationError> {
        let instance = self
            .describe_instance_by_id(&handle.instance_id)
            .await?
            .ok_or_else(|| WorkstationError::NotFound(handle.workstation_id.clone()))?;
        match instance_state(&instance).as_str() {
            "stopped" | "Stopped" => {
                self.ec2
                    .start_instances()
                    .instance_ids(&handle.instance_id)
                    .send()
                    .await
                    .map_err(ec2_err)?;
                self.wait_for_instance_state(&handle.instance_id, "running")
                    .await?;
                self.ensure_public_ip(&handle.instance_id, &handle.workstation_id)
                    .await?;
                append_uptime_event(&handle.workstation_id, "start")?;
                write_state(handle)?;
                let drift = self.bootstrap(&handle.workstation_id, profile).await?;
                Ok(UpOutcome::Resumed {
                    handle: handle.clone(),
                    bootstrap_drift: drift,
                    repo_clone_warning: self.repo_clone_warning(&handle.instance_id).await.ok(),
                })
            }
            "running" | "Running" => Ok(UpOutcome::AlreadyRunning {
                handle: handle.clone(),
            }),
            state => Err(WorkstationError::UnexpectedState {
                workstation_id: handle.workstation_id.clone(),
                state: state.to_string(),
            }),
        }
    }

    async fn create(
        &self,
        profile: &WorkstationProfile,
        workstation_override: Option<&str>,
    ) -> Result<UpOutcome, WorkstationError> {
        let workstation_id = workstation_override
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("ws-{}", Uuid::new_v4()));
        let subnet = self.resolve_subnet(profile.subnet_id.as_deref()).await?;
        let subnet_id = subnet.subnet_id.clone();
        let role_name = format!("tokeira-workstation-{workstation_id}-role");
        let profile_name = format!("tokeira-workstation-{workstation_id}-profile");
        let sg_name = format!("tokeira-workstation-{workstation_id}-sg");

        self.ensure_iam(&role_name, &profile_name).await?;
        let sg_id = self.create_security_group(&sg_name, &subnet.vpc_id).await?;
        let availability_zone = subnet.availability_zone;
        let cache_volume_id = self
            .create_volume(
                &workstation_id,
                "Cache",
                profile.cache_volume_gib,
                &availability_zone,
            )
            .await?;
        let repo_volume_id = self
            .create_volume(
                &workstation_id,
                "Repo",
                profile.repo_volume_gib,
                &availability_zone,
            )
            .await?;
        let toolchain = read_rust_toolchain_toml();
        let fingerprint = bootstrap::fingerprint(profile, &toolchain);
        let script = bootstrap::render(&BootstrapContext {
            workstation_id: workstation_id.clone(),
            bootstrap_fingerprint: fingerprint.clone(),
            profile: profile.clone(),
            cache_volume_id: cache_volume_id.clone(),
            repo_volume_id: repo_volume_id.clone(),
            rust_toolchain_toml: toolchain,
        });
        let ami_id = self.resolve_ami(profile.ami_family).await?;
        let run = self
            .ec2
            .run_instances()
            .image_id(ami_id)
            .instance_type(InstanceType::from(profile.instance_type.as_str()))
            .min_count(1)
            .max_count(1)
            .iam_instance_profile(
                IamInstanceProfileSpecification::builder()
                    .name(&profile_name)
                    .build(),
            )
            .network_interfaces(
                InstanceNetworkInterfaceSpecification::builder()
                    .device_index(0)
                    .subnet_id(&subnet_id)
                    .groups(&sg_id)
                    .associate_public_ip_address(profile.subnet_id.is_none())
                    .build(),
            )
            .block_device_mappings(
                BlockDeviceMapping::builder()
                    .device_name("/dev/sda1")
                    .ebs(
                        EbsBlockDevice::builder()
                            .volume_size(profile.root_volume_gib as i32)
                            .volume_type(VolumeType::Gp3)
                            .encrypted(true)
                            .delete_on_termination(true)
                            .build(),
                    )
                    .build(),
            )
            .tag_specifications(tag_specification(
                ResourceType::Instance,
                &workstation_id,
                Some(&fingerprint),
            ))
            .user_data(general_purpose::STANDARD.encode(script.as_bytes()))
            .send()
            .await
            .map_err(ec2_err)?;
        let instance_id = run
            .instances()
            .first()
            .and_then(|instance| instance.instance_id())
            .ok_or_else(|| {
                WorkstationError::Ec2("RunInstances did not return instance id".to_string())
            })?
            .to_string();

        self.wait_for_instance_state(&instance_id, "running")
            .await?;

        self.ec2
            .attach_volume()
            .instance_id(&instance_id)
            .volume_id(&cache_volume_id)
            .device("/dev/sdf")
            .send()
            .await
            .map_err(ec2_err)?;

        self.ec2
            .attach_volume()
            .instance_id(&instance_id)
            .volume_id(&repo_volume_id)
            .device("/dev/sdg")
            .send()
            .await
            .map_err(ec2_err)?;
        let repo_clone_warning = self
            .wait_for_bootstrap_fingerprint(&instance_id, &fingerprint)
            .await?;

        let handle = WorkstationHandle {
            workstation_id: workstation_id.clone(),
            instance_id,
            cache_volume_id,
            repo_volume_id,
            root_volume_id: String::new(),
            security_group_id: sg_id,
            iam_role_name: role_name,
            instance_profile_name: profile_name,
            region: self.region.clone(),
            subnet_id,
        };
        write_state(&handle)?;
        append_uptime_event(&workstation_id, "create")?;
        info!(%workstation_id, "created remote workstation");
        Ok(UpOutcome::Created {
            handle,
            bootstrap_fingerprint: fingerprint,
            repo_clone_warning,
        })
    }

    async fn discover(
        &self,
        workstation_id: Option<&str>,
    ) -> Result<Vec<WorkstationHandle>, WorkstationError> {
        let mut request = self.ec2.describe_instances().filters(
            Filter::builder()
                .name(format!("tag:{WORKSTATION_TAG_KEY}"))
                .values(WORKSTATION_TAG_VALUE)
                .build(),
        );
        if let Some(id) = workstation_id {
            request = request.filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_ID_TAG_KEY}"))
                    .values(id)
                    .build(),
            );
        }
        let output = request
            .send()
            .await
            .map_err(ec2_err)?;
        let mut handles = Vec::new();
        for reservation in output.reservations() {
            for instance in reservation.instances() {
                if let Some(handle) = handle_from_instance(instance, &self.region) {
                    handles.push(self.enrich_handle(handle).await?);
                }
            }
        }
        Ok(handles)
    }

    async fn enrich_handle(
        &self,
        mut handle: WorkstationHandle,
    ) -> Result<WorkstationHandle, WorkstationError> {
        if handle.cache_volume_id.is_empty()
            && let Some(volume_id) = self.find_volume_id(&handle.workstation_id, "Cache").await?
        {
            handle.cache_volume_id = volume_id;
        }
        if handle.repo_volume_id.is_empty()
            && let Some(volume_id) = self.find_volume_id(&handle.workstation_id, "Repo").await?
        {
            handle.repo_volume_id = volume_id;
        }
        Ok(handle)
    }

    async fn resolve_handle(
        &self,
        workstation_id: &str,
    ) -> Result<WorkstationHandle, WorkstationError> {
        let deployment_dir = crate::deployment::deployment_dir_for(workstation_id);
        let backend = tokeira_state::LocalBackend::new(deployment_dir.join("state/infra"));
        let store = tokeira_state::CasStore::<tokeira_iac::InfraState>::new(
            Box::new(backend),
            "infra".to_string(),
        );
        let (state, _version) = store.load().await.map_err(|e| {
            WorkstationError::Io(format!("failed to load workstation state: {e}"))
        })?;

        let instance_resource_id =
            tokeira_iac::ResourceId(format!("ec2-instance-tokeira-ws-{workstation_id}"));
        let instance_state = state
            .resources
            .get(&instance_resource_id)
            .ok_or_else(|| WorkstationError::NotFound(workstation_id.to_string()))?;

        let props = &instance_state.properties;

        let sg_resource_id = tokeira_iac::ResourceId(format!(
            "sg-tokeira-workstation-{workstation_id}-sg"
        ));
        let security_group_id = state
            .resources
            .get(&sg_resource_id)
            .map(|s| s.physical_id.clone())
            .unwrap_or_default();

        let cache_vol_resource_id = tokeira_iac::ResourceId(format!(
            "ebs-volume-tokeira-ws-{workstation_id}-cache"
        ));
        let cache_volume_id = state
            .resources
            .get(&cache_vol_resource_id)
            .map(|s| s.physical_id.clone())
            .unwrap_or_default();

        let repo_vol_resource_id = tokeira_iac::ResourceId(format!(
            "ebs-volume-tokeira-ws-{workstation_id}-repo"
        ));
        let repo_volume_id = state
            .resources
            .get(&repo_vol_resource_id)
            .map(|s| s.physical_id.clone())
            .unwrap_or_default();

        Ok(WorkstationHandle {
            workstation_id: workstation_id.to_string(),
            instance_id: instance_state.physical_id.clone(),
            cache_volume_id,
            repo_volume_id,
            root_volume_id: String::new(),
            security_group_id,
            iam_role_name: format!("tokeira-workstation-{workstation_id}-role"),
            instance_profile_name: format!("tokeira-workstation-{workstation_id}-profile"),
            region: self.region.clone(),
            subnet_id: props
                .get("subnet_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        })
    }

    /// Look up a live EC2 instance by its instance ID (from persisted state).
    async fn describe_instance_by_id(
        &self,
        instance_id: &str,
    ) -> Result<Option<aws_sdk_ec2::types::Instance>, WorkstationError> {
        let output = self
            .ec2
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .map_err(ec2_err)?;
        let instance = output
            .reservations()
            .iter()
            .flat_map(|r| r.instances())
            .next()
            .cloned();
        Ok(instance)
    }

    async fn find_instance(
        &self,
        workstation_id: Option<&str>,
    ) -> Result<Option<aws_sdk_ec2::types::Instance>, WorkstationError> {
        let mut request = self.ec2.describe_instances().filters(
            Filter::builder()
                .name(format!("tag:{WORKSTATION_TAG_KEY}"))
                .values(WORKSTATION_TAG_VALUE)
                .build(),
        );
        if let Some(id) = workstation_id {
            request = request.filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_ID_TAG_KEY}"))
                    .values(id)
                    .build(),
            );
        }
        let output = request
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(output
            .reservations()
            .iter()
            .flat_map(|reservation| reservation.instances().iter())
            .next()
            .cloned())
    }

    async fn remote_command_text(
        &self,
        instance_id: &str,
        command: &str,
    ) -> Result<String, WorkstationError> {
        let response = self
            .ssm
            .send_command()
            .document_name("AWS-RunShellScript")
            .instance_ids(instance_id)
            .parameters("commands", vec![command.to_string()])
            .send()
            .await
            .map_err(ssm_err)?;
        let command_id = response
            .command()
            .and_then(|command| command.command_id())
            .ok_or_else(|| {
                WorkstationError::Ssm("SSM SendCommand did not return command id".to_string())
            })?
            .to_string();
        loop {
            match self
                .ssm
                .get_command_invocation()
                .command_id(&command_id)
                .instance_id(instance_id)
                .send()
                .await
            {
                Err(e) => {
                    let debug = format!("{e:?}");
                    if debug.contains("InvocationDoesNotExist") {
                        // SSM hasn't registered the invocation yet — retry
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    return Err(ssm_err(e));
                }
                Ok(invocation) => {
                    let status = invocation
                        .status()
                        .map(|status| status.as_str())
                        .unwrap_or_default();
                    if matches!(status, "Success" | "Failed" | "Cancelled" | "TimedOut") {
                        if invocation.response_code() == 0 {
                            return Ok(invocation
                                .standard_output_content()
                                .unwrap_or_default()
                                .to_string());
                        }
                        let stderr = invocation
                            .standard_error_content()
                            .unwrap_or_default()
                            .to_string();
                        let stdout = invocation
                            .standard_output_content()
                            .unwrap_or_default()
                            .to_string();
                        let detail = if !stderr.is_empty() {
                            stderr
                        } else if !stdout.is_empty() {
                            stdout
                        } else {
                            format!("command exited with status {}", invocation.response_code())
                        };
                        return Err(WorkstationError::Command(detail));
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn repo_clone_warning(&self, instance_id: &str) -> Result<String, WorkstationError> {
        self.remote_command_text(
            instance_id,
            "cat /etc/tokeira/repo-clone-status 2>/dev/null || true",
        )
        .await
    }

    async fn ensure_iam(
        &self,
        role_name: &str,
        profile_name: &str,
    ) -> Result<(), WorkstationError> {
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ec2.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string();

        // Create role (ignore AlreadyExists — idempotent on retry)
        match self
            .iam
            .create_role()
            .role_name(role_name)
            .assume_role_policy_document(trust_policy)
            .send()
            .await
        {
            Ok(_) => info!(role_name, "created IAM role"),
            Err(err) => {
                let debug = format!("{err:?}");
                if debug.contains("EntityAlreadyExists") {
                    info!(role_name, "IAM role already exists, reusing");
                } else {
                    return Err(iam_err(err));
                }
            }
        }

        // Attach policy (idempotent — attaching an already-attached policy is a no-op)
        self.iam
            .attach_role_policy()
            .role_name(role_name)
            .policy_arn("arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
            .send()
            .await
            .map_err(iam_err)?;

        // Create instance profile (ignore AlreadyExists)
        match self
            .iam
            .create_instance_profile()
            .instance_profile_name(profile_name)
            .send()
            .await
        {
            Ok(_) => info!(profile_name, "created IAM instance profile"),
            Err(err) => {
                let debug = format!("{err:?}");
                if debug.contains("EntityAlreadyExists") {
                    info!(profile_name, "IAM instance profile already exists, reusing");
                } else {
                    return Err(iam_err(err));
                }
            }
        }

        // Add role to profile (ignore LimitExceeded which means role is already added)
        match self
            .iam
            .add_role_to_instance_profile()
            .instance_profile_name(profile_name)
            .role_name(role_name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                let debug = format!("{err:?}");
                if debug.contains("LimitExceeded") || debug.contains("EntityAlreadyExists") {
                    // Role already in profile — fine
                } else {
                    return Err(iam_err(err));
                }
            }
        }

        // IAM is eventually consistent. Poll until the instance profile is visible
        // to GetInstanceProfile before proceeding to RunInstances.
        info!(profile_name, "waiting for IAM instance profile propagation");
        for _ in 0..30 {
            match self
                .iam
                .get_instance_profile()
                .instance_profile_name(profile_name)
                .send()
                .await
            {
                Ok(output) => {
                    // Also verify the role is attached (not just the profile existing)
                    let has_role = output
                        .instance_profile()
                        .map(|p| !p.roles().is_empty())
                        .unwrap_or(false);
                    if has_role {
                        info!(profile_name, "IAM instance profile ready");
                        return Ok(());
                    }
                }
                Err(_) => {}
            }
            sleep(Duration::from_secs(2)).await;
        }
        Err(WorkstationError::Iam(format!(
            "instance profile {profile_name} did not become available within 60 seconds"
        )))
    }

    async fn create_security_group(
        &self,
        name: &str,
        vpc_id: &str,
    ) -> Result<String, WorkstationError> {
        let output = self
            .ec2
            .create_security_group()
            .group_name(name)
            .description("Tokeira remote workstation")
            .vpc_id(vpc_id)
            .send()
            .await
            .map_err(ec2_err)?;
        output.group_id().map(ToOwned::to_owned).ok_or_else(|| {
            WorkstationError::Ec2("CreateSecurityGroup did not return group id".to_string())
        })
    }

    async fn resolve_subnet(
        &self,
        explicit_subnet_id: Option<&str>,
    ) -> Result<ResolvedSubnet, WorkstationError> {
        let mut request = self.ec2.describe_subnets();
        if let Some(subnet_id) = explicit_subnet_id {
            request = request.subnet_ids(subnet_id);
        } else {
            request = request.filters(
                Filter::builder()
                    .name("default-for-az")
                    .values("true")
                    .build(),
            );
        }
        let output = request
            .send()
            .await
            .map_err(ec2_err)?;
        let subnet = output
            .subnets()
            .first()
            .ok_or(WorkstationError::NoPublicSubnet)?;
        Ok(ResolvedSubnet {
            subnet_id: subnet
                .subnet_id()
                .ok_or(WorkstationError::NoPublicSubnet)?
                .to_string(),
            vpc_id: subnet
                .vpc_id()
                .ok_or(WorkstationError::NoPublicSubnet)?
                .to_string(),
            availability_zone: subnet
                .availability_zone()
                .ok_or(WorkstationError::NoPublicSubnet)?
                .to_string(),
        })
    }

    async fn wait_for_instance_state(
        &self,
        instance_id: &str,
        desired_state: &str,
    ) -> Result<(), WorkstationError> {
        let timeout = Duration::from_secs(300);
        match desired_state {
            "running" => {
                self.ec2
                    .wait_until_instance_running()
                    .instance_ids(instance_id)
                    .wait(timeout)
                    .await
                    .map_err(|err| WorkstationError::Ec2(format!(
                        "timed out waiting for instance {instance_id} to reach running: {err}"
                    )))?;
            }
            "stopped" => {
                self.ec2
                    .wait_until_instance_stopped()
                    .instance_ids(instance_id)
                    .wait(timeout)
                    .await
                    .map_err(|err| WorkstationError::Ec2(format!(
                        "timed out waiting for instance {instance_id} to reach stopped: {err}"
                    )))?;
            }
            "terminated" => {
                self.ec2
                    .wait_until_instance_terminated()
                    .instance_ids(instance_id)
                    .wait(timeout)
                    .await
                    .map_err(|err| WorkstationError::Ec2(format!(
                        "timed out waiting for instance {instance_id} to reach terminated: {err}"
                    )))?;
            }
            _ => {
                // Fallback to manual polling for unexpected states
                for _ in 0..60 {
                    let output = self
                        .ec2
                        .describe_instances()
                        .instance_ids(instance_id)
                        .send()
                        .await
                        .map_err(ec2_err)?;
                    let state = output
                        .reservations()
                        .iter()
                        .flat_map(|reservation| reservation.instances().iter())
                        .next()
                        .map(instance_state)
                        .unwrap_or_default();
                    if state.eq_ignore_ascii_case(desired_state) {
                        return Ok(());
                    }
                    sleep(Duration::from_secs(5)).await;
                }
                return Err(WorkstationError::UnexpectedState {
                    workstation_id: instance_id.to_string(),
                    state: format!("did not reach {desired_state}"),
                });
            }
        }
        Ok(())
    }

    async fn wait_for_bootstrap_fingerprint(
        &self,
        instance_id: &str,
        expected: &str,
    ) -> Result<Option<String>, WorkstationError> {
        for _ in 0..90 {
            match self
                .remote_command_text(
                    instance_id,
                    "cat /etc/tokeira/workstation-fingerprint 2>/dev/null || echo MISSING",
                )
                .await
            {
                Ok(value) if value.trim() == expected => {
                    return Ok(self.repo_clone_warning(instance_id).await.ok());
                }
                Ok(_) | Err(_) => {
                    sleep(Duration::from_secs(10)).await;
                }
            }
        }
        Err(WorkstationError::UnexpectedState {
            workstation_id: instance_id.to_string(),
            state: "bootstrap fingerprint was not written within timeout".to_string(),
        })
    }

    async fn ensure_public_ip(
        &self,
        instance_id: &str,
        workstation_id: &str,
    ) -> Result<(), WorkstationError> {
        let allocation_id = match self.find_workstation_eip(workstation_id).await? {
            Some(allocation_id) => allocation_id,
            None => {
                let allocation = self
                    .ec2
                    .allocate_address()
                    .send()
                    .await
                    .map_err(ec2_err)?;
                let allocation_id = allocation
                    .allocation_id()
                    .ok_or_else(|| {
                        WorkstationError::Ec2(
                            "AllocateAddress did not return allocation id".to_string(),
                        )
                    })?
                    .to_string();
                self.ec2
                    .create_tags()
                    .resources(&allocation_id)
                    .tags(
                        Tag::builder()
                            .key(WORKSTATION_TAG_KEY)
                            .value(WORKSTATION_TAG_VALUE)
                            .build(),
                    )
                    .tags(
                        Tag::builder()
                            .key(WORKSTATION_ID_TAG_KEY)
                            .value(workstation_id)
                            .build(),
                    )
                    .tags(
                        Tag::builder()
                            .key(WORKSTATION_OWNED_EIP_TAG_KEY)
                            .value(WORKSTATION_TAG_VALUE)
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(ec2_err)?;
                allocation_id
            }
        };
        self.ec2
            .associate_address()
            .instance_id(instance_id)
            .allocation_id(allocation_id)
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(())
    }

    async fn release_owned_eip(&self, workstation_id: &str) -> Result<(), WorkstationError> {
        let output = self
            .ec2
            .describe_addresses()
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_TAG_KEY}"))
                    .values(WORKSTATION_TAG_VALUE)
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_ID_TAG_KEY}"))
                    .values(workstation_id)
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_OWNED_EIP_TAG_KEY}"))
                    .values(WORKSTATION_TAG_VALUE)
                    .build(),
            )
            .send()
            .await
            .map_err(ec2_err)?;
        for address in output.addresses() {
            if let Some(association_id) = address.association_id()
                && let Err(err) = self
                    .ec2
                    .disassociate_address()
                    .association_id(association_id)
                    .send()
                    .await
            {
                warn!(error = %err, "failed to disassociate workstation EIP");
            }
            if let Some(allocation_id) = address.allocation_id()
                && let Err(err) = self
                    .ec2
                    .release_address()
                    .allocation_id(allocation_id)
                    .send()
                    .await
            {
                warn!(error = %err, "failed to release workstation EIP");
            }
        }
        Ok(())
    }

    async fn find_workstation_eip(
        &self,
        workstation_id: &str,
    ) -> Result<Option<String>, WorkstationError> {
        let output = self
            .ec2
            .describe_addresses()
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_TAG_KEY}"))
                    .values(WORKSTATION_TAG_VALUE)
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_ID_TAG_KEY}"))
                    .values(workstation_id)
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_OWNED_EIP_TAG_KEY}"))
                    .values(WORKSTATION_TAG_VALUE)
                    .build(),
            )
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(output
            .addresses()
            .iter()
            .find_map(|address| address.allocation_id().map(ToOwned::to_owned)))
    }

    async fn create_volume(
        &self,
        workstation_id: &str,
        name: &str,
        size_gib: u32,
        availability_zone: &str,
    ) -> Result<String, WorkstationError> {
        let output = self
            .ec2
            .create_volume()
            .availability_zone(availability_zone)
            .size(size_gib as i32)
            .volume_type(VolumeType::Gp3)
            .encrypted(true)
            .tag_specifications(tag_specification(
                ResourceType::Volume,
                workstation_id,
                None,
            ))
            .send()
            .await
            .map_err(ec2_err)?;
        let volume_id = output
            .volume_id()
            .ok_or_else(|| {
                WorkstationError::Ec2("CreateVolume did not return volume id".to_string())
            })?
            .to_string();

        // Wait for volume to reach 'available' state before proceeding
        self.wait_for_volume_available(&volume_id).await?;

        self.ec2
            .create_tags()
            .resources(&volume_id)
            .tags(Tag::builder().key("Name").value(name).build())
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(volume_id)
    }

    async fn wait_for_volume_available(
        &self,
        volume_id: &str,
    ) -> Result<(), WorkstationError> {
        self.ec2
            .wait_until_volume_available()
            .volume_ids(volume_id)
            .wait(Duration::from_secs(120))
            .await
            .map_err(|err| WorkstationError::Ec2(format!(
                "timed out waiting for volume {volume_id} to reach available: {err}"
            )))?;
        Ok(())
    }

    async fn find_volume_id(
        &self,
        workstation_id: &str,
        name: &str,
    ) -> Result<Option<String>, WorkstationError> {
        let output = self
            .ec2
            .describe_volumes()
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_TAG_KEY}"))
                    .values(WORKSTATION_TAG_VALUE)
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name(format!("tag:{WORKSTATION_ID_TAG_KEY}"))
                    .values(workstation_id)
                    .build(),
            )
            .filters(Filter::builder().name("tag:Name").values(name).build())
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(output
            .volumes()
            .iter()
            .find_map(|volume| volume.volume_id().map(ToOwned::to_owned)))
    }

    async fn volume_status(
        &self,
        volume_id: &str,
    ) -> Result<Option<VolumeStatus>, WorkstationError> {
        if volume_id.is_empty() {
            return Ok(None);
        }
        let output = self
            .ec2
            .describe_volumes()
            .volume_ids(volume_id)
            .send()
            .await
            .map_err(ec2_err)?;
        Ok(output.volumes().first().map(|volume| VolumeStatus {
            size_gib: volume.size().map(|size| size as u32),
            state: volume.state().map(|state| state.as_str().to_string()),
        }))
    }

    async fn resolve_ami(&self, family: AmiFamily) -> Result<String, WorkstationError> {
        let name = match family {
            AmiFamily::Ubuntu2404 => {
                "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
            }
            AmiFamily::AmazonLinux2023 => {
                "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
            }
        };
        let output = self
            .ssm
            .get_parameter()
            .name(name)
            .send()
            .await
            .map_err(ssm_err)?;
        output
            .parameter()
            .and_then(|parameter| parameter.value())
            .map(ToOwned::to_owned)
            .ok_or_else(|| WorkstationError::Ssm(format!("parameter {name} had no value")))
    }

    async fn cleanup_deploy_keys(&self, workstation_id: &str) -> Result<(), WorkstationError> {
        let path = state_dir(workstation_id)?.join("deploy-keys.jsonl");
        let Ok(contents) = fs::read_to_string(path) else {
            return Ok(());
        };
        for entry in live_deploy_key_entries(&contents) {
            let Some((owner, name)) = entry.repo.split_once('/') else {
                warn!(repo = %entry.repo, "skipping invalid deploy-key registry repo");
                continue;
            };
            let settings_url = format!("https://github.com/{}/{}/settings/keys", owner, name);
            let status = Command::new("gh")
                .args([
                    "api",
                    "--method",
                    "DELETE",
                    &format!("repos/{owner}/{name}/keys/{}", entry.key_id),
                ])
                .status()
                .await;
            match status {
                Ok(status) if status.success() => {}
                Ok(_) => warn!(
                    repo = %entry.repo,
                    key_id = %entry.key_id,
                    settings_url,
                    workstation_id,
                    "failed to delete deploy key; remove manually from GitHub settings"
                ),
                Err(err) => warn!(
                    repo = %entry.repo,
                    key_id = %entry.key_id,
                    settings_url,
                    workstation_id,
                    error = %err,
                    "failed to invoke gh while cleaning deploy key"
                ),
            }
        }
        Ok(())
    }
}

pub fn hourly_rate(region: &str, instance_type: &str) -> Option<f64> {
    match (region, instance_type) {
        ("eu-west-2", "c8gd.8xlarge") => Some(1.87776),
        ("us-east-1", "c8gd.8xlarge") => Some(1.56768),
        _ => None,
    }
}

fn tag_specification(
    resource_type: ResourceType,
    workstation_id: &str,
    fingerprint: Option<&str>,
) -> TagSpecification {
    let mut tags = vec![
        Tag::builder()
            .key(WORKSTATION_TAG_KEY)
            .value(WORKSTATION_TAG_VALUE)
            .build(),
        Tag::builder()
            .key(WORKSTATION_ID_TAG_KEY)
            .value(workstation_id)
            .build(),
    ];
    if let Some(value) = fingerprint {
        tags.push(
            Tag::builder()
                .key(BOOTSTRAP_FINGERPRINT_TAG_KEY)
                .value(value)
                .build(),
        );
    }
    TagSpecification::builder()
        .resource_type(resource_type)
        .set_tags(Some(tags))
        .build()
}

fn tag_value(tags: &[Tag], key: &str) -> Option<String> {
    tags.iter()
        .find(|tag| tag.key() == Some(key))
        .and_then(|tag| tag.value())
        .map(ToOwned::to_owned)
}

fn handle_from_instance(
    instance: &aws_sdk_ec2::types::Instance,
    region: &str,
) -> Option<WorkstationHandle> {
    let tags = instance.tags();
    let workstation_id = tag_value(tags, WORKSTATION_ID_TAG_KEY)?;
    let instance_id = instance.instance_id()?.to_string();
    let mut volume_ids = instance
        .block_device_mappings()
        .iter()
        .filter_map(|mapping| mapping.ebs())
        .filter_map(|ebs| ebs.volume_id())
        .map(ToOwned::to_owned);
    let root_volume_id = volume_ids.next().unwrap_or_default();
    let cache_volume_id = volume_ids.next().unwrap_or_default();
    let repo_volume_id = volume_ids.next().unwrap_or_default();
    Some(WorkstationHandle {
        workstation_id: workstation_id.clone(),
        instance_id,
        cache_volume_id,
        repo_volume_id,
        root_volume_id,
        security_group_id: instance
            .security_groups()
            .first()
            .and_then(|group| group.group_id())
            .unwrap_or_default()
            .to_string(),
        iam_role_name: format!("tokeira-workstation-{workstation_id}-role"),
        instance_profile_name: format!("tokeira-workstation-{workstation_id}-profile"),
        region: region.to_string(),
        subnet_id: instance.subnet_id().unwrap_or_default().to_string(),
    })
}

fn instance_state(instance: &aws_sdk_ec2::types::Instance) -> String {
    instance
        .state()
        .and_then(|state| state.name())
        .map(|name| name.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn shell_command(cwd: &str, command: &[String]) -> String {
    let cwd = shlex::try_quote(cwd)
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| "''".to_string());
    let command = command
        .iter()
        .map(|part| {
            shlex::try_quote(part)
                .map(|value| value.into_owned())
                .unwrap_or_else(|_| "''".to_string())
        })
        .collect::<Vec<_>>()
        .join(" ");
    // Run as the tokeira user with a login shell so PATH and env are set
    format!("su tokeira -lc 'cd {cwd} && {command}'")
}

async fn write_delta(
    writer: &mut (impl AsyncWrite + Unpin),
    output: &str,
    seen: &mut usize,
) -> Result<(), WorkstationError> {
    let bytes = output.as_bytes();
    if *seen < bytes.len() {
        writer
            .write_all(&bytes[*seen..])
            .await
            .map_err(|err| WorkstationError::Io(err.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|err| WorkstationError::Io(err.to_string()))?;
        *seen = bytes.len();
    }
    Ok(())
}

fn default_repo_url() -> String {
    git_command(["config", "--get", "remote.origin.url"])
        .unwrap_or_else(|| "https://github.com/openai/tokeira.git".to_string())
}

fn git_config_value(key: &str) -> Option<String> {
    git_command(["config", "--get", key])
}

fn git_command<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn read_rust_toolchain_toml() -> String {
    fs::read_to_string("rust-toolchain.toml").unwrap_or_default()
}

fn state_root() -> Result<PathBuf, WorkstationError> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| WorkstationError::Io("HOME is not set".to_string()))?;
    Ok(home.join(".tokeira").join("workstations"))
}

fn state_dir(workstation_id: &str) -> Result<PathBuf, WorkstationError> {
    Ok(state_root()?.join(workstation_id))
}

fn write_state(handle: &WorkstationHandle) -> Result<(), WorkstationError> {
    let dir = state_dir(&handle.workstation_id)?;
    fs::create_dir_all(&dir).map_err(io_error)?;
    let bytes =
        serde_json::to_vec_pretty(handle).map_err(|err| WorkstationError::Io(err.to_string()))?;
    fs::write(dir.join("state.json"), bytes).map_err(io_error)?;
    fs::write(state_root()?.join(".latest"), &handle.workstation_id).map_err(io_error)?;
    Ok(())
}

fn write_state_with_status(
    handle: &WorkstationHandle,
    last_seen_state: &str,
) -> Result<(), WorkstationError> {
    let dir = state_dir(&handle.workstation_id)?;
    fs::create_dir_all(&dir).map_err(io_error)?;
    let value = serde_json::json!({
        "handle": handle,
        "last_seen_state": last_seen_state,
        "updated_at": Utc::now().to_rfc3339(),
    });
    let bytes =
        serde_json::to_vec_pretty(&value).map_err(|err| WorkstationError::Io(err.to_string()))?;
    fs::write(dir.join("state.json"), bytes).map_err(io_error)?;
    fs::write(state_root()?.join(".latest"), &handle.workstation_id).map_err(io_error)?;
    Ok(())
}

fn remove_state_dir(workstation_id: &str) -> Result<(), WorkstationError> {
    let dir = state_dir(workstation_id)?;
    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(io_error)?;
    }
    let latest = state_root()?.join(".latest");
    if let Ok(current) = fs::read_to_string(&latest)
        && current.trim() == workstation_id
    {
        fs::remove_file(latest).map_err(io_error)?;
    }
    Ok(())
}

fn append_uptime_event(workstation_id: &str, event: &str) -> Result<(), WorkstationError> {
    let dir = state_dir(workstation_id)?;
    fs::create_dir_all(&dir).map_err(io_error)?;
    let line = serde_json::json!({
        "event": event,
        "at": Utc::now().to_rfc3339(),
    });
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("uptime-log.jsonl"))
        .map_err(io_error)?;
    writeln!(file, "{line}").map_err(io_error)?;
    Ok(())
}

fn read_cumulative_uptime_hours(workstation_id: &str) -> f64 {
    let Ok(path) = state_dir(workstation_id) else {
        return 0.0;
    };
    let Ok(contents) = fs::read_to_string(path.join("uptime-log.jsonl")) else {
        return 0.0;
    };
    let mut last_start: Option<DateTime<Utc>> = None;
    let mut seconds = 0_i64;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let event = value
            .get("event")
            .and_then(|event| event.as_str())
            .unwrap_or_default();
        let Some(at) = value
            .get("at")
            .and_then(|at| at.as_str())
            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
            .map(|at| at.with_timezone(&Utc))
        else {
            continue;
        };
        match event {
            "create" | "start" => last_start = Some(at),
            "stop" => {
                if let Some(started_at) = last_start.take() {
                    seconds += (at - started_at).num_seconds().max(0);
                }
            }
            _ => {}
        }
    }
    if let Some(started_at) = last_start {
        seconds += (Utc::now() - started_at).num_seconds().max(0);
    }
    seconds as f64 / 3600.0
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn live_deploy_key_entries(contents: &str) -> Vec<DeployKeyRegistryEntry> {
    let mut entries = Vec::new();
    for line in contents.lines() {
        let Ok(entry) = serde_json::from_str::<DeployKeyRegistryEntry>(line) else {
            continue;
        };
        if entry.removed_at.is_some() {
            entries.retain(|existing: &DeployKeyRegistryEntry| {
                !(existing.repo == entry.repo && existing.key_id == entry.key_id)
            });
        } else {
            entries.push(entry);
        }
    }
    entries
}

fn io_error(error: io::Error) -> WorkstationError {
    WorkstationError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{GithubRepo, hourly_rate, live_deploy_key_entries};

    #[test]
    fn github_repo_parser_accepts_owner_name_only() {
        let repo = GithubRepo::parse("openai/tokeira.git").expect("owner/name should parse");
        assert_eq!(repo.owner, "openai");
        assert_eq!(repo.name, "tokeira");
        assert!(GithubRepo::parse("https://github.com/openai/tokeira").is_err());
        assert!(GithubRepo::parse("openai").is_err());
    }

    #[test]
    fn cost_table_returns_known_rates_and_unknowns() {
        assert_eq!(hourly_rate("eu-west-2", "c8gd.8xlarge"), Some(1.87776));
        assert_eq!(hourly_rate("eu-west-1", "c8gd.8xlarge"), None);
    }

    #[test]
    fn deploy_key_registry_keeps_only_live_entries() {
        let contents = r#"{"repo":"openai/tokeira","key_id":"1","removed_at":null}
{"repo":"openai/other","key_id":"2","removed_at":null}
{"repo":"openai/tokeira","key_id":"1","removed_at":"2026-05-11T00:00:00Z"}"#;
        let live = live_deploy_key_entries(contents);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].repo, "openai/other");
        assert_eq!(live[0].key_id, "2");
    }
}

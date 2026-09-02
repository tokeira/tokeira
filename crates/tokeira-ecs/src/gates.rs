//! ECS image-repository admission.
//!
//! Image publication is provider work and never writes deployment state.
//! Workloads instead resolve their private image coordinates from the ECR
//! repositories already recorded by infrastructure apply. This keeps the
//! existing deployment-state engine as the sole persistence owner while
//! preventing authored public or local references from reaching ECS.

use tokeira_iac::{InfraState, ResourceId, ResourceState};

/// Why an ECS workload could not resolve its private image repository.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EcsError {
    /// One or more required ECR repository records are absent or invalid.
    #[error("ECS image repositories are not deployable: {repositories:?}; {remediation}")]
    UnresolvedRepositories {
        /// Repository suffixes and concise validation evidence.
        repositories: Vec<String>,
        /// Operator command that establishes repository infrastructure.
        remediation: &'static str,
    },
}

/// Validate the definition-owned repository set required by one workload.
///
/// This crate deliberately has no catalogue of image names. The definition
/// passes the exact repositories used by the rendered task, keeping image
/// ownership at the authoring boundary while this function owns only the
/// provider-state checks common to every private ECR reference.
pub fn validate_repositories(
    state: &InfraState,
    project: &str,
    region: &str,
    repositories: &[&str],
) -> Result<(), EcsError> {
    validate_set(state, project, region, repositories)
}

/// Resolve a recorded ECR repository to the tagged reference ECS executes.
///
/// Infrastructure state, rather than a reconstructed registry hostname,
/// supplies the account-qualified repository URI. The caller supplies only
/// the repository suffix and the tag selected by the authored image policy.
pub fn resolved_image_ref(
    state: &InfraState,
    project: &str,
    region: &str,
    repository: &str,
    tag: &str,
) -> Result<String, EcsError> {
    validated_repository(state, project, region, repository)
        .map(|uri| format!("{uri}:{tag}"))
        .map_err(|reason| EcsError::UnresolvedRepositories {
            repositories: vec![format!("{repository} ({reason})")],
            remediation: "run `tkr infra apply` before deploying ECS workloads",
        })
}

fn validate_set(
    state: &InfraState,
    project: &str,
    region: &str,
    repositories: &[&str],
) -> Result<(), EcsError> {
    let invalid = repositories
        .iter()
        .filter_map(|repository| {
            validated_repository(state, project, region, repository)
                .err()
                .map(|reason| format!("{repository} ({reason})"))
        })
        .collect::<Vec<_>>();
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(EcsError::UnresolvedRepositories {
            repositories: invalid,
            remediation: "run `tkr infra apply` before deploying ECS workloads",
        })
    }
}

fn validated_repository(
    state: &InfraState,
    project: &str,
    region: &str,
    repository: &str,
) -> Result<String, String> {
    let expected_name = format!("{project}/{repository}");
    let id = ResourceId(format!("ecr-{expected_name}"));
    let record = state
        .resources
        .get(&id)
        .ok_or_else(|| format!("missing infrastructure state at `{}`", id.0))?;
    validate_record(record, &expected_name, region)
}

fn validate_record(
    record: &ResourceState,
    expected_name: &str,
    region: &str,
) -> Result<String, String> {
    if record.resource_type.0 != "EcrRepository" {
        return Err(format!(
            "recorded resource type is `{}`, expected `EcrRepository`",
            record.resource_type.0
        ));
    }
    let name = record
        .properties
        .get("repository_name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "recorded repository_name is missing".to_string())?;
    if name != expected_name {
        return Err(format!(
            "recorded repository_name is `{name}`, expected `{expected_name}`"
        ));
    }
    let uri = record
        .properties
        .get("repository_uri")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "recorded repository_uri is missing".to_string())?;
    validate_repository_uri(uri, expected_name, region)?;
    Ok(uri.to_string())
}

fn validate_repository_uri(uri: &str, expected_name: &str, region: &str) -> Result<(), String> {
    let (host, name) = uri
        .split_once('/')
        .ok_or_else(|| "recorded repository_uri has no registry host".to_string())?;
    if name != expected_name {
        return Err(format!(
            "recorded repository_uri names `{name}`, expected `{expected_name}`"
        ));
    }
    let commercial = format!(".dkr.ecr.{region}.amazonaws.com");
    let china = format!(".dkr.ecr.{region}.amazonaws.com.cn");
    let account = host
        .strip_suffix(&commercial)
        .or_else(|| host.strip_suffix(&china))
        .ok_or_else(|| format!("registry host `{host}` is not private ECR in `{region}`"))?;
    if account.len() != 12 || !account.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "registry host `{host}` has no 12-digit AWS account"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::ResourceType;

    const REGION: &str = "eu-west-2";
    const PROJECT: &str = "fixture";

    #[test]
    fn missing_state_names_the_definition_owned_set_and_remediation() {
        let state = InfraState::default();
        let repositories = ["runtime", "telemetry"];
        let error = validate_repositories(&state, PROJECT, REGION, &repositories)
            .expect_err("missing repositories")
            .to_string();
        for name in repositories {
            assert!(error.contains(name), "{error}");
        }
        assert!(error.contains("tkr infra apply"));
    }

    #[test]
    fn resolved_refs_use_recorded_account_region_and_authored_tag() {
        let mut state = InfraState::default();
        insert_repository(&mut state, "tokeirad", REGION);

        assert_eq!(
            resolved_image_ref(&state, PROJECT, REGION, "tokeirad", "latest")
                .expect("valid repository"),
            "123456789012.dkr.ecr.eu-west-2.amazonaws.com/fixture/tokeirad:latest"
        );

        let error = resolved_image_ref(&state, PROJECT, "us-east-1", "tokeirad", "latest")
            .expect_err("wrong region")
            .to_string();
        assert!(error.contains("not private ECR in `us-east-1`"), "{error}");
    }

    fn insert_repository(state: &mut InfraState, repository: &str, region: &str) {
        let name = format!("{PROJECT}/{repository}");
        state.resources.insert(
            ResourceId(format!("ecr-{name}")),
            ResourceState {
                resource_type: ResourceType::new("EcrRepository"),
                physical_id: format!("arn:aws:ecr:{region}:123456789012:repository/{name}"),
                properties: serde_json::json!({
                    "repository_name": name,
                    "repository_uri": format!(
                        "123456789012.dkr.ecr.{region}.amazonaws.com/{PROJECT}/{repository}"
                    ),
                }),
                dependencies: Vec::new(),
                created_at: "now".into(),
                updated_at: "now".into(),
                module: "images".into(),
            },
        );
    }
}

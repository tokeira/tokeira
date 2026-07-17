//! AWS SDK client bundle registered as a `ProvisionContext` extension.

pub(crate) mod ecr;

/// All AWS SDK clients needed by resource implementations.
///
/// Constructed once from a shared `SdkConfig` and registered on
/// `ProvisionContext` via `ctx.set_extension(AwsClients::new(&sdk_config))`.
/// Resource implementations retrieve it via `ctx.extension::<AwsClients>()`.
#[derive(Debug, Clone)]
pub struct AwsClients {
    pub ec2: aws_sdk_ec2::Client,
    pub eks: aws_sdk_eks::Client,
    pub ecs: aws_sdk_ecs::Client,
    pub iam: aws_sdk_iam::Client,
    pub ecr: aws_sdk_ecr::Client,
    pub autoscaling: aws_sdk_autoscaling::Client,
    pub s3: aws_sdk_s3::Client,
    pub dsql: aws_sdk_dsql::Client,
    pub dynamodb: aws_sdk_dynamodb::Client,
    pub elbv2: aws_sdk_elasticloadbalancingv2::Client,
    pub secretsmanager: aws_sdk_secretsmanager::Client,
    pub servicediscovery: aws_sdk_servicediscovery::Client,
    pub ssm: aws_sdk_ssm::Client,
    pub sts: aws_sdk_sts::Client,
}

impl AwsClients {
    /// Construct all SDK clients from a shared config.
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        Self {
            ec2: aws_sdk_ec2::Client::new(sdk_config),
            eks: aws_sdk_eks::Client::new(sdk_config),
            ecs: aws_sdk_ecs::Client::new(sdk_config),
            iam: aws_sdk_iam::Client::new(sdk_config),
            ecr: aws_sdk_ecr::Client::new(sdk_config),
            autoscaling: aws_sdk_autoscaling::Client::new(sdk_config),
            s3: aws_sdk_s3::Client::new(sdk_config),
            dsql: aws_sdk_dsql::Client::new(sdk_config),
            dynamodb: aws_sdk_dynamodb::Client::new(sdk_config),
            elbv2: aws_sdk_elasticloadbalancingv2::Client::new(sdk_config),
            secretsmanager: aws_sdk_secretsmanager::Client::new(sdk_config),
            servicediscovery: aws_sdk_servicediscovery::Client::new(sdk_config),
            ssm: aws_sdk_ssm::Client::new(sdk_config),
            sts: aws_sdk_sts::Client::new(sdk_config),
        }
    }
}

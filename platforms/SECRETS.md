# Secrets on the AWS platforms

The AWS-recommended secret handling for ECS and EKS, as of 4 August 2026, and how
Tokeira's platforms adopt it. This is the delivery policy for **secret values**;
references, schemas, and the engine seam are defined in the
[configuration model](../.kiro/specs/platform-config-dsl/proposals/005-configuration-model.md),
and none of this puts secret material into definitions, rendered documents, or state.

## The default, in one table

| Platform | Secret authority | Workload identity | Preferred delivery |
|---|---|---|---|
| ECS | AWS Secrets Manager | ECS task role | Application runtime retrieval, or an ordered bootstrap container writing to an ephemeral shared volume |
| EKS | AWS Secrets Manager | EKS Pod Identity | AWS Secrets and Configuration Provider (ASCP) with the Secrets Store CSI Driver, mounted as files |
| Non-secret config | SSM Parameter Store | same workload identity | Runtime retrieval, or mounted/configured separately |

Secrets Manager is the canonical store for passwords, API tokens, signing material,
and certificates — it owns rotation, version staging, and cross-account policy.
Parameter Store stays useful for ordinary configuration and simple encrypted
parameters. Environment-variable injection is the **compatibility escape hatch**,
not a default: it carries process-environment exposure and rotation only reaches a
replacement workload.

## The rules

1. Secrets Manager is the canonical secret store.
2. One IAM role per deployable workload — not per cluster or application family.
3. Grants name explicit secret ARNs; no broad wildcards. Add `kms:Decrypt` only
   when a customer-managed key's policy requires it.
4. Files or runtime APIs by default; environment variables only as the explicit
   escape hatch.
5. Secret values never appear in task definitions, manifests, Helm values, IaC
   state, plans, or Git. Definitions carry references and policy.
6. Rotation includes the consumer's behaviour — reload, reconnect, or rollout —
   not merely a new version in Secrets Manager.
7. Private tasks use interface VPC endpoints (Secrets Manager, KMS where needed,
   SSM if used, ECR API/DKR, CloudWatch Logs) with endpoint policies scoped to the
   accounts and secret prefixes in play.
8. Audit secret reads with CloudTrail; alert on unexpected principals, regions, or
   access patterns.

## ECS

**Two roles, deliberately distinct.** The **task role** is what application code
uses; the **execution role** is what ECS itself uses for image pulls, logs, and
task-definition secret injection. They are never shared, and each workload gets
its own task role scoped to its own secret ARNs
(`secretsmanager:GetSecretValue` + `DescribeSecret`).

**Best: application runtime retrieval.** The application fetches through the SDK
under its task role. That gives it what static injection cannot: caching with
jittered refresh (never a fetch per request), `AWSCURRENT`/`AWSPREVIOUS` handling,
retry behaviour during rotation, and reconnecting pools without replacing the
task.

**For file-only applications: the ordered bootstrap container.** A non-essential
bootstrap container reads Secrets Manager, writes files with restrictive
permissions to a task-scoped ephemeral volume, and exits; the application starts
on `dependsOn: SUCCESS` and reads the files read-only. The bootstrap runs
non-root with a read-only root filesystem and never logs values. One caveat to
hold onto: **every container in a task shares the task role**, so the bootstrap
is delivery, not a security boundary — when that distinction matters, prefer
application-side retrieval.

**Simple fallback: `containerDefinitions[].secrets`.** Native `valueFrom`
injection keeps the value out of the task definition and can select a JSON
property, but the value becomes an environment variable (visible to processes,
crash dumps, shell tooling, observability agents), and rotation reaches nothing
until a replacement task starts. Use it only where the application speaks only
env vars, rotation is infrequent and automated
(`Secrets Manager rotation → EventBridge → ecs:UpdateService(forceNewDeployment)`),
the credential tolerates old and new values overlapping the deployment window,
and the threat model accepts env exposure. Prefer alternating-user database
rotation so existing connections survive the window.

## EKS

**Best: Pod Identity plus ASCP/CSI file mounts.** Secrets Manager owns the
secret; a ServiceAccount identifies the workload; an EKS Pod Identity
association binds it to an IAM role; ASCP retrieves the secret; the Secrets
Store CSI Driver mounts it as files in the pod. The value never becomes a
Kubernetes API object.

**Never sync into Kubernetes `Secret` objects** unless a controller specifically
requires the Secret API. Direct CSI mounting keeps the value out of RBAC's
reach, out of admission tooling and backups, and bounds the blast radius to the
pod mount. Base64 in a committed `Secret` manifest is encoding, not protection —
never that.

**Environment variables stay second choice** — a mounted file avoids process
exposure, replaces atomically, and gives the application a reload path; an
env-consumed Secret changes only on container restart. Reserve `secretKeyRef`
env wiring for software that cannot read files.

**Rotation is a consumer contract.** The CSI driver can refresh mounted content,
but a refreshed file does not renew a long-lived database connection: the
application watches and reloads, reopens connections, takes a reload signal — or
a controller hashes secret metadata into the pod template and rolls the
Deployment. Never assume the refresh alone did the job.

**Pod Identity over IRSA** for new deployments; IRSA remains where a platform
already standardises on it, manifests must run on non-EKS clusters, an add-on
lacks Pod Identity support, or the trust architecture depends on OIDC federation.

**Fargate caveat.** ASCP/CSI mounting works on EC2-backed nodes, not Fargate.
There, use application-side retrieval under the pod's identity (least
surprising), or an init/bootstrap mechanism, accepting its refresh limits.

Native Kubernetes Secrets on 1.28+ get default envelope encryption of API data —
good defense in depth, but any principal with RBAC read on a Secret still gets
plaintext. Encryption at rest does not substitute for external secret ownership,
narrow RBAC, and workload identity.

## What a definition declares

References and policy, never values. The target vocabulary shape:

```yaml
secrets:
  - name: database
    provider: aws-secrets-manager
    secretRef: prod/payments/database
    delivery: file            # the portable default
    mountPath: /run/secrets/database
    fields:
      username: username
      password: password
    rotation:
      consumerStrategy: reload
```

The platform renders everything below it — task-role or Pod Identity policy, the
bootstrap volume or `SecretProviderClass` and CSI mount, endpoint policies, and
the redeployment hook where the escape hatch demands one. `delivery: file` is the
portable default (ASCP/CSI on EKS; bootstrap container or application retrieval
on ECS); `delivery: environment` is an explicit escape hatch that warns that
rotation requires workload replacement.

Values are populated only through controlled channels: an initial secure operator
action, a database-generated credential, a Secrets Manager rotation Lambda, an
external provisioning workflow, or a one-time CI operation with masked input.
CloudFormation/Terraform/Pulumi state, Helm values, GitOps repositories, and
plan output never contain the value.

## Where Tokeira stands today

- `SecretRef` (`env:` / `aws-sm:` / `aws-ssm:`) with the `SecretsProvider` seam
  and the `tokeira-secrets` AWS provider **is** the runtime-retrieval path — and
  the Fargate fallback. `env:` is the escape hatch the policy above describes.
- File delivery arrives with the platform secret vocabulary (the ECS bootstrap
  pattern, the EKS `SecretProviderClass`/CSI rendering); alongside it, `SecretRef`
  grows a `file:` form so a schema field can name a mounted secret.
- The ECS **config-document** channel (the rendered `tokeirad.toml` travelling as
  a Secrets Manager secret into `TOKEIRA_CONFIG_CONTENT`) is not a secret-value
  channel: rendered documents contain no secret material by construction, so the
  env-variable concerns above do not apply to it. Keep the two jobs apart.

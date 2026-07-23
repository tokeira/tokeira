# Security Policy

## Reporting a Vulnerability

Please do not report suspected vulnerabilities through public GitHub issues.
Use GitHub's private vulnerability reporting instead: on
[github.com/tokeira/tokeira](https://github.com/tokeira/tokeira), open
**Security → Report a vulnerability**. Include reproduction steps and the
deployment configuration involved. Reports are acknowledged and triaged as
quickly as we can manage.

## Supported Versions

Security fixes land on `main` and ship with the next release. Please report
against the most recent release.

## Posture

- **Authentication and authorization live at the compatibility edge** — the
  boundary that admits requests before they reach runtime resources.
- **Private-network defaults.** The ECS platform runs every service in private
  subnets with no public ingress; operator access is via SSM Session Manager
  port forwarding and ECS Exec. See the
  [ECS platform guide](docs/platforms/ecs/README.md).
- **Secret redaction.** Passwords, tokens, credentials, private keys,
  authorization headers, and credential-bearing connection strings are treated
  as sensitive by default and redacted before logs or configuration snapshots
  expose them.
- **Unsafe Rust is denied workspace-wide** (`unsafe_code = deny`), with four
  audited carve-outs, each carrying a documented `SAFETY` justification.
- **Dependency policy is enforced by `cargo-deny`** (licenses, bans, sources)
  on every merge, with a recurring advisories audit.

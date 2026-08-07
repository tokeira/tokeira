# Observability content — description, as files

The Grafana dashboards, alert rules, and backend config templates a Compose
deployment carries. Copied byte-for-byte from where they live today; only the
location is the statement.

TODAY: this content sits inside `crates/tokeira-compose` — the *provider*
crate — as `templates/`, `dashboards/`, and `alerts/`, `include_str!`-embedded
into `observability/mod.rs`. That placement was a mistake, and it costs:

- **The generic Docker provider ships product content.** `tokeira-compose` is
  supposed to be Docker capability (containers, networks, inspect, logs);
  instead it carries ten Tokeira-opinionated dashboards and a Mimir tuning
  file. Every binary linking the provider embeds ~93 KB of Grafana JSON,
  whether or not its platform has an observability stack.
- **Editing a dashboard is a provider release.** A panel tweak recompiles the
  engine's provider crate and arrives only with a new provisioner binary —
  then surfaces as config drift against every existing deployment, attributed
  to nothing an operator can see or diff.
- **The content is invisible as description.** A deployment's recorded
  definition should carry what the deployment IS. Compiled-in content cannot
  appear in a revision folder, cannot be diffed between revisions, and cannot
  be inspected without reading provider source.

HERE: the content lives beside `definition.tkd` because it is part of the same
description. The definition's `ObservabilityConfiguration` kind names the
parameters (`scrape_port`, `retention_hours`, `mimir_http_port`, …); the
files here are the bodies those parameters render into (askama-style
`{{ placeholder }}` substitution, unchanged). Mechanically they are
**desired-source companions** — the exact seam `tokeirad.toml` already uses:

- recorded with the definition at create, resolved against the definition's
  own directory (`definition_dir`), so a retained revision folder carries
  *its* dashboards and a baseline realization digests the retained bytes,
  never the live ones;
- digested into the content identity that already fences every consumer
  (`TOKEIRA_CONFIG_DIGEST` on mimir/loki/grafana/alloy), so a dashboard edit
  is a visible, plannable change to *this deployment*, exactly like a server
  config edit — not a side effect of a binary upgrade.

KEEP: the rendering machinery — parameter substitution, digesting, the
config-files resource fencing its consumers — is real work and stays with
the provider/engine. Only the CONTENT moves. The provider offers "render
these files with these parameters"; *which* files, it no longer knows.

Inventory (verbatim copies):

- `templates/` — mimir.yaml, loki.yaml, alloy.alloy, grafana-datasources.yaml,
  grafana-dashboards.yaml (the dashboard-provider config)
- `dashboards/` — ten Tokeira dashboards (edge, broker, storage/projection,
  DSQL, OCC, placement, autoscaler, projection workers, infrastructure, logs)
- `alerts/observability-alerts.yaml` — the Mimir rule file

# Repo Migration to the `tokeira` GitHub Org — Delivery Readiness

> Sibling of [`delivery.md`](./delivery.md). **Scaffold — owner input required.**
> Kiro does not have the migration details; this page captures the structure so the owner can fill it.

## Goal

Move the tokeira repositories under the `tokeira` GitHub organisation (from their current home) so the
public release lives in the canonical org.

## Open items (owner to confirm/fill)

<!-- OWNER: fill each item. -->

- [ ] Inventory of repos to migrate (engine, odori, sdk-core fork, deploy repos, tokeira.io, …) and
      their target names under the `tokeira` org.
- [ ] Org setup: teams, CODEOWNERS, branch protection, required checks, secrets.
- [ ] Migration mechanics: transfer vs. fresh push; preserving history, issues, PRs, tags, stars.
- [ ] Redirects / references: update remotes, CI, badge URLs, doc cross-links, `Cargo.toml`
      repository fields, container registry namespaces.
- [ ] Pinned external references (e.g. the `../temporal` v1.31.0 fork remote, vendored proto sources).
- [ ] Cutover plan + rollback, and the announcement.

## Status

Not started.

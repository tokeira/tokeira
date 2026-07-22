# Local Platform

The local platform runs `tokeirad` as a bare child process on the host. No
containers, no Docker, no observability stack. This is the fastest path from
zero to a running server for development and testing.

## Lifecycle

```bash
# Create
tkr deployment create --name dev --platform local --storage in-memory

# Start tokeirad (blocks, inherits stdio, forwards SIGINT)
tkr deploy apply --yes

# In another terminal
tkr config show
tkr version
```

No `infra` step is needed — the local platform has no infrastructure to
provision. `deploy apply` spawns `tokeirad` directly.

## Storage

For DSQL persistence, replace `in-memory` with `dsql` and configure the DSQL
endpoint in `tokeirad.toml` before starting.

## See also

- [Platform support matrix](../README.md)
- [Deployment model and the `tkr` command surface](../iac/configuration.md)

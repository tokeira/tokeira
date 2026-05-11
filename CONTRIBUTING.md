# Contributing

Use `tkr workstation` when local Rust builds are the bottleneck:

```bash
tkr workstation up
tkr workstation remote-exec cargo build --workspace
tkr workstation stop
```

The workstation is intended for compute-heavy builds and tests. Stop it when
finished; persistent cache and repository volumes survive `stop`, but
instance-store build output does not.

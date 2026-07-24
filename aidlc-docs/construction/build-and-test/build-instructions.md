# Build Instructions

## Verified Command

```bash
CARGO_TARGET_DIR=/tmp/chronos-target.uiqK2v cargo build --release --locked --bins
```

The command completed successfully and produced the Chronos release binaries in the isolated target directory. The isolated target avoids relying on stale workspace build state.

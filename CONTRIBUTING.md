# Contributing

Changes should be submitted through pull requests. Keep commits focused and include regression tests
for behavior changes.

Before requesting review, run:

```bash
make release-check-core
```

Changes to `tso.proto` must remain backward compatible and regenerate checked-in Go sources. Changes
to failover, leases, timestamp encoding, metadata formats, or ownership plans must include an etcd-
backed validation scenario or explain why an existing scenario covers the risk.

Do not merge when required CI checks are failing.

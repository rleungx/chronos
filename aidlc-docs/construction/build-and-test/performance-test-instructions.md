# Performance Test Instructions

## Intended Commands

```bash
make test-soak-quick
make test-chaos-quick
make test-scale-bench
make test-rebalance-bench
```

These gates require the repository's Docker-backed etcd environment. They were not executed locally because `docker info` reports that the Docker daemon is unavailable and neither `etcd` nor `etcdctl` is installed. CI or a host with Docker must run these gates before release promotion.

Static validation of all modified harnesses passed, and the release evidence verifier now enforces the new soak durations and chaos recovery-probe artifact.

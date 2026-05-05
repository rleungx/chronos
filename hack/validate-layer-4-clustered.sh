#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

export CHRONOS_TEST_ETCD_ENDPOINTS="${CHRONOS_TEST_ETCD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
ETCDCTL_ENDPOINTS="http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379"

docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${ETCDCTL_ENDPOINTS}" endpoint health
docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${ETCDCTL_ENDPOINTS}" endpoint status -w table
docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${ETCDCTL_ENDPOINTS}" member list -w table
docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${ETCDCTL_ENDPOINTS}" endpoint hashkv -w table

bash hack/validate-layer-4.sh

docker stop chronos-etcd-2 >/dev/null
docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="http://chronos-etcd-1:2379,http://chronos-etcd-3:2379" endpoint health
docker start chronos-etcd-2 >/dev/null
wait_for_etcd_cluster 60 1 "${ETCDCTL_ENDPOINTS}"

docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${ETCDCTL_ENDPOINTS}" endpoint hashkv -w table

echo "[layer4-clustered] success"

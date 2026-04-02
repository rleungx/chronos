ETCD_COMPOSE_FILE := hack/etcd/docker-compose.yml
ETCD_ENDPOINTS ?= 127.0.0.1:2379

.PHONY: \
	etcd-up \
	etcd-down \
	etcd-reset \
	etcd-health \
	test-layer-0 \
	test-layer-1 \
	test-layer-2 \
	test-layer-3 \
	test-layer-4 \
	test-etcd

etcd-up:
	docker compose -f $(ETCD_COMPOSE_FILE) up -d

etcd-down:
	docker compose -f $(ETCD_COMPOSE_FILE) down

etcd-reset:
	docker compose -f $(ETCD_COMPOSE_FILE) down -v

etcd-health:
	@if [ "$(ETCD_ENDPOINTS)" = "127.0.0.1:2379" ] || [ "$(ETCD_ENDPOINTS)" = "localhost:2379" ]; then \
		docker ps --format '{{.Names}}' | grep -qx chronos-etcd || { echo "chronos-etcd is not running; run 'make etcd-up' first"; exit 1; }; \
		docker exec chronos-etcd /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health; \
	else \
		docker run --rm quay.io/coreos/etcd:v3.5.15 /usr/local/bin/etcdctl --endpoints=http://$(ETCD_ENDPOINTS) endpoint health; \
	fi

test-layer-0:
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings

test-layer-1:
	cargo test --lib
	cargo test --test crate_root_api_smoke
	cargo test --test tso_planes_public_api
	cargo test --test lifecycle_semantics

test-layer-2:
	cargo test --bin chronos

test-layer-3:
	cargo test --test metadata_etcd_compat
	cargo test --test rpc_semantics
	cargo test --test timeline_proxy_semantics
	cargo test --test timeline_rebalance_and_scaling

test-layer-4:
	CHRONOS_TEST_ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) bash hack/validate-layer-4.sh

test-etcd: test-layer-4

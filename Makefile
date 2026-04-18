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
	test-etcd \
	test-failover-bench \
	test-rebalance-bench \
	test-soak \
	test-chaos \
	promtool-check \
	observability-check \
	dependency-check \
	release-check \
	observability-up \
	observability-down

PROMTOOL_IMAGE ?= prom/prometheus:v2.54.1

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
	cargo clippy --locked --all-targets -- -D warnings

test-layer-1:
	cargo test --locked --lib --test crate_root_api_smoke --test tso_planes_public_api --test lifecycle_semantics

test-layer-2:
	cargo test --locked --bin chronos

test-layer-3:
	cargo test --locked --test metadata_etcd_compat --test rpc_semantics --test timeline_proxy_semantics --test timeline_rebalance_and_scaling

test-layer-4:
	CHRONOS_TEST_ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) bash hack/validate-layer-4.sh

test-etcd: test-layer-4

test-failover-bench:
	bash hack/failover/failover-etcd.sh

test-rebalance-bench:
	bash hack/rebalance/rebalance-etcd.sh

test-soak:
	bash hack/soak/soak-etcd.sh

test-chaos:
	bash hack/chaos/lease-loss-shutdown.sh

promtool-check:
	docker run --rm \
		-v $(CURDIR)/observability/prometheus:/etc/prometheus:ro \
		$(PROMTOOL_IMAGE) \
		promtool check rules /etc/prometheus/alerts.yml
	docker run --rm \
		-v $(CURDIR)/observability/prometheus:/etc/prometheus:ro \
		$(PROMTOOL_IMAGE) \
		promtool test rules /etc/prometheus/alerts.test.yml

observability-check: promtool-check

dependency-check:
	cargo install --locked cargo-deny --version 0.19.4 >/dev/null 2>&1 || true
	cargo deny --all-features check advisories bans licenses sources

release-check:
	CHRONOS_PROFILE=production \
	CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) \
	CHRONOS_SECURITY_MODE=dev-insecure \
	CHRONOS_BIND_ADDR=127.0.0.1:50051 \
	CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051 \
	CHRONOS_METRICS_BIND_ADDR=127.0.0.1:9898 \
	cargo run --bin chronos -- --check-config
	CHRONOS_PROFILE=production \
	CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) \
	CHRONOS_SECURITY_MODE=dev-insecure \
	CHRONOS_BIND_ADDR=127.0.0.1:50051 \
	CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051 \
	CHRONOS_METRICS_BIND_ADDR=127.0.0.1:9898 \
	cargo run --bin chronos -- --print-effective-config
	$(MAKE) dependency-check
	cargo build --locked --release --bin chronos --bin chronos-bench --bin chronos-control-bench --bin chronos-failover-bench

observability-up:
	docker compose -f observability/docker-compose.yml up -d

observability-down:
	docker compose -f observability/docker-compose.yml down

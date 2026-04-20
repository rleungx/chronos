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
	container-image-check \
	container-startup-smoke \
	container-etcd-startup-smoke \
	container-check \
	release-shape-check \
	release-check \
	observability-up \
	observability-down

PROMTOOL_IMAGE ?= prom/prometheus:v2.54.1
CONTAINER_CHECK_IMAGE ?= chronos:container-check
CONTAINER_CHECK_FORCE_BUILD ?= 1
PRODUCTION_SHAPE_ETCD_ENDPOINTS := 127.0.0.1:2379
PRODUCTION_SHAPE_BIND_ADDR := 127.0.0.1:50051
PRODUCTION_SHAPE_ADVERTISE_ENDPOINT := 10.0.0.10:50051
PRODUCTION_SHAPE_METRICS_BIND_ADDR := 127.0.0.1:9898
PRODUCTION_SHAPE_SAFETY_GAP_MS := 1
PRODUCTION_SHAPE_GRPC_REQUEST_TIMEOUT_MS := 100
PRODUCTION_SHAPE_GRPC_MAX_REQUEST_BYTES := 1024
PRODUCTION_SHAPE_GRPC_MAX_CONCURRENT_REQUESTS := 16

define HOST_PRODUCTION_SHAPE_ENV
CHRONOS_PROFILE=production \
CHRONOS_METADATA=etcd \
CHRONOS_ETCD_ENDPOINTS=$(PRODUCTION_SHAPE_ETCD_ENDPOINTS) \
CHRONOS_ETCD_PREFIX=$(1) \
CHRONOS_WORKER_ID=$(2) \
CHRONOS_BIND_ADDR=$(PRODUCTION_SHAPE_BIND_ADDR) \
CHRONOS_ADVERTISE_ENDPOINT=$(PRODUCTION_SHAPE_ADVERTISE_ENDPOINT) \
CHRONOS_METRICS_BIND_ADDR=$(PRODUCTION_SHAPE_METRICS_BIND_ADDR) \
CHRONOS_SAFETY_GAP_MS=$(PRODUCTION_SHAPE_SAFETY_GAP_MS) \
CHRONOS_GRPC_TLS_CERT_FILE=$(3) \
CHRONOS_GRPC_TLS_KEY_FILE=$(4) \
CHRONOS_GRPC_CLIENT_CA_FILE=$(5) \
CHRONOS_GRPC_REQUEST_TIMEOUT_MS=$(PRODUCTION_SHAPE_GRPC_REQUEST_TIMEOUT_MS) \
CHRONOS_GRPC_MAX_REQUEST_BYTES=$(PRODUCTION_SHAPE_GRPC_MAX_REQUEST_BYTES) \
CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS=$(PRODUCTION_SHAPE_GRPC_MAX_CONCURRENT_REQUESTS)
endef

define DOCKER_PRODUCTION_SHAPE_ENV
-e CHRONOS_PROFILE=production \
		-e CHRONOS_METADATA=etcd \
		-e CHRONOS_ETCD_ENDPOINTS=$(PRODUCTION_SHAPE_ETCD_ENDPOINTS) \
		-e CHRONOS_ETCD_PREFIX=$(1) \
		-e CHRONOS_WORKER_ID=$(2) \
		-e CHRONOS_BIND_ADDR=$(PRODUCTION_SHAPE_BIND_ADDR) \
		-e CHRONOS_ADVERTISE_ENDPOINT=$(PRODUCTION_SHAPE_ADVERTISE_ENDPOINT) \
		-e CHRONOS_METRICS_BIND_ADDR=$(PRODUCTION_SHAPE_METRICS_BIND_ADDR) \
		-e CHRONOS_SAFETY_GAP_MS=$(PRODUCTION_SHAPE_SAFETY_GAP_MS) \
		-e CHRONOS_GRPC_TLS_CERT_FILE=$(3) \
		-e CHRONOS_GRPC_TLS_KEY_FILE=$(4) \
		-e CHRONOS_GRPC_CLIENT_CA_FILE=$(5) \
		-e CHRONOS_GRPC_REQUEST_TIMEOUT_MS=$(PRODUCTION_SHAPE_GRPC_REQUEST_TIMEOUT_MS) \
		-e CHRONOS_GRPC_MAX_REQUEST_BYTES=$(PRODUCTION_SHAPE_GRPC_MAX_REQUEST_BYTES) \
		-e CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS=$(PRODUCTION_SHAPE_GRPC_MAX_CONCURRENT_REQUESTS)
endef

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

container-startup-smoke:
	$(MAKE) CONTAINER_CHECK_IMAGE=$(CONTAINER_CHECK_IMAGE) CONTAINER_CHECK_FORCE_BUILD=$(CONTAINER_CHECK_FORCE_BUILD) container-image-check
	CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) bash hack/container-startup-smoke.sh

container-etcd-startup-smoke:
	$(MAKE) CONTAINER_CHECK_IMAGE=$(CONTAINER_CHECK_IMAGE) CONTAINER_CHECK_FORCE_BUILD=$(CONTAINER_CHECK_FORCE_BUILD) container-image-check
	CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) CHRONOS_CONTAINER_SMOKE_MODE=etcd bash hack/container-startup-smoke.sh

container-image-check:
	@if [ "$(CONTAINER_CHECK_FORCE_BUILD)" = "1" ] || ! docker image inspect $(CONTAINER_CHECK_IMAGE) >/dev/null 2>&1; then \
		docker build --build-arg CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) -t $(CONTAINER_CHECK_IMAGE) .; \
	else \
		echo "reusing existing image $(CONTAINER_CHECK_IMAGE)"; \
	fi

container-check:
	@tmpdir=$$(mktemp -d); \
	trap 'rm -rf "$$tmpdir"' EXIT; \
	touch "$$tmpdir/server.crt" "$$tmpdir/server.key" "$$tmpdir/ca.pem"; \
	chmod 600 "$$tmpdir/server.key"; \
	$(MAKE) CONTAINER_CHECK_IMAGE=$(CONTAINER_CHECK_IMAGE) CONTAINER_CHECK_FORCE_BUILD=$(CONTAINER_CHECK_FORCE_BUILD) container-image-check && \
	docker run --rm \
		-v "$$tmpdir:/tls:ro" \
		$(call DOCKER_PRODUCTION_SHAPE_ENV,/chronos-container-check,container-check-worker,/tls/server.crt,/tls/server.key,/tls/ca.pem) \
		$(CONTAINER_CHECK_IMAGE) --check-config && \
	docker run --rm \
		-v "$$tmpdir:/tls:ro" \
		$(call DOCKER_PRODUCTION_SHAPE_ENV,/chronos-container-check,container-check-worker,/tls/server.crt,/tls/server.key,/tls/ca.pem) \
		$(CONTAINER_CHECK_IMAGE) --print-effective-config && \
		CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) bash hack/container-startup-smoke.sh && \
		CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) CHRONOS_CONTAINER_SMOKE_MODE=etcd bash hack/container-startup-smoke.sh

release-shape-check:
	@tmpdir=$$(mktemp -d); \
	trap 'rm -rf "$$tmpdir"' EXIT; \
	touch "$$tmpdir/server.crt" "$$tmpdir/server.key" "$$tmpdir/ca.pem"; \
	chmod 600 "$$tmpdir/server.key"; \
	CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) \
	$(call HOST_PRODUCTION_SHAPE_ENV,/chronos-release-check,release-check-worker,"$$tmpdir/server.crt","$$tmpdir/server.key","$$tmpdir/ca.pem") \
	cargo run --bin chronos -- --check-config && \
	CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) \
	$(call HOST_PRODUCTION_SHAPE_ENV,/chronos-release-check,release-check-worker,"$$tmpdir/server.crt","$$tmpdir/server.key","$$tmpdir/ca.pem") \
	cargo run --bin chronos -- --print-effective-config

release-check:
	$(MAKE) release-shape-check
	$(MAKE) dependency-check
	$(MAKE) container-check
	cargo build --locked --release --bin chronos --bin chronos-bench --bin chronos-control-bench --bin chronos-failover-bench

observability-up:
	docker compose -f observability/docker-compose.yml up -d

observability-down:
	docker compose -f observability/docker-compose.yml down

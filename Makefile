ETCD_COMPOSE_FILE := hack/etcd/docker-compose.yml
ETCD_CLUSTER_COMPOSE_FILE := hack/etcd/docker-compose.cluster.yml
ETCD_ENDPOINTS ?= 127.0.0.1:2379

.PHONY: \
	etcd-up \
	etcd-down \
	etcd-reset \
	etcd-health \
	etcd-cluster-up \
	etcd-cluster-down \
	etcd-cluster-reset \
	etcd-cluster-health \
	test-layer-0 \
	test-layer-1 \
	test-layer-2 \
	test-layer-3 \
	test-release-core \
	test-layer-4 \
	test-layer-4-clustered \
	test-etcd \
	test-failover-bench \
	test-auto-failover-bench \
	test-scale-bench \
	test-scale-matrix \
	test-scale-matrix-production \
	scale-ownership-plan \
	test-rebalance-bench \
	test-soak \
	test-chaos \
	test-restore-dr \
	promtool-check \
	observability-check \
	kubernetes-manifest-check \
	release-evidence-check \
	dependency-check \
	container-vulnerability-scan \
	container-image-check \
	container-startup-smoke \
	container-etcd-startup-smoke \
	container-check \
	release-source-check \
	release-package \
	release-shape-check \
	release-check \
	release-gate-layer-4 \
	release-gate-layer-4-clustered \
	release-gate \
	client-check-go \
	client-check-java \
	client-check-cpp \
	client-check \
	observability-up \
	observability-down

PROMTOOL_IMAGE ?= prom/prometheus:v2.54.1
SYFT_IMAGE ?= anchore/syft:v1.20.0
TRIVY_IMAGE ?= aquasec/trivy:0.57.1
TRIVY_CACHE_DIR ?= $(CURDIR)/.cache/trivy
CONTAINER_CHECK_IMAGE ?= chronos:container-check
CONTAINER_CHECK_FORCE_BUILD ?= 1
CHRONOS_ALLOW_DIRTY_RELEASE ?= 0
CARGO_DENY_VERSION ?= 0.19.4
CHRONOS_SKIP_RELEASE_BUILD ?= 0
CHRONOS_RELEASE_BIN_DIR ?= $(CURDIR)/target/release
PRODUCTION_SHAPE_ETCD_ENDPOINTS := 127.0.0.1:2379
PRODUCTION_SHAPE_BIND_ADDR := 127.0.0.1:50051
PRODUCTION_SHAPE_ADVERTISE_ENDPOINT := 10.0.0.10:50051
PRODUCTION_SHAPE_METRICS_BIND_ADDR := 127.0.0.1:9898
PRODUCTION_SHAPE_SAFETY_GAP_MS := 1
PRODUCTION_SHAPE_GRPC_REQUEST_TIMEOUT_MS := 100
PRODUCTION_SHAPE_GRPC_MAX_REQUEST_BYTES := 1024
PRODUCTION_SHAPE_GRPC_MAX_CONCURRENT_REQUESTS := 16
PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT := 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

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
CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
CHRONOS_GRPC_STATUS_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
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
			-e CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
			-e CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
			-e CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
			-e CHRONOS_GRPC_STATUS_CERT_ALLOWLIST=$(PRODUCTION_SHAPE_CLIENT_CERT_FINGERPRINT) \
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

etcd-cluster-up:
	docker compose -f $(ETCD_CLUSTER_COMPOSE_FILE) up -d

etcd-cluster-down:
	docker compose -f $(ETCD_CLUSTER_COMPOSE_FILE) down

etcd-cluster-reset:
	docker compose -f $(ETCD_CLUSTER_COMPOSE_FILE) down -v

etcd-cluster-health:
	@docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints=http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379 endpoint health

test-layer-0:
	cargo fmt --all -- --check
	cargo clippy --locked --all-targets -- -D warnings

test-layer-1:
	cargo test --locked --lib --test crate_root_api_smoke --test tso_planes_public_api --test lifecycle_semantics

test-layer-2:
	cargo test --locked --bin chronos -- --test-threads=1

test-layer-3:
	cargo test --locked --test metadata_etcd_compat --test rpc_semantics --test timeline_proxy_semantics --test timeline_rebalance_and_scaling

test-release-core:
	cargo test --locked --lib --bin chronos --test crate_root_api_smoke --test tso_planes_public_api --test lifecycle_semantics --test metadata_etcd_compat --test rpc_semantics --test timeline_proxy_semantics --test timeline_rebalance_and_scaling -- --test-threads=1

test-layer-4:
	CHRONOS_TEST_ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) bash hack/validate-layer-4.sh

test-layer-4-clustered:
	CHRONOS_TEST_ETCD_ENDPOINTS=127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379 bash hack/validate-layer-4-clustered.sh

test-etcd: test-layer-4

test-failover-bench:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/failover/failover-etcd.sh

test-auto-failover-bench:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	CHRONOS_FAILOVER_ARTIFACT_NAME=auto-failover \
	CHRONOS_FAILOVER_BENCH_AUTO_FAILOVER_ENABLED=true \
	CHRONOS_FAILOVER_BENCH_AUTO_FAILOVER_INTERVAL_MS=100 \
	bash hack/failover/failover-etcd.sh

test-scale-bench:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/scale/scale-etcd.sh

test-scale-matrix:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/scale/scale-matrix.sh

test-scale-matrix-production:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	CHRONOS_SCALE_MATRIX_WORKERS=$${CHRONOS_SCALE_MATRIX_WORKERS:-2,3,5,8} \
	CHRONOS_SCALE_MATRIX_LINEAR_EFFICIENCY_MIN=$${CHRONOS_SCALE_MATRIX_LINEAR_EFFICIENCY_MIN:-0.80} \
	bash hack/scale/scale-matrix.sh

scale-ownership-plan:
	bash hack/scale/ownership-plan.sh

test-rebalance-bench:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/rebalance/rebalance-etcd.sh

test-soak:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/soak/soak-etcd.sh

test-chaos:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/chaos/lease-loss-shutdown.sh

test-restore-dr:
	CHRONOS_SKIP_RELEASE_BUILD=$(CHRONOS_SKIP_RELEASE_BUILD) \
	CHRONOS_RELEASE_BIN_DIR=$(CHRONOS_RELEASE_BIN_DIR) \
	bash hack/dr/restore-etcd.sh

promtool-check:
	docker run --rm \
		-v $(CURDIR)/observability/prometheus:/etc/prometheus:ro \
		--entrypoint sh \
		$(PROMTOOL_IMAGE) \
		-ec 'promtool check rules /etc/prometheus/alerts.yml && promtool test rules /etc/prometheus/alerts.test.yml'

observability-check: promtool-check

kubernetes-manifest-check:
	bash hack/validate-kubernetes-manifests.sh deploy/kubernetes/chronos.yaml

dependency-check:
	@if ! cargo deny --version 2>/dev/null | grep -q '$(CARGO_DENY_VERSION)'; then \
		cargo install --locked cargo-deny --version $(CARGO_DENY_VERSION) >/dev/null 2>&1; \
	fi
	cargo deny --all-features check advisories bans licenses sources

release-evidence-check:
	bash hack/verify-evidence.sh

client-check-go:
	cd clients/go && go mod verify && go test ./...

client-check-java:
	cd clients/java && gradle --no-daemon test

client-check-cpp:
	cmake -S clients/cpp -B clients/cpp/build && cmake --build clients/cpp/build && ctest --test-dir clients/cpp/build --output-on-failure

client-check:
	$(MAKE) client-check-go
	$(MAKE) client-check-java
	$(MAKE) client-check-cpp

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

container-vulnerability-scan:
	$(MAKE) CONTAINER_CHECK_IMAGE=$(CONTAINER_CHECK_IMAGE) CONTAINER_CHECK_FORCE_BUILD=$(CONTAINER_CHECK_FORCE_BUILD) container-image-check
	mkdir -p $(TRIVY_CACHE_DIR)
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$(TRIVY_CACHE_DIR):/root/.cache" \
		$(TRIVY_IMAGE) image \
		--severity HIGH,CRITICAL \
		--ignore-unfixed \
		--exit-code 1 \
		$(CONTAINER_CHECK_IMAGE)

container-check:
	@tmpdir=$$(mktemp -d); \
	tls_volume=chronos-container-check-tls-$$$$; \
	cleanup() { docker volume rm -f "$$tls_volume" >/dev/null 2>&1 || true; rm -rf "$$tmpdir"; }; \
	trap cleanup EXIT; \
	touch "$$tmpdir/server.crt" "$$tmpdir/server.key" "$$tmpdir/ca.pem"; \
	chmod 755 "$$tmpdir"; \
	chmod 600 "$$tmpdir/server.key"; \
	$(MAKE) CONTAINER_CHECK_IMAGE=$(CONTAINER_CHECK_IMAGE) CONTAINER_CHECK_FORCE_BUILD=$(CONTAINER_CHECK_FORCE_BUILD) container-image-check && \
	docker volume create "$$tls_volume" >/dev/null && \
	docker run --rm \
		--user 0 \
		--entrypoint /bin/sh \
		-v "$$tmpdir:/input:ro" \
		-v "$$tls_volume:/tls" \
		$(CONTAINER_CHECK_IMAGE) \
		-ec 'cp -R /input/. /tls/ && chown -R 10001:10001 /tls && chmod 0644 /tls/server.crt /tls/ca.pem && chmod 0600 /tls/server.key' && \
	docker run --rm \
		-v "$$tls_volume:/tls:ro" \
		$(call DOCKER_PRODUCTION_SHAPE_ENV,/chronos-container-check,container-check-worker,/tls/server.crt,/tls/server.key,/tls/ca.pem) \
		$(CONTAINER_CHECK_IMAGE) --check-config && \
	docker run --rm \
		-v "$$tls_volume:/tls:ro" \
		$(call DOCKER_PRODUCTION_SHAPE_ENV,/chronos-container-check,container-check-worker,/tls/server.crt,/tls/server.key,/tls/ca.pem) \
		$(CONTAINER_CHECK_IMAGE) --print-effective-config && \
		CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) bash hack/container-startup-smoke.sh && \
		CHRONOS_CONTAINER_SMOKE_IMAGE=$(CONTAINER_CHECK_IMAGE) CHRONOS_CONTAINER_SMOKE_MODE=etcd bash hack/container-startup-smoke.sh

release-shape-check:
	@tmpdir=$$(mktemp -d); \
	build_commit=$$(git rev-parse HEAD); \
	trap 'rm -rf "$$tmpdir"' EXIT; \
	touch "$$tmpdir/server.crt" "$$tmpdir/server.key" "$$tmpdir/ca.pem"; \
	chmod 600 "$$tmpdir/server.key"; \
	CHRONOS_BUILD_COMMIT="$$build_commit" cargo build --locked --bin chronos >/dev/null; \
	CHRONOS_BUILD_COMMIT="$$build_commit" \
	$(call HOST_PRODUCTION_SHAPE_ENV,/chronos-release-check,release-check-worker,"$$tmpdir/server.crt","$$tmpdir/server.key","$$tmpdir/ca.pem") \
	target/debug/chronos --check-config && \
	CHRONOS_BUILD_COMMIT="$$build_commit" \
	$(call HOST_PRODUCTION_SHAPE_ENV,/chronos-release-check,release-check-worker,"$$tmpdir/server.crt","$$tmpdir/server.key","$$tmpdir/ca.pem") \
	target/debug/chronos --print-effective-config

release-source-check:
	@if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
		echo "release packaging requires a git worktree" >&2; \
		exit 1; \
	fi
	@if [ "$(CHRONOS_ALLOW_DIRTY_RELEASE)" != "1" ] && [ -n "$$(git status --porcelain --untracked-files=normal)" ]; then \
		echo "release source tree is dirty; commit/stash changes or set CHRONOS_ALLOW_DIRTY_RELEASE=1 for a local non-production check" >&2; \
		git status --short; \
		exit 1; \
	fi

release-package:
	$(MAKE) release-source-check
	rm -rf artifacts/release
	mkdir -p artifacts/release
	CHRONOS_BUILD_COMMIT=$$(git rev-parse HEAD) cargo build --locked --release --bin chronos --bin chronos-bench --bin chronos-control-bench --bin chronos-failover-bench
	cp target/release/chronos artifacts/release/
	cp target/release/chronos-bench artifacts/release/
	cp target/release/chronos-control-bench artifacts/release/
	cp target/release/chronos-failover-bench artifacts/release/
	(cd artifacts/release && if command -v shasum >/dev/null 2>&1; then shasum -a 256 chronos chronos-bench chronos-control-bench chronos-failover-bench; else sha256sum chronos chronos-bench chronos-control-bench chronos-failover-bench; fi > SHA256SUMS)
	docker run --rm -v "$(CURDIR)/artifacts/release:/artifacts" $(SYFT_IMAGE) dir:/artifacts -o spdx-json > artifacts/release/chronos-release.spdx.json

release-check:
	$(MAKE) test-layer-0
	$(MAKE) test-release-core
	$(MAKE) observability-check
	$(MAKE) kubernetes-manifest-check
	$(MAKE) release-shape-check
	$(MAKE) dependency-check
	$(MAKE) client-check
	$(MAKE) container-check

release-gate-layer-4:
	@set -e; \
	cleanup() { $(MAKE) etcd-reset >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT; \
	$(MAKE) etcd-reset >/dev/null; \
	$(MAKE) etcd-up >/dev/null; \
	. hack/lib/common.sh; \
	wait_for_etcd 60 1; \
	$(MAKE) test-layer-4

release-gate-layer-4-clustered:
	@set -e; \
	cleanup() { $(MAKE) etcd-cluster-reset >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT; \
	$(MAKE) etcd-cluster-reset >/dev/null; \
	$(MAKE) etcd-cluster-up >/dev/null; \
	. hack/lib/common.sh; \
	wait_for_etcd_cluster 60 1 "http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379"; \
	$(MAKE) test-layer-4-clustered

release-gate:
	$(MAKE) release-check
	$(MAKE) release-package
	$(MAKE) release-gate-layer-4-clustered
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-soak
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-chaos
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-failover-bench
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-auto-failover-bench
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-scale-matrix-production
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-rebalance-bench
	$(MAKE) CHRONOS_SKIP_RELEASE_BUILD=1 test-restore-dr

observability-up:
	docker compose -f observability/docker-compose.yml up -d

observability-down:
	docker compose -f observability/docker-compose.yml down

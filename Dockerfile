# syntax=docker/dockerfile:1.7

FROM rust:1.94.0-bookworm@sha256:365468470075493dc4583f47387001854321c5a8583ea9604b297e67f01c5a4f AS builder

ARG CHRONOS_BUILD_COMMIT=unknown

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev ca-certificates

WORKDIR /workspace

ENV CHRONOS_BUILD_COMMIT=${CHRONOS_BUILD_COMMIT}
ENV PROTOC=/usr/bin/protoc
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_NET_RETRY=10
ENV CARGO_HTTP_TIMEOUT=600
ENV CARGO_HTTP_LOW_SPEED_LIMIT=1
ENV CARGO_HTTP_MULTIPLEXING=false

RUN mkdir -p /workspace/google \
    && cp -r /usr/include/google/protobuf /workspace/google/

COPY Cargo.toml Cargo.lock build.rs tso.proto ./

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/workspace/target,sharing=locked \
    mkdir -p src \
    && printf 'pub fn dependency_cache_anchor() {}\n' > src/lib.rs \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --locked --release --bin chronos \
    && rm -rf src

COPY . .

ENV RUSTUP_TOOLCHAIN=1.94.0

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/workspace/target,sharing=locked \
    touch build.rs tso.proto src/lib.rs src/main.rs \
    && cargo build --locked --release --bin chronos \
    && cp target/release/chronos /usr/local/bin/chronos

FROM debian:bookworm-slim@sha256:f9c6a2fd2ddbc23e336b6257a5245e31f996953ef06cd13a59fa0a1df2d5c252 AS runtime

ARG CHRONOS_BUILD_COMMIT=unknown

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends --only-upgrade \
        libc6 \
        libc-bin \
        libcap2 \
        libgnutls30 \
        libsystemd0 \
        libudev1 \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean \
    && useradd --uid 10001 --create-home --home-dir /var/lib/chronos --shell /usr/sbin/nologin --no-log-init chronos

LABEL org.opencontainers.image.title="chronos" \
      org.opencontainers.image.revision="${CHRONOS_BUILD_COMMIT}" \
      org.opencontainers.image.version="0.1.0"

WORKDIR /var/lib/chronos

COPY --from=builder /usr/local/bin/chronos /usr/local/bin/chronos

USER 10001:10001

EXPOSE 50051 9898

ENTRYPOINT ["/usr/local/bin/chronos"]

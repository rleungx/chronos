FROM rust:1.94.0-bookworm AS builder

ARG CHRONOS_BUILD_COMMIT=unknown

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . .

ENV CHRONOS_BUILD_COMMIT=${CHRONOS_BUILD_COMMIT}
ENV PROTOC=/usr/bin/protoc

RUN mkdir -p /workspace/google \
    && cp -r /usr/include/google/protobuf /workspace/google/ \
    && cargo build --locked --release --bin chronos

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /var/lib/chronos chronos

WORKDIR /var/lib/chronos

COPY --from=builder /workspace/target/release/chronos /usr/local/bin/chronos

USER 10001:10001

EXPOSE 50051 9898

ENTRYPOINT ["/usr/local/bin/chronos"]

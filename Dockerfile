# syntax=docker/dockerfile:1.7
FROM node:24-bookworm-slim AS frontend
WORKDIR /build/frontend
RUN corepack enable && corepack prepare pnpm@11.3.0 --activate
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile --ignore-scripts
COPY frontend/ ./
RUN pnpm build

FROM rust:1.94-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY src ./src
COPY scripts ./scripts
COPY --from=frontend /build/frontend/dist ./frontend/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && \
    cp /build/target/release/donkey /tmp/donkey

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl tini && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --home-dir /data --create-home donkey
COPY --from=builder /tmp/donkey /usr/local/bin/donkey
RUN mkdir -p /data && chown -R donkey:donkey /data
USER donkey
WORKDIR /data
ENV DONKEY_DATA_DIR=/data \
    DONKEY_ADMIN_ADDR=0.0.0.0:5003 \
    DONKEY_REGISTRY_ADDR=0.0.0.0:5443 \
    RUST_LOG=donkey=info
EXPOSE 5003 5443
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:5003/api/health >/dev/null || exit 1
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/donkey"]

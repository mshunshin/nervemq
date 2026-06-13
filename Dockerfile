# syntax=docker/dockerfile:1

# Multi-stage build for the single-binary NerveMQ server with the UI embedded.
#
#   1. `ui`     — build the Next.js static export (`out/`) with Bun.
#   2. `build`  — compile the Rust server, embedding `out/` via `embed-ui`.
#   3. runtime  — copy just the binary onto a slim Debian base.

############################
# Stage 1: build the UI
############################
FROM oven/bun:1 AS ui
WORKDIR /app

# Install JS dependencies first so this layer caches unless the manifest moves.
COPY package.json bun.lockb ./
RUN bun install --frozen-lockfile

# `next build` emits the static export into ./out (next.config.ts sets
# `output: "export"`).
COPY . .
RUN bun run build

############################
# Stage 2: build the server
############################
FROM rust:1-bookworm AS build
WORKDIR /app

# sqlx is built with the native-tls feature, which links against OpenSSL.
RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

# Crate sources. `examples/rust` is a workspace member, so its manifest must be
# present for Cargo to resolve the workspace even though we only build the bin.
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY migrations ./migrations
COPY examples/rust ./examples/rust

# The `embed-ui` feature embeds this at compile time; build.rs requires
# out/index.html to exist.
COPY --from=ui /app/out ./out

# Cache the Cargo registry, git and target directories across builds. The
# binary is copied out of the cached target dir within the same step so it
# survives into the image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --bin nervemq \
 && cp target/release/nervemq /usr/local/bin/nervemq

############################
# Stage 3: runtime
############################
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /home/nervemq --create-home nervemq \
 && mkdir -p /data \
 && chown nervemq:nervemq /data

COPY --from=build /usr/local/bin/nervemq /usr/local/bin/nervemq

# Listen on all interfaces (the default is loopback-only) and keep the SQLite
# databases in a volume so they survive container recreation.
ENV NERVEMQ_BIND_ADDRESS=0.0.0.0:8080
EXPOSE 8080
VOLUME ["/data"]

USER nervemq
ENTRYPOINT ["nervemq"]
CMD ["--data-dir", "/data"]

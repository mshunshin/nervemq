# NerveMQ tasks. Run `just` to list them.
#
# The server embeds the UI at compile time (the `embed-ui` feature), so the
# UI has to be built into ./out before the binary. The build recipes below
# do both, in that order.

# List the recipes.
default:
    @just --list

# Build the UI and the server, both in release mode: target/release/nervemq.
release: ui
    cargo build --release

# Build the UI and a debug server: target/debug/nervemq.
build: ui
    cargo build

# Build the UI into ./out.
ui:
    bun install --frozen-lockfile
    bun run build

# Build a release server without the UI (no Bun needed).
api-only:
    cargo build --release --no-default-features --features otel

# Check that every feature combination still compiles (there is no CI).
check-features:
    cargo check --no-default-features
    cargo check --no-default-features --features otel
    cargo check --no-default-features --features embed-ui

# Build the UI, then run the server on :8080 (extra args go to nervemq).
run *args: ui
    cargo run -- {{args}}

# Run the UI dev server on :3000 against a server on :8080 (start `just run` first).
dev:
    bun run dev

# Run every test: Rust (including the smoke test) and the UI's.
test:
    cargo test
    bun run test

# Benchmark a throwaway release server over SQS with the Rust AWS SDK
# (args go to the benchmark, e.g. `just bench --messages 1000 --concurrency 16`).
bench *args: release
    cargo run --release -p nervemq-example --bin benchmark -- --spawn target/release/nervemq {{args}}

# Run only the smoke test of the real binary.
smoke:
    cargo test --test smoke

# Lint the Rust and the UI.
lint: check-features
    cargo clippy --tests
    bun run lint

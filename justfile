default: ci

run network="localnet":
    @cargo run --quiet -- {{network}}

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

lint:
    cargo clippy --locked --all-targets -- -D warnings

test:
    cargo test --locked --all-targets

ci: fmt-check lint test

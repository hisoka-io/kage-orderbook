default: ci

run:
    cargo run

fmt:
    cargo fmt

lint:
    cargo clippy --all-targets -- -D warnings

test:
    cargo test

ci:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

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

submit-priced-order:
    cargo run --quiet --bin submit_priced_order

wrong-quote:
    cargo run --quiet --bin submit_priced_order -- --wrong-quote

test-intent-proof:
    bash ./scripts/test-intent-proof.sh

ci: fmt-check lint test

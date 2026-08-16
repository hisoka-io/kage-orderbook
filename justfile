default: ci

run:
    cargo run

fmt:
    cargo fmt

lint:
    cargo clippy --all-targets -- -D warnings

test:
    cargo test

submit-priced-order:
    ./scripts/submit-priced-order.sh

wrong-quote:
    ./scripts/wrong-quote.sh

ci:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

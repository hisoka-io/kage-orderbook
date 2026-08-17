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

test-intent-proof:
    bash ./scripts/test-intent-proof.sh

test-prover-worker:
    cargo test --bin mock_user prover_worker::tests::generates_a_real_proof_through_the_worker -- --ignored --nocapture

ci:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

# Kage Orderbook

The orderbook validates orders, checks market prices, routes orders to registered
solvers, stores encrypted proofs, and tracks on-chain settlement.

It runs independently from `kage-solver`. The services communicate through the
orderbook HTTP and WebSocket APIs.

## Run locally

Start the local chain, deploy the Darkpool and Registry contracts, and start the
pricing feed first.

```sh
cp .env.example .env.localnet
# Set KAGE_PRICING_FEED_TOKEN in .env.localnet

just run
```

The service listens on `127.0.0.1:3000` by default. Check readiness with:

```sh
curl -i http://127.0.0.1:3000/health/ready
```

Set `RUST_LOG` in `.env.localnet` to control log filtering, for example
`RUST_LOG=orderbook=debug,info,kage_registry=warn`.

Runtime settings come from `.env.<network>`. Order, chain, token, market,
pricing, and contract settings come from `config/<network>.json`. Localnet is
included; add both files before running another network with `just run <network>`.

## Development

```sh
just ci                    # format check, Clippy, and tests
just submit-priced-order   # submit a correctly priced order
just wrong-quote           # submit an invalid quote and expect HTTP 422
```

Run the mock user against the independently running orderbook and solver:

```sh
cargo run --bin mock_user -- --orders 1
```

Run the ignored real-prover integration test explicitly with:

```sh
just test-prover-worker
```

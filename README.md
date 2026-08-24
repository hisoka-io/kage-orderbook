# Kage Orderbook

The orderbook validates orders, checks market prices, routes orders to registered solvers, stores encrypted proofs, and tracks on-chain settlement.

It runs independently from `kage-solver`. The services communicate through the
orderbook HTTP and WebSocket APIs.

Endpoints live under `/v1` (`POST /v1/orders`, `GET /v1/events/user/ws`).
Health probes stay at `/health/live` and `/health/ready`.

## Layout

| Path | Contents |
| --- | --- |
| `src/api/` | HTTP and WebSocket layer |
| `src/core/` | Order model and lifecycle engine |
| `src/solver/` | Solver registry and sessions |
| `src/pricing/` | Price feed |
| `src/storage/` | Persistence |
| `src/proof/` | Encrypted proof transport |
| `src/bin/` | Executables |
| `tests/` | Integration tests |

## Run locally

Needs a running chain with the Darkpool and Registry contracts deployed, plus
the pricing feed.

```sh
cp .env.example .env.localnet   # then set KAGE_PRICING_FEED_TOKEN
just run                        # listens on 127.0.0.1:3000
curl -i http://127.0.0.1:3000/health/ready
```

## Config

| Source | Contents |
| --- | --- |
| `.env.<network>` | Runtime settings, `RUST_LOG` filtering |
| `config/<network>.json` | API limits, origins, order, chain, token, market, pricing, contracts |

Localnet ships with both. Add them for any other `just run <network>`.


## Commands

| Command | Does |
| --- | --- |
| `just ci` | Format check, Clippy, tests |
| `just submit-priced-order` | Submit a correctly priced order |
| `just wrong-quote` | Submit an invalid quote, expect HTTP 422 |
| `just test-prover-worker` | Ignored real-prover integration test |
| `cargo run --bin mock_user -- --orders 1` | Mock user against a running orderbook and solver |

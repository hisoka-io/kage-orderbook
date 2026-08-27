# Kage Orderbook

The orderbook validates prices, admits orders, and assigns them to registered solvers.
It never receives proofs, transaction hashes, nullifiers, or settlement status.

It runs independently from `kage-solver`. The services communicate through the
orderbook HTTP and WebSocket APIs.

Endpoints live under `/v1` (`POST /v1/orders`, `GET /v1/events/user/ws`).
Health probes stay at `/health/live` and `/health/ready`.

Once a solver has reserved an order, its
owner can fetch a short-lived signed ticket from
`GET /v1/orders/{order_id}/assignment` using the existing
`x-order-commitment` header. The ticket authorizes delivery to the configured
solver endpoint; the proof itself does not pass through this endpoint.

## Layout

| Path | Contents |
| --- | --- |
| `src/api/` | HTTP and WebSocket layer |
| `src/core/` | Order model and lifecycle engine |
| `src/solver/` | Solver registry and sessions |
| `src/pricing/` | Price feed |
| `src/storage/` | Persistence |
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

The V0 direct solver flow requires a dedicated `KAGE_ASSIGNMENT_PRIVATE_KEY`.
Set its derived address as `KAGE_ORDERBOOK_ASSIGNMENT_SIGNER` on every solver.
Solvers advertise their canonical public endpoint while authenticating; the
endpoint is signed into the challenge and retained only for the session lease.
The orderbook does not configure solver IDs or endpoints. Outside localnet,
solver endpoints must use HTTPS. Tickets default to 60 seconds and can never
outlive the order.


## Commands

| Command | Does |
| --- | --- |
| `just ci` | Format check, Clippy, tests |
| `just submit-priced-order` | Submit a correctly priced order |
| `just wrong-quote` | Submit an invalid quote, expect HTTP 422 |
| `just test-intent-proof` | Run the standalone intent-proof worker tests |

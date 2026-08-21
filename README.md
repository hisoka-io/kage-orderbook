# kage-orderbook

## Run

```sh
cp .env.example .env.localnet   # set KAGE_PRICING_FEED_TOKEN
just run                        # localnet
just run mainnet                # or: cargo run -- mainnet
```

Needs the pricing feed (`../kage-price-estimate`) running first.

## Networks

`localnet` (default), `testnet`, `mainnet`. The name picks all three together:

| network | env | config | database |
|---|---|---|---|
| `localnet` | `.env.localnet` | `config/localnet.json` | `orderbook.localnet.db` |
| `testnet` | `.env.testnet` | `config/testnet.json` | `orderbook.testnet.db` |
| `mainnet` | `.env.mainnet` | `config/mainnet.json` | `orderbook.mainnet.db` |

First argument for the orderbook, `KAGE_NETWORK` for other binaries. Each
database is stamped on first use (`PRAGMA user_version`), so opening one from
another network fails at startup with `NetworkMismatch`.

## Config

`config/<network>.json` (committed): chains, tokens, approved markets, TTL
limits, `max_order_usd_cents`, pricing freshness, BPS limits, database tuning.
Markets are directional — list each direction you want. A market BPS override
may only tighten the stricter of its two token limits.

`.env.<network>` (gitignored): `DATABASE_URL`, `KAGE_ORDERBOOK_LISTEN_ADDR`,
`KAGE_REGISTRY_URL`, `KAGE_PRICING_FEED_URL`, `KAGE_PRICING_FEED_TOKEN`.

## Commands

```sh
just                      # fmt, clippy, test
just submit-priced-order  # one live-priced order
just wrong-quote          # expect HTTP 422
just test-intent-proof
just test-prover-worker

cargo run --bin mock_user -- --orders 1   # needs ../kage-solver + mock_chain
curl -i http://127.0.0.1:3000/health/ready
```

## Proof worker

`tools/mock-kage-user` calls the Darkpool SDK at `../darkpool` (override with
`DARKPOOL_ROOT`). Envelopes are Noise-encrypted to the assigned solver's
registry key; the orderbook stores only the opaque payload.

```sh
pnpm proof:test      # from tools/
pnpm prover:sample
pnpm prover:worker
```

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cargo run --quiet --bin submit_priced_order

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
darkpool_root="${DARKPOOL_ROOT:-$repo_root/../darkpool}"
prover_root="$darkpool_root/packages/prover"
tsx_loaders=("$darkpool_root"/node_modules/.pnpm/tsx@*/node_modules/tsx/dist/loader.mjs)
tsx_loader="${tsx_loaders[0]}"
tsc="$repo_root/tools/node_modules/.bin/tsc"

if [[ ! -f "$tsx_loader" || ! -x "$tsc" ]]; then
  echo "Mock-user TypeScript dependencies are missing" >&2
  echo "Run: pnpm --dir $repo_root/tools install" >&2
  exit 1
fi

"$tsc" --noEmit -p "$repo_root/tools/tsconfig.json"

cd "$prover_root"
node --import "$tsx_loader" --test \
  "$repo_root/tools/mock-kage-user/test/intent-proof-v1.test.ts"

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
darkpool_root="${DARKPOOL_ROOT:-$repo_root/../darkpool}"
prover_root="$darkpool_root/packages/prover"

if [[ ! -f "$prover_root/dist/index.cjs" ]]; then
  echo "Darkpool prover is not built: $prover_root/dist/index.cjs" >&2
  echo "Run: pnpm --dir $darkpool_root --filter @hisoka/prover build" >&2
  exit 1
fi

tsx_loaders=("$darkpool_root"/node_modules/.pnpm/tsx@*/node_modules/tsx/dist/loader.mjs)
tsx_loader="${tsx_loaders[0]}"
if [[ ! -f "$tsx_loader" ]]; then
  echo "Darkpool prover TypeScript loader is missing" >&2
  echo "Run: pnpm --dir $darkpool_root install" >&2
  exit 1
fi

export DARKPOOL_ROOT="$darkpool_root"
cd "$prover_root"
node --import "$tsx_loader" \
  "$repo_root/tools/mock-kage-user/src/main.ts" "$@"
